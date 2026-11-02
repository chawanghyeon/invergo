//! Production-parity server for the bitmap engine on the real catalog, speaking a subset of the
//! Elasticsearch REST API so the same client and request bodies can hit either system:
//!   POST /real/_search   bool{filter:[term|range], must:{match}}, sort, size, _source, aggs.terms(brand)
//!   POST /real/_bulk     {"update":{"_id":..}} / {"doc":{"stock":bool}|{"price":int}}
//! Adds what the earlier benchmarks skipped:
//!   * document store on disk (same JSON _source as the ES index, read per hit with pread;
//!     live stock/price spliced in)
//!   * write-ahead log: every bulk request is appended and fsynced before it is applied
//!     (ES default index.translog.durability = request)
//!   * JSON query parsing and ES-shaped responses
//!
//!   prod FILE.tsv DATA_DIR
use bitmap_engine::realcat::{build as build_real, load, RealCat, RealIndex};
use bitmap_engine::*;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

struct Server {
    ri: RealIndex,
    vocab: HashMap<String, u32>,
    docs: std::fs::File,
    off: Vec<u64>,
    wal: Mutex<std::fs::File>,
}

fn static_source(rc: &RealCat, d: usize) -> String {
    // same fields and values as the ES index, minus the live ones (price, stock, rocket)
    let title: Vec<&str> = rc.tokens[d].iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
    json!({"pop": d, "cat": rc.cat[d].to_string(), "brand": rc.brand[d].to_string(), "title_tok": title.join(" ")}).to_string()
}

impl Server {
    fn source(&self, d: u32) -> Value {
        let (a, b) = (self.off[d as usize], self.off[d as usize + 1]);
        let mut buf = vec![0u8; (b - a) as usize];
        self.docs.read_exact_at(&mut buf, a).unwrap();
        let mut v: Value = serde_json::from_slice(&buf).unwrap();
        let ix = &self.ri.ix;
        let sh = ix.shards[(d / ix.shard_size) as usize].read();
        v["price"] = json!(ix.price[d as usize].load(Relaxed));
        v["stock"] = json!(sh.stock.contains(d));
        v["rocket"] = json!(sh.rocket.contains(d));
        v
    }

    /// Returns clauses and an optional exact price window (for ranges not on bucket edges).
    fn parse_filter(&self, f: &Value, out: &mut Vec<Clause>, price: &mut Option<(u32, u32)>) -> Option<()> {
        if let Some(t) = f.get("term") {
            let (field, val) = t.as_object()?.iter().next()?;
            let val = if val.is_object() { &val["value"] } else { val };
            out.push(match field.as_str() {
                "cat" => Clause::Cat(val.as_str()?.parse().ok()?),
                "title_tok" => Clause::Term(*self.vocab.get(val.as_str()?)?),
                "stock" if val.as_bool()? => Clause::Stock,
                "rocket" if val.as_bool()? => Clause::Rocket,
                _ => return None,
            });
        } else if let Some(r) = f.get("range") {
            let p = r.get("price")?;
            let lo = p.get("gte").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            let hi = p.get("lt").and_then(|x| x.as_u64()).map(|x| x as u32).unwrap_or(u32::MAX);
            let (b0, b1) = (bucket_of(lo), if hi == u32::MAX { N_BUCKET } else { bucket_of(hi.saturating_sub(1)) + 1 });
            out.push(Clause::Buckets(b0, b1));
            if edge(b0) != lo || (hi != u32::MAX && edge(b1) != hi) {
                *price = Some((lo, hi));
            }
        } else {
            return None;
        }
        Some(())
    }

    fn search(&self, req: &Value, exact_facets: bool) -> Value {
        let t = Instant::now();
        let size = req.get("size").and_then(|x| x.as_u64()).unwrap_or(10) as usize;
        let want_source = req.get("_source").map(|x| x.as_bool() != Some(false)).unwrap_or(true);
        let b = &req["query"]["bool"];
        let mut q = Vec::new();
        let mut price = None;
        let filters: Vec<Value> = match &b["filter"] {
            Value::Array(a) => a.clone(),
            Value::Null => vec![],
            x => vec![x.clone()],
        };
        for f in &filters {
            if self.parse_filter(f, &mut q, &mut price).is_none() {
                // unknown term (e.g. a word not in the vocabulary): empty result
                return json!({"took": 0, "hits": {"hits": []}});
            }
        }
        let mut hits: Vec<(u32, Option<f32>)> = Vec::new();
        if let Some(m) = b["must"].get("match") {
            let text = m["title_tok"].as_str().unwrap_or("");
            let ws: Vec<u32> = text.split_whitespace().filter_map(|w| self.vocab.get(w).copied()).collect();
            if !ws.is_empty() && size > 0 {
                hits = self.ri.bm25_filtered(&ws, &q, size).into_iter().map(|(s, d)| (d, Some(s))).collect();
            }
        } else if size > 0 {
            let by_price = req["sort"].get(0).and_then(|s| s.get("price")).is_some();
            let ok = |d: u32| price.map_or(true, |(lo, hi)| { let p = self.ri.ix.price[d as usize].load(Relaxed); p >= lo && p < hi });
            if by_price {
                hits = self.ri.ix.topk_price(&q).into_iter().filter(|x| ok(x.1)).take(size).map(|x| (x.1, None)).collect();
            } else {
                let mut k = size;
                loop {
                    let ids = self.ri.ix.topk_pop(&q, k);
                    let done = ids.len() < k;
                    hits = ids.into_iter().filter(|&d| ok(d)).take(size).map(|d| (d, None)).collect();
                    if hits.len() == size || done {
                        break;
                    }
                    k *= 4;
                }
            }
        }
        let hits_json: Vec<Value> = hits
            .iter()
            .map(|(d, s)| {
                let mut h = json!({"_index": "real", "_id": d.to_string(), "_score": s});
                if want_source {
                    h["_source"] = self.source(*d);
                }
                h
            })
            .collect();
        let mut out = json!({"took": t.elapsed().as_millis() as u64, "hits": {"hits": hits_json}});
        if let Some(aggs) = req.get("aggs").and_then(|a| a.as_object()) {
            for (name, a) in aggs {
                if a["terms"]["field"] == "brand" {
                    let n = a["terms"]["size"].as_u64().unwrap_or(10) as usize;
                    let f = if exact_facets { self.ri.facet_exact(&q) } else { self.ri.facet_fast(&q) };
                    let buckets: Vec<Value> = f.into_iter().take(n)
                        .map(|(b, c)| json!({"key": b.to_string(), "doc_count": c})).collect();
                    out["aggregations"][name] = json!({"buckets": buckets});
                }
            }
        }
        out
    }

    fn bulk(&self, body: &str) -> Value {
        let t = Instant::now();
        let mut ups = Vec::new();
        let mut lines = body.lines();
        while let (Some(a), Some(b)) = (lines.next(), lines.next()) {
            let a: Value = serde_json::from_str(a).unwrap();
            let b: Value = serde_json::from_str(b).unwrap();
            let d: u32 = a["update"]["_id"].as_str().unwrap().parse().unwrap();
            if let Some(s) = b["doc"].get("stock") {
                ups.push(Update::SetStock(d, s.as_bool().unwrap()));
            }
            if let Some(p) = b["doc"].get("price") {
                ups.push(Update::Price(d, p.as_u64().unwrap() as u32));
            }
        }
        {
            // durable before acknowledged and before applied
            let mut w = self.wal.lock();
            w.write_all(body.as_bytes()).unwrap();
            w.sync_data().unwrap();
        }
        let n = ups.len();
        self.ri.ix.apply_batch(&mut ups);
        json!({"took": t.elapsed().as_millis() as u64, "errors": false, "items": n})
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (path, dir) = (args[1].as_str(), std::path::PathBuf::from(&args[2]));
    std::fs::create_dir_all(&dir).unwrap();
    let rc = load(path);
    let ri = build_real(&rc, 16);
    for (name, secs) in &ri.build_times {
        println!("build {name}: {secs:.2}s");
    }
    let t = Instant::now();
    let mut off = vec![0u64];
    {
        let mut w = std::io::BufWriter::new(std::fs::File::create(dir.join("docs.json")).unwrap());
        for d in 0..rc.n as usize {
            let s = static_source(&rc, d);
            w.write_all(s.as_bytes()).unwrap();
            off.push(off.last().unwrap() + s.len() as u64);
        }
        w.into_inner().unwrap().sync_all().unwrap();
    }
    println!("doc store: {:.0} MB on disk in {:.1}s (offsets in RAM {:.0} MB)", *off.last().unwrap() as f64 / 1048576.0,
             t.elapsed().as_secs_f64(), off.len() as f64 * 8.0 / 1048576.0);
    let vocab: HashMap<String, u32> = rc.vocab.iter().enumerate().map(|(i, w)| (w.clone(), i as u32)).collect();
    let srv = Server {
        ri,
        vocab,
        docs: std::fs::File::open(dir.join("docs.json")).unwrap(),
        off,
        wal: Mutex::new(std::fs::OpenOptions::new().create(true).append(true).open(dir.join("wal.log")).unwrap()),
    };
    // WAL: replay on restart (REPLAY=1), otherwise start empty
    let wal_path = dir.join("wal.log");
    if std::env::var("REPLAY").is_ok() {
        let t = Instant::now();
        let data = std::fs::read_to_string(&wal_path).unwrap_or_default();
        let mut ups = Vec::new();
        let mut lines = data.lines();
        while let (Some(a), Some(b)) = (lines.next(), lines.next()) {
            let a: Value = serde_json::from_str(a).unwrap();
            let b: Value = serde_json::from_str(b).unwrap();
            let d: u32 = a["update"]["_id"].as_str().unwrap().parse().unwrap();
            if let Some(v) = b["doc"].get("stock") { ups.push(Update::SetStock(d, v.as_bool().unwrap())); }
            if let Some(v) = b["doc"].get("price") { ups.push(Update::Price(d, v.as_u64().unwrap() as u32)); }
        }
        let n = ups.len();
        for chunk in ups.chunks_mut(65536) {
            srv.ri.ix.apply_batch(chunk);
        }
        println!("WAL replay: {:.0} MB, {n} updates in {:.1}s ({:.0} updates/s)", data.len() as f64 / 1048576.0, t.elapsed().as_secs_f64(), n as f64 / t.elapsed().as_secs_f64());
    } else {
        srv.wal.lock().set_len(0).unwrap();
    }
    drop(rc);
    println!("{}", mem_status());
    // TCP_NODELAY on the listener (inherited by accepted sockets on Linux): without it, a response
    // written as header + body segments waits ~40 ms on Nagle + delayed ACK.
    let sock = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
    sock.set_reuse_address(true).unwrap();
    sock.set_tcp_nodelay(true).unwrap();
    sock.bind(&"127.0.0.1:9400".parse::<std::net::SocketAddr>().unwrap().into()).unwrap();
    sock.listen(1024).unwrap();
    let server = tiny_http::Server::from_listener(std::net::TcpListener::from(sock), None).unwrap();
    println!("serving ES-compatible subset on 127.0.0.1:9400 (4 workers)");
    std::thread::scope(|s| {
        for _ in 0..4 {
            let (server, srv) = (&server, &srv);
            s.spawn(move || loop {
                let mut req = match server.recv() { Ok(r) => r, Err(_) => break };
                let mut body = String::new();
                std::io::Read::read_to_string(req.as_reader(), &mut body).unwrap();
                let url = req.url().to_string();
                let out = if url.starts_with("/real/_bulk") {
                    srv.bulk(&body)
                } else if url.starts_with("/real/_search") {
                    srv.search(&serde_json::from_str(&body).unwrap(), url.contains("exact=1"))
                } else {
                    json!({"error": "unsupported"})
                };
                let _ = req.respond(tiny_http::Response::from_string(out.to_string())
                    .with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap()));
            });
        }
    });
}
