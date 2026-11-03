//! Production-parity server for the bitmap engine on the real catalog, speaking a subset of the
//! Elasticsearch REST API so the same client and request bodies can hit either system:
//!   POST /real/_search   bool{filter:[term|range], must:{match}}, sort, size, _source, aggs.terms(brand)
//!   POST /real/_bulk     {"update":{"_id":..}} / {"doc":{"stock":bool}|{"price":int}}
//!                        {"index":{"_id":..}} / full doc (insert or replace: title, category, brand, ...)
//!                        {"delete":{"_id":..}}
//! Adds what the earlier benchmarks skipped:
//!   * document store on disk (same JSON _source as the ES index, read per hit with pread;
//!     live stock/price spliced in)
//!   * write-ahead log: every bulk request is appended and fsynced before it is applied
//!     (ES default index.translog.durability = request)
//!   * JSON query parsing and ES-shaped responses
//!   * POST /_fold: fold the changes (replaced/new/deleted docs) into a freshly built index while
//!     serving and accepting writes, then swap it in (see `Server::fold`)
//!
//!   prod FILE.tsv DATA_DIR
use bitmap_engine::realcat::{build as build_real, build_state, load, RealCat, RealIndex};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use std::sync::Arc;
use bitmap_engine::*;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

/// One generation of the index: built index + changes since, its vocabulary and document store.
struct State {
    ri: RealIndex,
    vocab: HashMap<String, u32>,
    docs: std::fs::File,
    off: Vec<u64>,
}
struct Server {
    cur: RwLock<Arc<State>>,
    /// held while a bulk is logged and applied: WAL order == apply order
    wal: Mutex<std::fs::File>,
    wal_path: std::path::PathBuf,
    dir: std::path::PathBuf,
    folding: Mutex<u64>, // generation counter; one fold at a time
}

fn static_source(rc: &RealCat, d: usize) -> String {
    // same fields and values as the ES index, minus the live ones (price, stock, rocket)
    let title: Vec<&str> = rc.tokens[d].iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
    json!({"pop": d, "cat": rc.cat[d].to_string(), "brand": rc.brand[d].to_string(), "title_tok": title.join(" ")}).to_string()
}

impl State {
    fn source(&self, d: u32) -> Value {
        if let Some((src, price, stock, rocket)) = self.ri.delta_doc(d) {
            let mut v: Value = serde_json::from_str(&src).unwrap();
            v["price"] = json!(price);
            v["stock"] = json!(stock);
            v["rocket"] = json!(rocket);
            return v;
        }
        let mut v = self.built_source(d);
        let ix = &self.ri.ix;
        let sh = ix.shards[(d / ix.shard_size) as usize].read();
        v["price"] = json!(ix.price[d as usize].load(Relaxed));
        v["stock"] = json!(sh.stock.contains(d));
        v["rocket"] = json!(sh.rocket.contains(d));
        v
    }
    /// The built version of a doc, from the document store.
    fn built_source(&self, d: u32) -> Value {
        let (a, b) = (self.off[d as usize], self.off[d as usize + 1]);
        let mut buf = vec![0u8; (b - a) as usize];
        self.docs.read_exact_at(&mut buf, a).unwrap();
        serde_json::from_slice(&buf).unwrap()
    }
    /// Returns clauses and an optional exact price window (for ranges not on bucket edges).
    fn parse_filter(&self, f: &Value, out: &mut Vec<Clause>, price: &mut Option<(u32, u32)>) -> Option<()> {
        if let Some(t) = f.get("term") {
            let (field, val) = t.as_object()?.iter().next()?;
            let val = if val.is_object() { &val["value"] } else { val };
            out.push(match field.as_str() {
                "cat" => Clause::Cat(val.as_str()?.parse().ok()?),
                "title_tok" => Clause::Term(self.ri.term_id(&self.vocab, val.as_str()?)?),
                "stock" if val.as_bool()? => Clause::Stock,
                "rocket" if val.as_bool()? => Clause::Rocket,
                _ => return None,
            });
        } else if let Some(r) = f.get("range") {
            let p = r.get("price")?;
            let lo = p.get("gte").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            let hi = p.get("lt").and_then(|x| x.as_u64()).map(|x| x as u32).unwrap_or(u32::MAX);
            let ix = &self.ri.ix;
            let (b0, b1) = (ix.bucket_of(lo), if hi == u32::MAX { N_BUCKET } else { ix.bucket_of(hi.saturating_sub(1)) + 1 });
            out.push(Clause::Buckets(b0, b1));
            if ix.edge(b0) != lo || (hi != u32::MAX && ix.edge(b1) != hi) {
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
            let ws: Vec<u32> = text.split_whitespace().filter_map(|w| self.ri.term_id(&self.vocab, w)).collect();
            if !ws.is_empty() && size > 0 {
                hits = self.ri.bm25_filtered(&ws, &q, size).into_iter().map(|(s, d)| (d, Some(s))).collect();
            }
        } else if size > 0 {
            let by_price = req["sort"].get(0).and_then(|s| s.get("price")).is_some();
            let ok = |d: u32| price.map_or(true, |(lo, hi)| { let p = self.ri.price_of(d); p >= lo && p < hi });
            if by_price {
                let mut k = size.max(K);
                loop {
                    let v = self.ri.topk_price_live(&q, k);
                    let done = v.len() < k;
                    hits = v.into_iter().filter(|x| ok(x.1)).take(size).map(|x| (x.1, None)).collect();
                    if hits.len() == size || done {
                        break;
                    }
                    k *= 4;
                }
            } else {
                let mut k = size;
                loop {
                    let ids = self.ri.topk_pop_live(&q, k);
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

    /// Apply bulk NDJSON in order: attribute updates are batched until a document op needs them.
    fn apply(&self, body: &str) -> usize {
        let mut ups = Vec::new();
        let mut n = 0;
        let mut lines = body.lines().filter(|l| !l.trim().is_empty());
        while let Some(a) = lines.next() {
            let a: Value = serde_json::from_str(a).unwrap();
            let (op, meta) = a.as_object().unwrap().iter().next().unwrap();
            let d: u32 = meta["_id"].as_str().unwrap().parse().unwrap();
            n += 1;
            match op.as_str() {
                "update" => {
                    let b: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
                    if let Some(s) = b["doc"].get("stock") {
                        ups.push(Update::SetStock(d, s.as_bool().unwrap()));
                    }
                    if let Some(p) = b["doc"].get("price") {
                        ups.push(Update::Price(d, p.as_u64().unwrap() as u32));
                    }
                }
                "index" => {
                    let b: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
                    self.ri.apply_attrs(&mut std::mem::take(&mut ups));
                    let title = b["title_tok"].as_str().unwrap_or("");
                    let words: Vec<&str> = title.split_whitespace().collect();
                    let num = |k: &str| b[k].as_str().and_then(|x| x.parse().ok()).or_else(|| b[k].as_u64().map(|x| x as u32)).unwrap_or(0);
                    let src = json!({"pop": d, "cat": b["cat"], "brand": b["brand"], "title_tok": title}).to_string();
                    self.ri.upsert(d, &words, &self.vocab, num("cat"), num("brand"), b["price"].as_u64().unwrap_or(0) as u32,
                                   b["stock"].as_bool().unwrap_or(false), b["rocket"].as_bool().unwrap_or(false), src, &|| self.built_tokens(d));
                }
                "delete" => {
                    self.ri.apply_attrs(&mut std::mem::take(&mut ups));
                    self.ri.delete(d, &|| self.built_tokens(d));
                }
                o => panic!("unsupported bulk op {o}"),
            }
        }
        self.ri.apply_attrs(&mut ups);
        n
    }
    fn built_tokens(&self, d: u32) -> Vec<u32> {
        let v = self.built_source(d);
        v["title_tok"].as_str().unwrap_or("").split_whitespace().map(|w| self.vocab[w]).collect()
    }
}

fn write_docs(path: &std::path::Path, n: usize, src: &dyn Fn(usize) -> String) -> Vec<u64> {
    let mut off = vec![0u64];
    let mut w = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
    for d in 0..n {
        let s = src(d);
        w.write_all(s.as_bytes()).unwrap();
        off.push(off.last().unwrap() + s.len() as u64);
    }
    w.into_inner().unwrap().sync_all().unwrap();
    off
}

impl Server {
    fn state(&self) -> Arc<State> {
        self.cur.read().clone()
    }
    fn bulk(&self, body: &str) -> Value {
        let t = Instant::now();
        let mut w = self.wal.lock();
        // durable before acknowledged and before applied; the lock also fixes the apply order
        w.write_all(body.as_bytes()).unwrap();
        if !body.ends_with('\n') {
            w.write_all(b"\n").unwrap();
        }
        w.sync_data().unwrap();
        let n = self.state().apply(body);
        drop(w);
        json!({"took": t.elapsed().as_millis() as u64, "errors": false, "items": n})
    }
    fn wal_len(&self) -> u64 {
        std::fs::metadata(&self.wal_path).map(|m| m.len()).unwrap_or(0)
    }
    fn wal_read(&self, a: u64, b: u64) -> String {
        let f = std::fs::File::open(&self.wal_path).unwrap();
        let mut buf = vec![0u8; (b - a) as usize];
        f.read_exact_at(&mut buf, a).unwrap();
        String::from_utf8(buf).unwrap()
    }
    /// Fold all changes into a new generation without stopping reads or writes:
    ///  1. note the WAL position L0 (under the write lock, so no bulk is half applied)
    ///  2. read the current catalog state (fuzzy: writes go on meanwhile)
    ///  3. build a fresh index from it (docids kept: new docs after the built ones, deleted ones
    ///     left as holes), write a new document store
    ///  4. replay the WAL from L0 on the new generation; every op is an assignment (replace doc,
    ///     delete, set stock, set price), so replaying ops the fuzzy read already saw is harmless
    ///  5. replay the last few ops and swap under the write lock (the only write pause)
    fn fold(&self) -> Value {
        let mut generation = self.folding.lock();
        *generation += 1;
        let t0 = Instant::now();
        let (old, l0) = {
            let _w = self.wal.lock();
            (self.state(), self.wal_len())
        };
        // 2. catalog state
        let (delta, tomb, terms) = {
            let lv = old.ri.live.read();
            let delta: HashMap<u32, (Vec<u32>, u32, u32, u32, bool, bool, String)> = lv.docs.iter()
                .map(|(&k, d)| (k, (d.tokens.clone(), d.cat, d.brand, d.price, lv.stock.contains(k), lv.rocket.contains(k), d.src.clone()))).collect();
            let mut terms: Vec<(u32, String)> = lv.terms.iter().map(|(w, &i)| (i, w.clone())).collect();
            terms.sort();
            (delta, lv.tomb.clone(), terms)
        };
        let n_old = old.ri.ix.n;
        let n_new = delta.keys().copied().max().map(|m| (m + 1).max(n_old)).unwrap_or(n_old);
        let mut vocab_v: Vec<String> = vec![String::new(); old.vocab.len()];
        for (w, &i) in &old.vocab {
            vocab_v[i as usize] = w.clone();
        }
        for (i, w) in &terms {
            assert_eq!(*i as usize, vocab_v.len());
            vocab_v.push(w.clone());
        }
        let vmap: HashMap<String, u32> = vocab_v.iter().enumerate().map(|(i, w)| (w.clone(), i as u32)).collect();
        let n = n_new as usize;
        let mut rc = RealCat { n: n_new, vocab: vocab_v, tokens: vec![vec![]; n], cat: vec![0; n], brand: vec![0; n], price: vec![0; n], n_cat: 0, n_brand: 0 };
        // per doc: (tokens, cat, brand, price, stock, rocket, source, hole), in parallel
        use rayon::prelude::*;
        let rows: Vec<(Vec<u32>, u32, u32, u32, bool, bool, String, bool)> = (0..n_new).into_par_iter().map(|d| {
            let i = d as usize;
            if let Some((t, c, b, p, s, r, src)) = delta.get(&d) {
                (t.clone(), *c, *b, *p, *s, *r, src.clone(), false)
            } else if d < n_old && !tomb.contains(d) {
                let v = old.built_source(d);
                let t = v["title_tok"].as_str().unwrap_or("").split_whitespace().map(|w| old.vocab[w]).collect();
                let c = v["cat"].as_str().unwrap().parse().unwrap();
                let sh = old.ri.ix.shards[(d / old.ri.ix.shard_size) as usize].read();
                (t, c, old.ri.ix.brand[i], old.ri.ix.price[i].load(Relaxed), sh.stock.contains(d), sh.rocket.contains(d), v.to_string(), false)
            } else {
                (vec![], 0, 0, 0, false, false, "{}".to_string(), true)
            }
        }).collect();
        let (mut stock, mut rocket) = (vec![false; n], vec![false; n]);
        let mut srcs: Vec<String> = Vec::with_capacity(n);
        let mut holes = RoaringBitmap::new();
        for (i, (t, c, b, p, st, r, src, hole)) in rows.into_iter().enumerate() {
            (rc.tokens[i], rc.cat[i], rc.brand[i], rc.price[i], stock[i], rocket[i]) = (t, c, b, p, st, r);
            srcs.push(src);
            if hole {
                holes.insert(i as u32);
            }
        }
        rc.n_cat = rc.cat.iter().copied().max().unwrap_or(0) as usize + 1;
        rc.n_brand = rc.brand.iter().copied().max().unwrap_or(0) as usize + 1;
        let t_extract = t0.elapsed().as_secs_f64();
        // 3. build
        let t1 = Instant::now();
        let n_holes = holes.len();
        let ri = build_state(&rc, 16, &stock, &rocket, holes);
        let docs_path = self.dir.join(format!("docs_g{}.json", *generation));
        let off = write_docs(&docs_path, n, &|d| srcs[d].clone());
        let new = State { ri, vocab: vmap, docs: std::fs::File::open(&docs_path).unwrap(), off };
        drop(rc);
        let t_build = t1.elapsed().as_secs_f64();
        // 4. catch up
        let t2 = Instant::now();
        let mut pos = l0;
        let mut replayed = 0usize;
        loop {
            let len = self.wal_len();
            if len - pos < (1 << 20) {
                break;
            }
            replayed += new.apply(&self.wal_read(pos, len));
            pos = len;
        }
        // 5. last ops + swap
        let t3 = Instant::now();
        {
            let _w = self.wal.lock();
            let len = self.wal_len();
            if len > pos {
                replayed += new.apply(&self.wal_read(pos, len));
            }
            *self.cur.write() = Arc::new(new);
        }
        let pause_ms = t3.elapsed().as_secs_f64() * 1e3;
        let catchup = t2.elapsed().as_secs_f64();
        json!({"generation": *generation, "docs": n_new, "folded_changes": delta.len(), "replaced_or_deleted": tomb.len(), "holes": n_holes,
               "extract_s": t_extract, "build_s": t_build, "catchup_s": catchup, "replayed_ops": replayed, "write_pause_ms": pause_ms,
               "total_s": t0.elapsed().as_secs_f64(), "mem": mem_status()})
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
    let off = write_docs(&dir.join("docs.json"), rc.n as usize, &|d| static_source(&rc, d));
    println!("doc store: {:.0} MB on disk in {:.1}s (offsets in RAM {:.0} MB)", *off.last().unwrap() as f64 / 1048576.0,
             t.elapsed().as_secs_f64(), off.len() as f64 * 8.0 / 1048576.0);
    let vocab: HashMap<String, u32> = rc.vocab.iter().enumerate().map(|(i, w)| (w.clone(), i as u32)).collect();
    let wal_path = dir.join("wal.log");
    let st = State { ri, vocab, docs: std::fs::File::open(dir.join("docs.json")).unwrap(), off };
    // WAL: replay on restart (REPLAY=1), otherwise start empty
    if std::env::var("REPLAY").is_ok() {
        let t = Instant::now();
        let data = std::fs::read_to_string(&wal_path).unwrap_or_default();
        let n = st.apply(&data);
        println!("WAL replay: {:.0} MB, {n} ops in {:.1}s ({:.0} ops/s)", data.len() as f64 / 1048576.0, t.elapsed().as_secs_f64(), n as f64 / t.elapsed().as_secs_f64());
    } else {
        let _ = std::fs::File::create(&wal_path);
    }
    let srv = Server {
        cur: RwLock::new(Arc::new(st)),
        wal: Mutex::new(std::fs::OpenOptions::new().create(true).append(true).open(&wal_path).unwrap()),
        wal_path,
        dir: dir.clone(),
        folding: Mutex::new(0),
    };
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
                    srv.state().search(&serde_json::from_str(&body).unwrap(), url.contains("exact=1"))
                } else if url.starts_with("/_fold") {
                    srv.fold()
                } else {
                    json!({"error": "unsupported"})
                };
                let _ = req.respond(tiny_http::Response::from_string(out.to_string())
                    .with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap()));
            });
        }
    });
}
