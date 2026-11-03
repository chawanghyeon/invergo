//! Cluster node for the distributed tests. Indexes docids [lo, hi) of the real catalog (the whole
//! catalog for a replica, a popularity range for a partition) with global BM25 statistics, and
//! follows a shared update log instead of accepting writes.
//!
//!   node FILE.tsv DATA_DIR PORT LOG LO HI
//!
//! HTTP (keep-alive, TCP_NODELAY):
//!   POST /real/_search   same ES subset as `prod` (bool filter/must match, sort, size, _source, aggs)
//!                        aggs.terms.size may exceed 20: exact counts over this node's docs
//!   POST /_brandcounts   {"query":..., "brands":[..]} -> exact counts of those brands in the result
//!   GET  /_status        {"ready", "applied_seq", "applied_ts", "lo", "hi"}
//!   GET  /_digest        digest of (stock, price) over [lo, hi)
//!
//! Log format (written by the client/router): per batch a header line
//!   {"_batch": seq, "ts": unix_ns, "n": count}
//! followed by count update pairs in ES bulk form; a batch is applied once all its lines are present.
use bitmap_engine::realcat::{build_part, load, RealCat, RealIndex};
use bitmap_engine::*;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

struct Node {
    ri: RealIndex,
    vocab: HashMap<String, u32>,
    docs: std::fs::File,
    off: Vec<u64>,
    lo: u32,
    hi: u32,
    ready: AtomicBool,
    applied_seq: AtomicU64,
    applied_ts: AtomicU64,
    _wal_lock: Mutex<()>,
    vocab_list: Vec<String>,
    next_offset: AtomicU64, // Kafka: next offset to consume
    dups: AtomicU64,
    gaps: AtomicU64,
    snap_req: Mutex<Option<std::path::PathBuf>>,
    snap_result: Mutex<Option<Value>>,
    paused: AtomicBool, // ingestion paused (stand-in for a node cut off from the log)
}

fn static_source(rc: &RealCat, d: usize) -> String {
    let title: Vec<&str> = rc.tokens[d].iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
    json!({"pop": d, "cat": rc.cat[d].to_string(), "brand": rc.brand[d].to_string(), "title_tok": title.join(" ")}).to_string()
}

impl Node {
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

    fn parse_query(&self, b: &Value) -> Option<Vec<Clause>> {
        let mut q = Vec::new();
        let filters: Vec<Value> = match &b["filter"] {
            Value::Array(a) => a.clone(),
            Value::Null => vec![],
            x => vec![x.clone()],
        };
        for f in &filters {
            if let Some(t) = f.get("term") {
                let (field, val) = t.as_object()?.iter().next()?;
                let val = if val.is_object() { &val["value"] } else { val };
                q.push(match field.as_str() {
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
                if edge(b0) != lo || (hi != u32::MAX && edge(b1) != hi) {
                    return None; // only bucket-aligned ranges in this test
                }
                q.push(Clause::Buckets(b0, b1));
            } else {
                return None;
            }
        }
        Some(q)
    }

    /// Exact brand counts over this node's part of the result.
    fn brand_counts(&self, q: &[Clause]) -> HashMap<u32, u32> {
        let mut f: HashMap<u32, u32> = HashMap::new();
        for sh in &self.ri.ix.shards {
            for d in and_full(&sh.read(), q).iter() {
                *f.entry(self.ri.ix.brand[d as usize]).or_default() += 1;
            }
        }
        f
    }

    fn search(&self, req: &Value) -> Value {
        let t = Instant::now();
        let size = req.get("size").and_then(|x| x.as_u64()).unwrap_or(10) as usize;
        let want_source = req.get("_source").map(|x| x.as_bool() != Some(false)).unwrap_or(true);
        let b = &req["query"]["bool"];
        let q = match self.parse_query(b) {
            Some(q) => q,
            None => return json!({"took": 0, "hits": {"hits": []}}),
        };
        let mut hits: Vec<(u32, Option<f32>)> = Vec::new();
        if let Some(m) = b["must"].get("match") {
            let ws: Vec<u32> = m["title_tok"].as_str().unwrap_or("").split_whitespace().filter_map(|w| self.vocab.get(w).copied()).collect();
            if !ws.is_empty() && size > 0 {
                hits = self.ri.bm25_filtered(&ws, &q, size).into_iter().map(|(s, d)| (d, Some(s))).collect();
            }
        } else if size > 0 {
            if req["sort"].get(0).and_then(|s| s.get("price")).is_some() {
                hits = self.ri.ix.topk_price(&q).into_iter().take(size).map(|x| (x.1, None)).collect();
            } else {
                hits = self.ri.ix.topk_pop(&q, size).into_iter().map(|d| (d, None)).collect();
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
                    let full = self.lo == 0 && self.hi == self.ri.ix.n;
                    let top: Vec<(u32, u32)> = if full && n <= 20 {
                        self.ri.facet_fast(&q)
                    } else {
                        let mut v: Vec<(u32, u32)> = self.brand_counts(&q).into_iter().map(|(b, c)| (c, b)).collect();
                        v.sort_unstable_by(|a, b| b.cmp(a));
                        v.into_iter().take(n).map(|(c, b)| (b, c)).collect()
                    };
                    let buckets: Vec<Value> = top.into_iter().take(n).map(|(b, c)| json!({"key": b.to_string(), "doc_count": c})).collect();
                    out["aggregations"][name] = json!({"buckets": buckets});
                }
            }
        }
        out
    }

    fn digest(&self) -> u64 {
        let mut h = 0xcbf29ce484222325u64;
        let mut eat = |x: u64| h = (h ^ x).wrapping_mul(0x100000001b3).rotate_left(5);
        let ix = &self.ri.ix;
        for d in self.lo..self.hi {
            let sh = ix.shards[(d / ix.shard_size) as usize].read();
            eat(((d as u64) << 33) | ((sh.stock.contains(d) as u64) << 32) | ix.price[d as usize].load(Relaxed) as u64);
        }
        h
    }

    /// Apply one batch (header line + update pairs) unless it is a duplicate; count gaps.
    fn apply_batch_text(&self, text: &[u8]) {
        let mut lines = text.split(|&c| c == b'\n').filter(|l| !l.is_empty());
        let hdr: Value = serde_json::from_slice(lines.next().unwrap()).unwrap();
        let seq = hdr["_batch"].as_u64().unwrap();
        let prev = self.applied_seq.load(Relaxed);
        if seq <= prev {
            self.dups.fetch_add(1, Relaxed);
            return;
        }
        if seq != prev + 1 {
            self.gaps.fetch_add(1, Relaxed);
        }
        let mut ups = Vec::new();
        while let (Some(a), Some(b)) = (lines.next(), lines.next()) {
            let a: Value = serde_json::from_slice(a).unwrap();
            let b: Value = serde_json::from_slice(b).unwrap();
            let d: u32 = a["update"]["_id"].as_str().unwrap().parse().unwrap();
            if d < self.lo || d >= self.hi {
                continue;
            }
            if let Some(v) = b["doc"].get("stock") {
                ups.push(Update::SetStock(d, v.as_bool().unwrap()));
            }
            if let Some(v) = b["doc"].get("price") {
                ups.push(Update::Price(d, v.as_u64().unwrap() as u32));
            }
        }
        self.ri.ix.apply_batch(&mut ups);
        self.applied_seq.store(seq, Relaxed);
        self.applied_ts.store(hdr["ts"].as_u64().unwrap(), Relaxed);
    }

    /// Take a snapshot between batches if one was requested (consistent with next_offset).
    fn maybe_snapshot(&self) {
        let req = self.snap_req.lock().take();
        if let Some(dir) = req {
            let t = Instant::now();
            let bytes = bitmap_engine::realcat::snapshot::save(&self.ri, &self.vocab_list, &dir, self.next_offset.load(Relaxed), self.applied_seq.load(Relaxed)).unwrap();
            *self.snap_result.lock() = Some(json!({"bytes": bytes, "secs": t.elapsed().as_secs_f64(),
                "offset": self.next_offset.load(Relaxed), "applied_seq": self.applied_seq.load(Relaxed)}));
        }
    }

    /// Consume partition 0 of the topic from `start` offset; ready once the startup high
    /// watermark is reached.
    fn tail_kafka(&self, brokers: &str, topic: &str, start: u64) {
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::Message;
        let c: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", format!("node-{}-{}", self.lo, std::process::id()))
            .set("enable.auto.commit", "false")
            .set("fetch.wait.max.ms", "5")
            .set("fetch.min.bytes", "1")
            .create()
            .unwrap();
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(topic, 0, rdkafka::Offset::Offset(start as i64)).unwrap();
        c.assign(&tpl).unwrap();
        self.next_offset.store(start, Relaxed);
        let hw = c.fetch_watermarks(topic, 0, Duration::from_secs(10)).map(|w| w.1 as u64).unwrap_or(0);
        if start >= hw {
            self.ready.store(true, Relaxed);
        }
        loop {
            self.maybe_snapshot();
            if self.paused.load(Relaxed) {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            match c.poll(Duration::from_millis(20)) {
                Some(Ok(m)) => {
                    if let Some(p) = m.payload() {
                        self.apply_batch_text(p);
                    }
                    self.next_offset.store(m.offset() as u64 + 1, Relaxed);
                    if m.offset() as u64 + 1 >= hw {
                        self.ready.store(true, Relaxed);
                    }
                }
                Some(Err(e)) => eprintln!("kafka: {e}"),
                None => {}
            }
        }
    }

    /// Follow the update log from the start; apply complete batches, keep only this node's docs.
    fn tail(&self, log: &std::path::Path) {
        while !log.exists() {
            std::thread::sleep(Duration::from_millis(20));
        }
        let mut f = std::fs::File::open(log).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = vec![0u8; 1 << 20];
        let mut caught_up_once = false;
        loop {
            let n = f.read(&mut chunk).unwrap();
            if n == 0 {
                if !caught_up_once {
                    caught_up_once = true;
                    self.ready.store(true, Relaxed);
                }
                std::thread::sleep(Duration::from_micros(500));
                continue;
            }
            buf.extend_from_slice(&chunk[..n]);
            let mut p = 0;
            loop {
                // header line
                let Some(e) = buf[p..].iter().position(|&c| c == b'\n') else { break };
                let hdr: Value = serde_json::from_slice(&buf[p..p + e]).unwrap();
                let cnt = hdr["n"].as_u64().unwrap() as usize;
                // need 2 * cnt more complete lines
                let mut q = p + e + 1;
                let mut lines = Vec::with_capacity(2 * cnt);
                let mut complete = true;
                for _ in 0..2 * cnt {
                    match buf[q..].iter().position(|&c| c == b'\n') {
                        Some(e2) => {
                            lines.push((q, q + e2));
                            q += e2 + 1;
                        }
                        None => {
                            complete = false;
                            break;
                        }
                    }
                }
                if !complete {
                    break;
                }
                let mut ups = Vec::with_capacity(cnt);
                for pair in lines.chunks(2) {
                    let a: Value = serde_json::from_slice(&buf[pair[0].0..pair[0].1]).unwrap();
                    let b: Value = serde_json::from_slice(&buf[pair[1].0..pair[1].1]).unwrap();
                    let d: u32 = a["update"]["_id"].as_str().unwrap().parse().unwrap();
                    if d < self.lo || d >= self.hi {
                        continue;
                    }
                    if let Some(v) = b["doc"].get("stock") {
                        ups.push(Update::SetStock(d, v.as_bool().unwrap()));
                    }
                    if let Some(v) = b["doc"].get("price") {
                        ups.push(Update::Price(d, v.as_u64().unwrap() as u32));
                    }
                }
                self.ri.ix.apply_batch(&mut ups);
                self.applied_seq.store(hdr["_batch"].as_u64().unwrap(), Relaxed);
                self.applied_ts.store(hdr["ts"].as_u64().unwrap(), Relaxed);
                p = q;
            }
            buf.drain(..p);
        }
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (path, dir, port, log) = (&a[1], std::path::PathBuf::from(&a[2]), &a[3], std::path::PathBuf::from(&a[4]));
    let t0 = Instant::now();
    let snap_load = std::env::var("SNAP_LOAD").ok().map(std::path::PathBuf::from);
    let (ri, vocab_list, lo, hi, start_offset, start_seq, rc) = if let Some(sd) = &snap_load {
        let (ri, vocab, off, seq) = bitmap_engine::realcat::snapshot::load(sd).unwrap();
        let (lo, hi) = (a[5].parse::<u32>().unwrap(), a.get(6).map(|x| x.parse::<u32>().unwrap()).unwrap_or(ri.ix.n).min(ri.ix.n));
        println!("node :{port} loaded snapshot {} (offset {off}, batch {seq}) in {:.1}s", sd.display(), t0.elapsed().as_secs_f64());
        (ri, vocab, lo, hi, off, seq, None)
    } else {
        let rc = load(path);
        let (lo, hi) = (a[5].parse::<u32>().unwrap(), a.get(6).map(|x| x.parse::<u32>().unwrap()).unwrap_or(rc.n).min(rc.n));
        let ri = build_part(&rc, 16, lo, hi);
        let v = rc.vocab.clone();
        (ri, v, lo, hi, 0, 0, Some(rc))
    };
    // shared document store: written once, offsets cached next to it
    std::fs::create_dir_all(&dir).unwrap();
    let (dp, op) = (dir.join("docs.json"), dir.join("docs.off"));
    if !op.exists() {
        let rc = rc.as_ref().expect("document store missing and no catalog loaded");
        let mut off = vec![0u64];
        let mut w = std::io::BufWriter::new(std::fs::File::create(&dp).unwrap());
        for d in 0..rc.n as usize {
            let s = static_source(&rc, d);
            w.write_all(s.as_bytes()).unwrap();
            off.push(off.last().unwrap() + s.len() as u64);
        }
        w.into_inner().unwrap().sync_all().unwrap();
        std::fs::write(&op, off.iter().flat_map(|x| x.to_le_bytes()).collect::<Vec<u8>>()).unwrap();
    }
    let ob = std::fs::read(&op).unwrap();
    let off: Vec<u64> = ob.chunks(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect();
    let vocab: HashMap<String, u32> = vocab_list.iter().enumerate().map(|(i, w)| (w.clone(), i as u32)).collect();
    drop(rc);
    let node = Node {
        ri,
        vocab,
        docs: std::fs::File::open(&dp).unwrap(),
        off,
        lo,
        hi,
        ready: AtomicBool::new(false),
        applied_seq: AtomicU64::new(start_seq),
        applied_ts: AtomicU64::new(0),
        _wal_lock: Mutex::new(()),
        vocab_list,
        next_offset: AtomicU64::new(start_offset),
        dups: AtomicU64::new(0),
        gaps: AtomicU64::new(0),
        snap_req: Mutex::new(None),
        snap_result: Mutex::new(None),
        paused: AtomicBool::new(false),
    };
    println!("node :{port} docids [{lo}, {hi}) built in {:.1}s, {}", t0.elapsed().as_secs_f64(), mem_status());

    let sock = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
    sock.set_reuse_address(true).unwrap();
    sock.set_tcp_nodelay(true).unwrap();
    sock.bind(&format!("127.0.0.1:{port}").parse::<std::net::SocketAddr>().unwrap().into()).unwrap();
    sock.listen(1024).unwrap();
    let server = tiny_http::Server::from_listener(std::net::TcpListener::from(sock), None).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| {
            let l = log.to_string_lossy().to_string();
            if let Some(rest) = l.strip_prefix("kafka:") {
                let (brokers, topic) = rest.rsplit_once('/').unwrap();
                node.tail_kafka(brokers, topic, start_offset);
            } else {
                node.tail(&log);
            }
        });
        s.spawn(|| {
            while !node.ready.load(Relaxed) {
                std::thread::sleep(Duration::from_millis(10));
            }
            println!("node :{port} ready (caught up with the log) after {:.1}s", t0.elapsed().as_secs_f64());
        });
        for _ in 0..4 {
            let (server, node) = (&server, &node);
            s.spawn(move || loop {
                let mut req = match server.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let mut body = String::new();
                std::io::Read::read_to_string(req.as_reader(), &mut body).unwrap();
                let url = req.url().to_string();
                let out = if url.starts_with("/_status") {
                    json!({"ready": node.ready.load(Relaxed), "applied_seq": node.applied_seq.load(Relaxed),
                           "applied_ts": node.applied_ts.load(Relaxed), "lo": node.lo, "hi": node.hi,
                           "next_offset": node.next_offset.load(Relaxed), "dups": node.dups.load(Relaxed), "gaps": node.gaps.load(Relaxed)})
                } else if url.starts_with("/_pause") {
                    node.paused.store(url.contains("on=1"), Relaxed);
                    json!({"paused": node.paused.load(Relaxed)})
                } else if url.starts_with("/_snapshot") {
                    // the tail thread takes it between batches; wait for the result
                    let dir = std::path::PathBuf::from(body.trim());
                    *node.snap_result.lock() = None;
                    *node.snap_req.lock() = Some(dir);
                    loop {
                        if let Some(v) = node.snap_result.lock().take() {
                            break v;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                } else if url.starts_with("/_digest") {
                    json!({"digest": format!("{:016x}", node.digest()), "applied_seq": node.applied_seq.load(Relaxed)})
                } else if url.starts_with("/_brandcounts") {
                    let v: Value = serde_json::from_str(&body).unwrap();
                    match node.parse_query(&v["query"]["bool"]) {
                        Some(q) => {
                            let f = node.brand_counts(&q);
                            let counts: Vec<u32> = v["brands"].as_array().unwrap().iter()
                                .map(|b| *f.get(&b.as_str().unwrap().parse::<u32>().unwrap()).unwrap_or(&0)).collect();
                            json!({"counts": counts})
                        }
                        None => json!({"counts": []}),
                    }
                } else if url.starts_with("/real/_search") {
                    node.search(&serde_json::from_str(&body).unwrap())
                } else {
                    json!({"error": "unsupported"})
                };
                let _ = req.respond(tiny_http::Response::from_string(out.to_string())
                    .with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap()));
            });
        }
    });
}
