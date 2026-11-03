//! Distributed tests against `node` processes, with routing done in the client (smart client):
//! health tracking, retry on another replica, scatter-gather and merging for partitions.
//!
//!   distbench scale    FILE  P1,P2,P3                  replica throughput with 1..n nodes
//!   distbench writes   FILE  P1,P2,P3 LOG RATE         queries + log writes; lag; final digests
//!   distbench failover FILE  P1,P2,P3 LOG RATE         as writes, kill -9 one node, restart it ($RESTART_CMD)
//!   distbench sharded  FILE  S1,S2,S3 REF R1,R2,R3     partition correctness vs REF, and cost vs replicas
use bitmap_engine::realcat::{self, RealCat};
use bitmap_engine::*;
use rayon::prelude::*;
use serde_json::{json, Value};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new().timeout(Duration::from_secs(10)).timeout_connect(Duration::from_secs(2)).build()
}
thread_local! {
    static AGENT: ureq::Agent = agent();
}
fn post(a: &ureq::Agent, port: u16, path: &str, body: &str) -> Result<Value, String> {
    a.post(&format!("http://127.0.0.1:{port}{path}"))
        .set("Content-Type", "application/json")
        .send_string(body)
        .map_err(|e| e.to_string())
        .and_then(|r| r.into_json::<Value>().map_err(|e| e.to_string()))
}
fn get(a: &ureq::Agent, port: u16, path: &str) -> Result<Value, String> {
    a.get(&format!("http://127.0.0.1:{port}{path}")).call().map_err(|e| e.to_string()).and_then(|r| r.into_json::<Value>().map_err(|e| e.to_string()))
}
fn stats(mut v: Vec<f64>) -> (f64, f64, f64) {
    if v.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |x: f64| v[((v.len() - 1) as f64 * x).round() as usize];
    (p(0.5), p(0.99), *v.last().unwrap())
}
fn cpu_secs() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let f: Vec<&str> = s.rsplit(')').next().unwrap().split_whitespace().collect();
    (f[11].parse::<f64>().unwrap() + f[12].parse::<f64>().unwrap()) / 100.0
}
fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}
fn ports(s: &str) -> Vec<u16> {
    s.split(',').map(|x| x.parse().unwrap()).collect()
}

// ------------------------------------------------------------------ request bodies (same DSL as the ES tests)
fn filt(rc: &RealCat, c: &Clause) -> Value {
    match c {
        Clause::Cat(x) => json!({"term": {"cat": x.to_string()}}),
        Clause::Term(x) => json!({"term": {"title_tok": rc.vocab[*x as usize]}}),
        Clause::Stock => json!({"term": {"stock": true}}),
        Clause::Rocket => json!({"term": {"rocket": true}}),
        Clause::Buckets(lo, hi) if *hi >= N_BUCKET => json!({"range": {"price": {"gte": edge(*lo)}}}),
        Clause::Buckets(lo, hi) => json!({"range": {"price": {"gte": edge(*lo), "lt": edge(*hi)}}}),
    }
}
fn q_pop(rc: &RealCat, q: &[Clause]) -> Value {
    json!({"size": K, "sort": [{"pop": "asc"}], "query": {"bool": {"filter": q.iter().map(|c| filt(rc, c)).collect::<Vec<_>>()}}})
}
fn q_bm25(rc: &RealCat, ws: &[u32], f: &[Clause]) -> Value {
    let text: Vec<&str> = ws.iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
    json!({"size": K, "query": {"bool": {"must": {"match": {"title_tok": text.join(" ")}}, "filter": f.iter().map(|c| filt(rc, c)).collect::<Vec<_>>()}}})
}
fn q_facet(rc: &RealCat, q: &[Clause], n: usize) -> Value {
    json!({"size": 0, "query": {"bool": {"filter": q.iter().map(|c| filt(rc, c)).collect::<Vec<_>>()}}, "aggs": {"brands": {"terms": {"field": "brand", "size": n}}}})
}
/// Fresh held-out workload (seeds 600/700): BM25 bodies (+stock, +category&stock) and filter bodies.
fn workload(rc: &RealCat) -> (Vec<Value>, Vec<Value>, Vec<Vec<Clause>>) {
    let (mut bm, mut fl, mut fq) = (Vec::new(), Vec::new(), Vec::new());
    for tail in [false, true] {
        let h = realcat::heldout_base(rc, 300, tail, if tail { 700 } else { 600 });
        bm.extend(h.bm25.iter().map(|b| q_bm25(rc, b, &[Clause::Stock])));
        bm.extend(h.bm25_cat.iter().map(|(b, c)| q_bm25(rc, b, &[Clause::Cat(*c), Clause::Stock])));
        fl.extend(h.word.iter().chain(&h.two).chain(&h.cat_word).map(|q| q_pop(rc, q)));
        fq.extend(h.word.iter().cloned());
    }
    (bm, fl, fq)
}

// ------------------------------------------------------------------ replicated routing
fn now_ms() -> u64 {
    now_ns() / 1_000_000
}
struct Replicas {
    ports: Vec<u16>,
    down_until: Vec<AtomicU64>, // circuit breaker: skip a replica until this time (ms), then try again
    up: Vec<AtomicBool>,
    rr: AtomicUsize,
    retries: AtomicU64,
    failures: AtomicU64,
    errs: parking_lot::Mutex<std::collections::HashMap<String, u64>>,
}
impl Replicas {
    fn new(ports: Vec<u16>) -> Self {
        let up = ports.iter().map(|_| AtomicBool::new(true)).collect();
        let down_until = ports.iter().map(|_| AtomicU64::new(0)).collect();
        Replicas { ports, down_until, up, rr: AtomicUsize::new(0), retries: AtomicU64::new(0), failures: AtomicU64::new(0), errs: Default::default() }
    }
    /// Round-robin over healthy replicas; on a connection error mark the replica down and retry
    /// the next one. Counts a failure only if every replica failed.
    fn search(&self, a: &ureq::Agent, body: &str, active: usize) -> Option<Value> {
        let start = self.rr.fetch_add(1, Relaxed);
        let now = now_ms();
        for k in 0..active {
            let i = (start + k) % active;
            if !self.up[i].load(Relaxed) || self.down_until[i].load(Relaxed) > now {
                continue;
            }
            // one immediate retry on the same node covers a stale keep-alive connection
            let r = post(a, self.ports[i], "/real/_search", body).or_else(|_| post(a, self.ports[i], "/real/_search", body));
            match r {
                Ok(v) => return Some(v),
                Err(e) => {
                    *self.errs.lock().entry(e.chars().take(90).collect()).or_default() += 1;
                    self.down_until[i].store(now_ms() + 500, Relaxed);
                    self.retries.fetch_add(1, Relaxed);
                }
            }
        }
        self.failures.fetch_add(1, Relaxed);
        None
    }
    /// Health checker: a replica is up when /_status answers and reports ready.
    fn check(&self, a: &ureq::Agent) {
        for (i, p) in self.ports.iter().enumerate() {
            let ok = get(a, *p, "/_status").map(|v| v["ready"].as_bool() == Some(true)).unwrap_or(false);
            self.up[i].store(ok, Relaxed);
        }
    }
}

fn load_run(n_threads: usize, secs: u64, f: impl Fn(&ureq::Agent, usize) -> bool + Sync) -> (f64, f64, f64, f64, u64) {
    let stop = AtomicBool::new(false);
    let done = AtomicU64::new(0);
    let errs = AtomicU64::new(0);
    let (c0, t0) = (cpu_secs(), Instant::now());
    let lats = std::thread::scope(|s| {
        let hs: Vec<_> = (0..n_threads)
            .map(|t| {
                let (stop, done, errs, f) = (&stop, &done, &errs, &f);
                s.spawn(move || {
                    let a = agent();
                    let mut l = Vec::new();
                    let mut j = t * 97;
                    while !stop.load(Relaxed) {
                        let t1 = Instant::now();
                        if f(&a, j) {
                            l.push(t1.elapsed().as_secs_f64() * 1e3);
                            done.fetch_add(1, Relaxed);
                        } else {
                            errs.fetch_add(1, Relaxed);
                        }
                        j += 1;
                    }
                    l
                })
            })
            .collect();
        std::thread::sleep(Duration::from_secs(secs));
        stop.store(true, Relaxed);
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
    });
    let wall = t0.elapsed().as_secs_f64();
    let (p50, p99, _) = stats(lats);
    (done.load(Relaxed) as f64 / wall, p50, p99, 100.0 * (cpu_secs() - c0) / wall, errs.load(Relaxed))
}

fn scale(path: &str, ps: Vec<u16>) {
    let rc = realcat::load(path);
    let (bm, fl, _) = workload(&rc);
    let (bm, fl): (Vec<String>, Vec<String>) = (bm.iter().map(|v| v.to_string()).collect(), fl.iter().map(|v| v.to_string()).collect());
    let r = Replicas::new(ps.clone());
    let a = agent();
    for p in &ps {
        for b in bm.iter().chain(&fl) {
            post(&a, *p, "/real/_search", b).unwrap(); // warm every replica
        }
    }
    println!("T1 replica scaling (each node pinned to its own core; client on core 0; 16 client threads, 15 s)");
    for (name, bodies) in [("BM25 (+stock / +cat&stock)", &bm), ("filters top-10 by popularity", &fl)] {
        for n in 1..=ps.len() {
            let (qps, p50, p99, cpu, e) = load_run(16, 15, |a, j| r.search(a, &bodies[j % bodies.len()], n).is_some());
            println!("  {name:<30} {n} node(s): {qps:7.0} QPS, p50 {p50:.2} p99 {p99:.2} ms, client core {cpu:.0}%, errors {e}");
            let mut es: Vec<(String, u64)> = r.errs.lock().drain().collect();
            es.sort_by(|a, b| b.1.cmp(&a.1));
            if !es.is_empty() { println!("      underlying errors: {:?}", &es[..es.len().min(3)]); }
        }
    }
}

// ------------------------------------------------------------------ log writer and expected state
struct Writer {
    file: std::fs::File,
    seq: u64,
    written_ts: Vec<u64>,
}
impl Writer {
    fn open(log: &str) -> Self {
        Writer { file: std::fs::OpenOptions::new().append(true).create(true).open(log).unwrap(), seq: 0, written_ts: vec![0] }
    }
    /// Append one batch (header + updates) and fsync before returning (acknowledged = durable).
    fn append(&mut self, ups: &[(u32, Option<bool>, Option<u32>)]) {
        self.seq += 1;
        let ts = now_ns();
        let mut s = format!("{}\n", json!({"_batch": self.seq, "ts": ts, "n": ups.len()}));
        for (d, st, pr) in ups {
            let doc = match (st, pr) {
                (Some(v), _) => json!({"stock": v}),
                (_, Some(p)) => json!({"price": p}),
                _ => unreachable!(),
            };
            s.push_str(&format!("{{\"update\":{{\"_id\":\"{d}\"}}}}\n{}\n", json!({"doc": doc})));
        }
        self.file.write_all(s.as_bytes()).unwrap();
        self.file.sync_data().unwrap();
        self.written_ts.push(ts);
    }
}
fn initial_state(rc: &RealCat) -> (Vec<bool>, Vec<u32>) {
    ((0..rc.n as u64).map(|d| unif(d, 6) < 0.9).collect(), rc.price.clone())
}
fn digest(st: &[bool], pr: &[u32], lo: u32, hi: u32) -> String {
    let mut h = 0xcbf29ce484222325u64;
    for d in lo..hi {
        let x = ((d as u64) << 33) | ((st[d as usize] as u64) << 32) | pr[d as usize] as u64;
        h = (h ^ x).wrapping_mul(0x100000001b3).rotate_left(5);
    }
    format!("{h:016x}")
}
fn batch(n: u32, b: u64) -> Vec<(u32, Option<bool>, Option<u32>)> {
    (0..1000u64)
        .map(|i| {
            let k = b * 1000 + i;
            let d = (mix(k ^ 0x5eed) % n as u64) as u32;
            if k % 2 == 0 { (d, Some(mix(k) % 10 != 0), None) } else { (d, None, Some(1000 + (mix(k + 3) % 100_000) as u32)) }
        })
        .collect()
}

/// Queries (16 threads through the replica router) while a writer appends batches at RATE
/// updates/s. Optionally kill one replica at 10 s and restart it at 25 s.
fn writes(path: &str, ps: Vec<u16>, log: &str, rate: u64, failover: bool) {
    let rc = realcat::load(path);
    let n = rc.n;
    let (bm, _, _) = workload(&rc);
    let bm: Vec<String> = bm.iter().map(|v| v.to_string()).collect();
    let (mut st, mut pr) = initial_state(&rc);
    drop(rc);
    let r = Replicas::new(ps.clone());
    let stop = AtomicBool::new(false);
    let dur = if failover { 100 } else { 30 };
    let ready_at_ms = AtomicU64::new(0);
    let t0 = Instant::now();
    let lat_windows: parking_lot::Mutex<Vec<(f64, f64, bool)>> = Default::default(); // (t, ms, ok)
    let lag_samples: parking_lot::Mutex<Vec<(usize, f64, u64)>> = Default::default(); // (node, lag ms, seq lag)
    let written = AtomicU64::new(0);
    let events: parking_lot::Mutex<Vec<String>> = Default::default();
    let mut w = Writer::open(log);
    let written_ts: parking_lot::Mutex<Vec<u64>> = parking_lot::Mutex::new(vec![0]);
    let total_batches = std::thread::scope(|s| {
        // writer: batches of 1000 at `rate` updates/s, fsync each
        let wh = s.spawn(|| {
            let mut b = 0u64;
            while !stop.load(Relaxed) {
                let due = (t0.elapsed().as_secs_f64() * rate as f64 / 1000.0) as u64;
                if b >= due {
                    std::thread::sleep(Duration::from_micros(200));
                    continue;
                }
                let ups = batch(n, b);
                w.append(&ups);
                written_ts.lock().push(*w.written_ts.last().unwrap());
                for (d, sv, pv) in &ups {
                    if let Some(v) = sv { st[*d as usize] = *v; }
                    if let Some(p) = pv { pr[*d as usize] = *p; }
                }
                b += 1;
                written.store(b, Relaxed);
            }
            (b, st, pr)
        });
        // health checker every 100 ms, and replication lag sampling
        s.spawn(|| {
            let a = agent();
            while !stop.load(Relaxed) {
                r.check(&a);
                let wb = written.load(Relaxed);
                for (i, p) in ps.iter().enumerate() {
                    if let Ok(v) = get(&a, *p, "/_status") {
                        if v["ready"].as_bool() == Some(true) {
                            let seq = v["applied_seq"].as_u64().unwrap();
                            // lag = age of the oldest batch the node has not applied yet (0 if caught up)
                            let ts = written_ts.lock();
                            let lag_ms = if seq + 1 < ts.len() as u64 { (now_ns().saturating_sub(ts[(seq + 1) as usize])) as f64 / 1e6 } else { 0.0 };
                            lag_samples.lock().push((i, lag_ms, wb.saturating_sub(seq)));
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        // fault injection
        if failover {
            s.spawn(|| {
                std::thread::sleep(Duration::from_secs(10));
                let pid = std::env::var("KILL_PID").unwrap();
                std::process::Command::new("kill").args(["-9", &pid]).status().unwrap();
                events.lock().push(format!("{:.1}s kill -9 node :{} (pid {pid})", t0.elapsed().as_secs_f64(), ps[1]));
                std::thread::sleep(Duration::from_secs(15));
                let cmd = std::env::var("RESTART_CMD").unwrap();
                // detach: the restarted node must not inherit this process's stdout
                std::process::Command::new("sh").args(["-c", &cmd]).stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
                let tr = t0.elapsed().as_secs_f64();
                events.lock().push(format!("{tr:.1}s restart node :{}", ps[1]));
                let a = agent();
                loop {
                    if get(&a, ps[1], "/_status").map(|v| v["ready"].as_bool() == Some(true)).unwrap_or(false) {
                        ready_at_ms.store((t0.elapsed().as_secs_f64() * 1000.0) as u64, Relaxed);
                        events.lock().push(format!("{:.1}s node :{} ready again (build + log replay {:.1}s)", t0.elapsed().as_secs_f64(), ps[1], t0.elapsed().as_secs_f64() - tr));
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            });
        }
        // queries
        let qh: Vec<_> = (0..16)
            .map(|t| {
                let (stop, r, bm, lat_windows) = (&stop, &r, &bm, &lat_windows);
                s.spawn(move || {
                    let a = agent();
                    let mut j = t * 97;
                    let mut local = Vec::new();
                    while !stop.load(Relaxed) {
                        let t1 = Instant::now();
                        let ok = r.search(&a, &bm[j % bm.len()], r.ports.len()).is_some();
                        local.push((t0.elapsed().as_secs_f64(), t1.elapsed().as_secs_f64() * 1e3, ok));
                        j += 1;
                    }
                    lat_windows.lock().extend(local);
                })
            })
            .collect();
        std::thread::sleep(Duration::from_secs(dur));
        stop.store(true, Relaxed);
        for h in qh {
            h.join().unwrap();
        }
        wh.join().unwrap()
    });
    let (nb, st, pr) = total_batches;
    for e in events.lock().iter() {
        println!("  event: {e}");
    }
    let lw = lat_windows.lock();
    let windows: Vec<(&str, f64, f64)> = if failover {
        let ra = ready_at_ms.load(Relaxed) as f64 / 1000.0;
        let ra = if ra > 0.0 { ra } else { dur as f64 };
        vec![("before kill (0-10 s)", 0.0, 10.0), ("one replica down until rejoin", 10.0, ra), ("after rejoin", ra, dur as f64 + 1.0)]
    } else {
        vec![("whole run", 0.0, 61.0)]
    };
    for (name, a, b) in windows {
        let sel: Vec<&(f64, f64, bool)> = lw.iter().filter(|x| x.0 >= a && x.0 < b).collect();
        let ok: Vec<f64> = sel.iter().filter(|x| x.2).map(|x| x.1).collect();
        let fails = sel.len() - ok.len();
        let (p50, p99, mx) = stats(ok.clone());
        println!("  queries {name:<28} {:6.0} QPS, p50 {p50:.2} p99 {p99:.2} max {mx:.1} ms, failed {fails}", ok.len() as f64 / (b.min(dur as f64) - a));
    }
    println!("  router: {} retries on another replica, {} requests failed on every replica", r.retries.load(Relaxed), r.failures.load(Relaxed));
    println!("  writes: {nb} batches x 1000 = {} updates in {dur} s ({:.0} updates/s), fsync per batch", nb * 1000, nb as f64 * 1000.0 / dur as f64);
    let ls = lag_samples.lock();
    for (i, p) in ps.iter().enumerate() {
        let l: Vec<f64> = ls.iter().filter(|x| x.0 == i).map(|x| x.1).collect();
        let sl: Vec<f64> = ls.iter().filter(|x| x.0 == i).map(|x| x.2 as f64).collect();
        let (p50, p99, mx) = stats(l);
        let (_, s99, smax) = stats(sl);
        println!("  replication lag node :{p}: p50 {p50:.1} p99 {p99:.1} max {mx:.1} ms (batches behind p99 {s99:.0}, max {smax:.0})");
    }
    // wait until every node has applied the last batch, then compare digests with the expected state
    let a = agent();
    let t = Instant::now();
    loop {
        let all = ps.iter().all(|p| get(&a, *p, "/_status").map(|v| v["applied_seq"].as_u64() == Some(nb)).unwrap_or(false));
        if all || t.elapsed() > Duration::from_secs(120) {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let want = digest(&st, &pr, 0, n);
    for p in &ps {
        let v = get(&a, *p, "/_digest").unwrap();
        println!("  final state node :{p}: applied batch {} of {nb}, digest {} {}", v["applied_seq"], v["digest"].as_str().unwrap(),
                 if v["digest"].as_str().unwrap() == want { "== expected" } else { "!= expected" });
    }
}

// ------------------------------------------------------------------ partitions
fn hits(v: &Value) -> Vec<(u32, f64)> {
    v["hits"]["hits"].as_array().map(|h| h.iter().map(|x| (x["_id"].as_str().unwrap().parse().unwrap(), x["_score"].as_f64().unwrap_or(0.0))).collect()).unwrap_or_default()
}
/// Popularity-sorted top-k over popularity-range partitions: ask partitions in order and stop
/// once k hits are collected (later partitions only hold less popular docs).
fn tiered(sh: &[u16], body: &str) -> Vec<u32> {
    let mut out = Vec::new();
    AGENT.with(|a| {
        for p in sh {
            out.extend(hits(&post(a, *p, "/real/_search", body).unwrap()).into_iter().map(|h| h.0));
            if out.len() >= K {
                break;
            }
        }
    });
    out.truncate(K);
    out
}
fn scatter(sh: &[u16], body: &str) -> Vec<Vec<(u32, f64)>> {
    sh.par_iter().map(|p| AGENT.with(|a| hits(&post(a, *p, "/real/_search", body).unwrap()))).collect()
}
fn merge_pop(parts: Vec<Vec<(u32, f64)>>) -> Vec<u32> {
    let mut v: Vec<u32> = parts.into_iter().flatten().map(|h| h.0).collect();
    v.sort_unstable();
    v.truncate(K);
    v
}
fn merge_score(parts: Vec<Vec<(u32, f64)>>) -> Vec<(u32, f64)> {
    let mut v: Vec<(u32, f64)> = parts.into_iter().flatten().collect();
    v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));
    v.truncate(K);
    v
}
/// Exact top-20 brand facet over partitions in two phases: each partition proposes its top 100
/// brands; exact counts for the union are then summed across partitions.
fn facet_2phase(rc: &RealCat, sh: &[u16], q: &[Clause]) -> Vec<(u32, u32)> {
    let p1 = q_facet(rc, q, 100).to_string();
    let cands: Vec<String> = {
        let mut c: Vec<String> = sh
            .par_iter()
            .flat_map_iter(|p| AGENT.with(|a| post(a, *p, "/real/_search", &p1).unwrap())["aggregations"]["brands"]["buckets"]
                .as_array().unwrap().iter().map(|b| b["key"].as_str().unwrap().to_string()).collect::<Vec<_>>())
            .collect();
        c.sort();
        c.dedup();
        c
    };
    let body = json!({"query": q_facet(rc, q, 0)["query"], "brands": cands}).to_string();
    let parts: Vec<Vec<u64>> = sh.par_iter().map(|p| AGENT.with(|a| post(a, *p, "/_brandcounts", &body).unwrap())["counts"]
        .as_array().unwrap().iter().map(|x| x.as_u64().unwrap()).collect()).collect();
    let mut v: Vec<(u32, u32)> = cands.iter().enumerate().map(|(i, b)| (parts.iter().map(|p| p[i]).sum::<u64>() as u32, b.parse().unwrap())).collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    v.into_iter().take(20).map(|(c, b)| (b, c)).collect()
}

fn sharded(path: &str, sh: Vec<u16>, refp: u16, reps: Vec<u16>) {
    let rc = realcat::load(path);
    let (bm, fl, fq) = workload(&rc);
    let a = agent();
    println!("T4 partitions by popularity range {:?} vs a single full node :{refp}", sh);
    // correctness
    let (mut t_ok, mut s_ok) = (0, 0);
    for b in &fl {
        let s = b.to_string();
        let want: Vec<u32> = hits(&post(&a, refp, "/real/_search", &s).unwrap()).into_iter().map(|h| h.0).collect();
        t_ok += (tiered(&sh, &s) == want) as u32;
        s_ok += (merge_pop(scatter(&sh, &s)) == want) as u32;
    }
    println!("  filters top-10 identical to single node: tiered {t_ok}/{}, scatter-gather {s_ok}/{}", fl.len(), fl.len());
    let mut b_ok = 0;
    for b in &bm {
        let s = b.to_string();
        let want = hits(&post(&a, refp, "/real/_search", &s).unwrap());
        let got = merge_score(scatter(&sh, &s));
        b_ok += (want.len() == got.len() && want.iter().zip(&got).all(|(x, y)| x.0 == y.0 && (x.1 - y.1).abs() <= 1e-5 * x.1.abs().max(1.0))) as u32;
    }
    println!("  BM25 top-10 (ids and scores) identical to single node: {b_ok}/{}", bm.len());
    let mut f_ok = 0;
    for q in fq.iter().step_by(3) {
        let want: Vec<(u32, u32)> = post(&a, refp, "/real/_search", &q_facet(&rc, q, 20).to_string()).unwrap()["aggregations"]["brands"]["buckets"]
            .as_array().unwrap().iter().map(|b| (b["key"].as_str().unwrap().parse().unwrap(), b["doc_count"].as_u64().unwrap() as u32)).collect();
        let got = facet_2phase(&rc, &sh, q);
        f_ok += (want.iter().map(|x| x.1).collect::<Vec<_>>() == got.iter().map(|x| x.1).collect::<Vec<_>>()) as u32;
    }
    println!("  brand facet top-20 counts (two-phase) identical to single node: {f_ok}/{}", fq.len().div_ceil(3));
    // how far tiered querying goes
    let mut depth = [0usize; 4];
    for b in &fl {
        let s = b.to_string();
        let mut got = 0;
        let mut used = 0;
        for p in &sh {
            used += 1;
            got += hits(&post(&a, *p, "/real/_search", &s).unwrap()).len();
            if got >= K {
                break;
            }
        }
        depth[used] += 1;
    }
    println!("  tiered: partitions queried per filter query: 1 -> {}, 2 -> {}, 3 -> {}", depth[1], depth[2], depth[3]);
    // cost: partitions vs replicas (same 3 cores)
    let (bms, fls): (Vec<String>, Vec<String>) = (bm.iter().map(|v| v.to_string()).collect(), fl.iter().map(|v| v.to_string()).collect());
    for p in sh.iter().chain(&reps) {
        for b in bms.iter().chain(&fls) {
            post(&a, *p, "/real/_search", b).unwrap();
        }
    }
    let r = Replicas::new(reps.clone());
    println!("  cost, 16 client threads x 15 s, 3 nodes either way:");
    let rows: Vec<(&str, Box<dyn Fn(&ureq::Agent, usize) -> bool + Sync>)> = vec![
        ("BM25, 3 replicas (round robin)", Box::new(|a: &ureq::Agent, j: usize| r.search(a, &bms[j % bms.len()], 3).is_some())),
        ("BM25, 3 partitions (scatter-gather)", Box::new(|_: &ureq::Agent, j: usize| { merge_score(scatter(&sh, &bms[j % bms.len()])); true })),
        ("filters, 3 replicas", Box::new(|a: &ureq::Agent, j: usize| r.search(a, &fls[j % fls.len()], 3).is_some())),
        ("filters, 3 partitions (tiered)", Box::new(|_: &ureq::Agent, j: usize| { tiered(&sh, &fls[j % fls.len()]); true })),
        ("filters, 3 partitions (scatter-gather)", Box::new(|_: &ureq::Agent, j: usize| { merge_pop(scatter(&sh, &fls[j % fls.len()])); true })),
    ];
    for (name, f) in rows {
        let (qps, p50, p99, cpu, e) = load_run(16, 15, |a, j| f(a, j));
        println!("    {name:<40} {qps:7.0} QPS, p50 {p50:.2} p99 {p99:.2} ms, client core {cpu:.0}%, errors {e}");
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    rayon::ThreadPoolBuilder::new().num_threads(8).build_global().unwrap();
    match a[1].as_str() {
        "scale" => scale(&a[2], ports(&a[3])),
        "writes" => writes(&a[2], ports(&a[3]), &a[4], a[5].parse().unwrap(), false),
        "failover" => writes(&a[2], ports(&a[3]), &a[4], a[5].parse().unwrap(), true),
        "sharded" => sharded(&a[2], ports(&a[3]), a[4].parse().unwrap(), ports(&a[5])),
        _ => panic!("mode"),
    }
}
