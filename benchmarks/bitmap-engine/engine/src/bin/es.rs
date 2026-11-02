//! Elasticsearch side of the comparison: same synthetic catalog, same queries.
//!   es load N        bulk-load docs 0..N (index sorted by pop, 4 shards, 0 replicas), refresh, force-merge
//!   es bench N       latency per query class, cross-check vs bitmap engine, throughput, updates, visibility
use bitmap_engine::*;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

const ES: &str = "http://127.0.0.1:9200";
const IDX: &str = "catalog";

fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new().timeout(Duration::from_secs(600)).build()
}
fn post(a: &ureq::Agent, path: &str, body: &str, ctype: &str) -> Value {
    match a.post(&format!("{ES}{path}")).set("Content-Type", ctype).send_string(body) {
        Ok(r) => r.into_json().unwrap(),
        Err(ureq::Error::Status(c, r)) => panic!("{path} -> {c}: {}", r.into_string().unwrap()),
        Err(e) => panic!("{path}: {e}"),
    }
}
fn search(a: &ureq::Agent, body: &Value) -> Value {
    post(a, &format!("/{IDX}/_search"), &body.to_string(), "application/json")
}

fn doc_json(d: u64) -> String {
    let x = doc(d);
    let title: Vec<String> = x.terms.iter().map(|t| format!("w{t}")).collect();
    json!({"pop": d, "cat": x.cat.to_string(), "brand": x.brand.to_string(), "price": x.price,
           "stock": x.stock, "rocket": x.rocket, "title": title.join(" ")})
    .to_string()
}

fn load(n: u32) {
    let a = agent();
    let _ = a.delete(&format!("{ES}/{IDX}")).call();
    a.put(&format!("{ES}/{IDX}")).set("Content-Type", "application/json").send_string(&json!({
        "settings": {"number_of_shards": 4, "number_of_replicas": 0, "refresh_interval": "-1",
                     "index.sort.field": "pop", "index.sort.order": "asc"},
        "mappings": {"properties": {
            "pop": {"type": "integer"}, "cat": {"type": "keyword"}, "brand": {"type": "keyword"},
            "price": {"type": "integer"}, "stock": {"type": "boolean"}, "rocket": {"type": "boolean"},
            "title": {"type": "text"}}}}).to_string()).expect("create index");
    let t = Instant::now();
    let next = AtomicU64::new(0);
    let batch = 5000u64;
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                let a = agent();
                loop {
                    let lo = next.fetch_add(batch, Relaxed);
                    if lo >= n as u64 {
                        break;
                    }
                    let mut body = String::new();
                    for d in lo..(lo + batch).min(n as u64) {
                        body.push_str(&format!("{{\"index\":{{\"_id\":\"{d}\"}}}}\n{}\n", doc_json(d)));
                    }
                    let r = post(&a, &format!("/{IDX}/_bulk"), &body, "application/x-ndjson");
                    assert!(!r["errors"].as_bool().unwrap(), "bulk errors");
                }
            });
        }
    });
    let load_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    post(&a, &format!("/{IDX}/_refresh"), "", "application/json");
    let refresh_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    post(&a, &format!("/{IDX}/_forcemerge?max_num_segments=1"), "", "application/json");
    let merge_s = t.elapsed().as_secs_f64();
    a.put(&format!("{ES}/{IDX}/_settings"))
        .set("Content-Type", "application/json")
        .send_string(&json!({"index": {"refresh_interval": "1s"}}).to_string())
        .unwrap();
    println!("ES load {n} docs: bulk {load_s:.1}s ({:.0} docs/s), refresh {refresh_s:.1}s, force-merge {merge_s:.1}s", n as f64 / load_s);
}

// ------------------------------------------------------------------ queries
fn filt(c: &Clause) -> Value {
    match c {
        Clause::Cat(x) => json!({"term": {"cat": x.to_string()}}),
        Clause::Term(x) => json!({"term": {"title": format!("w{x}")}}),
        Clause::Stock => json!({"term": {"stock": true}}),
        Clause::Rocket => json!({"term": {"rocket": true}}),
        Clause::Buckets(lo, hi) => json!({"range": {"price": {"gte": edge(*lo), "lt": edge(*hi)}}}),
    }
}
fn q_pop(q: &[Clause]) -> Value {
    json!({"size": K, "track_total_hits": false, "_source": false, "sort": [{"pop": "asc"}],
           "query": {"bool": {"filter": q.iter().map(filt).collect::<Vec<_>>()}}})
}
fn q_price(q: &[Clause]) -> Value {
    json!({"size": K, "track_total_hits": false, "_source": false, "sort": [{"price": "asc"}, {"pop": "asc"}],
           "query": {"bool": {"filter": q.iter().map(filt).collect::<Vec<_>>()}}})
}
fn q_facet(q: &[Clause]) -> Value {
    json!({"size": 0, "track_total_hits": true,
           "query": {"bool": {"filter": q.iter().map(filt).collect::<Vec<_>>()}},
           "aggs": {"brands": {"terms": {"field": "brand", "size": 20}}}})
}
fn q_bm25(i: u64) -> Value {
    let (a, b) = q_terms(i);
    json!({"size": K, "track_total_hits": false, "_source": false,
           "query": {"bool": {"must": {"match": {"title": format!("w{a} w{b}")}}, "filter": {"term": {"stock": true}}}}})
}
fn ids(r: &Value) -> Vec<u32> {
    r["hits"]["hits"].as_array().unwrap().iter().map(|h| h["_id"].as_str().unwrap().parse().unwrap()).collect()
}

fn stats(mut v: Vec<f64>) -> (f64, f64, f64) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |x: f64| v[((v.len() - 1) as f64 * x).round() as usize];
    (p(0.5), p(0.99), *v.last().unwrap())
}
fn measure(a: &ureq::Agent, bodies: &[Value]) -> String {
    for b in bodies.iter().take(50) {
        search(a, b); // warm-up
    }
    let (mut client, mut took) = (Vec::new(), Vec::new());
    for b in bodies {
        let t = Instant::now();
        let r = search(a, b);
        client.push(t.elapsed().as_secs_f64() * 1e3);
        took.push(r["took"].as_f64().unwrap());
    }
    let (c50, c99, cmax) = stats(client);
    let (t50, t99, _) = stats(took);
    format!("client p50 {c50:8.3} p99 {c99:8.3} max {cmax:8.3} ms | ES took p50 {t50:.0} p99 {t99:.0} ms")
}

fn bench(n: u32) {
    let a = agent();
    let nq = 1000u64;
    let qf: Vec<_> = (0..nq).map(q_filter).collect();
    let qp: Vec<_> = (0..nq).map(q_filter_price).collect();
    let q2: Vec<_> = (0..nq).map(q_kw2).collect();
    let q1: Vec<_> = (0..nq).map(q_kw1).collect();

    // cross-check against the bitmap engine on the same data
    let ix = build(n, 16, 4);
    let mut mism = 0;
    for q in qf.iter().chain(&qp).chain(&q2).chain(&q1).step_by(10) {
        if ids(&search(&a, &q_pop(q))) != ix.topk_pop(q, K) {
            mism += 1;
        }
    }
    for q in q1.iter().step_by(10) {
        let es: Vec<u32> = ids(&search(&a, &q_price(q)));
        let me: Vec<u32> = ix.topk_price(q).iter().map(|x| x.1).collect();
        if es != me {
            mism += 1;
        }
    }
    println!("cross-check vs bitmap engine: {mism} mismatches out of 500 queries");
    drop(ix);

    println!("ES latency (single client, 1000 queries each, after 50 warm-up)");
    for (name, qs) in [("cat & stock & rocket", &qf), ("cat & stock & price range", &qp), ("t1 & t2 & stock", &q2), ("t1 & stock", &q1)] {
        let bodies: Vec<Value> = qs.iter().map(|q| q_pop(q)).collect();
        println!("  top10 pop  {name:<26} {}", measure(&a, &bodies));
    }
    println!("  top10 price t1 & stock                {}", measure(&a, &q1.iter().map(|q| q_price(q)).collect::<Vec<_>>()));
    println!("  brand facet t1 & stock (exact total)  {}", measure(&a, &q1.iter().map(|q| q_facet(q)).collect::<Vec<_>>()));
    println!("  BM25 'w_a w_b' + stock filter         {}", measure(&a, &(0..nq).map(q_bm25).collect::<Vec<_>>()));

    // throughput: top-k mix, 8 client threads, 20 s
    let mixq: Vec<Value> = qf.iter().chain(&qp).chain(&q2).chain(&q1).map(|q| q_pop(q)).collect();
    for threads in [4usize, 8, 16] {
        let stop = AtomicBool::new(false);
        let done = AtomicU64::new(0);
        let lats = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads)
                .map(|t| {
                    let (mix, stop, done) = (&mixq, &stop, &done);
                    s.spawn(move || {
                        let a = agent();
                        let mut l = Vec::new();
                        let mut j = t * 997;
                        while !stop.load(Relaxed) {
                            let t0 = Instant::now();
                            search(&a, &mix[j % mix.len()]);
                            l.push(t0.elapsed().as_secs_f64() * 1e3);
                            done.fetch_add(1, Relaxed);
                            j += 1;
                        }
                        l
                    })
                })
                .collect();
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Relaxed);
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
        });
        let (p50, p99, _) = stats(lats);
        println!("throughput top-k mix, {threads:2} client threads: {:.0} QPS, p50 {p50:.2} p99 {p99:.2} ms", done.load(Relaxed) as f64 / 20.0);
    }

    // partial updates (stock toggle) via bulk, 4 threads x 1000 docs per request, 20 s
    let stop = AtomicBool::new(false);
    let done = AtomicU64::new(0);
    std::thread::scope(|s| {
        for t in 0..4u64 {
            let (stop, done) = (&stop, &done);
            s.spawn(move || {
                let a = agent();
                let mut i = t << 40;
                while !stop.load(Relaxed) {
                    let mut body = String::new();
                    for _ in 0..1000 {
                        let d = mix(i) % n as u64;
                        body.push_str(&format!("{{\"update\":{{\"_id\":\"{d}\"}}}}\n{{\"doc\":{{\"stock\":{}}}}}\n", i % 2 == 0));
                        i += 1;
                    }
                    post(&a, &format!("/{IDX}/_bulk"), &body, "application/x-ndjson");
                    done.fetch_add(1000, Relaxed);
                }
            });
        }
        std::thread::sleep(Duration::from_secs(20));
        stop.store(true, Relaxed);
    });
    println!("ES partial updates (bulk 1000, 4 threads, refresh_interval 1s): {:.0} docs/s", done.load(Relaxed) as f64 / 20.0);

    // query latency while updates run in the background
    let stop = AtomicBool::new(false);
    let lat = std::thread::scope(|s| {
        for t in 0..2u64 {
            let stop = &stop;
            s.spawn(move || {
                let a = agent();
                let mut i = (t + 7) << 40;
                while !stop.load(Relaxed) {
                    let mut body = String::new();
                    for _ in 0..1000 {
                        let d = mix(i) % n as u64;
                        body.push_str(&format!("{{\"update\":{{\"_id\":\"{d}\"}}}}\n{{\"doc\":{{\"stock\":{}}}}}\n", i % 2 == 0));
                        i += 1;
                    }
                    post(&a, &format!("/{IDX}/_bulk"), &body, "application/x-ndjson");
                }
            });
        }
        let r = measure(&a, &mixq.iter().step_by(4).cloned().collect::<Vec<_>>());
        stop.store(true, Relaxed);
        r
    });
    println!("ES top-k mix latency during 2-thread bulk updates: {lat}");

    // visibility: flip one doc's rocket flag, poll until a filtered search sees it
    let mut vis = Vec::new();
    for k in 0..20u64 {
        let d = 1000 + k * 7919;
        let cur = doc(d).rocket;
        let t = Instant::now();
        a.post(&format!("{ES}/{IDX}/_update/{d}"))
            .set("Content-Type", "application/json")
            .send_string(&json!({"doc": {"rocket": !cur}}).to_string())
            .unwrap();
        let probe = json!({"size": 0, "query": {"bool": {"filter": [{"term": {"pop": d}}, {"term": {"rocket": !cur}}]}}});
        loop {
            if search(&a, &probe)["hits"]["total"]["value"].as_u64().unwrap() == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        vis.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let (p50, p99, mx) = stats(vis);
    println!("ES update -> searchable delay (refresh_interval 1s): p50 {p50:.0} ms, max {mx:.0} ms (p99 {p99:.0})");
}

// ------------------------------------------------------------------ the bitmap engine over HTTP, same client
const OURS: &str = "http://127.0.0.1:9400/search";

fn ours_body(q: &[Clause], sort: &str) -> String {
    let cs: Vec<Value> = q
        .iter()
        .map(|c| match c {
            Clause::Cat(x) => json!(["cat", x]),
            Clause::Term(x) => json!(["term", x]),
            Clause::Stock => json!(["stock"]),
            Clause::Rocket => json!(["rocket"]),
            Clause::Buckets(a, b) => json!(["buckets", a, b]),
        })
        .collect();
    json!({"q": cs, "sort": sort}).to_string()
}
fn ours_search(a: &ureq::Agent, body: &str) -> Value {
    a.post(OURS).set("Content-Type", "application/json").send_string(body).unwrap().into_json().unwrap()
}
fn ours_measure(a: &ureq::Agent, bodies: &[String]) -> String {
    for b in bodies.iter().take(50) {
        ours_search(a, b);
    }
    let (mut client, mut took) = (Vec::new(), Vec::new());
    for b in bodies {
        let t = Instant::now();
        let r = ours_search(a, b);
        client.push(t.elapsed().as_secs_f64() * 1e3);
        took.push(r["took_us"].as_f64().unwrap() / 1e3);
    }
    let (c50, c99, cmax) = stats(client);
    let (t50, t99, _) = stats(took);
    format!("client p50 {c50:8.3} p99 {c99:8.3} max {cmax:8.3} ms | server p50 {t50:.3} p99 {t99:.3} ms")
}

fn ours() {
    let a = agent();
    let nq = 1000u64;
    let qf: Vec<_> = (0..nq).map(q_filter).collect();
    let qp: Vec<_> = (0..nq).map(q_filter_price).collect();
    let q2: Vec<_> = (0..nq).map(q_kw2).collect();
    let q1: Vec<_> = (0..nq).map(q_kw1).collect();
    println!("bitmap engine over HTTP (single client, 1000 queries each, after 50 warm-up)");
    for (name, qs) in [("cat & stock & rocket", &qf), ("cat & stock & price range", &qp), ("t1 & t2 & stock", &q2), ("t1 & stock", &q1)] {
        let bodies: Vec<String> = qs.iter().map(|q| ours_body(q, "pop")).collect();
        println!("  top10 pop  {name:<26} {}", ours_measure(&a, &bodies));
    }
    println!("  top10 price t1 & stock                {}", ours_measure(&a, &q1.iter().map(|q| ours_body(q, "price")).collect::<Vec<_>>()));
    let mixq: Vec<String> = qf.iter().chain(&qp).chain(&q2).chain(&q1).map(|q| ours_body(q, "pop")).collect();
    for threads in [4usize, 8, 16] {
        let stop = AtomicBool::new(false);
        let done = AtomicU64::new(0);
        let lats = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads)
                .map(|t| {
                    let (mix, stop, done) = (&mixq, &stop, &done);
                    s.spawn(move || {
                        let a = agent();
                        let mut l = Vec::new();
                        let mut j = t * 997;
                        while !stop.load(Relaxed) {
                            let t0 = Instant::now();
                            ours_search(&a, &mix[j % mix.len()]);
                            l.push(t0.elapsed().as_secs_f64() * 1e3);
                            done.fetch_add(1, Relaxed);
                            j += 1;
                        }
                        l
                    })
                })
                .collect();
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Relaxed);
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
        });
        let (p50, p99, _) = stats(lats);
        println!("throughput top-k mix over HTTP, {threads:2} client threads: {:.0} QPS, p50 {p50:.2} p99 {p99:.2} ms", done.load(Relaxed) as f64 / 20.0);
    }
}

// ------------------------------------------------------------------ real Amazon catalog
const RIDX: &str = "real";
fn rsearch(a: &ureq::Agent, body: &Value, dfs: bool) -> Value {
    let qs = if dfs { "?search_type=dfs_query_then_fetch" } else { "" };
    post(a, &format!("/{RIDX}/_search{qs}"), &body.to_string(), "application/json")
}
fn rfilt(rc: &realcat::RealCat, c: &Clause) -> Value {
    match c {
        Clause::Cat(x) => json!({"term": {"cat": x.to_string()}}),
        Clause::Term(x) => json!({"term": {"title_tok": rc.vocab[*x as usize]}}),
        Clause::Stock => json!({"term": {"stock": true}}),
        Clause::Rocket => json!({"term": {"rocket": true}}),
        Clause::Buckets(lo, hi) if *hi >= N_BUCKET => json!({"range": {"price": {"gte": edge(*lo)}}}),
        Clause::Buckets(lo, hi) => json!({"range": {"price": {"gte": edge(*lo), "lt": edge(*hi)}}}),
    }
}
fn rq_pop(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"size": K, "track_total_hits": false, "_source": false, "sort": [{"pop": "asc"}],
           "query": {"bool": {"filter": q.iter().map(|c| rfilt(rc, c)).collect::<Vec<_>>()}}})
}
fn rq_price(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"size": K, "track_total_hits": false, "_source": false, "sort": [{"price": "asc"}, {"pop": "asc"}],
           "query": {"bool": {"filter": q.iter().map(|c| rfilt(rc, c)).collect::<Vec<_>>()}}})
}
fn rq_facet(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"size": 0, "track_total_hits": true, "query": {"bool": {"filter": q.iter().map(|c| rfilt(rc, c)).collect::<Vec<_>>()}},
           "aggs": {"brands": {"terms": {"field": "brand", "size": 20}}}})
}
fn rq_facet_exact(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    let mut v = rq_facet(rc, q);
    v["aggs"]["brands"]["terms"]["shard_size"] = json!(300_000);
    v
}
fn rq_bm25(rc: &realcat::RealCat, ws: &[u32]) -> Value {
    let text: Vec<&str> = ws.iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
    json!({"size": K, "track_total_hits": false, "_source": false,
           "query": {"bool": {"must": {"match": {"title_tok": text.join(" ")}}, "filter": {"term": {"stock": true}}}}})
}

fn realload(path: &str) {
    let rc = realcat::load(path);
    let a = agent();
    let _ = a.delete(&format!("{ES}/{RIDX}")).call();
    a.put(&format!("{ES}/{RIDX}")).set("Content-Type", "application/json").send_string(&json!({
        "settings": {"number_of_shards": std::env::var("SHARDS").ok().and_then(|v| v.parse::<u32>().ok()).unwrap_or(4), "number_of_replicas": 0, "refresh_interval": "-1",
                     "index.sort.field": "pop", "index.sort.order": "asc",
                     "analysis": {"analyzer": {"ws": {"type": "custom", "tokenizer": "whitespace"}}}},
        "mappings": {"properties": {
            "pop": {"type": "integer"}, "cat": {"type": "keyword"}, "brand": {"type": "keyword"},
            "price": {"type": "integer"}, "stock": {"type": "boolean"}, "rocket": {"type": "boolean"},
            "title_tok": {"type": "text", "analyzer": "ws"}}}}).to_string()).expect("create index");
    let t = Instant::now();
    let next = AtomicU64::new(0);
    let n = rc.n as u64;
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                let a = agent();
                loop {
                    let lo = next.fetch_add(5000, Relaxed);
                    if lo >= n {
                        break;
                    }
                    let mut body = String::new();
                    for d in lo..(lo + 5000).min(n) {
                        let i = d as usize;
                        let title: Vec<&str> = rc.tokens[i].iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
                        let doc = json!({"pop": d, "cat": rc.cat[i].to_string(), "brand": rc.brand[i].to_string(), "price": rc.price[i],
                                         "stock": unif(d, 6) < 0.9, "rocket": unif(d, 7) < 0.4, "title_tok": title.join(" ")});
                        body.push_str(&format!("{{\"index\":{{\"_id\":\"{d}\"}}}}\n{doc}\n"));
                    }
                    let r = post(&a, &format!("/{RIDX}/_bulk"), &body, "application/x-ndjson");
                    assert!(!r["errors"].as_bool().unwrap(), "bulk errors");
                }
            });
        }
    });
    let load_s = t.elapsed().as_secs_f64();
    post(&a, &format!("/{RIDX}/_refresh"), "", "application/json");
    let t = Instant::now();
    post(&a, &format!("/{RIDX}/_forcemerge?max_num_segments=1"), "", "application/json");
    let merge_s = t.elapsed().as_secs_f64();
    a.put(&format!("{ES}/{RIDX}/_settings")).set("Content-Type", "application/json")
        .send_string(&json!({"index": {"refresh_interval": "1s"}}).to_string()).unwrap();
    println!("ES real load {n} docs: bulk {load_s:.1}s ({:.0} docs/s), force-merge {merge_s:.1}s, refresh_interval back to 1s", n as f64 / load_s);
}

fn realbench(path: &str) {
    let rc = realcat::load(path);
    let ri = realcat::build(&rc, 16);
    let q = realcat::queries(&rc, 1000);
    let a = agent();
    let ids = |r: &Value| -> Vec<u32> { r["hits"]["hits"].as_array().unwrap().iter().map(|h| h["_id"].as_str().unwrap().parse().unwrap()).collect() };

    // cross-check filters and price sort
    let mut mism = 0;
    let mut total = 0;
    for qq in q.word.iter().chain(&q.two).chain(&q.cat).chain(&q.cat_price).chain(&q.cat_word).step_by(10) {
        total += 1;
        if ids(&rsearch(&a, &rq_pop(&rc, qq), false)) != ri.ix.topk_pop(qq, K) {
            mism += 1;
        }
    }
    for qq in q.word.iter().step_by(10) {
        total += 1;
        if ids(&rsearch(&a, &rq_price(&rc, qq), false)) != ri.ix.topk_price(qq).iter().map(|x| x.1).collect::<Vec<_>>() {
            mism += 1;
        }
    }
    println!("cross-check filters/price sort vs ES: {mism} mismatches out of {total} queries");

    // BM25: score lists must match; doc lists must match except for ties at the 10th score
    let (mut same_scores, mut same_docs, mut max_rel) = (0, 0, 0f64);
    for b in &q.bm25 {
        let r = rsearch(&a, &rq_bm25(&rc, b), true);
        let es: Vec<(f64, u32)> = r["hits"]["hits"].as_array().unwrap().iter()
            .map(|h| (h["_score"].as_f64().unwrap(), h["_id"].as_str().unwrap().parse().unwrap())).collect();
        let me: Vec<(f64, u32)> = ri.bm25_topk(b, K).into_iter().map(|(s, d)| (s as f64, d)).collect();
        if es.len() != me.len() {
            continue;
        }
        let rel = es.iter().zip(&me).map(|(x, y)| (x.0 - y.0).abs() / x.0.abs().max(1e-9)).fold(0f64, f64::max);
        max_rel = max_rel.max(rel);
        if rel < 1e-5 {
            same_scores += 1;
        }
        let cut = es.last().map(|x| x.0).unwrap_or(0.0);
        let above = |v: &[(f64, u32)]| { let mut s: Vec<u32> = v.iter().filter(|x| x.0 > cut * (1.0 + 1e-6)).map(|x| x.1).collect(); s.sort(); s };
        if above(&es) == above(&me) {
            same_docs += 1;
        }
    }
    println!("BM25 vs ES (dfs_query_then_fetch): identical top-10 score lists (rel err < 1e-5) {same_scores}/1000, max rel err {max_rel:.2e}; identical docs above the 10th score (ties excluded) {same_docs}/1000");

    // facets: ES terms agg vs our exact; compare counts as multisets and brands above the 20th count
    let (mut f_counts, mut f_brands, mut f_ov) = (0, 0, 0.0);
    for qq in &q.word {
        let r = rsearch(&a, &rq_facet(&rc, qq), false);
        let es: Vec<(u32, u32)> = r["aggregations"]["brands"]["buckets"].as_array().unwrap().iter()
            .map(|b| (b["key"].as_str().unwrap().parse().unwrap(), b["doc_count"].as_u64().unwrap() as u32)).collect();
        let me = ri.facet_exact(qq);
        let cnts = |v: &[(u32, u32)]| v.iter().map(|x| x.1).collect::<Vec<_>>();
        if cnts(&es) == cnts(&me) {
            f_counts += 1;
        }
        let cut = me.last().map(|x| x.1).unwrap_or(0);
        let above = |v: &[(u32, u32)]| { let mut s: Vec<(u32, u32)> = v.iter().filter(|x| x.1 > cut).cloned().collect(); s.sort(); s };
        if above(&es) == above(&me) {
            f_brands += 1;
        }
        f_ov += es.iter().take(10).filter(|x| me.iter().take(10).any(|y| y.0 == x.0)).count() as f64 / me.len().clamp(1, 10) as f64;
    }
    let mut fx = 0;
    for qq in &q.word {
        let r = rsearch(&a, &rq_facet_exact(&rc, qq), false);
        let es: Vec<u32> = r["aggregations"]["brands"]["buckets"].as_array().unwrap().iter().map(|b| b["doc_count"].as_u64().unwrap() as u32).collect();
        if es == ri.facet_exact(qq).iter().map(|x| x.1).collect::<Vec<_>>() {
            fx += 1;
        }
    }
    println!("facets: ES terms agg with shard_size 300000 (exact) same top-20 counts as our exact {fx}/1000");
    println!("facets vs exact: ES terms agg same top-20 counts {f_counts}/1000, same brands+counts above the 20th count {f_brands}/1000, top-10 overlap {:.1}%", 100.0 * f_ov / 1000.0);

    println!("ES latency on real data (single client, 1000 queries each, after 50 warm-up)");
    let m = |bodies: Vec<Value>, dfs: bool| -> String {
        for b in bodies.iter().take(50) { rsearch(&a, b, dfs); }
        let (mut c, mut t) = (Vec::new(), Vec::new());
        for b in &bodies {
            let t0 = Instant::now();
            let r = rsearch(&a, b, dfs);
            c.push(t0.elapsed().as_secs_f64() * 1e3);
            t.push(r["took"].as_f64().unwrap());
        }
        let ((c50, c99, _), (t50, t99, _)) = (stats(c), stats(t));
        format!("client p50 {c50:7.3} p99 {c99:7.3} ms | took p50 {t50:.0} p99 {t99:.0} ms")
    };
    for (name, qs) in [("word & stock", &q.word), ("word1 & word2 & stock", &q.two), ("category & stock & rocket", &q.cat),
                       ("category & stock & price window", &q.cat_price), ("category & word & stock", &q.cat_word)] {
        println!("  top10 pop {name:<32} {}", m(qs.iter().map(|x| rq_pop(&rc, x)).collect(), false));
    }
    println!("  top10 price word & stock          {}", m(q.word.iter().map(|x| rq_price(&rc, x)).collect(), false));
    println!("  brand facet word & stock          {}", m(q.word.iter().map(|x| rq_facet(&rc, x)).collect(), false));
    println!("  brand facet word & stock (exact)  {}", m(q.word.iter().map(|x| rq_facet_exact(&rc, x)).collect(), false));
    println!("  BM25 2 words + stock              {}", m(q.bm25.iter().map(|x| rq_bm25(&rc, x)).collect(), false));
    println!("  BM25 2 words + stock (dfs)        {}", m(q.bm25.iter().map(|x| rq_bm25(&rc, x)).collect(), true));

    let mixq: Vec<Value> = q.word.iter().chain(&q.two).chain(&q.cat).chain(&q.cat_price).chain(&q.cat_word).map(|x| rq_pop(&rc, x)).collect();
    for threads in [4usize, 16] {
        let stop = AtomicBool::new(false);
        let done = AtomicU64::new(0);
        let lats = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads).map(|t| {
                let (mix, stop, done) = (&mixq, &stop, &done);
                s.spawn(move || {
                    let a = agent();
                    let mut l = Vec::new();
                    let mut j = t * 997;
                    while !stop.load(Relaxed) {
                        let t0 = Instant::now();
                        rsearch(&a, &mix[j % mix.len()], false);
                        l.push(t0.elapsed().as_secs_f64() * 1e3);
                        done.fetch_add(1, Relaxed);
                        j += 1;
                    }
                    l
                })
            }).collect();
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Relaxed);
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
        });
        let (p50, p99, _) = stats(lats);
        println!("ES throughput real top-k mix, {threads:2} client threads: {:.0} QPS, p50 {p50:.2} p99 {p99:.2} ms", done.load(Relaxed) as f64 / 20.0);
    }
}

/// Print ES explain vs our BM25 inputs for the first queries whose top score differs.
fn realbm25dbg(path: &str) {
    let rc = realcat::load(path);
    let ri = realcat::build(&rc, 16);
    let q = realcat::queries(&rc, 1000);
    let a = agent();
    let mut shown = 0;
    for b in &q.bm25 {
        let mut body = rq_bm25(&rc, b);
        body["explain"] = json!(true);
        body["size"] = json!(1);
        let r = rsearch(&a, &body, true);
        let h = &r["hits"]["hits"][0];
        let es_score = h["_score"].as_f64().unwrap();
        let me = ri.bm25_topk(b, K);
        if (me[0].0 as f64 - es_score).abs() / es_score < 1e-5 {
            continue;
        }
        let d: u32 = h["_id"].as_str().unwrap().parse().unwrap();
        let words: Vec<&str> = b.iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
        println!("query {words:?} ES top doc {d} score {es_score} | ours top {:?}", &me[..1]);
        println!("  our view of doc {d}: tokens {:?} len {} norm {} | doc_count {} avgdl {}",
                 rc.tokens[d as usize].iter().map(|w| rc.vocab[*w as usize].as_str()).collect::<Vec<_>>(),
                 rc.tokens[d as usize].len(), ri.norm[d as usize], ri.doc_count, ri.avgdl);
        for w in b {
            println!("  term {} df {} idf {}", rc.vocab[*w as usize], ri.df[*w as usize], ri.idf(*w));
        }
        println!("  ES explain: {}", serde_json::to_string(&h["_explanation"]).unwrap().chars().take(3000).collect::<String>());
        shown += 1;
        if shown == 2 {
            break;
        }
    }
}

// ------------------------------------------------------------------ production parity: same bodies, either server
fn with_source(mut v: Value) -> Value {
    v.as_object_mut().unwrap().remove("_source");
    v
}
fn post_to(a: &ureq::Agent, base: &str, path: &str, body: &str, ctype: &str) -> Value {
    match a.post(&format!("{base}{path}")).set("Content-Type", ctype).send_string(body) {
        Ok(r) => r.into_json().unwrap(),
        Err(ureq::Error::Status(c, r)) => panic!("{path} -> {c}: {}", r.into_string().unwrap()),
        Err(e) => panic!("{path}: {e}"),
    }
}
struct ProdBodies {
    classes: Vec<(&'static str, Vec<Value>)>,
    mix: Vec<Value>,
    bm25: Vec<Value>,
}
fn prod_bodies(rc: &realcat::RealCat) -> ProdBodies {
    let q = realcat::queries(rc, 1000);
    let pop = |qs: &Vec<Vec<Clause>>| qs.iter().map(|x| with_source(rq_pop(rc, x))).collect::<Vec<_>>();
    let classes = vec![
        ("top10+_source word & stock", pop(&q.word)),
        ("top10+_source word1 & word2 & stock", pop(&q.two)),
        ("top10+_source category & stock & rocket", pop(&q.cat)),
        ("top10+_source category & price window", pop(&q.cat_price)),
        ("top10+_source category & word & stock", pop(&q.cat_word)),
        ("top10+_source by price, word & stock", q.word.iter().map(|x| with_source(rq_price(rc, x))).collect()),
        ("brand facet top-20, word & stock", q.word.iter().map(|x| rq_facet(rc, x)).collect()),
        ("BM25 top10+_source", q.bm25.iter().map(|x| with_source(rq_bm25(rc, x))).collect()),
    ];
    let mix = classes[..5].iter().flat_map(|c| c.1.clone()).collect();
    let bm25 = classes[7].1.clone();
    ProdBodies { classes, mix, bm25 }
}
fn hit_ids(r: &Value) -> Vec<String> {
    r["hits"]["hits"].as_array().unwrap().iter().map(|h| h["_id"].as_str().unwrap().to_string()).collect()
}

/// Both servers up: same bodies, compare ids, scores, facet counts and _source.
fn prodcheck(path: &str) {
    let rc = realcat::load(path);
    let pb = prod_bodies(&rc);
    let a = agent();
    let (es, me) = ("http://127.0.0.1:9200", "http://127.0.0.1:9400");
    for (name, bodies) in &pb.classes {
        let (mut same, mut n, mut src_same) = (0, 0, 0);
        for b in bodies.iter().step_by(5) {
            let s = b.to_string();
            let (x, y) = (post_to(&a, es, "/real/_search", &s, "application/json"), post_to(&a, me, "/real/_search", &s, "application/json"));
            n += 1;
            if name.starts_with("brand") {
                let c = |r: &Value| r["aggregations"]["brands"]["buckets"].as_array().unwrap().iter().map(|b| b["doc_count"].as_u64().unwrap()).collect::<Vec<_>>();
                same += (c(&x) == c(&y)) as u32;
            } else if name.starts_with("BM25") {
                let sc = |r: &Value| r["hits"]["hits"].as_array().unwrap().iter().map(|h| h["_score"].as_f64().unwrap()).collect::<Vec<_>>();
                let (p, q) = (sc(&x), sc(&y));
                same += (p.len() == q.len() && p.iter().zip(&q).all(|(u, v)| (u - v).abs() <= 1e-5 * u.abs().max(1.0))) as u32;
            } else {
                same += (hit_ids(&x) == hit_ids(&y)) as u32;
            }
            if let (Some(h1), Some(h2)) = (x["hits"]["hits"].get(0), y["hits"]["hits"].get(0)) {
                if h1["_id"] == h2["_id"] {
                    src_same += (h1["_source"] == h2["_source"]) as u32;
                }
            }
        }
        println!("  {name:<40} same {same}/{n}{}", if name.starts_with("brand") { "" } else { " (BM25: scores; others: ids)" });
        let _ = src_same;
    }
    let mut src_eq = 0;
    for b in pb.mix.iter().step_by(25) {
        let s = b.to_string();
        let (x, y) = (post_to(&a, es, "/real/_search", &s, "application/json"), post_to(&a, me, "/real/_search", &s, "application/json"));
        src_eq += (x["hits"]["hits"] .as_array().unwrap().iter().map(|h| &h["_source"]).collect::<Vec<_>>()
            == y["hits"]["hits"].as_array().unwrap().iter().map(|h| &h["_source"]).collect::<Vec<_>>()) as u32;
    }
    println!("  _source of all 10 hits identical on {src_eq}/{} filter queries", pb.mix.len().div_ceil(25));
}

fn prodbench(base: &str, path: &str) {
    let rc = realcat::load(path);
    let pb = prod_bodies(&rc);
    let n = rc.n as u64;
    drop(rc);
    let a = agent();
    let srch = |a: &ureq::Agent, b: &Value| post_to(a, base, "/real/_search", &b.to_string(), "application/json");
    // warm-up: one full pass over every body
    for (_, bodies) in &pb.classes {
        for b in bodies {
            srch(&a, b);
        }
    }
    println!("[{base}] single client, 1000 queries per class (after a warm-up pass)");
    for (name, bodies) in &pb.classes {
        let lat: Vec<f64> = bodies.iter().map(|b| { let t = Instant::now(); srch(&a, b); t.elapsed().as_secs_f64() * 1e3 }).collect();
        let (p50, p99, _) = stats(lat);
        println!("  {name:<40} p50 {p50:7.3} p99 {p99:7.3} ms");
    }
    let load = |bodies: &Vec<Value>, threads: usize| -> (f64, f64, f64) {
        let stop = AtomicBool::new(false);
        let done = AtomicU64::new(0);
        let lats = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads).map(|t| {
                let (stop, done) = (&stop, &done);
                s.spawn(move || {
                    let a = agent();
                    let mut l = Vec::new();
                    let mut j = t * 997;
                    while !stop.load(Relaxed) {
                        let t0 = Instant::now();
                        srch(&a, &bodies[j % bodies.len()]);
                        l.push(t0.elapsed().as_secs_f64() * 1e3);
                        done.fetch_add(1, Relaxed);
                        j += 1;
                    }
                    l
                })
            }).collect();
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Relaxed);
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
        });
        let (p50, p99, _) = stats(lats);
        (done.load(Relaxed) as f64 / 20.0, p50, p99)
    };
    for (name, bodies) in [("filter top-k mix +_source", &pb.mix), ("BM25 +_source", &pb.bm25)] {
        for th in [4usize, 16] {
            let (qps, p50, p99) = load(bodies, th);
            println!("  throughput {name:<26} {th:2} clients: {qps:7.0} QPS, p50 {p50:.2} p99 {p99:.2} ms");
        }
    }
    // durable bulk updates: 4 threads x 1000 docs per request, stock and price alternating
    let bulk_body = |seed: u64| -> String {
        let mut body = String::new();
        for i in 0..1000u64 {
            let d = mix(seed + i) % n;
            if i % 2 == 0 {
                body.push_str(&format!("{{\"update\":{{\"_id\":\"{d}\"}}}}\n{{\"doc\":{{\"stock\":{}}}}}\n", (seed + i) % 4 != 0));
            } else {
                body.push_str(&format!("{{\"update\":{{\"_id\":\"{d}\"}}}}\n{{\"doc\":{{\"price\":{}}}}}\n", 1000 + (mix(seed + i + 7) % 100_000)));
            }
        }
        body
    };
    let stop = AtomicBool::new(false);
    let done = AtomicU64::new(0);
    std::thread::scope(|s| {
        for t in 0..4u64 {
            let (stop, done) = (&stop, &done);
            s.spawn(move || {
                let a = agent();
                let mut seed = t << 40;
                while !stop.load(Relaxed) {
                    let r = post_to(&a, base, "/real/_bulk", &bulk_body(seed), "application/x-ndjson");
                    assert!(!r["errors"].as_bool().unwrap());
                    seed += 1000;
                    done.fetch_add(1000, Relaxed);
                }
            });
        }
        std::thread::sleep(Duration::from_secs(20));
        stop.store(true, Relaxed);
    });
    println!("  durable bulk updates (fsync per request), 4 clients x 1000 docs: {:.0} docs/s", done.load(Relaxed) as f64 / 20.0);
    let stop = AtomicBool::new(false);
    let lat = std::thread::scope(|s| {
        for t in 0..2u64 {
            let stop = &stop;
            s.spawn(move || {
                let a = agent();
                let mut seed = (t + 9) << 40;
                while !stop.load(Relaxed) {
                    post_to(&a, base, "/real/_bulk", &bulk_body(seed), "application/x-ndjson");
                    seed += 1000;
                }
            });
        }
        let lat: Vec<f64> = pb.mix.iter().step_by(2).map(|b| { let t = Instant::now(); srch(&a, b); t.elapsed().as_secs_f64() * 1e3 }).collect();
        stop.store(true, Relaxed);
        stats(lat)
    });
    println!("  filter top-k +_source latency during 2-client bulk updates: p50 {:.2} p99 {:.2} max {:.2} ms", lat.0, lat.1, lat.2);
}

// ------------------------------------------------------------------ Vespa: same data, same query classes
const VESPA: &str = "http://127.0.0.1:8080";

/// Feed file (puts) and an update file (partial updates, same pattern as the ES/engine bulk test).
fn vespafeed(path: &str, out_dir: &str) {
    let rc = realcat::load(path);
    let dir = std::path::Path::new(out_dir);
    let mut w = std::io::BufWriter::new(std::fs::File::create(dir.join("feed.jsonl")).unwrap());
    for d in 0..rc.n as usize {
        let title: Vec<&str> = rc.tokens[d].iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
        let doc = json!({"put": format!("id:real:product::{d}"), "fields": {"pop": d, "cat": rc.cat[d].to_string(),
            "brand": rc.brand[d].to_string(), "price": rc.price[d], "stock": unif(d as u64, 6) < 0.9,
            "rocket": unif(d as u64, 7) < 0.4, "title_tok": title.join(" ")}});
        std::io::Write::write_all(&mut w, format!("{doc}\n").as_bytes()).unwrap();
    }
    let n = rc.n as u64;
    let mut u = std::io::BufWriter::new(std::fs::File::create(dir.join("updates.jsonl")).unwrap());
    for i in 0..2_000_000u64 {
        let d = mix((7u64 << 40) + i) % n;
        let f = if i % 2 == 0 { json!({"stock": {"assign": i % 4 != 0}}) } else { json!({"price": {"assign": 1000 + mix(i + 7) % 100_000}}) };
        std::io::Write::write_all(&mut u, format!("{}\n", json!({"update": format!("id:real:product::{d}"), "fields": f})).as_bytes()).unwrap();
    }
    println!("wrote feed.jsonl ({} docs) and updates.jsonl (2,000,000 partial updates)", rc.n);
}

fn vwhere(rc: &realcat::RealCat, q: &[Clause]) -> String {
    q.iter()
        .map(|c| match c {
            Clause::Cat(x) => format!("cat contains \"{x}\""),
            Clause::Term(x) => format!("title_tok contains \"{}\"", rc.vocab[*x as usize]),
            Clause::Stock => "stock = true".into(),
            Clause::Rocket => "rocket = true".into(),
            Clause::Buckets(lo, hi) if *hi >= N_BUCKET => format!("price >= {}", edge(*lo)),
            Clause::Buckets(lo, hi) => format!("(price >= {} and price < {})", edge(*lo), edge(*hi)),
        })
        .collect::<Vec<_>>()
        .join(" and ")
}
fn vq_pop(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"yql": format!("select * from product where {} order by pop asc", vwhere(rc, q)), "hits": K, "ranking": "unranked", "timeout": "20s"})
}
fn vq_price(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"yql": format!("select * from product where {} order by price asc, pop asc", vwhere(rc, q)), "hits": K, "ranking": "unranked", "timeout": "20s"})
}
fn vq_facet(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"yql": format!("select * from product where {} limit 0 | all(group(brand) max(20) order(-count()) each(output(count())))", vwhere(rc, q)),
           "hits": 0, "ranking": "unranked", "timeout": "20s"})
}
fn vq_bm25(rc: &realcat::RealCat, ws: &[u32]) -> Value {
    let or: Vec<String> = ws.iter().map(|w| format!("title_tok contains \"{}\"", rc.vocab[*w as usize])).collect();
    json!({"yql": format!("select * from product where ({}) and stock = true", or.join(" or ")), "hits": K, "ranking": "bm25", "timeout": "20s"})
}
/// Vespa-tuned variants: early termination via match-phase on pop, exact grouping, weakAnd BM25.
fn vq_pop_mp(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"yql": format!("select * from product where {}", vwhere(rc, q)), "hits": K, "ranking": "popfirst", "timeout": "20s"})
}
fn vq_facet_exact(rc: &realcat::RealCat, q: &[Clause]) -> Value {
    json!({"yql": format!("select * from product where {} limit 0 | all(group(brand) max(20) precision(1000000) order(-count()) each(output(count())))", vwhere(rc, q)),
           "hits": 0, "ranking": "unranked", "timeout": "20s"})
}
fn vq_bm25_wand(rc: &realcat::RealCat, ws: &[u32]) -> Value {
    let t: Vec<String> = ws.iter().map(|w| format!("title_tok contains \"{}\"", rc.vocab[*w as usize])).collect();
    json!({"yql": format!("select * from product where ({{targetHits:10}}weakAnd({})) and stock = true", t.join(", ")), "hits": K, "ranking": "bm25", "timeout": "20s"})
}
fn vsearch(a: &ureq::Agent, body: &Value) -> Value {
    post_to(a, VESPA, "/search/", &body.to_string(), "application/json")
}
fn vhits(r: &Value) -> Vec<(u32, f64)> {
    r["root"]["children"].as_array().map(|v| v.iter().filter_map(|h| {
        let id = h["id"].as_str()?;
        Some((id.rsplit("::").next()?.parse().ok()?, h["relevance"].as_f64().unwrap_or(0.0)))
    }).collect()).unwrap_or_default()
}
fn vgroups(r: &Value) -> Vec<(u32, u32)> {
    fn walk(v: &Value, out: &mut Vec<(u32, u32)>) {
        if let (Some(val), Some(c)) = (v.get("value").and_then(|x| x.as_str()), v["fields"].get("count()").and_then(|x| x.as_u64())) {
            if let Ok(b) = val.parse() {
                out.push((b, c as u32));
            }
        }
        if let Some(ch) = v.get("children").and_then(|c| c.as_array()) {
            for c in ch {
                walk(c, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(&r["root"], &mut out);
    out
}

fn vespabench(path: &str) {
    let rc = realcat::load(path);
    let ri = realcat::build(&rc, 16);
    let q = realcat::queries(&rc, 1000);
    let a = agent();
    let classes: Vec<(&str, &Vec<Vec<Clause>>)> = vec![("word & stock", &q.word), ("word1 & word2 & stock", &q.two), ("category & stock & rocket", &q.cat),
                                                         ("category & price window", &q.cat_price), ("category & word & stock", &q.cat_word)];
    // correctness vs the bitmap engine (which matched ES exactly)
    let (mut same, mut tot) = (0, 0);
    for (_, qs) in &classes {
        for x in qs.iter().step_by(10) {
            tot += 1;
            same += (vhits(&vsearch(&a, &vq_pop(&rc, x))).iter().map(|h| h.0).collect::<Vec<_>>() == ri.ix.topk_pop(x, K)) as u32;
        }
    }
    for x in q.word.iter().step_by(10) {
        tot += 1;
        same += (vhits(&vsearch(&a, &vq_price(&rc, x))).iter().map(|h| h.0).collect::<Vec<_>>() == ri.ix.topk_price(x).iter().map(|y| y.1).collect::<Vec<_>>()) as u32;
    }
    println!("Vespa vs engine, filters + price sort: identical top-10 on {same}/{tot}");
    let (mut same_mp, mut tot_mp) = (0, 0);
    for (_, qs) in &classes {
        for x in qs.iter().step_by(10) {
            tot_mp += 1;
            same_mp += (vhits(&vsearch(&a, &vq_pop_mp(&rc, x))).iter().map(|h| h.0).collect::<Vec<_>>() == ri.ix.topk_pop(x, K)) as u32;
        }
    }
    println!("Vespa match-phase (tuned) vs engine, filters: identical top-10 on {same_mp}/{tot_mp}");
    let mut fx = 0;
    for x in q.word.iter().step_by(10) {
        let v = vgroups(&vsearch(&a, &vq_facet_exact(&rc, x)));
        fx += (v.iter().map(|g| g.1).collect::<Vec<_>>() == ri.facet_exact(x).iter().map(|g| g.1).collect::<Vec<_>>()) as u32;
    }
    println!("Vespa grouping with precision(1e6) vs exact facets: identical top-20 counts on {fx}/100");
    let mut ovw = 0.0;
    for b in q.bm25.iter().step_by(5) {
        let v = vhits(&vsearch(&a, &vq_bm25_wand(&rc, b)));
        let me = ri.bm25_topk(b, K);
        ovw += v.iter().filter(|h| me.iter().any(|m| m.1 == h.0)).count() as f64 / me.len().clamp(1, 10) as f64;
    }
    println!("Vespa weakAnd(targetHits 10) bm25: top-10 overlap with exact BM25 {:.1}%", 100.0 * ovw / 200.0);
    let mut fs = 0;
    for x in q.word.iter().step_by(10) {
        let v = vgroups(&vsearch(&a, &vq_facet(&rc, x)));
        fs += (v.iter().map(|g| g.1).collect::<Vec<_>>() == ri.facet_exact(x).iter().map(|g| g.1).collect::<Vec<_>>()) as u32;
    }
    println!("Vespa grouping vs exact facets: identical top-20 counts on {fs}/100");

    let (mut ov, mut ratio_sd) = (0.0, Vec::new());
    for b in q.bm25.iter().step_by(5) {
        let v = vhits(&vsearch(&a, &vq_bm25(&rc, b)));
        let me = ri.bm25_topk(b, K);
        ov += v.iter().filter(|h| me.iter().any(|m| m.1 == h.0)).count() as f64 / me.len().clamp(1, 10) as f64;
        if let (Some(x), Some(y)) = (v.first(), me.first()) {
            ratio_sd.push(x.1 / y.0 as f64);
        }
    }
    let mean = ratio_sd.iter().sum::<f64>() / ratio_sd.len() as f64;
    println!("Vespa bm25 vs Lucene-identical BM25: top-10 overlap {:.1}% over 200 queries; top score ratio mean {mean:.3}", 100.0 * ov / 200.0);

    // latency (single client, after a warm-up pass) and throughput
    let mut all: Vec<(String, Vec<Value>)> = classes.iter().map(|(n, qs)| (format!("top10 {n}"), qs.iter().map(|x| vq_pop(&rc, x)).collect())).collect();
    all.push(("top10 by price, word & stock".into(), q.word.iter().map(|x| vq_price(&rc, x)).collect()));
    all.push(("brand facet top-20 (grouping)".into(), q.word.iter().map(|x| vq_facet(&rc, x)).collect()));
    all.push(("BM25 top10 (exact OR)".into(), q.bm25.iter().map(|x| vq_bm25(&rc, x)).collect()));
    let tuned: Vec<(String, Vec<Value>)> = vec![
        ("tuned: top10 word & stock (match-phase)".into(), q.word.iter().map(|x| vq_pop_mp(&rc, x)).collect()),
        ("tuned: top10 category&stock&rocket (mp)".into(), q.cat.iter().map(|x| vq_pop_mp(&rc, x)).collect()),
        ("tuned: top10 category&word&stock (mp)".into(), q.cat_word.iter().map(|x| vq_pop_mp(&rc, x)).collect()),
        ("tuned: brand facet exact (precision)".into(), q.word.iter().map(|x| vq_facet_exact(&rc, x)).collect()),
        ("tuned: BM25 weakAnd(targetHits 10)".into(), q.bm25.iter().map(|x| vq_bm25_wand(&rc, x)).collect()),
    ];
    all.extend(tuned);
    for (_, bodies) in &all {
        for b in bodies {
            vsearch(&a, b);
        }
    }
    println!("Vespa single client (with all summary fields), 1000 queries per class after warm-up");
    for (name, bodies) in &all {
        let lat: Vec<f64> = bodies.iter().map(|b| { let t = Instant::now(); vsearch(&a, b); t.elapsed().as_secs_f64() * 1e3 }).collect();
        let (p50, p99, _) = stats(lat);
        println!("  {name:<40} p50 {p50:7.3} p99 {p99:7.3} ms");
    }
    let mixb: Vec<Value> = all[..5].iter().flat_map(|c| c.1.clone()).collect();
    let bm25b = all[7].1.clone();
    let mix_mp: Vec<Value> = [&q.word, &q.two, &q.cat, &q.cat_price, &q.cat_word].iter().flat_map(|qs| qs.iter().map(|x| vq_pop_mp(&rc, x))).collect();
    let bm25w = all[12].1.clone();
    for (name, bodies) in [("filter top-k mix", &mixb), ("BM25", &bm25b), ("tuned filter mix (mp)", &mix_mp), ("tuned BM25 weakAnd", &bm25w)] {
        for th in [4usize, 16] {
            let stop = AtomicBool::new(false);
            let done = AtomicU64::new(0);
            let lats = std::thread::scope(|s| {
                let hs: Vec<_> = (0..th).map(|t| {
                    let (stop, done) = (&stop, &done);
                    s.spawn(move || {
                        let a = agent();
                        let mut l = Vec::new();
                        let mut j = t * 997;
                        while !stop.load(Relaxed) {
                            let t0 = Instant::now();
                            vsearch(&a, &bodies[j % bodies.len()]);
                            l.push(t0.elapsed().as_secs_f64() * 1e3);
                            done.fetch_add(1, Relaxed);
                            j += 1;
                        }
                        l
                    })
                }).collect();
                std::thread::sleep(Duration::from_secs(20));
                stop.store(true, Relaxed);
                hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
            });
            let (p50, p99, _) = stats(lats);
            println!("  throughput {name:<18} {th:2} clients: {:7.0} QPS, p50 {p50:.2} p99 {p99:.2} ms", done.load(Relaxed) as f64 / 20.0);
        }
    }
}

/// Query latency while partial updates are fed (run vespa-feed-client concurrently).
fn vespalat_during(path: &str, secs: u64) {
    let rc = realcat::load(path);
    let q = realcat::queries(&rc, 1000);
    let a = agent();
    let bodies: Vec<Value> = q.word.iter().chain(&q.two).chain(&q.cat).chain(&q.cat_price).chain(&q.cat_word).map(|x| vq_pop(&rc, x)).collect();
    let t0 = Instant::now();
    let mut lat = Vec::new();
    let mut j = 0;
    while t0.elapsed() < Duration::from_secs(secs) {
        let t = Instant::now();
        vsearch(&a, &bodies[j % bodies.len()]);
        lat.push(t.elapsed().as_secs_f64() * 1e3);
        j += 1;
    }
    let (p50, p99, mx) = stats(lat);
    println!("Vespa filter top-k latency during feed: p50 {p50:.2} p99 {p99:.2} max {mx:.2} ms ({j} queries)");
}

// ------------------------------------------------------------------ debiased comparison
fn cpu_secs() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let f: Vec<&str> = s.rsplit(')').next().unwrap().split_whitespace().collect();
    (f[11].parse::<f64>().unwrap() + f[12].parse::<f64>().unwrap()) / 100.0 // utime + stime, 100 Hz
}
/// CPU seconds of the server process given by env SPID (utime + stime incl. all threads).
fn spid_cpu() -> Option<f64> {
    let pid = std::env::var("SPID").ok()?;
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let f: Vec<&str> = s.rsplit(')').next()?.split_whitespace().collect();
    Some((f[11].parse::<f64>().ok()? + f[12].parse::<f64>().ok()?) / 100.0)
}
fn rq_bm25f(rc: &realcat::RealCat, ws: &[u32], filters: &[Clause]) -> Value {
    let text: Vec<&str> = ws.iter().map(|w| rc.vocab[*w as usize].as_str()).collect();
    json!({"size": K, "track_total_hits": false,
           "query": {"bool": {"must": {"match": {"title_tok": text.join(" ")}}, "filter": filters.iter().map(|c| rfilt(rc, c)).collect::<Vec<_>>()}}})
}
fn vq_bm25f(rc: &realcat::RealCat, ws: &[u32], filters: &[Clause]) -> Value {
    let t: Vec<String> = ws.iter().map(|w| format!("title_tok contains \"{}\"", rc.vocab[*w as usize])).collect();
    json!({"yql": format!("select * from product where ({{targetHits:10}}weakAnd({})) and {}", t.join(", "), vwhere(rc, filters)),
           "hits": K, "ranking": "bm25", "timeout": "20s"})
}

#[derive(Clone, Copy, PartialEq)]
enum Sys { Es, Ours, Vespa }
impl Sys {
    fn base(self) -> &'static str {
        match self { Sys::Es => "http://127.0.0.1:9200", Sys::Ours => "http://127.0.0.1:9400", Sys::Vespa => VESPA }
    }
    fn path(self) -> &'static str {
        if self == Sys::Vespa { "/search/" } else { "/real/_search" }
    }
}
struct Wl {
    name: &'static str,
    filters: Vec<Vec<Clause>>, // word, two words, category & word (all & stock), sorted by popularity
    bm25: Vec<(Vec<u32>, Vec<Clause>)>,
    facet: Vec<Vec<Clause>>,
}
fn workloads(rc: &realcat::RealCat) -> Vec<Wl> {
    let p = realcat::queries(rc, 300);
    let mut v = vec![Wl {
        name: "popular-weighted (the set used for tuning)",
        filters: p.word.iter().chain(&p.two).chain(&p.cat_word).cloned().collect(),
        bm25: p.bm25.iter().map(|b| (b.clone(), vec![Clause::Stock])).collect(),
        facet: p.word.clone(),
    }];
    for (name, tail) in [("uniform held-out", false), ("long-tail held-out", true)] {
        let h = realcat::heldout(rc, 300, tail);
        v.push(Wl {
            name,
            filters: h.word.iter().chain(&h.two).chain(&h.cat_word).cloned().collect(),
            bm25: h.bm25.iter().map(|b| (b.clone(), vec![Clause::Stock]))
                .chain(h.bm25_cat.iter().map(|(b, c)| (b.clone(), vec![Clause::Cat(*c), Clause::Stock]))).collect(),
            facet: h.word.clone(),
        });
    }
    v
}
fn body_filter(sys: Sys, rc: &realcat::RealCat, q: &[Clause]) -> Value {
    if sys == Sys::Vespa { vq_pop_mp(rc, q) } else { with_source(rq_pop(rc, q)) }
}
fn body_bm25(sys: Sys, rc: &realcat::RealCat, b: &(Vec<u32>, Vec<Clause>)) -> Value {
    if sys == Sys::Vespa { vq_bm25f(rc, &b.0, &b.1) } else { rq_bm25f(rc, &b.0, &b.1) }
}
fn body_facet(sys: Sys, rc: &realcat::RealCat, q: &[Clause]) -> Value {
    if sys == Sys::Vespa { vq_facet_exact(rc, q) } else { rq_facet(rc, q) }
}
fn s_ids(sys: Sys, r: &Value) -> Vec<u32> {
    if sys == Sys::Vespa { vhits(r).into_iter().map(|h| h.0).collect() } else { hit_ids(r).into_iter().map(|x| x.parse().unwrap()).collect() }
}
fn s_scores(sys: Sys, r: &Value) -> Vec<(f64, u32)> {
    if sys == Sys::Vespa {
        vhits(r).into_iter().map(|(d, s)| (s, d)).collect()
    } else {
        r["hits"]["hits"].as_array().unwrap().iter().map(|h| (h["_score"].as_f64().unwrap(), h["_id"].as_str().unwrap().parse().unwrap())).collect()
    }
}
fn s_facet(sys: Sys, r: &Value) -> Vec<u32> {
    if sys == Sys::Vespa { vgroups(r).into_iter().map(|g| g.1).collect() }
    else { r["aggregations"]["brands"]["buckets"].as_array().unwrap().iter().map(|b| b["doc_count"].as_u64().unwrap() as u32).collect() }
}
fn s_search(a: &ureq::Agent, sys: Sys, b: &Value) -> Value {
    post_to(a, sys.base(), sys.path(), &b.to_string(), "application/json")
}

/// All three servers up (ES with 1 shard): compare results on every workload.
fn faircheck(path: &str) {
    let rc = realcat::load(path);
    let a = agent();
    let vespa_up = a.get(&format!("{VESPA}/state/v1/health")).call().is_ok();
    for wl in workloads(&rc) {
        println!("[{}]", wl.name);
        let (mut f_es, mut f_v, mut n) = (0, 0, 0);
        for q in wl.filters.iter().step_by(3) {
            let me = s_ids(Sys::Ours, &s_search(&a, Sys::Ours, &body_filter(Sys::Ours, &rc, q)));
            f_es += (s_ids(Sys::Es, &s_search(&a, Sys::Es, &body_filter(Sys::Es, &rc, q))) == me) as u32;
            if vespa_up {
                f_v += (s_ids(Sys::Vespa, &s_search(&a, Sys::Vespa, &body_filter(Sys::Vespa, &rc, q))) == me) as u32;
            }
            n += 1;
        }
        println!("  filters top-10 identical: ES {f_es}/{n}, Vespa {f_v}/{n}");
        let (mut b_es, mut b_docs, mut b_v, mut nb) = (0, 0, 0.0, 0);
        for b in wl.bm25.iter().step_by(3) {
            let me = s_scores(Sys::Ours, &s_search(&a, Sys::Ours, &body_bm25(Sys::Ours, &rc, b)));
            let es = s_scores(Sys::Es, &s_search(&a, Sys::Es, &body_bm25(Sys::Es, &rc, b)));
            nb += 1;
            if es.len() == me.len() && es.iter().zip(&me).all(|(x, y)| (x.0 - y.0).abs() <= 1e-5 * x.0.abs().max(1.0)) {
                b_es += 1;
            }
            let cut = me.last().map(|x| x.0).unwrap_or(0.0);
            let above = |v: &[(f64, u32)]| { let mut s: Vec<u32> = v.iter().filter(|x| x.0 > cut * (1.0 + 1e-6)).map(|x| x.1).collect(); s.sort(); s };
            b_docs += (above(&es) == above(&me)) as u32;
            if vespa_up {
                let v = s_scores(Sys::Vespa, &s_search(&a, Sys::Vespa, &body_bm25(Sys::Vespa, &rc, b)));
                b_v += v.iter().filter(|x| me.iter().any(|y| y.1 == x.1)).count() as f64 / me.len().clamp(1, 10) as f64;
            }
        }
        println!("  BM25(+filters): ES identical scores {b_es}/{nb}, identical docs above 10th score {b_docs}/{nb}; Vespa weakAnd top-10 overlap {:.1}%", 100.0 * b_v / nb as f64);
        let (mut fx, mut fes, mut fv, mut nf) = (0, 0, 0, 0);
        for q in wl.facet.iter().step_by(3) {
            let fast = s_facet(Sys::Ours, &s_search(&a, Sys::Ours, &body_facet(Sys::Ours, &rc, q)));
            let exact = s_facet(Sys::Ours, &post_to(&a, Sys::Ours.base(), "/real/_search?exact=1", &body_facet(Sys::Ours, &rc, q).to_string(), "application/json"));
            fx += (fast == exact) as u32;
            fes += (s_facet(Sys::Es, &s_search(&a, Sys::Es, &body_facet(Sys::Es, &rc, q))) == exact) as u32;
            if vespa_up {
                fv += (s_facet(Sys::Vespa, &s_search(&a, Sys::Vespa, &body_facet(Sys::Vespa, &rc, q))) == exact) as u32;
            }
            nf += 1;
        }
        println!("  facets (exact counts): ours fast path == exact {fx}/{nf}; ES (1 shard) == exact {fes}/{nf}; Vespa precision grouping == exact {fv}/{nf}");
    }
}

fn fair(sys_name: &str, path: &str, filters_only: bool) {
    let sys = match sys_name { "es" => Sys::Es, "ours" => Sys::Ours, "vespa" => Sys::Vespa, _ => panic!() };
    let rc = realcat::load(path);
    let wls = workloads(&rc);
    let a = agent();
    println!("== {sys_name} ({})", sys.base());
    for wl in &wls {
        let fb: Vec<Value> = wl.filters.iter().map(|q| body_filter(sys, &rc, q)).collect();
        let bb: Vec<Value> = wl.bm25.iter().map(|b| body_bm25(sys, &rc, b)).collect();
        let cb: Vec<Value> = wl.facet.iter().map(|q| body_facet(sys, &rc, q)).collect();
        let mut sets: Vec<(&str, &Vec<Value>)> = vec![("filters top-10 by popularity", &fb), ("brand facet top-20", &cb)];
        if !filters_only {
            sets.push(("BM25 (+stock / +category&stock)", &bb));
        }
        for (_, bodies) in &sets {
            for b in bodies.iter() {
                s_search(&a, sys, b); // warm-up pass
            }
        }
        println!("[{}]", wl.name);
        for (name, bodies) in &sets {
            let lat: Vec<f64> = bodies.iter().map(|b| { let t = Instant::now(); s_search(&a, sys, b); t.elapsed().as_secs_f64() * 1e3 }).collect();
            let (p50, p99, _) = stats(lat);
            print!("  {name:<34} single p50 {p50:7.3} p99 {p99:7.3} ms");
            if *name != "brand facet top-20" {
                let stop = AtomicBool::new(false);
                let done = AtomicU64::new(0);
                let (c0, t0, srv0) = (cpu_secs(), Instant::now(), spid_cpu());
                let lats = std::thread::scope(|s| {
                    let hs: Vec<_> = (0..16).map(|t| {
                        let (stop, done) = (&stop, &done);
                        s.spawn(move || {
                            let a = agent();
                            let mut l = Vec::new();
                            let mut j = t * 97;
                            while !stop.load(Relaxed) {
                                let t1 = Instant::now();
                                s_search(&a, sys, &bodies[j % bodies.len()]);
                                l.push(t1.elapsed().as_secs_f64() * 1e3);
                                done.fetch_add(1, Relaxed);
                                j += 1;
                            }
                            l
                        })
                    }).collect();
                    std::thread::sleep(Duration::from_secs(10));
                    stop.store(true, Relaxed);
                    hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
                });
                let wall = t0.elapsed().as_secs_f64();
                let (q50, q99, _) = stats(lats);
                print!(" | 16 clients {:7.0} QPS p50 {q50:.2} p99 {q99:.2} ms (client core busy {:.0}%)",
                       done.load(Relaxed) as f64 / wall, 100.0 * (cpu_secs() - c0) / wall);
                if let Some(s1) = spid_cpu() {
                    print!(" (server {:.0}% of 3 cores)", 100.0 * (s1 - srv0.unwrap_or(0.0)) / wall / 3.0);
                }
            }
            println!();
        }
    }
}

/// Reset every doc touched by updates.jsonl back to its original stock and price (Vespa feed file).
fn vreset(path: &str) {
    let rc = realcat::load(path);
    let n = rc.n as u64;
    let mut touched: Vec<u64> = (0..2_000_000u64).map(|i| mix((7u64 << 40) + i) % n).collect();
    touched.sort_unstable();
    touched.dedup();
    let mut u = std::io::BufWriter::new(std::fs::File::create("vespa/reset.jsonl").unwrap());
    for &d in &touched {
        let line = json!({"update": format!("id:real:product::{d}"), "fields": {"stock": {"assign": unif(d, 6) < 0.9}, "price": {"assign": rc.price[d as usize]}}});
        std::io::Write::write_all(&mut u, format!("{line}\n").as_bytes()).unwrap();
    }
    println!("wrote vespa/reset.jsonl ({} docs)", touched.len());
}

/// Apply the correlated out-of-stock state: bulk to ES / ours (same bodies), feed file for Vespa.
fn oos(sys_name: &str, path: &str) {
    let rc = realcat::load(path);
    let n = rc.n as u64;
    if sys_name == "vespa" {
        let mut u = std::io::BufWriter::new(std::fs::File::create("vespa/oos.jsonl").unwrap());
        for d in 0..rc.n as usize {
            let line = json!({"update": format!("id:real:product::{d}"), "fields": {"stock": {"assign": realcat::oos_stock(&rc, d)}}});
            std::io::Write::write_all(&mut u, format!("{line}\n").as_bytes()).unwrap();
        }
        println!("wrote vespa/oos.jsonl");
        return;
    }
    let base = if sys_name == "es" { "http://127.0.0.1:9200" } else { "http://127.0.0.1:9400" };
    let next = AtomicU64::new(0);
    let t = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..4 {
            s.spawn(|| {
                let a = agent();
                loop {
                    let lo = next.fetch_add(5000, Relaxed);
                    if lo >= n {
                        break;
                    }
                    let mut body = String::new();
                    for d in lo..(lo + 5000).min(n) {
                        body.push_str(&format!("{{\"update\":{{\"_id\":\"{d}\"}}}}\n{{\"doc\":{{\"stock\":{}}}}}\n", realcat::oos_stock(&rc, d as usize)));
                    }
                    let r = post_to(&a, base, "/real/_bulk", &body, "application/x-ndjson");
                    assert!(!r["errors"].as_bool().unwrap());
                }
            });
        }
    });
    if sys_name == "es" {
        post_to(&agent(), base, "/real/_refresh", "", "application/json");
    }
    let oos_n = (0..rc.n as usize).filter(|&d| !realcat::oos_stock(&rc, d)).count();
    println!("{sys_name}: set stock on {n} docs in {:.1}s; out of stock now {oos_n} ({:.1}%)", t.elapsed().as_secs_f64(), 100.0 * oos_n as f64 / n as f64);
}

/// ES BM25 throughput with many clients (dfs off: per-shard stats, like a default ES query).
fn realbm25qps(path: &str) {
    let rc = realcat::load(path);
    let q = realcat::queries(&rc, 1000);
    let bodies: Vec<Value> = q.bm25.iter().map(|b| rq_bm25(&rc, b)).collect();
    for threads in [4usize, 16] {
        let stop = AtomicBool::new(false);
        let done = AtomicU64::new(0);
        let lats = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads).map(|t| {
                let (bodies, stop, done) = (&bodies, &stop, &done);
                s.spawn(move || {
                    let a = agent();
                    let mut l = Vec::new();
                    let mut j = t * 997;
                    while !stop.load(Relaxed) {
                        let t0 = Instant::now();
                        rsearch(&a, &bodies[j % bodies.len()], false);
                        l.push(t0.elapsed().as_secs_f64() * 1e3);
                        done.fetch_add(1, Relaxed);
                        j += 1;
                    }
                    l
                })
            }).collect();
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Relaxed);
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
        });
        let (p50, p99, _) = stats(lats);
        println!("ES BM25 throughput, {threads:2} client threads: {:.0} QPS, p50 {p50:.2} p99 {p99:.2} ms", done.load(Relaxed) as f64 / 20.0);
    }
}

/// BM25 over HTTP against the bitmap engine (real serve), same client and queries as realbm25qps.
fn realbm25ours(path: &str) {
    let rc = realcat::load(path);
    let q = realcat::queries(&rc, 1000);
    let bodies: Vec<String> = q.bm25.iter().map(|b| json!({"bm25": b}).to_string()).collect();
    let a = agent();
    println!("engine BM25 over HTTP, single client: {}", ours_measure(&a, &bodies));
    for threads in [4usize, 16] {
        let stop = AtomicBool::new(false);
        let done = AtomicU64::new(0);
        let lats = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads).map(|t| {
                let (bodies, stop, done) = (&bodies, &stop, &done);
                s.spawn(move || {
                    let a = agent();
                    let mut l = Vec::new();
                    let mut j = t * 997;
                    while !stop.load(Relaxed) {
                        let t0 = Instant::now();
                        ours_search(&a, &bodies[j % bodies.len()]);
                        l.push(t0.elapsed().as_secs_f64() * 1e3);
                        done.fetch_add(1, Relaxed);
                        j += 1;
                    }
                    l
                })
            }).collect();
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Relaxed);
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
        });
        let (p50, p99, _) = stats(lats);
        println!("engine BM25 throughput over HTTP, {threads:2} client threads: {:.0} QPS, p50 {p50:.2} p99 {p99:.2} ms", done.load(Relaxed) as f64 / 20.0);
    }
}

/// Same real top-k mix against the bitmap engine's HTTP server (real serve).
fn realours(path: &str) {
    let rc = realcat::load(path);
    let q = realcat::queries(&rc, 1000);
    let a = agent();
    let mixq: Vec<String> = q.word.iter().chain(&q.two).chain(&q.cat).chain(&q.cat_price).chain(&q.cat_word).map(|x| ours_body(x, "pop")).collect();
    for (name, qs) in [("word & stock", &q.word), ("word1 & word2 & stock", &q.two), ("category & stock & rocket", &q.cat),
                       ("category & stock & price window", &q.cat_price), ("category & word & stock", &q.cat_word)] {
        println!("  top10 pop {name:<32} {}", ours_measure(&a, &qs.iter().map(|x| ours_body(x, "pop")).collect::<Vec<_>>()));
    }
    for threads in [4usize, 16] {
        let stop = AtomicBool::new(false);
        let done = AtomicU64::new(0);
        let lats = std::thread::scope(|s| {
            let hs: Vec<_> = (0..threads).map(|t| {
                let (mix, stop, done) = (&mixq, &stop, &done);
                s.spawn(move || {
                    let a = agent();
                    let mut l = Vec::new();
                    let mut j = t * 997;
                    while !stop.load(Relaxed) {
                        let t0 = Instant::now();
                        ours_search(&a, &mix[j % mix.len()]);
                        l.push(t0.elapsed().as_secs_f64() * 1e3);
                        done.fetch_add(1, Relaxed);
                        j += 1;
                    }
                    l
                })
            }).collect();
            std::thread::sleep(Duration::from_secs(20));
            stop.store(true, Relaxed);
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect::<Vec<f64>>()
        });
        let (p50, p99, _) = stats(lats);
        println!("engine throughput real top-k mix over HTTP, {threads:2} client threads: {:.0} QPS, p50 {p50:.2} p99 {p99:.2} ms", done.load(Relaxed) as f64 / 20.0);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = args.get(2).cloned().unwrap_or_default();
    let n: u32 = arg.parse().unwrap_or(10_000_000);
    match args.get(1).map(String::as_str) {
        Some("load") => load(n),
        Some("bench") => bench(n),
        Some("ours") => ours(),
        Some("realload") => realload(&arg),
        Some("realbench") => realbench(&arg),
        Some("realours") => realours(&arg),
        Some("realbm25dbg") => realbm25dbg(&arg),
        Some("realbm25qps") => realbm25qps(&arg),
        Some("realbm25ours") => realbm25ours(&arg),
        Some("prodcheck") => prodcheck(&arg),
        Some("vespafeed") => vespafeed(&arg, &args[3]),
        Some("faircheck") => faircheck(&arg),
        Some("fair") => fair(&arg, &args[3], args.get(4).map(|x| x == "filters").unwrap_or(false)),
        Some("oos") => oos(&arg, &args[3]),
        Some("vreset") => vreset(&arg),
        Some("vespabench") => vespabench(&arg),
        Some("vespalat") => vespalat_during(&arg, args[3].parse().unwrap()),
        Some("prodbench") => prodbench(&arg, &args[3]),
        _ => panic!("usage: es load|bench|ours N | realload|realbench|realours FILE"),
    }
}
