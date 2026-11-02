//! Real product titles (Amazon Reviews 2023 metadata) in the bitmap engine.
//!   real bench FILE.tsv    correctness + latency for filters, BM25, facets, memory
//!   real serve FILE.tsv    HTTP API on 127.0.0.1:9400 (same as the synthetic server)
use bitmap_engine::realcat::{build as build_real, load, queries};
use bitmap_engine::*;
use rayon::prelude::*;
use std::time::Instant;

fn stats(mut v: Vec<f64>) -> String {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |x: f64| v[((v.len() - 1) as f64 * x).round() as usize];
    format!("p50 {:7.3} p99 {:7.3} max {:7.3} ms", p(0.5), p(0.99), v.last().unwrap())
}
fn time<Q, T>(qs: &[Q], f: impl Fn(&Q) -> T) -> String {
    stats(qs.iter().map(|q| { let t = Instant::now(); std::hint::black_box(f(q)); t.elapsed().as_secs_f64() * 1e3 }).collect())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mode, path) = (args[1].as_str(), args[2].as_str());
    let t = Instant::now();
    let rc = load(path);
    let ntok: usize = rc.tokens.iter().map(|t| t.len()).sum();
    println!(
        "real catalog: {} items, {ntok} title tokens ({:.1}/item), vocab {}, categories {}, brands {} | load {:.1}s",
        rc.n, ntok as f64 / rc.n as f64, rc.vocab.len(), rc.n_cat, rc.n_brand, t.elapsed().as_secs_f64()
    );
    let t = Instant::now();
    let ri = build_real(&rc, 16);
    let (bm, tm) = ri.ix.bitmap_mb();
    let brand_mb = ri.brands.iter().map(|b| b.bytes()).sum::<usize>() as f64 / 1048576.0;
    let tf2_n: usize = ri.tf2.iter().map(|m| m.values().map(|v| v.0.len()).sum::<usize>()).sum();
    let extra_mb = (ri.norm.len() + ri.df.len() * 5 + tf2_n * 5) as f64 / 1048576.0;
    println!(
        "index: filters+titles {bm:.0} MB (titles {tm:.0}) + brand postings {brand_mb:.0} MB + columns {:.0} MB + BM25 norms/df/tf {extra_mb:.0} MB | {} head terms with facet candidates | build {:.1}s | {}",
        ri.ix.column_mb(), ri.head_cands.len(), t.elapsed().as_secs_f64(), mem_status()
    );
    let bmx_mb = ri.bm.iter().map(|r| r.len()).sum::<usize>() as f64 / 1048576.0;
    println!("  block-max: {} terms x {} windows of {} docids = {bmx_mb:.0} MB", ri.bm.len(), ri.bm.first().map(|r| r.len()).unwrap_or(0), bitmap_engine::realcat::bm_w());
    let total = bm + brand_mb + ri.ix.column_mb() + extra_mb + bmx_mb;
    println!("  => {:.0} bytes per item", total * 1048576.0 / rc.n as f64);
    if mode == "serve" {
        drop(rc);
        serve_real(&ri);
        return;
    }
    if mode == "bm25dev" {
        bm25dev(&rc, &ri);
        return;
    }
    let q = queries(&rc, 1000);
    let ix = &ri.ix;

    // correctness
    for qq in q.word.iter().chain(&q.two).chain(&q.cat).chain(&q.cat_price).chain(&q.cat_word).step_by(5) {
        assert_eq!(ix.topk_pop(qq, K), ix.topk_pop_baseline(qq));
    }
    for qq in q.word.iter().step_by(20) {
        assert_eq!(ix.topk_price(qq), ix.topk_price_full(qq));
    }
    let mut bm_ok = 0;
    for b in &q.bm25 {
        let (a, e) = (ri.bm25_topk(b, K), ri.bm25_exhaustive(b, K));
        if a.iter().map(|x| x.1).collect::<Vec<_>>() == e.iter().map(|x| x.1).collect::<Vec<_>>() {
            bm_ok += 1;
        }
    }
    println!("correctness: top-k filters identical to baseline; BM25 MaxScore == exhaustive on {bm_ok}/1000 queries");

    println!("top-10 by popularity, single thread (baseline full AND | optimized)");
    for (name, qs) in [("word & stock", &q.word), ("word1 & word2 & stock", &q.two), ("category & stock & rocket", &q.cat),
                       ("category & stock & price window", &q.cat_price), ("category & word & stock", &q.cat_word)] {
        println!("  {name:<32} {} | {}", time(qs, |x| ix.topk_pop_baseline(x)), time(qs, |x| ix.topk_pop(x, K)));
    }
    println!("  top-10 by price, word & stock    full {} | bucket walk {}", time(&q.word, |x| ix.topk_price_full(x)), time(&q.word, |x| ix.topk_price(x)));
    println!("BM25 top-10, 2 words OR + in-stock filter (single thread)");
    if std::env::var("SLOW").is_ok() {
        let mut v: Vec<(f64, usize)> = q.bm25.iter().enumerate().map(|(i, b)| {
            let t = Instant::now(); std::hint::black_box(ri.bm25_topk(b, K)); (t.elapsed().as_secs_f64() * 1e3, i) }).collect();
        v.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        for (ms, i) in v.iter().take(6) {
            let b = &q.bm25[*i];
            let top = ri.bm25_topk(b, K);
            println!("  slow {ms:.1} ms: {:?} df {:?} idf {:?} maxtni {:?} 10th score {:.3}",
                b.iter().map(|w| rc.vocab[*w as usize].as_str()).collect::<Vec<_>>(), b.iter().map(|w| ri.df[*w as usize]).collect::<Vec<_>>(),
                b.iter().map(|w| ri.weight(*w)).collect::<Vec<_>>(), b.iter().map(|w| ri.maxtni[*w as usize]).collect::<Vec<_>>(), top.last().map(|x| x.0).unwrap_or(0.0));
        }
    }
    let mut par_ok = 0;
    for b in &q.bm25 {
        par_ok += (ri.bm25_topk_par(b, K).iter().map(|x| x.1).collect::<Vec<_>>() == ri.bm25_exhaustive(b, K).iter().map(|x| x.1).collect::<Vec<_>>()) as u32;
    }
    println!("  parallel MaxScore == exhaustive on {par_ok}/1000");
    println!("  exhaustive {} | MaxScore {} | MaxScore shards-parallel {}", time(&q.bm25, |b| ri.bm25_exhaustive(b, K)),
             time(&q.bm25, |b| ri.bm25_topk(b, K)), time(&q.bm25, |b| ri.bm25_topk_par(b, K)));

    // facets: exact vs (head-term candidates + live exact counts)
    for (name, qs) in [("word & stock", &q.word), ("word1 & word2 & stock", &q.two)] {
        let (mut same, mut ov) = (0, 0.0);
        for qq in qs.iter() {
            let (e, f) = (ri.facet_exact(qq), ri.facet_fast(qq));
            if e == f {
                same += 1;
            }
            ov += f.iter().take(10).filter(|x| e.iter().take(10).any(|y| y.0 == x.0)).count() as f64 / e.len().clamp(1, 10) as f64;
        }
        println!(
            "brand facet top-20 on {name}: exact {} | candidates+live {} | identical to exact (brands and counts) {same}/1000, top-10 overlap {:.1}%",
            time(qs, |x| ri.facet_exact(x)), time(qs, |x| ri.facet_fast(x)), 100.0 * ov / 1000.0
        );
    }

    let mix: Vec<&Vec<Clause>> = q.word.iter().chain(&q.two).chain(&q.cat).chain(&q.cat_price).chain(&q.cat_word).collect();
    let t = Instant::now();
    (0..20 * mix.len()).into_par_iter().for_each(|i| { std::hint::black_box(ix.topk_pop(mix[i % mix.len()], K)); });
    println!("throughput top-k mix (4 threads): {:.0} QPS", (20 * mix.len()) as f64 / t.elapsed().as_secs_f64());
    let t = Instant::now();
    (0..5 * q.bm25.len()).into_par_iter().for_each(|i| { std::hint::black_box(ri.bm25_topk(&q.bm25[i % q.bm25.len()], K)); });
    println!("throughput BM25 (4 threads): {:.0} QPS", (5 * q.bm25.len()) as f64 / t.elapsed().as_secs_f64());
    println!("{}", mem_status());
}

/// HTTP API with BM25: POST /search {"bm25":[term ids]} -> BM25 top-10; otherwise the filter API.
fn serve_real(ri: &bitmap_engine::realcat::RealIndex) {
    let server = tiny_http::Server::http("127.0.0.1:9400").unwrap();
    println!("serving on 127.0.0.1:9400 with 4 worker threads (filters + BM25)");
    std::thread::scope(|s| {
        for _ in 0..4 {
            let server = &server;
            s.spawn(move || loop {
                let mut req = match server.recv() { Ok(r) => r, Err(_) => break };
                let mut body = String::new();
                std::io::Read::read_to_string(req.as_reader(), &mut body).unwrap();
                let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                let t = Instant::now();
                let ids: Vec<u32> = if let Some(b) = v.get("bm25") {
                    let ws: Vec<u32> = b.as_array().unwrap().iter().map(|x| x.as_u64().unwrap() as u32).collect();
                    ri.bm25_topk(&ws, K).into_iter().map(|x| x.1).collect()
                } else {
                    let q: Vec<Clause> = v["q"].as_array().unwrap().iter().map(|c| {
                        let a = c.as_array().unwrap();
                        let n = |i: usize| a[i].as_u64().unwrap() as u32;
                        match a[0].as_str().unwrap() {
                            "cat" => Clause::Cat(n(1)), "term" => Clause::Term(n(1)), "stock" => Clause::Stock,
                            "rocket" => Clause::Rocket, "buckets" => Clause::Buckets(n(1), n(2)), x => panic!("{x}"),
                        }
                    }).collect();
                    ri.ix.topk_pop(&q, K)
                };
                let out = serde_json::json!({"took_us": t.elapsed().as_micros() as u64,
                    "hits": ids.iter().map(|d| serde_json::json!({"_id": d.to_string()})).collect::<Vec<_>>()});
                let _ = req.respond(tiny_http::Response::from_string(out.to_string())
                    .with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap()));
            });
        }
    });
}

/// Development check for filtered BM25 (dev seeds only): exactness vs exhaustive, in-process latency.
fn bm25dev(rc: &bitmap_engine::realcat::RealCat, ri: &bitmap_engine::realcat::RealIndex) {
    let mut qs: Vec<(Vec<u32>, Vec<Clause>, &str)> = Vec::new();
    for tail in [false, true] {
        let h = bitmap_engine::realcat::dev(rc, 250, tail);
        qs.extend(h.bm25.iter().map(|b| (b.clone(), vec![Clause::Stock], "stock")));
        qs.extend(h.bm25_cat.iter().map(|(b, c)| (b.clone(), vec![Clause::Cat(*c), Clause::Stock], "cat+stock")));
    }
    let mut ok = 0;
    for (b, f, _) in &qs {
        let (x, y) = (ri.bm25_filtered(b, f, K), ri.bm25_exhaustive_f(b, f, K));
        let same = x.len() == y.len() && x.iter().zip(&y).all(|(a, b)| a.1 == b.1 && (a.0 - b.0).abs() <= 1e-5 * a.0.abs().max(1.0));
        ok += same as u32;
    }
    println!("filtered BM25 == exhaustive (ids and scores) on {ok}/{}", qs.len());
    for kind in ["stock", "cat+stock"] {
        let sel: Vec<&(Vec<u32>, Vec<Clause>, &str)> = qs.iter().filter(|q| q.2 == kind).collect();
        println!("  {kind:<10} single thread {}", time(&sel, |q| ri.bm25_filtered(&q.0, &q.1, K)));
    }
    // slowest dev queries per kind, with term df and filter sizes
    for kind in ["stock", "cat+stock"] {
        let mut v: Vec<(f64, usize)> = qs.iter().enumerate().filter(|(_, q)| q.2 == kind).map(|(i, q)| {
            let t = Instant::now(); std::hint::black_box(ri.bm25_filtered(&q.0, &q.1, K)); (t.elapsed().as_secs_f64() * 1e3, i) }).collect();
        v.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        for (ms, i) in v.iter().take(4) {
            let (b, f, _) = &qs[*i];
            let fsz: Vec<u64> = f.iter().map(|c| ri.ix.shards.iter().map(|sh| clause_src(&sh.read(), c).iter().map(|s| s.len()).sum::<u64>()).sum()).collect();
            let top = ri.bm25_filtered(b, f, K);
            println!("  slow {kind} {ms:.1} ms: words {:?} df {:?} filter sizes {fsz:?} hits {} 10th {:.2}",
                     b.iter().map(|w| rc.vocab[*w as usize].as_str()).collect::<Vec<_>>(), b.iter().map(|w| ri.df[*w as usize]).collect::<Vec<_>>(),
                     top.len(), top.last().map(|x| x.0).unwrap_or(0.0));
        }
    }
    let t = Instant::now();
    (0..4 * qs.len()).into_par_iter().for_each(|i| { std::hint::black_box(ri.bm25_filtered(&qs[i % qs.len()].0, &qs[i % qs.len()].1, K)); });
    println!("  throughput 4 threads (in-process): {:.0} QPS", (4 * qs.len()) as f64 / t.elapsed().as_secs_f64());
}
