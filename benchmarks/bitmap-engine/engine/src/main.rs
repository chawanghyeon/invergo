use bitmap_engine::*;
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

fn peak_rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmHWM:")).map(|l| l.split_whitespace().nth(1).unwrap().parse::<f64>().unwrap()))
        .unwrap_or(0.0)
        / 1024.0
}

fn stats(mut v: Vec<f64>) -> (f64, f64, f64) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |x: f64| v[((v.len() - 1) as f64 * x).round() as usize];
    (p(0.5), p(0.99), *v.last().unwrap())
}
fn time_ms<T>(qs: &[Vec<Clause>], f: impl Fn(&[Clause]) -> T) -> (f64, f64, f64) {
    stats(
        qs.iter()
            .map(|q| {
                let t = Instant::now();
                std::hint::black_box(f(q));
                t.elapsed().as_secs_f64() * 1e3
            })
            .collect(),
    )
}
fn fmt((a, b, c): (f64, f64, f64)) -> String {
    format!("p50 {a:8.3} p99 {b:8.3} max {c:8.3} ms")
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("bench");
    let n: u32 = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(10_000_000);
    let s: u32 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(16);
    let baseline = !args.iter().any(|a| a == "--no-baseline");

    let t = Instant::now();
    let ix = build(n, s, 4);
    let (bm, tm) = ix.bitmap_mb();
    println!(
        "N={n} shards={s} build {:.1}s | bitmaps {bm:.0} MB (terms {tm:.0}) + columns {:.0} MB | peak RSS after build {:.0} MB",
        t.elapsed().as_secs_f64(),
        ix.column_mb(),
        peak_rss_mb()
    );
    let path = std::path::PathBuf::from(args.get(4).cloned().unwrap_or_else(|| "snap".into()));
    match mode {
        "bench" => bench(&ix, baseline),
        "concurrent" => concurrent(&ix),
        "serve" => serve_http(&ix),
        "snapshot" => snapshot(&ix, &path),
        "primary" => primary(&ix, &path, args.get(5).map(|r| r.parse().unwrap()).unwrap_or(200_000)),
        "replica" => replica(&ix, &path),
        "rerank" => rerank(&ix),
        _ => panic!("mode"),
    }
    println!("peak RSS {:.0} MB", peak_rss_mb());
}

/// Save to per-shard files, reload, and check the reloaded index answers identically.
fn snapshot(ix: &Index, dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
    let t = Instant::now();
    let bytes = snap::save(ix, dir).unwrap();
    let ws = t.elapsed().as_secs_f64();
    println!("snapshot write: {:.0} MB in {ws:.2}s ({:.0} MB/s, fsync per file)", bytes as f64 / 1048576.0, bytes as f64 / 1048576.0 / ws);
    let dropped = std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok();
    let t = Instant::now();
    let ix2 = snap::load(dir, ix.shards.len()).unwrap();
    let rs = t.elapsed().as_secs_f64();
    println!(
        "snapshot load: {rs:.2}s ({:.0} MB/s) [page cache {}]",
        bytes as f64 / 1048576.0 / rs,
        if dropped { "dropped first: cold read" } else { "could not be dropped: warm read" }
    );
    assert_eq!(snap::state_digest(ix), snap::state_digest(&ix2));
    let mut checked = 0;
    for i in 0..1000 {
        for q in [q_filter(i), q_filter_price(i), q_kw2(i), q_kw1(i)] {
            assert_eq!(ix.topk_pop(&q, K), ix2.topk_pop(&q, K));
            checked += 1;
        }
        let q = q_kw1(i);
        assert_eq!(ix.topk_price(&q), ix2.topk_price(&q));
        checked += 1;
    }
    println!("reloaded index: state digest equal, {checked} queries identical");
}

// Update log: per batch [u64 ts_ns][u32 count] then count x [u8 kind][u32 doc][u32 price]; kind 9 = end + digest.
fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Apply batches at `rate` updates/s for 20 s, appending each batch to the log before applying it.
fn primary(ix: &Index, log: &std::path::Path, rate: u64) {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().create(true).truncate(true).write(true).open(log).unwrap();
    std::thread::sleep(Duration::from_secs(2)); // let the replica attach
    let t0 = Instant::now();
    let mut i = 0u64;
    while t0.elapsed() < Duration::from_secs(20) {
        if i >= (t0.elapsed().as_secs_f64() * rate as f64) as u64 {
            std::thread::sleep(Duration::from_micros(100));
            continue;
        }
        let mut ups: Vec<Update> = (i..i + 1024)
            .map(|j| {
                let d = (mix_u(j) % ix.n as u64) as u32;
                if j % 2 == 0 {
                    Update::ToggleStock(d)
                } else {
                    let p = ix.price[d as usize].load(Relaxed);
                    Update::Price(d, ((p as f64) * (0.8 + 0.4 * unif(j, 99))) as u32 + 1)
                }
            })
            .collect();
        let mut rec = Vec::with_capacity(12 + ups.len() * 9);
        rec.extend_from_slice(&now_ns().to_le_bytes());
        rec.extend_from_slice(&(ups.len() as u32).to_le_bytes());
        for u in &ups {
            let (k, d, p) = match *u {
                Update::ToggleStock(d) => (0u8, d, 0),
                Update::Price(d, p) => (1u8, d, p),
                Update::SetStock(d, v) => (2u8, d, v as u32),
            };
            rec.push(k);
            rec.extend_from_slice(&d.to_le_bytes());
            rec.extend_from_slice(&p.to_le_bytes());
        }
        f.write_all(&rec).unwrap();
        ix.apply_batch(&mut ups);
        i += 1024;
    }
    let digest = snap::state_digest(ix);
    let mut end = Vec::new();
    end.extend_from_slice(&now_ns().to_le_bytes());
    end.extend_from_slice(&u32::MAX.to_le_bytes());
    end.extend_from_slice(&digest.to_le_bytes());
    f.write_all(&end).unwrap();
    f.sync_all().unwrap();
    println!("primary: {i} updates in 20 s ({:.0}/s), state digest {digest:016x}", i as f64 / 20.0);
}

/// Tail the log, apply each batch, record lag (append time on primary -> applied on replica).
fn replica(ix: &Index, log: &std::path::Path) {
    use std::io::Read;
    while !log.exists() {
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut f = std::fs::File::open(log).unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 1 << 20];
    let (mut lags, mut applied) = (Vec::new(), 0u64);
    loop {
        let n = f.read(&mut chunk).unwrap();
        if n == 0 {
            std::thread::sleep(Duration::from_micros(200));
            continue;
        }
        buf.extend_from_slice(&chunk[..n]);
        let mut p = 0;
        loop {
            if buf.len() - p < 12 {
                break;
            }
            let ts = u64::from_le_bytes(buf[p..p + 8].try_into().unwrap());
            let cnt = u32::from_le_bytes(buf[p + 8..p + 12].try_into().unwrap());
            if cnt == u32::MAX {
                if buf.len() - p < 20 {
                    break;
                }
                let want = u64::from_le_bytes(buf[p + 12..p + 20].try_into().unwrap());
                let got = snap::state_digest(ix);
                let (p50, p99, mx) = stats(lags);
                println!(
                    "replica: applied {applied} updates; lag primary-append -> replica-applied p50 {p50:.2} p99 {p99:.2} max {mx:.2} ms; digest {got:016x} {} primary",
                    if got == want { "==" } else { "!=" }
                );
                assert_eq!(got, want);
                return;
            }
            let need = 12 + cnt as usize * 9;
            if buf.len() - p < need {
                break;
            }
            let mut ups: Vec<Update> = (0..cnt as usize)
                .map(|k| {
                    let r = p + 12 + k * 9;
                    let d = u32::from_le_bytes(buf[r + 1..r + 5].try_into().unwrap());
                    let pr = u32::from_le_bytes(buf[r + 5..r + 9].try_into().unwrap());
                    if buf[r] == 0 { Update::ToggleStock(d) } else { Update::Price(d, pr) }
                })
                .collect();
            ix.apply_batch(&mut ups);
            applied += cnt as u64;
            lags.push((now_ns().saturating_sub(ts)) as f64 / 1e6);
            p += need;
        }
        buf.drain(..p);
    }
}

/// Second stage: rerank the top-N popularity candidates by a dot product with a user vector
/// (64-d item embeddings, f32 and int8), then take the top 10.
fn rerank(ix: &Index) {
    const D: usize = 64;
    let n = ix.n as usize;
    let emb_f: Vec<f32> = (0..n * D).into_par_iter().map(|i| (unif(i as u64, 77) as f32) * 2.0 - 1.0).collect();
    let emb_q: Vec<i8> = emb_f.par_iter().map(|x| (x * 127.0) as i8).collect();
    println!("item embeddings: f32 {:.0} MB, int8 {:.0} MB ({} bytes/item)", (n * D * 4) as f64 / 1048576.0, (n * D) as f64 / 1048576.0, D);
    let user: Vec<f32> = (0..D).map(|i| unif(i as u64, 78) as f32 * 2.0 - 1.0).collect();
    let user_q: Vec<i8> = user.iter().map(|x| (x * 127.0) as i8).collect();
    let qs: Vec<Vec<Clause>> = (0..1000).map(q_kw1).collect();
    for cands in [100usize, 1000, 10_000] {
        let run = |int8: bool| -> (f64, f64, f64) {
            stats(
                qs.iter()
                    .map(|q| {
                        let t = Instant::now();
                        let ids = ix.topk_pop(q, cands);
                        let mut s: Vec<(f32, u32)> = ids
                            .iter()
                            .enumerate()
                            .map(|(rank, &d)| {
                                let o = d as usize * D;
                                let dot = if int8 {
                                    emb_q[o..o + D].iter().zip(&user_q).map(|(a, b)| *a as i32 * *b as i32).sum::<i32>() as f32 / 16129.0
                                } else {
                                    emb_f[o..o + D].iter().zip(&user).map(|(a, b)| a * b).sum::<f32>()
                                };
                                (dot - 0.001 * rank as f32, d) // blend with popularity rank
                            })
                            .collect();
                        if s.len() > K {
                            s.select_nth_unstable_by(K, |a, b| b.0.partial_cmp(&a.0).unwrap());
                            s.truncate(K);
                        }
                        std::hint::black_box(s);
                        t.elapsed().as_secs_f64() * 1e3
                    })
                    .collect(),
            )
        };
        let (a, b) = (run(false), run(true));
        println!(
            "rerank top-{cands:>5} candidates (retrieve + score + top-10): f32 p50 {:.3} p99 {:.3} ms | int8 p50 {:.3} p99 {:.3} ms",
            a.0, a.1, b.0, b.1
        );
    }
}

fn bench(ix: &Index, baseline: bool) {
    let nq = 1000u64;
    let qf: Vec<_> = (0..nq).map(q_filter).collect();
    let qp: Vec<_> = (0..nq).map(q_filter_price).collect();
    let q2: Vec<_> = (0..nq).map(q_kw2).collect();
    let q1: Vec<_> = (0..nq).map(q_kw1).collect();

    // correctness
    let nc = if baseline { 1000 } else { 100 };
    for q in qf.iter().chain(&qp).chain(&q2).chain(&q1).take(4 * nc) {
        assert_eq!(ix.topk_pop(q, K), ix.topk_pop_baseline(q), "{q:?}");
    }
    for q in q1.iter().take(nc / 5) {
        assert_eq!(ix.topk_price(q), ix.topk_price_full(q), "{q:?}");
    }
    println!("correctness: top-k by popularity and by price identical to full-AND baseline");

    println!("top-k by popularity (single thread, 1000 queries each)");
    for (name, qs) in [("cat & stock & rocket", &qf), ("cat & stock & price range", &qp), ("t1 & t2 & stock", &q2), ("t1 & stock", &q1)] {
        let opt = fmt(time_ms(qs, |q| ix.topk_pop(q, K)));
        let base = if baseline { fmt(time_ms(qs, |q| ix.topk_pop_baseline(q))) } else { "-".into() };
        println!("  {name:<28} baseline {base} | optimized {opt}");
    }

    let sizes: Vec<u64> = q1.iter().map(|q| ix.facet_exact(q).0).collect();
    let mut sz = sizes.clone();
    sz.sort();
    println!("t1 & stock result sizes: p50 {}, p99 {}, max {}", sz[500], sz[990], sz[999]);

    println!("top-10 by price on t1 & stock (shards in parallel)");
    println!("  full AND + select           {}", fmt(time_ms(&q1, |q| ix.topk_price_full(q))));
    println!("  cheapest-bucket walk        {}", fmt(time_ms(&q1, |q| ix.topk_price(q))));

    println!("brand facet top-20 on t1 & stock");
    println!("  exact (shards in parallel)  {}", fmt(time_ms(&q1, |q| ix.facet_exact(q))));
    for m in [10_000usize, 100_000] {
        let lat = fmt(time_ms(&q1, |q| ix.facet_sampled(q, m)));
        // accuracy on queries whose result exceeds the sample (others are exact by construction)
        let (mut o10, mut o20, mut cnt) = (0.0, 0.0, 0);
        for (q, &size) in q1.iter().zip(&sizes) {
            if size as usize > m {
                let ex: Vec<u32> = ix.facet_exact(q).1.iter().map(|x| x.1).collect();
                let sa: Vec<u32> = ix.facet_sampled(q, m).1.iter().map(|x| x.1).collect();
                let ov = |k: usize| sa.iter().take(k).filter(|b| ex.iter().take(k).any(|e| e == *b)).count() as f64 / k as f64;
                o10 += ov(10);
                o20 += ov(20);
                cnt += 1;
            }
        }
        println!(
            "  sampled first {m:>6}       {lat} | top-10 overlap {:.0}%, top-20 overlap {:.0}% (over {cnt} queries larger than sample)",
            100.0 * o10 / cnt.max(1) as f64,
            100.0 * o20 / cnt.max(1) as f64
        );
    }

    let mix: Vec<&Vec<Clause>> = qf.iter().chain(&qp).chain(&q2).chain(&q1).collect();
    let rounds = 20;
    let t = Instant::now();
    (0..rounds * mix.len()).into_par_iter().for_each(|i| {
        std::hint::black_box(ix.topk_pop(mix[i % mix.len()], K));
    });
    println!(
        "throughput top-k mix ({} threads): {:.0} QPS",
        rayon::current_num_threads(),
        (rounds * mix.len()) as f64 / t.elapsed().as_secs_f64()
    );

    let u = 2_000_000u64;
    let t = Instant::now();
    for i in 0..u {
        ix.toggle_stock((mix_u(i) % ix.n as u64) as u32);
    }
    let sr = u as f64 / t.elapsed().as_secs_f64();
    let t = Instant::now();
    for i in 0..u {
        let d = (mix_u(i + u) % ix.n as u64) as u32;
        let p = ix.price[d as usize].load(Relaxed);
        ix.set_price(d, ((p as f64) * (0.8 + 0.4 * unif(i, 99))) as u32 + 1);
    }
    let pr = u as f64 / t.elapsed().as_secs_f64();
    println!("updates (single thread, one lock per update): stock toggle {sr:.0}/s | price change {pr:.0}/s");
    let t = Instant::now();
    for c in 0..(u / 1024) {
        let mut ups: Vec<Update> = (c * 1024..(c + 1) * 1024)
            .map(|j| {
                let d = (mix_u(j + 2 * u) % ix.n as u64) as u32;
                if j % 2 == 0 {
                    Update::ToggleStock(d)
                } else {
                    let p = ix.price[d as usize].load(Relaxed);
                    Update::Price(d, ((p as f64) * (0.8 + 0.4 * unif(j, 99))) as u32 + 1)
                }
            })
            .collect();
        ix.apply_batch(&mut ups);
    }
    println!("updates (single thread, batches of 1024, 50/50 stock/price): {:.0}/s", (u / 1024 * 1024) as f64 / t.elapsed().as_secs_f64());
}

fn mix_u(i: u64) -> u64 {
    mix(i ^ 0xDEADBEEF)
}

/// 3 reader threads run the top-k mix while 1 writer applies stock/price updates at a target rate,
/// either one lock per update or in batches of 1024 (one lock per shard per batch).
fn concurrent(ix: &Index) {
    let qs: Vec<Vec<Clause>> = (0..1000).flat_map(|i| [q_filter(i), q_filter_price(i), q_kw2(i), q_kw1(i)]).collect();
    println!("concurrent: 3 reader threads (top-k mix) + 1 writer thread, 10 s per level");
    for (batch, target) in [(1usize, 0u64), (1, 100_000), (1, u64::MAX), (1024, 100_000), (1024, 1_000_000), (1024, u64::MAX)] {
        let stop = AtomicBool::new(false);
        let writes = AtomicU64::new(0);
        let (lat, reads) = std::thread::scope(|sc| {
            if target > 0 {
                sc.spawn(|| {
                    let t0 = Instant::now();
                    let mut i = 0u64;
                    while !stop.load(Relaxed) {
                        if target != u64::MAX {
                            let due = (t0.elapsed().as_secs_f64() * target as f64) as u64;
                            if i >= due {
                                std::thread::sleep(Duration::from_micros(50));
                                continue;
                            }
                        }
                        let mut ups: Vec<Update> = (i..i + batch as u64)
                            .map(|j| {
                                let d = (mix_u(j) % ix.n as u64) as u32;
                                if j % 2 == 0 {
                                    Update::ToggleStock(d)
                                } else {
                                    let p = ix.price[d as usize].load(Relaxed);
                                    Update::Price(d, ((p as f64) * (0.8 + 0.4 * unif(j, 99))) as u32 + 1)
                                }
                            })
                            .collect();
                        if batch == 1 {
                            match ups[0] {
                                Update::ToggleStock(d) => ix.toggle_stock(d),
                                Update::Price(d, p) => ix.set_price(d, p),
                                Update::SetStock(..) => ix.apply_batch(&mut ups),
                            }
                        } else {
                            ix.apply_batch(&mut ups);
                        }
                        i += batch as u64;
                    }
                    writes.store(i, Relaxed);
                });
            }
            let readers: Vec<_> = (0..3)
                .map(|r| {
                    let (qs, stop) = (&qs, &stop);
                    sc.spawn(move || {
                        let mut lat = Vec::new();
                        let mut j = r * 997;
                        while !stop.load(Relaxed) {
                            let t = Instant::now();
                            std::hint::black_box(ix.topk_pop(&qs[j % qs.len()], K));
                            lat.push(t.elapsed().as_secs_f64() * 1e3);
                            j += 1;
                        }
                        lat
                    })
                })
                .collect();
            std::thread::sleep(Duration::from_secs(10));
            stop.store(true, Relaxed);
            let all: Vec<f64> = readers.into_iter().flat_map(|h| h.join().unwrap()).collect();
            let n = all.len();
            (stats(all), n)
        });
        let w = writes.load(Relaxed) as f64 / 10.0;
        let label = if target == u64::MAX { "max".to_string() } else { target.to_string() };
        println!(
            "  batch {batch:>4} target {label:>8}/s: achieved {w:>10.0} writes/s | reads {:>8.0} QPS, p50 {:.3} p99 {:.3} max {:.3} ms",
            reads as f64 / 10.0,
            lat.0,
            lat.1,
            lat.2
        );
    }
}
