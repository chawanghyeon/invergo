//! Relevance path: Tantivy BM25 (block-max WAND) over the same synthetic titles, with a stock filter.
//!   tv N DIR   build the index for docs 0..N in DIR (if missing), then measure the same BM25 query as ES
use bitmap_engine::*;
use std::time::Instant;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, ConstScoreQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, FAST, INDEXED, TEXT};
use tantivy::{doc, Index, Term};

fn stats(mut v: Vec<f64>) -> (f64, f64, f64) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p = |x: f64| v[((v.len() - 1) as f64 * x).round() as usize];
    (p(0.5), p(0.99), *v.last().unwrap())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: u64 = args[1].parse().unwrap();
    let dir = std::path::PathBuf::from(&args[2]);

    let mut sb = Schema::builder();
    let title = sb.add_text_field("title", TEXT);
    let stock = sb.add_u64_field("stock", INDEXED | FAST);
    let pop = sb.add_u64_field("pop", FAST);
    let schema = sb.build();

    let index = if dir.join("meta.json").exists() {
        Index::open_in_dir(&dir).unwrap()
    } else {
        std::fs::create_dir_all(&dir).unwrap();
        let index = Index::create_in_dir(&dir, schema.clone()).unwrap();
        let t = Instant::now();
        let mut w = index.writer_with_num_threads(4, 2_000_000_000).unwrap();
        for d in 0..n {
            let x = doc(d);
            let t: Vec<String> = x.terms.iter().map(|t| format!("w{t}")).collect();
            w.add_document(doc!(title => t.join(" "), stock => x.stock as u64, pop => d)).unwrap();
        }
        w.commit().unwrap();
        w.wait_merging_threads().unwrap();
        println!("tantivy index {n} docs: {:.1}s", t.elapsed().as_secs_f64());
        index
    };
    let size: u64 = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().metadata().unwrap().len()).sum();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    println!("tantivy: {} segments, {:.0} MB on disk", searcher.segment_readers().len(), size as f64 / 1048576.0);

    let build_q = |i: u64| -> Box<dyn Query> {
        let (a, b) = q_terms(i);
        let tq = |w: u32| -> Box<dyn Query> {
            Box::new(TermQuery::new(Term::from_field_text(title, &format!("w{w}")), IndexRecordOption::WithFreqs))
        };
        let text = BooleanQuery::new(vec![(Occur::Should, tq(a)), (Occur::Should, tq(b))]);
        let filt = ConstScoreQuery::new(Box::new(TermQuery::new(Term::from_field_u64(stock, 1), IndexRecordOption::Basic)), 0.0);
        Box::new(BooleanQuery::new(vec![(Occur::Must, Box::new(text)), (Occur::Must, Box::new(filt))]))
    };
    let qs: Vec<Box<dyn Query>> = (0..1000).map(build_q).collect();
    for q in qs.iter().take(50) {
        searcher.search(q.as_ref(), &TopDocs::with_limit(K).order_by_score()).unwrap();
    }
    let lat: Vec<f64> = qs
        .iter()
        .map(|q| {
            let t = Instant::now();
            std::hint::black_box(searcher.search(q.as_ref(), &TopDocs::with_limit(K).order_by_score()).unwrap());
            t.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    let (p50, p99, mx) = stats(lat);
    println!("tantivy BM25 'w_a w_b' + stock filter (single thread): p50 {p50:.3} p99 {p99:.3} max {mx:.3} ms");

    let t = Instant::now();
    let rounds = 5;
    std::thread::scope(|s| {
        for th in 0..4 {
            let (qs, searcher) = (&qs, &searcher);
            s.spawn(move || {
                for i in (th..rounds * qs.len()).step_by(4) {
                    std::hint::black_box(searcher.search(qs[i % qs.len()].as_ref(), &TopDocs::with_limit(K).order_by_score()).unwrap());
                }
            });
        }
    });
    println!("tantivy BM25 throughput (4 threads): {:.0} QPS", (rounds * qs.len()) as f64 / t.elapsed().as_secs_f64());
}
