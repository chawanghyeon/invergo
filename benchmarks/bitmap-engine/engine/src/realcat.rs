//! Real catalog (Amazon Reviews 2023 item metadata as TSV: rating_number, price_cents, store, category, title).
//! docid = popularity rank by rating_number. Title tokens: lowercase, split on non-alphanumeric,
//! small stopword list. The same token strings are sent to Elasticsearch (whitespace analyzer), so
//! both engines index identical tokens and BM25 inputs.
//!
//! Adds to the bitmap engine:
//!   * BM25 top-k (Lucene's formula, norm quantization and float arithmetic) with MaxScore pruning
//!   * brand facets: exact counting, or candidate brands precomputed offline for head terms with
//!     live exact counts at query time

use crate::*;
use parking_lot::RwLock;
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering::Relaxed};

const STOP: &[&str] = &["the", "and", "for", "with", "of", "a", "an", "to", "in", "on", "by", "or", "is"];
pub const HEAD_DF: u32 = 10_000;
pub const FACET_CANDIDATES: usize = 100;
pub const BM_DF: u32 = 2_000; // terms with block-max metadata
/// Block-max window in docids (env BMW overrides, default 128).
pub fn bm_w() -> u32 {
    static W: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *W.get_or_init(|| std::env::var("BMW").ok().and_then(|v| v.parse().ok()).unwrap_or(128))
}
pub const FACET_EXACT_MAX: u64 = 100_000; // estimated result size below which facets are counted exactly
const K1: f32 = 1.2;
const B: f32 = 0.75;

pub struct RealCat {
    pub n: u32,
    pub vocab: Vec<String>,
    pub tokens: Vec<Vec<u32>>, // with repeats, title order
    pub cat: Vec<u32>,
    pub brand: Vec<u32>,
    pub price: Vec<u32>,
    pub n_cat: usize,
    pub n_brand: usize,
}

pub fn tokenize(s: &str) -> Vec<String> {
    s.to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty() && !STOP.contains(w))
        .map(String::from)
        .collect()
}

pub fn load(path: &str) -> RealCat {
    let mut rows: Vec<(u64, u32, String, String, String)> = BufReader::new(std::fs::File::open(path).unwrap())
        .lines()
        .filter_map(|l| {
            let l = l.ok()?;
            let f: Vec<&str> = l.splitn(5, '\t').collect();
            (f.len() == 5).then(|| (f[0].parse().unwrap_or(0), f[1].parse().unwrap_or(0), f[2].into(), f[3].into(), f[4].into()))
        })
        .collect();
    rows.sort_by(|a, b| b.0.cmp(&a.0)); // most reviewed first; stable, so file order breaks ties
    let mut vmap: HashMap<String, u32> = HashMap::new();
    let mut vocab = Vec::new();
    let (mut cmap, mut bmap): (HashMap<String, u32>, HashMap<String, u32>) = (HashMap::new(), HashMap::new());
    let mut rc = RealCat { n: rows.len() as u32, vocab: vec![], tokens: vec![], cat: vec![], brand: vec![], price: vec![], n_cat: 0, n_brand: 0 };
    for (_, cents, store, cat, title) in rows {
        let ts = tokenize(&title)
            .into_iter()
            .map(|w| {
                *vmap.entry(w.clone()).or_insert_with(|| {
                    vocab.push(w);
                    vocab.len() as u32 - 1
                })
            })
            .collect();
        rc.tokens.push(ts);
        let l = cmap.len() as u32;
        rc.cat.push(*cmap.entry(cat).or_insert(l));
        let l = bmap.len() as u32;
        rc.brand.push(*bmap.entry(store.to_lowercase()).or_insert(l));
        rc.price.push(if cents == 0 { 9_999_999 } else { cents.min(9_999_999) });
    }
    rc.vocab = vocab;
    rc.n_cat = cmap.len();
    rc.n_brand = bmap.len();
    rc
}

// ------------------------------------------------------------------ Lucene BM25 pieces
fn long_to_int4(i: u64) -> u32 {
    let bits = 64 - i.leading_zeros();
    if bits < 4 {
        i as u32
    } else {
        let shift = bits - 4;
        (((i >> shift) as u32) & 0x07) | ((shift + 1) << 3)
    }
}
fn int4_to_long(i: u32) -> u64 {
    let bits = (i & 0x07) as u64;
    let shift = (i >> 3) as i32 - 1;
    if shift == -1 { bits } else { (bits | 0x08) << shift }
}
const NUM_FREE: u32 = 255 - 231; // 255 - long_to_int4(i32::MAX)
/// Lucene SmallFloat.intToByte4: document length -> 1-byte norm.
pub fn int_to_byte4(i: u32) -> u8 {
    if i < NUM_FREE { i as u8 } else { (NUM_FREE + long_to_int4((i - NUM_FREE) as u64)) as u8 }
}
pub fn byte4_to_int(b: u8) -> u32 {
    let i = b as u32;
    if i < NUM_FREE { i } else { (NUM_FREE as u64 + int4_to_long(i - NUM_FREE)) as u32 }
}

pub struct RealIndex {
    pub ix: Index,
    pub brands: Vec<Terms>, // per shard, keyed by brand
    pub norm: Vec<u8>,
    pub df: Vec<u32>,
    pub tfmax: Vec<u8>,
    pub maxtni: Vec<f32>, // per term: max over its postings of tf * norm_inv (exact max impact)
    pub bm_row: HashMap<u32, usize>, // term -> row in bm (terms with df >= BM_DF)
    pub bm: Vec<Vec<u8>>,            // per row: per docid window, ceil-quantized max tf * norm_inv
    pub tni_max: f32,                // quantization scale
    pub tf2: Vec<HashMap<u32, (Vec<u32>, Vec<u8>)>>, // per shard: term -> sorted docs with tf >= 2, and their tf
    pub doc_count: u32,          // docs with >= 1 title token (Lucene docCount for the field)
    pub avgdl: f32,
    pub norm_inv: [f32; 256],
    pub head_cands: HashMap<u32, Vec<u32>>, // head term -> candidate brands (offline)
    pub n_brand: usize,
    pub build_times: Vec<(String, f64)>,
}

pub fn build(rc: &RealCat, s: u32) -> RealIndex {
    build_part(rc, s, 0, rc.n)
}
/// Index only docids in [keep_lo, keep_hi) (a shard node of a partitioned cluster), while BM25
/// statistics, block-max bounds and facet candidates are computed over the whole catalog, so a
/// partition scores exactly like a single node and partition results can be merged.
pub fn build_part(rc: &RealCat, s: u32, keep_lo: u32, keep_hi: u32) -> RealIndex {
    let t_all = std::time::Instant::now();
    let n = rc.n;
    let shard_size = n.div_ceil(s);
    let mut shards = Vec::new();
    let mut brands = Vec::new();
    let mut tf2 = Vec::new();
    for si in 0..s {
        let (lo, hi) = (si * shard_size, ((si + 1) * shard_size).min(n));
        let (lo, hi) = (lo.max(keep_lo).min(hi), hi.min(keep_hi).max(lo.max(keep_lo).min(hi)));
        let pairs = |f: &(dyn Fn(usize) -> u32 + Sync)| -> Vec<u64> {
            (lo..hi).into_par_iter().map(|d| ((f(d as usize) as u64) << 32) | d as u64).collect()
        };
        let tp: Vec<u64> = (lo..hi)
            .into_par_iter()
            .flat_map_iter(|d| rc.tokens[d as usize].iter().map(move |&w| ((w as u64) << 32) | d as u64))
            .collect();
        let mut t2: HashMap<u32, (Vec<u32>, Vec<u8>)> = HashMap::new();
        for d in lo..hi {
            let ts = &rc.tokens[d as usize];
            if ts.len() > 1 {
                let mut c: HashMap<u32, u8> = HashMap::new();
                for &w in ts {
                    *c.entry(w).or_default() += 1;
                }
                for (w, tf) in c {
                    if tf >= 2 {
                        let e = t2.entry(w).or_default();
                        e.0.push(d);
                        e.1.push(tf);
                    }
                }
            }
        }
        // stock/rocket are not in the dataset: synthetic, same rates as the synthetic catalog
        let mut stock = RoaringBitmap::from_sorted_iter((lo..hi).filter(|&d| unif(d as u64, 6) < 0.9)).unwrap();
        let mut rocket = RoaringBitmap::from_sorted_iter((lo..hi).filter(|&d| unif(d as u64, 7) < 0.4)).unwrap();
        stock.optimize();
        rocket.optimize();
        shards.push(RwLock::new(Shard {
            cat: bitmaps_from_pairs(pairs(&|i| rc.cat[i]), rc.n_cat),
            bucket: bitmaps_from_pairs(pairs(&|i| bucket_of(rc.price[i])), N_BUCKET as usize),
            term: Terms::from_pairs(tp, rc.vocab.len()),
            stock,
            rocket,
        }));
        brands.push(Terms::from_pairs(pairs(&|i| rc.brand[i]), rc.n_brand));
        tf2.push(t2);
    }
    let ix = Index {
        shards,
        shard_size,
        n,
        brand: rc.brand.clone(),
        price: rc.price.iter().map(|&p| AtomicU32::new(p)).collect(),
        bucket: rc.price.iter().map(|&p| AtomicU8::new(bucket_of(p) as u8)).collect(),
    };

    let t_index = t_all.elapsed().as_secs_f64();
    let t_bm25 = std::time::Instant::now();
    let norm: Vec<u8> = rc.tokens.iter().map(|t| int_to_byte4(t.len() as u32)).collect();
    let mut df = vec![0u32; rc.vocab.len()];
    let mut tfmax = vec![1u8; rc.vocab.len()];
    for ts in &rc.tokens {
        let mut u = ts.clone();
        u.sort_unstable();
        for g in u.chunk_by(|a, b| a == b) {
            df[g[0] as usize] += 1;
            tfmax[g[0] as usize] = tfmax[g[0] as usize].max(g.len().min(255) as u8);
        }
    }
    let doc_count = rc.tokens.iter().filter(|t| !t.is_empty()).count() as u32;
    let sum_ttf: u64 = rc.tokens.iter().map(|t| t.len() as u64).sum();
    let avgdl = (sum_ttf as f64 / doc_count as f64) as f32;
    let mut norm_inv = [0f32; 256];
    for (i, x) in norm_inv.iter_mut().enumerate() {
        *x = 1f32 / (K1 * ((1f32 - B) + B * byte4_to_int(i as u8) as f32 / avgdl));
    }

    let mut maxtni = vec![0f32; rc.vocab.len()];
    for (d, ts) in rc.tokens.iter().enumerate() {
        let ni = norm_inv[norm[d] as usize];
        let mut u = ts.clone();
        u.sort_unstable();
        for g in u.chunk_by(|a, b| a == b) {
            let x = g.len() as f32 * ni;
            if x > maxtni[g[0] as usize] {
                maxtni[g[0] as usize] = x;
            }
        }
    }
    let t_bmx = std::time::Instant::now();
    // block-max: per (term, window of bm_w() docids) max impact, quantized up so bounds stay >= true
    let tni_max = maxtni.iter().cloned().fold(0f32, f32::max) * 1.0001;
    let bm_terms: Vec<u32> = (0..rc.vocab.len() as u32).filter(|&w| df[w as usize] >= BM_DF).collect();
    let bm_row: HashMap<u32, usize> = bm_terms.iter().enumerate().map(|(i, &w)| (w, i)).collect();
    let n_win = (n / bm_w() + 1) as usize;
    let mut bm = vec![vec![0u8; n_win]; bm_terms.len()];
    for (d, ts) in rc.tokens.iter().enumerate() {
        let ni = norm_inv[norm[d] as usize];
        let mut u = ts.clone();
        u.sort_unstable();
        for g in u.chunk_by(|a, b| a == b) {
            if let Some(&r) = bm_row.get(&g[0]) {
                let q = ((g.len() as f32 * ni / tni_max) * 255.0).ceil().min(255.0) as u8;
                let cell = &mut bm[r][d / bm_w() as usize];
                *cell = (*cell).max(q);
            }
        }
    }

    let (bmx_s, bm25_s) = (t_bmx.elapsed().as_secs_f64(), t_bm25.elapsed().as_secs_f64());
    let t_fc = std::time::Instant::now();
    // offline: candidate brands for head terms = most frequent brands among the term's items
    let heads: Vec<u32> = (0..rc.vocab.len() as u32).filter(|&w| df[w as usize] >= HEAD_DF).collect();
    let mut docs_of: HashMap<u32, Vec<u32>> = heads.iter().map(|&w| (w, Vec::new())).collect();
    for (d, ts) in rc.tokens.iter().enumerate() {
        for w in ts {
            if let Some(v) = docs_of.get_mut(w) {
                if v.last() != Some(&(d as u32)) {
                    v.push(d as u32);
                }
            }
        }
    }
    let head_cands: HashMap<u32, Vec<u32>> = docs_of
        .into_par_iter()
        .map(|(w, ds)| {
            let mut c: HashMap<u32, u32> = HashMap::new();
            for d in ds {
                *c.entry(rc.brand[d as usize]).or_default() += 1;
            }
            let mut v: Vec<(u32, u32)> = c.into_iter().map(|(b, n)| (n, b)).collect();
            v.sort_unstable_by(|a, b| b.cmp(a));
            (w, v.into_iter().take(FACET_CANDIDATES).map(|x| x.1).collect())
        })
        .collect();
    let build_times = vec![
        ("postings + attribute bitmaps".to_string(), t_index),
        ("BM25 stats (norms, df, tf, max impact) incl. block-max".to_string(), bm25_s),
        ("  of which block-max windows".to_string(), bmx_s),
        ("facet candidate brands (offline)".to_string(), t_fc.elapsed().as_secs_f64()),
        ("total".to_string(), t_all.elapsed().as_secs_f64()),
    ];
    RealIndex { ix, brands, norm, df, tfmax, maxtni, bm_row, bm, tni_max, tf2, doc_count, avgdl, norm_inv, head_cands, n_brand: rc.n_brand, build_times }
}

impl RealIndex {
    pub fn idf(&self, w: u32) -> f32 {
        let df = self.df[w as usize] as f64;
        (1.0 + (self.doc_count as f64 - df + 0.5) / (df + 0.5)).ln() as f32
    }
    /// Lucene/ES term weight: boost (k1 + 1) * idf, in float.
    pub fn weight(&self, w: u32) -> f32 {
        (K1 + 1f32) * self.idf(w)
    }
    fn tf(&self, shard: usize, w: u32, d: u32) -> u8 {
        match self.tf2[shard].get(&w) {
            Some((ds, tfs)) => ds.binary_search(&d).map(|i| tfs[i]).unwrap_or(1),
            None => 1,
        }
    }
    #[inline]
    fn term_score(&self, idf: f32, tf: u8, d: u32) -> f32 {
        let ni = self.norm_inv[self.norm[d as usize] as usize];
        idf - idf / (1f32 + tf as f32 * ni)
    }

    /// Exhaustive BM25 (reference): OR over terms, in-stock filter, top-k by score then docid.
    pub fn bm25_exhaustive(&self, terms: &[u32], k: usize) -> Vec<(f32, u32)> {
        self.bm25_exhaustive_f(terms, &[Clause::Stock], k)
    }
    pub fn bm25_exhaustive_f(&self, terms: &[u32], filters: &[Clause], k: usize) -> Vec<(f32, u32)> {
        let mut scores: HashMap<u32, f32> = HashMap::new();
        for (si, sh) in self.ix.shards.iter().enumerate() {
            let sh = sh.read();
            let fb: Vec<RoaringBitmap> = filters.iter().map(|c| clause_src(&sh, c).iter().fold(RoaringBitmap::new(), |mut a, s| { a |= s.to_bitmap(); a })).collect();
            for &w in terms {
                let idf = self.weight(w);
                let mut it = sh.term.src(w).iter();
                while let Some(d) = it.next() {
                    if fb.iter().all(|b| b.contains(d)) {
                        *scores.entry(d).or_default() += self.term_score(idf, self.tf(si, w, d), d);
                    }
                }
            }
        }
        let mut v: Vec<(f32, u32)> = scores.into_iter().map(|(d, s)| (s, d)).collect();
        v.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then(a.1.cmp(&b.1)));
        v.truncate(k);
        v
    }

    /// MaxScore BM25: terms sorted by upper bound; once the k-th score exceeds the sum of the
    /// smallest bounds, those terms only score candidates produced by the others.
    pub fn bm25_topk(&self, terms: &[u32], k: usize) -> Vec<(f32, u32)> {
        self.bm25_filtered(terms, &[Clause::Stock], k)
    }
    /// BM25 over terms (OR), restricted to docs matching every filter clause.
    pub fn bm25_filtered(&self, terms: &[u32], filters: &[Clause], k: usize) -> Vec<(f32, u32)> {
        let all: Vec<usize> = (0..self.ix.shards.len()).collect();
        self.bm25_shards_f(terms, filters, k, &all)
    }
    /// Same, one task per shard in parallel, then merge (lower latency, less pruning per shard).
    pub fn bm25_topk_par(&self, terms: &[u32], k: usize) -> Vec<(f32, u32)> {
        let mut v: Vec<(f32, u32)> =
            (0..self.ix.shards.len()).into_par_iter().flat_map_iter(|si| self.bm25_shards_f(terms, &[Clause::Stock], k, &[si])).collect();
        v.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then(a.1.cmp(&b.1)));
        v.truncate(k);
        v
    }
    fn bm25_shards_f(&self, terms: &[u32], filters: &[Clause], k: usize, shard_ids: &[usize]) -> Vec<(f32, u32)> {
        let mut tw: Vec<(f32, f32, u32)> = terms
            .iter()
            .map(|&w| {
                let idf = self.weight(w);
                let ub = idf - idf / (1f32 + self.maxtni[w as usize]);
                (ub, idf, w)
            })
            .collect();
        tw.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let m = tw.len();
        let mut prefix = vec![0f32; m + 1];
        for i in 0..m {
            prefix[i + 1] = prefix[i] + tw[i].0;
        }
        let rows: Vec<Option<usize>> = tw.iter().map(|t| self.bm_row.get(&t.2).copied()).collect();
        let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(OrdF32, std::cmp::Reverse<u32>)>> = Default::default();
        let mut theta = f32::NEG_INFINITY;
        for &si in shard_ids {
            let sh = self.ix.shards[si].read();
            // filter clauses as leapfrog cursors, most selective first
            let mut fcur: Vec<Cur> = filters.iter().map(|c| Cur::new(clause_src(&sh, c))).collect();
            fcur.sort_by_key(|c| c.len());
            let mut its: Vec<It> = tw.iter().map(|x| sh.term.src(x.2).iter()).collect();
            let mut heads: Vec<Option<u32>> = its.iter_mut().map(|i| i.next()).collect();
            // tf >= 2 entries per term, walked monotonically
            let tfl: Vec<Option<&(Vec<u32>, Vec<u8>)>> = tw.iter().map(|x| self.tf2[si].get(&x.2)).collect();
            let mut tfp = vec![0usize; m];
            let mut tf_at = |i: usize, d: u32| -> u8 {
                match tfl[i] {
                    None => 1,
                    Some((ds, tfs)) => {
                        while tfp[i] < ds.len() && ds[tfp[i]] < d {
                            tfp[i] += 1;
                        }
                        if tfp[i] < ds.len() && ds[tfp[i]] == d { tfs[tfp[i]] } else { 1 }
                    }
                }
            };
            // per-window term bounds: block-max rows are cached per window; terms without a row
            // are re-evaluated per candidate (their bound depends on whether a posting is in the window)
            let mut wub = vec![0f32; m];
            let mut wpre = vec![0f32; m + 1];
            let mut cached_win = u32::MAX;
            loop {
                let mut e = 0;
                while e < m && prefix[e + 1] <= theta {
                    e += 1;
                }
                if e == m {
                    break;
                }
                let d = match heads[e..].iter().flatten().min() {
                    Some(&d) => d,
                    None => break,
                };
                // filters: jump to the first doc >= d that passes every filter; if that is past d,
                // move the essential lists there and pick a new candidate
                if !fcur.is_empty() {
                    let mut t = d;
                    let mut exhausted = false;
                    'f: loop {
                        for c in fcur.iter_mut() {
                            c.seek(t);
                            match c.cur() {
                                None => {
                                    exhausted = true;
                                    break 'f;
                                }
                                Some(v) if v > t => {
                                    t = v;
                                    continue 'f;
                                }
                                _ => {}
                            }
                        }
                        break;
                    }
                    if exhausted {
                        break;
                    }
                    if t > d {
                        for i in e..m {
                            if let Some(h) = heads[i] {
                                if h < t {
                                    its[i].advance_to(t);
                                    heads[i] = its[i].next();
                                }
                            }
                        }
                        continue;
                    }
                }
                let win = d / bm_w();
                let (ws, we) = (win * bm_w(), (win + 1) * bm_w());
                // per-term bound in this window: block-max row if present, else the global bound
                // if the term has a posting inside the window, else 0
                let new_win = win != cached_win;
                cached_win = win;
                for i in 0..m {
                    if let Some(h) = heads[i] {
                        if h < ws {
                            its[i].advance_to(ws);
                            heads[i] = its[i].next();
                        }
                    }
                    match rows[i] {
                        Some(r) if new_win => {
                            let q = self.bm[r][win as usize];
                            wub[i] = if q == 0 { 0.0 } else { let x = q as f32 / 255.0 * self.tni_max; tw[i].1 - tw[i].1 / (1f32 + x) };
                        }
                        Some(_) => {}
                        None => {
                            wub[i] = match heads[i] {
                                Some(h) if h < we => tw[i].0,
                                _ => 0.0,
                            }
                        }
                    }
                }
                if theta > f32::NEG_INFINITY && wub.iter().sum::<f32>() <= theta {
                    for i in e..m {
                        if let Some(h) = heads[i] {
                            if h < we {
                                its[i].advance_to(we);
                                heads[i] = its[i].next();
                            }
                        }
                    }
                    continue;
                }
                let mut score = 0f32;
                for i in e..m {
                    if heads[i] == Some(d) {
                        score += self.term_score(tw[i].1, tf_at(i, d), d);
                        heads[i] = its[i].next();
                    }
                }
                wpre[0] = 0.0;
                for j in 0..e {
                    wpre[j + 1] = wpre[j] + wub[j];
                }
                let mut pruned = false;
                for j in (0..e).rev() {
                    if score + wpre[j + 1] <= theta {
                        pruned = true;
                        break;
                    }
                    if let Some(h) = heads[j] {
                        if h < d {
                            its[j].advance_to(d);
                            heads[j] = its[j].next();
                        }
                        if heads[j] == Some(d) {
                            score += self.term_score(tw[j].1, tf_at(j, d), d);
                        }
                    }
                }
                if pruned {
                    continue;
                }
                if heap.len() < k {
                    heap.push(std::cmp::Reverse((OrdF32(score), std::cmp::Reverse(d))));
                } else if score > theta {
                    heap.pop();
                    heap.push(std::cmp::Reverse((OrdF32(score), std::cmp::Reverse(d))));
                }
                if heap.len() == k {
                    theta = heap.peek().unwrap().0 .0 .0;
                }
            }
        }
        let mut v: Vec<(f32, u32)> = heap.into_iter().map(|r| (r.0 .0 .0, r.0 .1 .0)).collect();
        v.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then(a.1.cmp(&b.1)));
        v
    }

    /// Upper bound of one term's contribution in a docid window (global bound if no metadata).
    #[inline]
    fn term_block_ub(&self, t: &(f32, f32, u32), win: usize) -> f32 {
        match self.bm_row.get(&t.2) {
            Some(&r) => {
                let q = self.bm[r][win];
                if q == 0 {
                    0.0
                } else {
                    let x = q as f32 / 255.0 * self.tni_max;
                    t.1 - t.1 / (1f32 + x)
                }
            }
            None => t.0,
        }
    }
    fn block_bound(&self, tw: &[(f32, f32, u32)], win: usize) -> f32 {
        tw.iter().map(|t| self.term_block_ub(t, win)).sum()
    }

    // -------------------------------------------------------------- facets
    /// Exact brand counts over the full result (shards in parallel).
    pub fn facet_exact(&self, q: &[Clause]) -> Vec<(u32, u32)> {
        let parts: Vec<HashMap<u32, u32>> = self
            .ix
            .shards
            .par_iter()
            .map(|sh| {
                let mut f = HashMap::new();
                for d in and_full(&sh.read(), q).iter() {
                    *f.entry(self.ix.brand[d as usize]).or_default() += 1;
                }
                f
            })
            .collect();
        let mut f: HashMap<u32, u32> = HashMap::new();
        for p in parts {
            for (b, c) in p {
                *f.entry(b).or_default() += c;
            }
        }
        top20(f.into_iter())
    }

    /// If the query has a head term: live exact counts for that term's offline candidate brands
    /// (|result ∩ brand| per shard). Otherwise the result is small: exact counting.
    pub fn facet_fast(&self, q: &[Clause]) -> Vec<(u32, u32)> {
        let head = q
            .iter()
            .filter_map(|c| if let Clause::Term(w) = c { self.head_cands.get(w).map(|v| (self.df[*w as usize], v)) } else { None })
            .min_by_key(|x| x.0);
        // Exact counting is cheap unless the result is large; estimate it by intersecting the
        // two rarest term postings of the first shard and scaling up.
        let est = {
            let sh = self.ix.shards[0].read();
            let mut ts: Vec<Src> =
                q.iter().filter_map(|c| if let Clause::Term(w) = c { Some(sh.term.src(*w)) } else { None }).collect();
            ts.sort_by_key(|s| s.len());
            let first = match ts.first() { Some(s) => s.to_bitmap(), None => RoaringBitmap::new() };
            let r = ts.get(1).map(|s| first.intersection_len(&s.to_bitmap())).unwrap_or(first.len());
            r * self.ix.shards.len() as u64
        };
        match head {
            Some((_, cands)) if est >= FACET_EXACT_MAX => {
                let counts: Vec<Vec<u32>> = self
                    .ix
                    .shards
                    .par_iter()
                    .enumerate()
                    .map(|(si, sh)| {
                        let r = and_full(&sh.read(), q);
                        cands
                            .iter()
                            .map(|&b| match self.brands[si].src(b) {
                                Src::Bm(bm) => r.intersection_len(bm) as u32,
                                s => {
                                    let mut it = s.iter();
                                    let mut c = 0;
                                    while let Some(d) = it.next() {
                                        c += r.contains(d) as u32;
                                    }
                                    c
                                }
                            })
                            .collect()
                    })
                    .collect();
                top20(cands.iter().enumerate().map(|(i, &b)| (b, counts.iter().map(|c| c[i]).sum())))
            }
            _ => self.facet_exact(q),
        }
    }
}

fn top20(it: impl Iterator<Item = (u32, u32)>) -> Vec<(u32, u32)> {
    let mut v: Vec<(u32, u32)> = it.filter(|x| x.1 > 0).map(|(b, c)| (c, b)).collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    v.truncate(20);
    v.into_iter().map(|(c, b)| (b, c)).collect()
}

#[derive(PartialEq, PartialOrd, Clone, Copy)]
pub struct OrdF32(pub f32);
impl Eq for OrdF32 {}
impl Ord for OrdF32 {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&o.0).unwrap()
    }
}

// ------------------------------------------------------------------ query workload (shared with ES)
pub struct RealQueries {
    pub word: Vec<Vec<Clause>>,
    pub two: Vec<Vec<Clause>>,
    pub cat: Vec<Vec<Clause>>,
    pub cat_price: Vec<Vec<Clause>>,
    pub cat_word: Vec<Vec<Clause>>,
    pub bm25: Vec<Vec<u32>>,
}
/// Pick an item with Zipf weight on popularity and take words from its title (users searching
/// for popular items); filters use that item's real category and price.
pub fn queries(rc: &RealCat, nq: u64) -> RealQueries {
    let n = rc.n;
    let pick = |i: u64, f: u64| -> usize {
        let mut d = zipf(n, qu(i, f)) as usize;
        while rc.tokens[d].is_empty() {
            d = (d + 1) % n as usize;
        }
        d
    };
    let word_of = |d: usize, i: u64, f: u64| rc.tokens[d][(qu(i, f) * rc.tokens[d].len() as f64) as usize % rc.tokens[d].len()];
    RealQueries {
        word: (0..nq).map(|i| { let d = pick(i, 20); vec![Clause::Term(word_of(d, i, 21)), Clause::Stock] }).collect(),
        two: (0..nq).map(|i| { let d = pick(i, 22); vec![Clause::Term(word_of(d, i, 23)), Clause::Term(word_of(d, i, 24)), Clause::Stock] }).collect(),
        cat: (0..nq).map(|i| vec![Clause::Cat(rc.cat[pick(i, 25)]), Clause::Stock, Clause::Rocket]).collect(),
        cat_price: (0..nq)
            .map(|i| {
                let d = pick(i, 26);
                let b = bucket_of(rc.price[d]);
                vec![Clause::Cat(rc.cat[d]), Clause::Stock, Clause::Buckets(b.saturating_sub(18), (b + 19).min(N_BUCKET))]
            })
            .collect(),
        cat_word: (0..nq).map(|i| { let d = pick(i, 27); vec![Clause::Cat(rc.cat[d]), Clause::Term(word_of(d, i, 28)), Clause::Stock] }).collect(),
        bm25: (0..nq).map(|i| { let d = pick(i, 29); vec![word_of(d, i, 30), word_of(d, i, 31)] }).collect(),
    }
}

/// Held-out workloads (never used for tuning): items drawn uniformly, or from the least popular
/// half (long tail), with seeds disjoint from `queries`.
pub struct HeldOut {
    pub word: Vec<Vec<Clause>>,
    pub two: Vec<Vec<Clause>>,
    pub cat_word: Vec<Vec<Clause>>,
    pub bm25: Vec<Vec<u32>>,
    pub bm25_cat: Vec<(Vec<u32>, u32)>, // words, category
}
pub fn heldout(rc: &RealCat, nq: u64, tail: bool) -> HeldOut {
    heldout_base(rc, nq, tail, if tail { 300 } else { 200 })
}
/// Development set for tuning: same shapes, seeds disjoint from the reporting sets (200/300).
pub fn dev(rc: &RealCat, nq: u64, tail: bool) -> HeldOut {
    heldout_base(rc, nq, tail, if tail { 500 } else { 400 })
}
pub fn heldout_base(rc: &RealCat, nq: u64, tail: bool, base: u64) -> HeldOut {
    let n = rc.n as u64;
    let pick = |i: u64, f: u64| -> usize {
        let u = qu(i, base + f);
        let mut d = if tail { (n / 2 + (u * (n / 2) as f64) as u64) as usize } else { (u * n as f64) as usize };
        while rc.tokens[d].is_empty() {
            d = (d + 1) % n as usize;
        }
        d
    };
    let w = |d: usize, i: u64, f: u64| rc.tokens[d][(qu(i, base + f) * rc.tokens[d].len() as f64) as usize % rc.tokens[d].len()];
    HeldOut {
        word: (0..nq).map(|i| { let d = pick(i, 1); vec![Clause::Term(w(d, i, 2)), Clause::Stock] }).collect(),
        two: (0..nq).map(|i| { let d = pick(i, 3); vec![Clause::Term(w(d, i, 4)), Clause::Term(w(d, i, 5)), Clause::Stock] }).collect(),
        cat_word: (0..nq).map(|i| { let d = pick(i, 6); vec![Clause::Cat(rc.cat[d]), Clause::Term(w(d, i, 7)), Clause::Stock] }).collect(),
        bm25: (0..nq).map(|i| { let d = pick(i, 8); vec![w(d, i, 9), w(d, i, 10)] }).collect(),
        bm25_cat: (0..nq).map(|i| { let d = pick(i, 11); (vec![w(d, i, 12), w(d, i, 13)], rc.cat[d]) }).collect(),
    }
}

/// Correlated out-of-stock: half of the top 10% most popular items, 60% of items in every fifth
/// category, 5% elsewhere.
pub fn oos_stock(rc: &RealCat, d: usize) -> bool {
    let p = if (d as u64) < rc.n as u64 / 10 { 0.5 } else if rc.cat[d] % 5 == 0 { 0.6 } else { 0.05 };
    unif(d as u64, 333) >= p
}
