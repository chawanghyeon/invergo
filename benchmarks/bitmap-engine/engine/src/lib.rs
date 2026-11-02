//! Bitmap-state e-commerce search engine (v2).
//!
//! docid = popularity rank (0 = most popular). Each node holds S docid-range shards.
//!   * top-k by popularity: leapfrog AND (advance_to) shard by shard, stop at k
//!   * top-k by price: walk 256 log price buckets cheapest-first, stop once k candidates are found
//!   * facets: exact (all shards in parallel) or sampled over the first M matches
//!   * updates: bit flips under a per-shard RwLock, visible to the next query

use parking_lot::RwLock;
use rayon::prelude::*;
use roaring::bitmap::Iter;
use roaring::RoaringBitmap;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering::Relaxed};

pub mod realcat;

pub const TOK: u64 = 12;
pub const VOCAB: u32 = 200_000;
pub const N_CAT: u32 = 5_000;
pub const N_BRAND: u32 = 50_000;
pub const N_BUCKET: u32 = 256;
pub const K: usize = 10;

// ------------------------------------------------------------------ deterministic synthetic catalog
pub fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
pub fn unif(d: u64, f: u64) -> f64 {
    (mix(d.wrapping_mul(0x100000001B3) ^ f.wrapping_mul(0xC2B2AE3D27D4EB4F)) >> 11) as f64 / (1u64 << 53) as f64
}
pub fn zipf(v: u32, u: f64) -> u32 {
    (((v as f64).ln() * u).exp().floor() as u32).saturating_sub(1).min(v - 1)
}

pub struct Doc {
    pub cat: u32,
    pub brand: u32,
    pub price: u32,
    pub stock: bool,
    pub rocket: bool,
    pub terms: [u32; TOK as usize],
}
pub fn doc(d: u64) -> Doc {
    let (u1, u2) = (unif(d, 4).max(1e-12), unif(d, 5));
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    let mut terms = [0u32; TOK as usize];
    for (t, x) in terms.iter_mut().enumerate() {
        *x = zipf(VOCAB, unif(d, 100 + t as u64));
    }
    Doc {
        cat: zipf(N_CAT, unif(d, 1)),
        brand: zipf(N_BRAND, unif(d, 2)),
        price: ((30_000f64.ln() + z).exp() as u32 + 100).min(9_999_999),
        stock: unif(d, 6) < 0.9,
        rocket: unif(d, 7) < 0.4,
        terms,
    }
}

/// Price bucket b covers [edge(b), edge(b+1)), log-spaced between 100 and 10M won (integer edges).
fn edges() -> &'static [u32] {
    static E: std::sync::OnceLock<Vec<u32>> = std::sync::OnceLock::new();
    E.get_or_init(|| {
        (0..=N_BUCKET)
            .map(|b| match b {
                0 => 0,
                b if b >= N_BUCKET => u32::MAX,
                b => (100f64.ln() + (1e7f64.ln() - 100f64.ln()) * b as f64 / N_BUCKET as f64).exp().round() as u32,
            })
            .collect()
    })
}
pub fn edge(b: u32) -> u32 {
    edges()[b.min(N_BUCKET) as usize]
}
pub fn bucket_of(p: u32) -> u32 {
    // largest b with edge(b) <= p
    (edges()[..N_BUCKET as usize].partition_point(|&e| e <= p) - 1) as u32
}

pub fn mem_status() -> String {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .filter(|l| l.starts_with("VmRSS") || l.starts_with("VmHWM"))
        .map(|l| l.split_whitespace().take(2).collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join(", ")
}

// ------------------------------------------------------------------ index
pub struct Shard {
    pub cat: Vec<RoaringBitmap>,
    pub bucket: Vec<RoaringBitmap>,
    pub term: Terms,
    pub stock: RoaringBitmap,
    pub rocket: RoaringBitmap,
}

/// Postings for one shard, keyed 0..n_keys (title terms, or any other key such as brand).
/// Sparse keys: varint(count) then varint deltas in one byte arena (off[k]..off[k+1]).
/// Dense keys (>= DENSE_MIN docs in the shard): Roaring bitmaps, looked up by binary search.
pub struct Terms {
    off: Vec<u32>,
    arena: Vec<u8>,
    dense_keys: Vec<u32>,
    dense: Vec<RoaringBitmap>,
}
pub const DENSE_MIN: usize = 4096;

fn put_varint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}
#[inline]
fn get_varint(b: &[u8], p: &mut usize) -> u32 {
    let (mut v, mut s) = (0u32, 0);
    loop {
        let x = b[*p];
        *p += 1;
        v |= ((x & 0x7f) as u32) << s;
        if x < 0x80 {
            return v;
        }
        s += 7;
    }
}

impl Terms {
    /// From (key << 32 | docid) pairs; duplicates (a title repeating a word) are collapsed.
    pub fn from_pairs(mut pairs: Vec<u64>, n_keys: usize) -> Terms {
        pairs.par_sort_unstable();
        pairs.dedup();
        let mut t = Terms { off: Vec::with_capacity(n_keys + 1), arena: Vec::new(), dense_keys: Vec::new(), dense: Vec::new() };
        let mut i = 0;
        for k in 0..n_keys as u64 {
            let start = i;
            while i < pairs.len() && pairs[i] >> 32 == k {
                i += 1;
            }
            t.off.push(t.arena.len() as u32);
            let run = pairs[start..i].iter().map(|p| *p as u32);
            if i - start >= DENSE_MIN {
                let mut b = RoaringBitmap::from_sorted_iter(run).expect("sorted, unique");
                b.optimize();
                t.dense_keys.push(k as u32);
                t.dense.push(b);
            } else if i > start {
                put_varint(&mut t.arena, (i - start) as u32);
                let mut prev = 0u32;
                for d in run {
                    put_varint(&mut t.arena, d - prev);
                    prev = d;
                }
            }
        }
        t.off.push(t.arena.len() as u32);
        t.arena.shrink_to_fit();
        t
    }
    pub fn src(&self, k: u32) -> Src<'_> {
        if let Ok(i) = self.dense_keys.binary_search(&k) {
            return Src::Bm(&self.dense[i]);
        }
        let b = &self.arena[self.off[k as usize] as usize..self.off[k as usize + 1] as usize];
        if b.is_empty() {
            return Src::Vb(b, 0);
        }
        let mut p = 0;
        let n = get_varint(b, &mut p);
        Src::Vb(&b[p..], n)
    }
    pub fn bytes(&self) -> usize {
        self.arena.len() + self.off.len() * 4 + self.dense_keys.len() * 28 + self.dense.iter().map(|b| b.serialized_size()).sum::<usize>()
    }
}

// ------------------------------------------------------------------ snapshot (object-storage stand-in)
pub mod snap {
    use super::*;
    use std::io::{Read, Write};

    fn w_u32s(w: &mut impl Write, v: &[u32]) -> std::io::Result<()> {
        w.write_all(&(v.len() as u64).to_le_bytes())?;
        w.write_all(unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 4) })
    }
    fn r_u32s(r: &mut impl Read) -> std::io::Result<Vec<u32>> {
        let n = r_u64(r)? as usize;
        let mut v = vec![0u32; n];
        r.read_exact(unsafe { std::slice::from_raw_parts_mut(v.as_mut_ptr() as *mut u8, n * 4) })?;
        Ok(v)
    }
    fn w_bytes(w: &mut impl Write, v: &[u8]) -> std::io::Result<()> {
        w.write_all(&(v.len() as u64).to_le_bytes())?;
        w.write_all(v)
    }
    fn r_bytes(r: &mut impl Read) -> std::io::Result<Vec<u8>> {
        let n = r_u64(r)? as usize;
        let mut v = vec![0u8; n];
        r.read_exact(&mut v)?;
        Ok(v)
    }
    fn r_u64(r: &mut impl Read) -> std::io::Result<u64> {
        let mut b = [0u8; 8];
        r.read_exact(&mut b)?;
        Ok(u64::from_le_bytes(b))
    }
    fn w_bms(w: &mut impl Write, v: &[RoaringBitmap]) -> std::io::Result<()> {
        w.write_all(&(v.len() as u64).to_le_bytes())?;
        for b in v {
            b.serialize_into(&mut *w)?;
        }
        Ok(())
    }
    fn r_bms(r: &mut impl Read) -> std::io::Result<Vec<RoaringBitmap>> {
        let n = r_u64(r)? as usize;
        (0..n).map(|_| RoaringBitmap::deserialize_unchecked_from(&mut *r)).collect()
    }

    pub fn write_shard(sh: &Shard, w: &mut impl Write) -> std::io::Result<()> {
        w_bms(w, &sh.cat)?;
        w_bms(w, &sh.bucket)?;
        w_u32s(w, &sh.term.off)?;
        w_bytes(w, &sh.term.arena)?;
        w_u32s(w, &sh.term.dense_keys)?;
        w_bms(w, &sh.term.dense)?;
        w_bms(w, std::slice::from_ref(&sh.stock))?;
        w_bms(w, std::slice::from_ref(&sh.rocket))
    }
    pub fn read_shard(r: &mut impl Read) -> std::io::Result<Shard> {
        let cat = r_bms(r)?;
        let bucket = r_bms(r)?;
        let term = Terms { off: r_u32s(r)?, arena: r_bytes(r)?, dense_keys: r_u32s(r)?, dense: r_bms(r)? };
        let stock = r_bms(r)?.pop().unwrap();
        let rocket = r_bms(r)?.pop().unwrap();
        Ok(Shard { cat, bucket, term, stock, rocket })
    }

    /// One file per shard plus one for the columns; shards written/read in parallel.
    pub fn save(ix: &Index, dir: &std::path::Path) -> std::io::Result<u64> {
        std::fs::create_dir_all(dir)?;
        ix.shards.par_iter().enumerate().try_for_each(|(i, sh)| -> std::io::Result<()> {
            let mut w = std::io::BufWriter::with_capacity(1 << 22, std::fs::File::create(dir.join(format!("shard{i:03}.bin")))?);
            write_shard(&sh.read(), &mut w)?;
            w.into_inner()?.sync_all()
        })?;
        let mut w = std::io::BufWriter::new(std::fs::File::create(dir.join("columns.bin"))?);
        w.write_all(&(ix.n as u64).to_le_bytes())?;
        w.write_all(&(ix.shard_size as u64).to_le_bytes())?;
        w_u32s(&mut w, &ix.brand)?;
        w_u32s(&mut w, &ix.price.iter().map(|p| p.load(Relaxed)).collect::<Vec<_>>())?;
        w_bytes(&mut w, &ix.bucket.iter().map(|b| b.load(Relaxed)).collect::<Vec<_>>())?;
        w.into_inner()?.sync_all()?;
        Ok(std::fs::read_dir(dir)?.map(|e| e.unwrap().metadata().unwrap().len()).sum())
    }
    pub fn load(dir: &std::path::Path, s: usize) -> std::io::Result<Index> {
        let shards: Vec<RwLock<Shard>> = (0..s)
            .into_par_iter()
            .map(|i| {
                let mut r = std::io::BufReader::with_capacity(1 << 22, std::fs::File::open(dir.join(format!("shard{i:03}.bin")))?);
                read_shard(&mut r).map(RwLock::new)
            })
            .collect::<std::io::Result<_>>()?;
        let mut r = std::io::BufReader::new(std::fs::File::open(dir.join("columns.bin"))?);
        let n = r_u64(&mut r)? as u32;
        let shard_size = r_u64(&mut r)? as u32;
        let brand = r_u32s(&mut r)?;
        let price = r_u32s(&mut r)?.into_iter().map(AtomicU32::new).collect();
        let bucket = r_bytes(&mut r)?.into_iter().map(AtomicU8::new).collect();
        Ok(Index { shards, shard_size, n, brand, price, bucket })
    }

    /// 64-bit digest of the mutable state (stock bitmaps, price buckets, prices) for replica checks.
    pub fn state_digest(ix: &Index) -> u64 {
        let mut h = 0xcbf29ce484222325u64;
        let mut eat = |x: u64| h = (h ^ x).wrapping_mul(0x100000001b3).rotate_left(5);
        for sh in &ix.shards {
            let sh = sh.read();
            for d in sh.stock.iter() {
                eat(d as u64);
            }
            for (b, bm) in sh.bucket.iter().enumerate() {
                eat(1 << 40 | b as u64);
                for d in bm.iter() {
                    eat(d as u64);
                }
            }
        }
        for p in &ix.price {
            eat(p.load(Relaxed) as u64);
        }
        h
    }
}

pub struct Index {
    pub shards: Vec<RwLock<Shard>>,
    pub shard_size: u32,
    pub n: u32,
    pub brand: Vec<u32>,
    pub price: Vec<AtomicU32>,
    pub bucket: Vec<AtomicU8>,
}

/// Build bitmaps for keys 0..n_keys from (key << 32 | docid) pairs: sort once, then create each
/// bitmap from its sorted run so containers are allocated at their final size.
pub fn bitmaps_from_pairs(mut pairs: Vec<u64>, n_keys: usize) -> Vec<RoaringBitmap> {
    pairs.par_sort_unstable();
    pairs.dedup(); // a title can repeat a word
    let mut out: Vec<RoaringBitmap> = Vec::with_capacity(n_keys);
    let mut i = 0;
    for k in 0..n_keys as u64 {
        let start = i;
        while i < pairs.len() && pairs[i] >> 32 == k {
            i += 1;
        }
        let mut b = RoaringBitmap::from_sorted_iter(pairs[start..i].iter().map(|p| *p as u32)).expect("sorted, unique");
        b.optimize();
        out.push(b);
    }
    out
}

/// Build one shard at a time; generation and sorting are parallel inside each shard.
pub fn build(n: u32, s: u32, _parallel: usize) -> Index {
    let shard_size = n.div_ceil(s);
    let mut shards = Vec::with_capacity(s as usize);
    for si in 0..s {
        let (lo, hi) = (si * shard_size, ((si + 1) * shard_size).min(n));
        let docs: Vec<(u32, Doc)> = (lo..hi).into_par_iter().map(|d| (d, doc(d as u64))).collect();
        let pairs = |f: &(dyn Fn(&Doc) -> u32 + Sync)| -> Vec<u64> {
            docs.par_iter().map(|(d, x)| ((f(x) as u64) << 32) | *d as u64).collect()
        };
        let cat = bitmaps_from_pairs(pairs(&|x| x.cat), N_CAT as usize);
        let bucket = bitmaps_from_pairs(pairs(&|x| bucket_of(x.price)), N_BUCKET as usize);
        let tp: Vec<u64> = docs
            .par_iter()
            .flat_map_iter(|(d, x)| x.terms.iter().map(move |t| ((*t as u64) << 32) | *d as u64))
            .collect();
        let term = Terms::from_pairs(tp, VOCAB as usize);
        let mut stock = RoaringBitmap::from_sorted_iter(docs.iter().filter(|x| x.1.stock).map(|x| x.0)).unwrap();
        let mut rocket = RoaringBitmap::from_sorted_iter(docs.iter().filter(|x| x.1.rocket).map(|x| x.0)).unwrap();
        stock.optimize();
        rocket.optimize();
        shards.push(RwLock::new(Shard { cat, bucket, term, stock, rocket }));
        if std::env::var("MEMDEBUG").is_ok() {
            eprintln!("shard {si}: {}", mem_status());
        }
    }
    let brand: Vec<u32> = (0..n as u64).into_par_iter().map(|d| doc(d).brand).collect();
    let price: Vec<AtomicU32> = (0..n as u64).into_par_iter().map(|d| AtomicU32::new(doc(d).price)).collect();
    let bucket: Vec<AtomicU8> =
        price.par_iter().map(|p| AtomicU8::new(bucket_of(p.load(Relaxed)) as u8)).collect();
    Index { shards, shard_size, n, brand, price, bucket }
}

impl Index {
    pub fn bitmap_mb(&self) -> (f64, f64) {
        let (mut all, mut terms) = (0usize, 0usize);
        for sh in &self.shards {
            let sh = sh.read();
            let t: usize = sh.term.bytes();
            terms += t;
            all += t
                + sh.cat.iter().chain(&sh.bucket).map(|b| b.serialized_size()).sum::<usize>()
                + sh.stock.serialized_size()
                + sh.rocket.serialized_size();
        }
        (all as f64 / 1048576.0, terms as f64 / 1048576.0)
    }
    pub fn column_mb(&self) -> f64 {
        (self.n as usize * 9) as f64 / 1048576.0
    }

    // -------------------------------------------------------------- updates
    pub fn toggle_stock(&self, d: u32) {
        let mut sh = self.shards[(d / self.shard_size) as usize].write();
        if !sh.stock.remove(d) {
            sh.stock.insert(d);
        }
    }
    pub fn set_price(&self, d: u32, p: u32) {
        let (ob, nb) = (self.bucket[d as usize].load(Relaxed) as u32, bucket_of(p));
        if ob != nb {
            let mut sh = self.shards[(d / self.shard_size) as usize].write();
            sh.bucket[ob as usize].remove(d);
            sh.bucket[nb as usize].insert(d);
            self.bucket[d as usize].store(nb as u8, Relaxed);
        }
        self.price[d as usize].store(p, Relaxed);
    }

    /// Apply a batch (e.g. one Kafka poll) taking each shard's write lock once.
    pub fn apply_batch(&self, batch: &mut [Update]) {
        batch.sort_unstable_by_key(|u| u.doc());
        for group in batch.chunk_by(|a, b| a.doc() / self.shard_size == b.doc() / self.shard_size) {
            let mut sh = self.shards[(group[0].doc() / self.shard_size) as usize].write();
            for u in group {
                match *u {
                    Update::ToggleStock(d) => {
                        if !sh.stock.remove(d) {
                            sh.stock.insert(d);
                        }
                    }
                    Update::SetStock(d, v) => {
                        if v {
                            sh.stock.insert(d);
                        } else {
                            sh.stock.remove(d);
                        }
                    }
                    Update::Price(d, p) => {
                        let (ob, nb) = (self.bucket[d as usize].load(Relaxed) as u32, bucket_of(p));
                        if ob != nb {
                            sh.bucket[ob as usize].remove(d);
                            sh.bucket[nb as usize].insert(d);
                            self.bucket[d as usize].store(nb as u8, Relaxed);
                        }
                        self.price[d as usize].store(p, Relaxed);
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
pub enum Update {
    ToggleStock(u32),
    SetStock(u32, bool),
    Price(u32, u32),
}
impl Update {
    pub fn doc(&self) -> u32 {
        match *self {
            Update::ToggleStock(d) | Update::SetStock(d, _) | Update::Price(d, _) => d,
        }
    }
}

// ------------------------------------------------------------------ queries
#[derive(Clone, Debug)]
pub enum Clause {
    Cat(u32),
    Term(u32),
    Stock,
    Rocket,
    Buckets(u32, u32), // union of buckets [lo, hi)
}

/// A posting source: a Roaring bitmap (dense) or a varint-delta byte run (sparse) with its count.
#[derive(Clone, Copy)]
pub enum Src<'a> {
    Bm(&'a RoaringBitmap),
    Vb(&'a [u8], u32),
}
impl<'a> Src<'a> {
    pub fn len(&self) -> u64 {
        match self {
            Src::Bm(b) => b.len(),
            Src::Vb(_, n) => *n as u64,
        }
    }
    pub fn iter(self) -> It<'a> {
        match self {
            Src::Bm(b) => It::Bm(b.iter()),
            Src::Vb(b, n) => It::Vb { b, p: 0, prev: 0, left: n },
        }
    }
    pub fn to_bitmap(self) -> RoaringBitmap {
        match self {
            Src::Bm(b) => b.clone(),
            Src::Vb(..) => {
                let mut it = self.iter();
                RoaringBitmap::from_sorted_iter(std::iter::from_fn(|| it.next())).unwrap()
            }
        }
    }
}

pub fn clause_src<'a>(sh: &'a Shard, c: &Clause) -> Vec<Src<'a>> {
    match c {
        Clause::Cat(x) => vec![Src::Bm(&sh.cat[*x as usize])],
        Clause::Term(x) => vec![sh.term.src(*x)],
        Clause::Stock => vec![Src::Bm(&sh.stock)],
        Clause::Rocket => vec![Src::Bm(&sh.rocket)],
        Clause::Buckets(a, b) => sh.bucket[*a as usize..*b as usize].iter().map(Src::Bm).collect(),
    }
}
fn union(v: Vec<Src>) -> RoaringBitmap {
    if v.len() == 1 {
        return v[0].to_bitmap();
    }
    v.into_iter().fold(RoaringBitmap::new(), |mut a, s| {
        match s {
            Src::Bm(b) => a |= b,
            Src::Vb(..) => a |= s.to_bitmap(),
        }
        a
    })
}

/// Full AND within one shard. A sparse (varint) driver walks the other clauses with monotone
/// cursors; otherwise container-wise bitmap AND.
pub fn and_full(sh: &Shard, q: &[Clause]) -> RoaringBitmap {
    let mut sets: Vec<(u64, Vec<Src>)> =
        q.iter().map(|c| { let v = clause_src(sh, c); (v.iter().map(|b| b.len()).sum(), v) }).collect();
    sets.sort_by_key(|x| x.0);
    if let [Src::Vb(..)] = sets[0].1[..] {
        let mut drv = sets[0].1[0].iter();
        let mut rest: Vec<Cur> = sets[1..].iter().map(|(_, v)| Cur::new(v.clone())).collect();
        let mut out = Vec::new();
        'd: while let Some(d) = drv.next() {
            for c in rest.iter_mut() {
                c.seek(d);
                match c.cur() {
                    None => break 'd,
                    Some(v) if v != d => continue 'd,
                    _ => {}
                }
            }
            out.push(d);
        }
        return RoaringBitmap::from_sorted_iter(out).unwrap();
    }
    let mut it = sets.into_iter();
    let mut acc = union(it.next().unwrap().1);
    for (_, bs) in it {
        if acc.is_empty() {
            break;
        }
        match bs[..] {
            [Src::Bm(b)] => acc &= b,
            _ => acc &= union(bs),
        }
    }
    acc
}

pub enum It<'a> {
    Bm(Iter<'a>),
    Vb { b: &'a [u8], p: usize, prev: u32, left: u32 },
}
impl It<'_> {
    #[inline]
    pub fn next(&mut self) -> Option<u32> {
        match self {
            It::Bm(i) => i.next(),
            It::Vb { b, p, prev, left } => {
                if *left == 0 {
                    return None;
                }
                *left -= 1;
                *prev += get_varint(b, p);
                Some(*prev)
            }
        }
    }
    /// Position so that the next `next()` returns the first value >= t.
    pub fn advance_to(&mut self, t: u32) {
        match self {
            It::Bm(i) => i.advance_to(t),
            It::Vb { b, p, prev, left } => {
                // sequential decode; lists are short (< DENSE_MIN per shard)
                while *left > 0 {
                    let mut q = *p;
                    let v = *prev + get_varint(b, &mut q);
                    if v >= t {
                        return;
                    }
                    *p = q;
                    *prev = v;
                    *left -= 1;
                }
            }
        }
    }
}

pub struct Cur<'a> {
    its: Vec<It<'a>>,
    heads: Vec<Option<u32>>,
    size: u64,
}
impl<'a> Cur<'a> {
    pub fn new(bs: Vec<Src<'a>>) -> Self {
        let size = bs.iter().map(|b| b.len()).sum();
        let mut its: Vec<It<'a>> = bs.into_iter().map(|b| b.iter()).collect();
        let heads = its.iter_mut().map(|i| i.next()).collect();
        Cur { its, heads, size }
    }
    pub fn cur(&self) -> Option<u32> {
        self.heads.iter().flatten().min().copied()
    }
    pub fn seek(&mut self, t: u32) {
        for (it, h) in self.its.iter_mut().zip(self.heads.iter_mut()) {
            if let Some(v) = *h {
                if v < t {
                    it.advance_to(t);
                    *h = it.next();
                }
            }
        }
    }
}

/// Leapfrog AND within one shard, appending up to `need` matches in docid order.
pub fn and_leapfrog(sh: &Shard, q: &[Clause], need: usize, out: &mut Vec<u32>) {
    let mut curs: Vec<Cur> = q.iter().map(|c| Cur::new(clause_src(sh, c))).collect();
    curs.sort_by_key(|c| c.size);
    let mut target = match curs[0].cur() { Some(v) => v, None => return };
    let mut got = 0;
    'outer: loop {
        for c in curs.iter_mut() {
            c.seek(target);
            match c.cur() {
                None => break 'outer,
                Some(v) if v > target => {
                    target = v;
                    continue 'outer;
                }
                _ => {}
            }
        }
        out.push(target);
        got += 1;
        if got == need {
            return;
        }
        target += 1;
    }
}

impl Index {
    /// Baseline: full AND over every shard, single thread.
    pub fn topk_pop_baseline(&self, q: &[Clause]) -> Vec<u32> {
        let mut all = RoaringBitmap::new();
        for sh in &self.shards {
            all |= and_full(&sh.read(), q);
        }
        all.iter().take(K).collect()
    }

    pub fn topk_pop(&self, q: &[Clause], k: usize) -> Vec<u32> {
        let mut out = Vec::with_capacity(k);
        for sh in &self.shards {
            and_leapfrog(&sh.read(), q, k - out.len(), &mut out);
            if out.len() == k {
                break;
            }
        }
        out
    }

    /// Baseline: full AND, then select k cheapest (all shards in parallel).
    pub fn topk_price_full(&self, q: &[Clause]) -> Vec<(u32, u32)> {
        let mut c: Vec<(u32, u32)> = self
            .shards
            .par_iter()
            .flat_map_iter(|sh| {
                let r = and_full(&sh.read(), q);
                let mut v: Vec<(u32, u32)> = r.iter().map(|d| (self.price[d as usize].load(Relaxed), d)).collect();
                if v.len() > K {
                    v.select_nth_unstable(K);
                    v.truncate(K);
                }
                v
            })
            .collect();
        c.sort_unstable();
        c.truncate(K);
        c
    }

    /// Cheapest-first bucket walk: AND the query with one price bucket at a time (shards in
    /// parallel) and stop once k candidates are collected; exact because every cheaper bucket
    /// has been fully scanned.
    pub fn topk_price(&self, q: &[Clause]) -> Vec<(u32, u32)> {
        // Small results: one full AND is cheaper than up to 256 bucket passes.
        let est: u64 = q
            .iter()
            .map(|c| self.shards.iter().map(|sh| clause_src(&sh.read(), c).iter().map(|b| b.len()).sum::<u64>()).sum::<u64>())
            .min()
            .unwrap_or(0);
        if est < 200_000 {
            return self.topk_price_full(q);
        }
        let mut cand: Vec<(u32, u32)> = Vec::new();
        for b in 0..N_BUCKET {
            let mut qb = q.to_vec();
            qb.push(Clause::Buckets(b, b + 1));
            let found: Vec<(u32, u32)> = self
                .shards
                .par_iter()
                .flat_map_iter(|sh| {
                    let r = and_full(&sh.read(), &qb);
                    r.iter().map(|d| (self.price[d as usize].load(Relaxed), d)).collect::<Vec<_>>()
                })
                .collect();
            cand.extend(found);
            if cand.len() >= K {
                break;
            }
        }
        cand.sort_unstable();
        cand.truncate(K);
        cand
    }

    /// Exact brand facet (top 20) over the full result, shards in parallel.
    pub fn facet_exact(&self, q: &[Clause]) -> (u64, Vec<(u32, u32)>) {
        let parts: Vec<(u64, Vec<u32>)> = self
            .shards
            .par_iter()
            .map(|sh| {
                let r = and_full(&sh.read(), q);
                let mut f = vec![0u32; N_BRAND as usize];
                for d in r.iter() {
                    f[self.brand[d as usize] as usize] += 1;
                }
                (r.len(), f)
            })
            .collect();
        let mut total = 0;
        let mut f = vec![0u32; N_BRAND as usize];
        for (c, p) in parts {
            total += c;
            f.iter_mut().zip(p).for_each(|(a, b)| *a += b);
        }
        (total, top_facets(f))
    }

    /// Approximate brand facet over the first `m` matches (most popular first).
    pub fn facet_sampled(&self, q: &[Clause], m: usize) -> (u64, Vec<(u32, u32)>) {
        let ids = self.topk_pop(q, m);
        let mut f = vec![0u32; N_BRAND as usize];
        for d in &ids {
            f[self.brand[*d as usize] as usize] += 1;
        }
        (ids.len() as u64, top_facets(f))
    }
}

fn top_facets(f: Vec<u32>) -> Vec<(u32, u32)> {
    let mut v: Vec<(u32, u32)> = f.into_iter().enumerate().filter(|x| x.1 > 0).map(|(b, c)| (c, b as u32)).collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    v.truncate(20);
    v
}

// ------------------------------------------------------------------ query workload (same for every engine)
pub fn qu(i: u64, f: u64) -> f64 {
    unif(1_000_000_000 + i, f)
}
pub fn q_filter(i: u64) -> Vec<Clause> {
    vec![Clause::Cat(zipf(N_CAT, qu(i, 1))), Clause::Stock, Clause::Rocket]
}
/// Price window of ~1/7 of the log range (about 37 buckets), starting in the mid range.
pub fn q_price_window(i: u64) -> (u32, u32) {
    let lo = 64 + (qu(i, 3) * 96.0) as u32;
    (lo, lo + 37)
}
pub fn q_filter_price(i: u64) -> Vec<Clause> {
    let (lo, hi) = q_price_window(i);
    vec![Clause::Cat(zipf(N_CAT, qu(i, 1))), Clause::Stock, Clause::Buckets(lo, hi)]
}
pub fn q_terms(i: u64) -> (u32, u32) {
    (zipf(VOCAB, qu(i, 10)), zipf(VOCAB, qu(i, 11)))
}
pub fn q_kw2(i: u64) -> Vec<Clause> {
    let (a, b) = q_terms(i);
    vec![Clause::Term(a), Clause::Term(b), Clause::Stock]
}
pub fn q_kw1(i: u64) -> Vec<Clause> {
    vec![Clause::Term(q_terms(i).0), Clause::Stock]
}

/// HTTP JSON API on 127.0.0.1:9400 (keep-alive), for a like-for-like comparison with ES over HTTP.
/// POST /search {"q":[["cat",12],["term",5],["stock"],["rocket"],["buckets",64,101]],"sort":"pop"|"price"}
pub fn serve_http(ix: &Index) {
    let server = tiny_http::Server::http("127.0.0.1:9400").unwrap();
    println!("serving on 127.0.0.1:9400 with 4 worker threads");
    std::thread::scope(|s| {
        for _ in 0..4 {
            let server = &server;
            s.spawn(move || loop {
                let mut req = match server.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let mut body = String::new();
                std::io::Read::read_to_string(req.as_reader(), &mut body).unwrap();
                let v: serde_json::Value = serde_json::from_str(&body).unwrap();
                let q: Vec<Clause> = v["q"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| {
                        let a = c.as_array().unwrap();
                        let n = |i: usize| a[i].as_u64().unwrap() as u32;
                        match a[0].as_str().unwrap() {
                            "cat" => crate::Clause::Cat(n(1)),
                            "term" => crate::Clause::Term(n(1)),
                            "stock" => crate::Clause::Stock,
                            "rocket" => crate::Clause::Rocket,
                            "buckets" => crate::Clause::Buckets(n(1), n(2)),
                            x => panic!("{x}"),
                        }
                    })
                    .collect();
                let t = std::time::Instant::now();
                let ids: Vec<u32> = if v["sort"] == "price" {
                    ix.topk_price(&q).into_iter().map(|x| x.1).collect()
                } else {
                    ix.topk_pop(&q, K)
                };
                let out = serde_json::json!({"took_us": t.elapsed().as_micros() as u64,
                    "hits": ids.iter().map(|d| serde_json::json!({"_id": d.to_string()})).collect::<Vec<_>>()});
                let resp = tiny_http::Response::from_string(out.to_string())
                    .with_header("Content-Type: application/json".parse::<tiny_http::Header>().unwrap());
                let _ = req.respond(resp);
            });
        }
    });
}

