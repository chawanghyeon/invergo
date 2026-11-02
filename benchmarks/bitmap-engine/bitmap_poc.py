"""Quick validation of a bitmap-state search engine for e-commerce.

docid = popularity rank (0 = most popular), so "top-k by popularity" = first k set bits.
Attribute values and title terms are Roaring bitmaps; stock/price updates are bit flips.
"""
import array
import os
import random
import sys
import time

import numpy as np
from pyroaring import BitMap

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000
TOK = 12              # title tokens per SKU
VOCAB = 200_000
N_CAT, N_BRAND, N_BUCKET = 5_000, 50_000, 40
Q = 1000              # queries per class
rng = np.random.default_rng(42)
random.seed(42)


def zipf(n_vals, size):
    """rank = floor(V ** u) - 1, P(r) ~ 1/r"""
    return (np.floor(np.exp(rng.random(size) * np.log(n_vals))) - 1).astype(np.uint32)


def bm(ids):
    return BitMap(array.array("I", np.ascontiguousarray(ids, dtype=np.uint32).tobytes()))


def group_bitmaps(keys, n_keys, docids=None):
    order = np.argsort(keys, kind="stable")
    sk = keys[order]
    ids = order.astype(np.uint32) if docids is None else docids[order]
    bounds = np.searchsorted(sk, np.arange(n_keys + 1))
    return [bm(ids[bounds[k]:bounds[k + 1]]) for k in range(n_keys)]


def rss_mb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024


def lat(fn, args_list):
    out = []
    for a in args_list:
        t0 = time.perf_counter()
        fn(*a)
        out.append((time.perf_counter() - t0) * 1000)
    out.sort()
    return f"p50 {np.percentile(out, 50):7.3f} ms  p99 {np.percentile(out, 99):7.3f} ms  max {out[-1]:7.3f} ms"


# ------------------------------------------------------------------ build
t0 = time.perf_counter()
category = zipf(N_CAT, N)
brand = zipf(N_BRAND, N)
price = np.exp(rng.normal(np.log(30_000), 1.0, N)).astype(np.uint32) + 100
edges = np.exp(np.linspace(np.log(100), np.log(int(price.max()) + 1), N_BUCKET + 1))
bucket = np.clip(np.searchsorted(edges, price, side="right") - 1, 0, N_BUCKET - 1).astype(np.uint32)
in_stock_mask = rng.random(N) < 0.9
rocket_mask = rng.random(N) < 0.4
tokens = zipf(VOCAB, N * TOK)
gen_s = time.perf_counter() - t0

t0 = time.perf_counter()
cat_bm = group_bitmaps(category, N_CAT)
brand_bm = group_bitmaps(brand, N_BRAND)
bucket_bm = group_bitmaps(bucket, N_BUCKET)
in_stock = bm(np.flatnonzero(in_stock_mask))
rocket = bm(np.flatnonzero(rocket_mask))
term_bm = group_bitmaps(tokens, VOCAB, docids=np.repeat(np.arange(N, dtype=np.uint32), TOK))
del tokens
all_bms = cat_bm + brand_bm + bucket_bm + term_bm + [in_stock, rocket]
for b in all_bms:
    b.run_optimize()
build_s = time.perf_counter() - t0

size = lambda bs: sum(len(b.serialize()) for b in bs) / 2**20
cols_mb = (category.nbytes + brand.nbytes + price.nbytes + bucket.nbytes) / 2**20
print(f"N={N:,} SKUs, {N * TOK:,} title tokens | gen {gen_s:.1f}s, bitmap build {build_s:.1f}s")
print(f"memory: terms {size(term_bm):,.0f} MB | attrs {size(cat_bm + brand_bm + bucket_bm + [in_stock, rocket]):,.0f} MB"
      f" | columns {cols_mb:,.0f} MB | process RSS {rss_mb():,.0f} MB")

# ------------------------------------------------------------------ queries
qcat = zipf(N_CAT, Q)
qterms = zipf(VOCAB, (Q, 2))
qlo = rng.integers(10, 25, Q)


def q_filter(c):  # category & in stock & rocket, top-10 by popularity
    r = cat_bm[c] & in_stock & rocket
    return r[:10] if len(r) >= 10 else r


def q_filter_price(c, lo):  # + price range (6 buckets), top-10
    pr = BitMap.union(*bucket_bm[lo:lo + 6])
    r = cat_bm[c] & in_stock & pr
    return r[:10] if len(r) >= 10 else r


def q_keyword(t1, t2):  # 2-term AND & in stock, top-10
    r = term_bm[t1] & term_bm[t2] & in_stock
    return r[:10] if len(r) >= 10 else r


def q_keyword_facet(t1):  # 1 term & in stock -> top-10 + brand facet counts (top 20)
    r = term_bm[t1] & in_stock
    ids = r.to_array()
    counts = np.bincount(brand[np.frombuffer(ids, dtype=np.uint32)], minlength=N_BRAND)
    return r[:10] if len(r) >= 10 else r, np.argpartition(-counts, 20)[:20]


def q_keyword_price_sort(t1, c):  # term & category & in stock, sort by price asc top-10
    r = term_bm[t1] & in_stock
    ids = np.frombuffer(r.to_array(), dtype=np.uint32)
    if len(ids) > 10:
        ids = ids[np.argpartition(price[ids], 10)[:10]]
    return ids


print("\nqueries (single thread, 1000 each; query terms/categories drawn Zipf like real traffic)")
print("  filter  cat & stock & rocket        ", lat(q_filter, [(int(c),) for c in qcat]))
print("  filter  cat & stock & price-range   ", lat(q_filter_price, [(int(c), int(l)) for c, l in zip(qcat, qlo)]))
print("  keyword t1 AND t2 & stock           ", lat(q_keyword, [(int(a), int(b)) for a, b in qterms]))
print("  keyword t1 & stock + brand facets   ", lat(q_keyword_facet, [(int(a),) for a, _ in qterms]))
print("  keyword t1 & stock, sort by price   ", lat(q_keyword_price_sort, [(int(a), 0) for a, _ in qterms]))
sizes = np.array([len(term_bm[int(a)]) for a, _ in qterms])
print(f"  (1-term result sizes: p50 {int(np.median(sizes)):,}, max {sizes.max():,} SKUs)")

# ------------------------------------------------------------------ updates
U = 1_000_000
docs = rng.integers(0, N, U).astype(np.uint32)
t0 = time.perf_counter()
for d in docs.tolist():  # stock toggle
    if d in in_stock:
        in_stock.discard(d)
    else:
        in_stock.add(d)
stock_s = time.perf_counter() - t0

new_prices = (price[docs] * rng.uniform(0.8, 1.2, U)).astype(np.uint32) + 1
new_buckets = np.clip(np.searchsorted(edges, new_prices, side="right") - 1, 0, N_BUCKET - 1)
t0 = time.perf_counter()
for d, p, nb in zip(docs.tolist(), new_prices.tolist(), new_buckets.tolist()):  # price change
    ob = bucket[d]
    if ob != nb:
        bucket_bm[ob].discard(d)
        bucket_bm[nb].add(d)
        bucket[d] = nb
    price[d] = p
price_s = time.perf_counter() - t0
print(f"\nupdates (single thread, Python loop): stock toggle {U / stock_s:,.0f}/s | price change {U / price_s:,.0f}/s")
print("visible immediately: next query reads the same bitmaps (no refresh / reindex)")
print(f"process RSS {rss_mb():,.0f} MB, cpu {os.cpu_count()}")
