# exact price windows (not on bucket edges): filter-only and with stock, sorted by popularity and by price;
# results ES vs ours and client-side latency
import json, random, time, urllib.request, statistics
def q(base, body):
    t = time.perf_counter()
    r = urllib.request.urlopen(urllib.request.Request(base + "/real/_search", json.dumps(body).encode(), {"Content-Type": "application/json"})).read()
    return (time.perf_counter() - t) * 1e3, [h["_id"] for h in json.loads(r)["hits"]["hits"]]
random.seed(7)
qs = []
for i in range(400):
    kind = i % 4
    if kind == 0:   lo = random.randint(100, 9_999_000); hi = lo + random.choice([1, 10, 1000])          # narrow, anywhere
    elif kind == 1: lo = 9_999_999; hi = 10_000_000                                                       # exactly the "no price" value
    elif kind == 2: lo = 9_999_000 + random.randint(0, 900); hi = lo + random.randint(1, 50)               # just below it (empty)
    else:           lo = random.randint(1000, 50_000); hi = lo + random.randint(1, 20_000)                 # ordinary ranges
    f = [{"range": {"price": {"gte": lo, "lt": hi}}}] + ([{"term": {"stock": True}}] if i % 2 else [])
    sort = [{"pop": "asc"}] if i % 3 else [{"price": "asc"}]
    qs.append({"size": 10, "track_total_hits": False, "_source": False, "sort": sort, "query": {"bool": {"filter": f}}})
for b in qs[:50]: q("http://127.0.0.1:9200", b); q("http://127.0.0.1:9400", b)
res = {"es": [], "ours": []}; same = 0; same_set = 0
for b in qs:
    le, ie = q("http://127.0.0.1:9200", b); lo_, io = q("http://127.0.0.1:9400", b)
    res["es"].append(le); res["ours"].append(lo_); same += ie == io; same_set += sorted(ie) == sorted(io)
def p(v, x): v = sorted(v); return v[min(len(v) - 1, int(round((len(v) - 1) * x)))]
for k in res: print(f"{k:5} p50 {p(res[k], .5):.2f} p99 {p(res[k], .99):.2f} max {max(res[k]):.1f} ms")
print(f"top-10 identical {same}/{len(qs)} (same set {same_set})")
import collections
by = collections.defaultdict(lambda: {"es": [], "ours": []})
names = ["narrow anywhere", "= no-price value", "just below it", "ordinary range"]
for i, b in enumerate(qs):
    key = (names[i % 4], "pop" if i % 3 else "price", "stock" if i % 2 else "-")
    by[key]["es"].append(res["es"][i]); by[key]["ours"].append(res["ours"][i])
for k in sorted(by):
    print(k, "ES max %.1f ours max %.1f ms (n=%d)" % (max(by[k]["es"]), max(by[k]["ours"]), len(by[k]["es"])))
