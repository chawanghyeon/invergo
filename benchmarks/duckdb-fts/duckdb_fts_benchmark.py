#!/usr/bin/env python3
"""DuckDB full-text search (fts extension) benchmark.

Measures on a synthetic, Zipf-distributed corpus:
  * bulk load time and FTS index build time
  * single-client BM25 top-k latency (p50/p95/p99) per query class
  * concurrent query throughput (QPS) for several worker counts
  * update cost: batched and single-row inserts and deletes with an
    incremental index (fts on DuckDB 2.0+), or staleness + full rebuild
    with a static index
  * process memory (RSS) and database file size

Results are printed and written as JSON.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import platform
import random
import string
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Appended rows carry this token so index visibility can be checked exactly.
MARKER = "zqxvjupdatemarker"

QUERY_SQL = (
    "SELECT id, score FROM ("
    "SELECT id, fts_main_docs.match_bm25(id, ?{opts}) AS score FROM docs"
    ") sq WHERE score IS NOT NULL ORDER BY score DESC LIMIT {top_k}"
)
COUNT_SQL = (
    "SELECT count(*) FROM ("
    "SELECT fts_main_docs.match_bm25(id, ?) AS score FROM docs"
    ") sq WHERE score IS NOT NULL"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=100_000, help="documents in the initial corpus")
    p.add_argument("--append", type=int, default=10_000, help="documents appended in the update phase (0 = skip)")
    p.add_argument("--append-batch", type=int, default=1_000, help="rows per INSERT statement when appending")
    p.add_argument("--single-inserts", type=int, default=100, help="single-row INSERT statements to time")
    p.add_argument("--delete", type=int, default=None, help="appended rows to delete (default: append/10)")
    p.add_argument("--workers", default="1,4,8", help="comma-separated concurrent worker counts")
    p.add_argument("--threads", type=int, default=os.cpu_count() or 4, help="DuckDB threads setting")
    p.add_argument("--duration", type=float, default=15.0, help="seconds per concurrency run")
    p.add_argument("--queries", type=int, default=50, help="queries per class in the latency phase")
    p.add_argument("--warmup", type=int, default=10, help="warm-up queries before measuring")
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--vocab", type=int, default=100_000, help="vocabulary size")
    p.add_argument("--min-words", type=int, default=20)
    p.add_argument("--max-words", type=int, default=120)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--load-batch", type=int, default=1_000_000, help="rows per INSERT during bulk load")
    p.add_argument("--db", type=Path, default=Path("fts_bench.duckdb"), help="database file (recreated)")
    p.add_argument("--out", type=Path, default=Path("results.json"))
    p.add_argument("--memory-limit", default=None, help="DuckDB memory_limit, e.g. 8GB")
    p.add_argument("--incremental", choices=["auto", "on", "off"], default="auto",
                   help="incremental index (DuckDB 2.0+ fts); auto falls back to static if unsupported")
    p.add_argument("--cluster-terms", action="store_true", help="pass cluster_terms=1 (DuckDB 2.0+ fts)")
    p.add_argument("--conjunctive", action="store_true", help="AND semantics for multi-term queries")
    p.add_argument("--fts-extension", default=None, help="path to a fts.duckdb_extension file to LOAD")
    p.add_argument("--allow-unsigned", action="store_true",
                   help="set allow_unsigned_extensions (needed for a locally built --fts-extension)")
    args = p.parse_args()
    args.worker_counts = [int(w) for w in args.workers.split(",") if w.strip()]
    if args.delete is None:
        args.delete = args.append // 10
    args.delete = min(args.delete, args.append)
    return args


# --------------------------------------------------------------------------- helpers

def percentile(sorted_vals: list[float], pct: float) -> float:
    k = (len(sorted_vals) - 1) * pct / 100
    lo, hi = math.floor(k), math.ceil(k)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def latency_summary(seconds: list[float]) -> dict:
    if not seconds:
        return {"count": 0}
    ms = sorted(s * 1000 for s in seconds)
    return {
        "count": len(ms),
        "mean_ms": round(sum(ms) / len(ms), 3),
        "p50_ms": round(percentile(ms, 50), 3),
        "p95_ms": round(percentile(ms, 95), 3),
        "p99_ms": round(percentile(ms, 99), 3),
        "max_ms": round(ms[-1], 3),
    }


def memory_snapshot(con: duckdb.DuckDBPyConnection) -> dict:
    out: dict = {}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    out["rss_mb"] = round(int(line.split()[1]) / 1024, 1)
                elif line.startswith("VmHWM:"):
                    out["peak_rss_mb"] = round(int(line.split()[1]) / 1024, 1)
    except OSError:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        out["peak_rss_mb"] = round(peak / (2**20 if sys.platform == "darwin" else 1024), 1)
    try:
        used = con.execute("SELECT sum(memory_usage_bytes) FROM duckdb_memory()").fetchone()[0]
        out["duckdb_buffer_mb"] = round(used / 2**20, 1)
    except duckdb.Error:
        pass
    return out


def storage_snapshot(con: duckdb.DuckDBPyConnection, db: Path) -> dict:
    con.execute("CHECKPOINT")
    wal = Path(f"{db}.wal")
    return {
        "db_file_mb": round(db.stat().st_size / 2**20, 1),
        "wal_mb": round(wal.stat().st_size / 2**20, 1) if wal.exists() else 0.0,
    }


def timed(fn, *a, **kw) -> tuple[float, object]:
    t0 = time.perf_counter()
    res = fn(*a, **kw)
    return time.perf_counter() - t0, res


def sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


# --------------------------------------------------------------------------- setup

def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    for f in (args.db, Path(f"{args.db}.wal")):
        if f.exists():
            f.unlink()
    config = {"threads": args.threads}
    if args.memory_limit:
        config["memory_limit"] = args.memory_limit
    if args.allow_unsigned:
        config["allow_unsigned_extensions"] = True
    # Incremental fts indexes need a v2.0.0+ storage format on persistent databases.
    try:
        con = duckdb.connect(str(args.db), config={**config, "storage_compatibility_version": "latest"})
    except duckdb.Error:
        con = duckdb.connect(str(args.db), config=config)
    if args.fts_extension:
        con.execute(f"LOAD {sql_str(args.fts_extension)}")
    else:
        try:
            con.execute("LOAD fts")
        except duckdb.Error:
            con.execute("INSTALL fts")
            con.execute("LOAD fts")
    return con


def environment(con: duckdb.DuckDBPyConnection) -> dict:
    fts = con.execute(
        "SELECT extension_version, install_mode FROM duckdb_extensions() WHERE extension_name = 'fts'"
    ).fetchone()
    mem_gb = None
    try:
        mem_gb = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (ValueError, OSError, AttributeError):
        pass
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duckdb_version": duckdb.__version__,
        "duckdb_source_id": con.execute("SELECT source_id FROM pragma_version()").fetchone()[0],
        "fts_extension_version": fts[0] if fts else None,
        "fts_install_mode": fts[1] if fts else None,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "memory_gb": mem_gb,
        "storage_compatibility_version": con.execute(
            "SELECT current_setting('storage_compatibility_version')").fetchone()[0],
    }


def make_vocab(size: int, seed: int) -> list[str]:
    """Unique letter-only words (the default fts tokenizer drops digits). Index i = Zipf rank i+1."""
    rng = random.Random(seed)
    words: set[str] = set()
    while len(words) < size:
        w = "".join(rng.choices(string.ascii_lowercase, k=rng.randint(4, 10)))
        if w != MARKER:
            words.add(w)
    ordered = sorted(words)
    rng.shuffle(ordered)
    return ordered


def generate_docs_sql(table: str, args: argparse.Namespace, start: int, end: int, marker: bool = False) -> str:
    """Deterministic docs: length uniform in [min_words, max_words], word rank ~ Zipf(s=1).

    rank = floor(V ** u) for u ~ U[0,1) gives P(rank = r) proportional to ln(1 + 1/r) ~ 1/r.
    """
    span = args.max_words - args.min_words + 1
    u = f"((hash(i, x, {args.seed}) >> 11)::DOUBLE / 9007199254740992.0)"
    body = (
        f"array_to_string(list_transform(range({args.min_words} + (hash(i, {args.seed}) % {span})::BIGINT), "
        f"lambda x: v.words[floor(exp({u} * ln({args.vocab})))::BIGINT]), ' ')"
    )
    if marker:
        body += f" || ' {MARKER}'"
    return (
        f"INSERT INTO {table} SELECT i AS id, {body} AS body FROM range({start}, {end}) t(i), "
        f"(SELECT list(word ORDER BY rank) AS words FROM bench_vocab) v"
    )


def make_queries(words: list[str], per_class: int, seed: int) -> dict[str, list[str]]:
    rng = random.Random(seed + 1)
    v = len(words)
    head_hi = max(1, v // 1000)
    mid_hi = max(head_hi + 1, v // 10)

    def zipf_word() -> str:
        return words[int(math.exp(rng.random() * math.log(v))) - 1]

    return {
        "head": [words[rng.randint(1, head_hi) - 1] for _ in range(per_class)],
        "mid": [words[rng.randint(head_hi + 1, mid_hi) - 1] for _ in range(per_class)],
        "tail": [words[rng.randint(mid_hi + 1, v - 1) - 1] for _ in range(per_class)],
        "multi3": [" ".join(zipf_word() for _ in range(3)) for _ in range(per_class)],
    }


# --------------------------------------------------------------------------- phases

def build_index(con: duckdb.DuckDBPyConnection, args: argparse.Namespace, incremental: bool,
                overwrite: bool = False) -> float:
    opts = []
    if overwrite:
        opts.append("overwrite=1")
    if incremental:
        opts.append("incremental=1")
    if args.cluster_terms:
        opts.append("cluster_terms=1")
    sql = "PRAGMA create_fts_index('docs', 'id', 'body'" + "".join(f", {o}" for o in opts) + ")"
    elapsed, _ = timed(con.execute, sql)
    return elapsed


def initial_index(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> tuple[str, float, str | None]:
    """Returns (mode, build seconds, fallback reason)."""
    if args.incremental == "off":
        return "static", build_index(con, args, incremental=False), None
    try:
        return "incremental", build_index(con, args, incremental=True), None
    except duckdb.Error as e:
        msg = str(e)
        if args.incremental == "on" or not any(w in msg.lower() for w in ("incremental", "trigger")):
            raise
        return "static", build_index(con, args, incremental=False), msg.splitlines()[0]


def term_df(con: duckdb.DuckDBPyConnection, terms: list[str]) -> float | None:
    try:
        return con.execute(
            "SELECT avg(coalesce(d.df, 0)) FROM (SELECT unnest(?::VARCHAR[]) AS w) q "
            "LEFT JOIN fts_main_docs.dict d ON d.term = stem(q.w, 'porter')",
            [terms],
        ).fetchone()[0]
    except duckdb.Error:
        return None


def run_latency(con: duckdb.DuckDBPyConnection, queries: dict[str, list[str]], sqls: dict[str, str],
                warmup: int) -> dict:
    cur = con.cursor()
    mixed = [q for qs in queries.values() for q in qs]
    for q in mixed[:: max(1, len(mixed) // max(1, warmup))][:warmup]:
        cur.execute(sqls["multi3"], [q]).fetchall()
    out = {}
    for cls, qs in queries.items():
        lat, hits = [], 0
        for q in qs:
            t0 = time.perf_counter()
            hits += len(cur.execute(sqls[cls], [q]).fetchall())
            lat.append(time.perf_counter() - t0)
        out[cls] = {**latency_summary(lat), "mean_hits": round(hits / len(qs), 2)}
    cur.close()
    return out


def run_concurrency(con: duckdb.DuckDBPyConnection, queries: dict[str, list[str]], sqls: dict[str, str],
                    workers: int, duration: float, seed: int) -> dict:
    tasks = [(cls, q) for cls, qs in queries.items() for q in qs]
    lat: list[list[float]] = [[] for _ in range(workers)]
    errors: list[str] = []
    barrier = threading.Barrier(workers + 1)
    deadline = [0.0]

    def worker(idx: int) -> None:
        cur = con.cursor()
        order = tasks[:]
        random.Random(seed + idx).shuffle(order)
        barrier.wait()
        i = 0
        try:
            while time.perf_counter() < deadline[0]:
                cls, q = order[i % len(order)]
                i += 1
                t0 = time.perf_counter()
                cur.execute(sqls[cls], [q]).fetchall()
                lat[idx].append(time.perf_counter() - t0)
        except Exception as e:  # noqa: BLE001 - recorded in the results
            errors.append(f"worker {idx}: {e}")
        finally:
            cur.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    deadline[0] = time.perf_counter() + duration
    barrier.wait()
    start = time.perf_counter()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    flat = [x for w in lat for x in w]
    return {
        "workers": workers,
        "elapsed_s": round(elapsed, 2),
        "queries": len(flat),
        "qps": round(len(flat) / elapsed, 2),
        **latency_summary(flat),
        "errors": errors,
    }


def run_updates(con: duckdb.DuckDBPyConnection, args: argparse.Namespace, mode: str) -> dict:
    base, n_append, n_single = args.rows, args.append, args.single_inserts
    first_single = base + n_append
    # Stage rows outside the timed regions; the no-index baseline gets identical statements.
    con.execute("CREATE TABLE bench_staged AS SELECT * FROM docs LIMIT 0")
    con.execute(generate_docs_sql("bench_staged", args, base, first_single + n_single, marker=True))
    con.execute("CREATE TABLE bench_noindex AS SELECT * FROM docs LIMIT 0")

    # Each timed region starts from an empty WAL so an automatic checkpoint does not land in only one of them.
    def append(table: str) -> float:
        con.execute("CHECKPOINT")
        t0 = time.perf_counter()
        for lo in range(base, first_single, args.append_batch):
            hi = min(lo + args.append_batch, first_single)
            con.execute(f"INSERT INTO {table} SELECT * FROM bench_staged WHERE id >= ? AND id < ?", [lo, hi])
        return time.perf_counter() - t0

    def singles(table: str) -> list[float]:
        con.execute("CHECKPOINT")
        lat = []
        for i in range(first_single, first_single + n_single):
            t0 = time.perf_counter()
            con.execute(f"INSERT INTO {table} SELECT * FROM bench_staged WHERE id = ?", [i])
            lat.append(time.perf_counter() - t0)
        return lat

    out: dict = {"index_mode": mode, "append_rows": n_append, "append_batch": args.append_batch}
    noidx_s, idx_s = append("bench_noindex"), append("docs")
    out["batched_insert"] = {
        "indexed_table_s": round(idx_s, 3),
        "indexed_rows_per_s": round(n_append / idx_s, 1) if idx_s else None,
        "no_index_baseline_s": round(noidx_s, 3),
        "no_index_rows_per_s": round(n_append / noidx_s, 1) if noidx_s else None,
        "slowdown_vs_no_index_x": round(idx_s / noidx_s, 2) if noidx_s else None,
    }
    if n_single:
        out["single_row_insert"] = {
            "indexed_table": latency_summary(singles("docs")),
            "no_index_baseline": latency_summary(singles("bench_noindex")),
        }

    expected = n_append + n_single
    visible = con.execute(COUNT_SQL, [MARKER]).fetchone()[0]
    out["after_insert"] = {"expected_visible": expected, "visible": visible, "index_up_to_date": visible == expected}

    if args.delete:
        lo, hi = base, base + args.delete
        con.execute("CHECKPOINT")
        noidx_s, _ = timed(con.execute, "DELETE FROM bench_noindex WHERE id >= ? AND id < ?", [lo, hi])
        con.execute("CHECKPOINT")
        idx_s, _ = timed(con.execute, "DELETE FROM docs WHERE id >= ? AND id < ?", [lo, hi])
        visible = con.execute(COUNT_SQL, [MARKER]).fetchone()[0]
        out["delete"] = {
            "rows": args.delete,
            "indexed_table_s": round(idx_s, 3),
            "no_index_baseline_s": round(noidx_s, 3),
            "expected_visible": expected - args.delete,
            "visible": visible,
        }

    num_docs = con.execute("SELECT count(*) FROM docs").fetchone()[0]
    try:
        index_docs = con.execute("SELECT num_docs FROM fts_main_docs.stats").fetchone()[0]
    except duckdb.Error:
        index_docs = None
    out["bm25_stats"] = {"table_rows": num_docs, "index_num_docs": index_docs,
                         "stats_current": index_docs == num_docs}
    con.execute("DROP TABLE bench_staged")
    con.execute("DROP TABLE bench_noindex")
    return out


# --------------------------------------------------------------------------- main

def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> int:
    args = parse_args()
    con = connect(args)
    res: dict = {
        "environment": environment(con),
        "config": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
    }
    phases: dict = {}
    res["phases"] = phases
    log(f"DuckDB {res['environment']['duckdb_version']}, fts {res['environment']['fts_extension_version']}, "
        f"threads={args.threads}")

    # Load
    words = make_vocab(args.vocab, args.seed)
    con.execute("CREATE TABLE bench_vocab AS SELECT unnest(range(1, ?::BIGINT + 1)) AS rank, "
                "unnest(?::VARCHAR[]) AS word", [len(words), words])
    con.execute("CREATE TABLE docs (id BIGINT NOT NULL, body VARCHAR)")
    t0 = time.perf_counter()
    for lo in range(0, args.rows, args.load_batch):
        con.execute(generate_docs_sql("docs", args, lo, min(lo + args.load_batch, args.rows)))
    load_s = time.perf_counter() - t0
    tokens, text_bytes = con.execute("SELECT sum(len(string_split(body, ' '))), sum(strlen(body)) FROM docs").fetchone()
    phases["load"] = {
        "rows": args.rows,
        "seconds": round(load_s, 2),
        "rows_per_s": round(args.rows / load_s, 1),
        "tokens": int(tokens),
        "text_mb": round(text_bytes / 2**20, 1),
        "storage": storage_snapshot(con, args.db),
        "memory": memory_snapshot(con),
    }
    log(f"load: {args.rows:,} docs in {load_s:.1f}s ({phases['load']['text_mb']} MB text)")

    # Index
    mode, build_s, fallback = initial_index(con, args)
    phases["index"] = {
        "mode": mode,
        "cluster_terms": args.cluster_terms,
        "seconds": round(build_s, 2),
        "docs_per_s": round(args.rows / build_s, 1),
        "incremental_fallback_reason": fallback,
        "storage": storage_snapshot(con, args.db),
        "memory": memory_snapshot(con),
    }
    phases["index"]["index_overhead_mb"] = round(
        phases["index"]["storage"]["db_file_mb"] - phases["load"]["storage"]["db_file_mb"], 1)
    log(f"index ({mode}): {build_s:.1f}s" + (f"  [fallback: {fallback}]" if fallback else ""))

    # Queries
    queries = make_queries(words, args.queries, args.seed)
    conj = ", conjunctive := 1" if args.conjunctive else ""
    sqls = {cls: QUERY_SQL.format(opts=conj if cls == "multi3" else "", top_k=args.top_k) for cls in queries}
    phases["latency"] = run_latency(con, queries, sqls, args.warmup)
    for cls in ("head", "mid", "tail"):
        phases["latency"][cls]["mean_df"] = term_df(con, queries[cls])
    phases["latency"]["memory"] = memory_snapshot(con)
    for cls in queries:
        s = phases["latency"][cls]
        log(f"latency {cls:6s}: p50 {s['p50_ms']:.1f} ms  p95 {s['p95_ms']:.1f} ms  p99 {s['p99_ms']:.1f} ms")

    phases["concurrency"] = []
    for w in args.worker_counts:
        r = run_concurrency(con, queries, sqls, w, args.duration, args.seed)
        r["memory"] = memory_snapshot(con)
        phases["concurrency"].append(r)
        log(f"concurrency {w:2d} workers: {r['qps']:.1f} QPS  p50 {r.get('p50_ms', 0):.1f} ms  "
            f"p99 {r.get('p99_ms', 0):.1f} ms" + (f"  errors={len(r['errors'])}" if r["errors"] else ""))

    # Updates
    if args.append:
        upd = run_updates(con, args, mode)
        upd["latency_after_updates"] = run_latency(con, queries, sqls, args.warmup)
        upd["storage_after_updates"] = storage_snapshot(con, args.db)
        rebuild_s = build_index(con, args, incremental=(mode == "incremental"), overwrite=True)
        visible = con.execute(COUNT_SQL, [MARKER]).fetchone()[0]
        upd["full_rebuild"] = {"seconds": round(rebuild_s, 2), "visible_after_rebuild": visible,
                               "storage": storage_snapshot(con, args.db)}
        upd["memory"] = memory_snapshot(con)
        phases["updates"] = upd
        bi = upd["batched_insert"]
        log(f"append {args.append:,} rows (batch {args.append_batch}): {bi['indexed_table_s']:.2f}s "
            f"({bi['slowdown_vs_no_index_x']}x no-index); visible {upd['after_insert']['visible']}/"
            f"{upd['after_insert']['expected_visible']}; full rebuild {rebuild_s:.1f}s")

    res["final"] = {"storage": storage_snapshot(con, args.db), "memory": memory_snapshot(con)}
    con.close()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(res, indent=2, ensure_ascii=False) + "\n")
    log(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
