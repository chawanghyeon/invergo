"""Stream Amazon Reviews 2023 item metadata (JSONL on stdin) to TSV:
rating_number, price_cents, store, category leaf, title  (tabs/newlines stripped)."""
import json
import sys

out = sys.stdout
n = 0
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    title = (m.get("title") or "").replace("\t", " ").replace("\n", " ").strip()
    if not title:
        continue
    price = m.get("price")
    try:
        cents = int(round(float(price) * 100)) if price not in (None, "", "None") else 0
    except (TypeError, ValueError):
        cents = 0
    cats = m.get("categories") or []
    cat = (cats[-1] if cats else (m.get("main_category") or "")).replace("\t", " ")
    store = (m.get("store") or "").replace("\t", " ").replace("\n", " ")
    out.write(f"{m.get('rating_number') or 0}\t{cents}\t{store}\t{cat}\t{title}\n")
    n += 1
print(n, file=sys.stderr)
