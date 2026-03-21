"""
build_summary.py — pre-computes analytics summaries for instant queries
Run ONCE after ingestion: py -3.14 build_summary.py
Takes ~2-5 minutes, makes analytics + category instant forever after.
"""
import sqlite3, time
from pathlib import Path

DB = "research_catalogue.db"
if not Path(DB).exists():
    print(f"ERROR: {DB} not found"); exit(1)

conn = sqlite3.connect(DB)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA cache_size=-128000")
conn.execute("PRAGMA temp_store=MEMORY")
cur  = conn.cursor()
t0   = time.perf_counter()

total = cur.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
print(f"Building summaries for {total:,} records...")

# ── 1. Year summary table ─────────────────────────────────────────────────────
print("[1] Year counts...")
conn.execute("DROP TABLE IF EXISTS summary_by_year")
conn.execute("""
    CREATE TABLE summary_by_year AS
    SELECT year, COUNT(*) as count
    FROM publications
    WHERE year IS NOT NULL AND year > 0
    GROUP BY year
    ORDER BY year
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_sby_year ON summary_by_year(year)")
conn.commit()
yr_rows = cur.execute("SELECT COUNT(*) FROM summary_by_year").fetchone()[0]
print(f"    {yr_rows} year rows built")

# ── 2. Category summary table ─────────────────────────────────────────────────
print("[2] Category counts (this takes a few minutes)...")
conn.execute("DROP TABLE IF EXISTS summary_by_category")
conn.execute("""
    CREATE TABLE summary_by_category (
        category TEXT PRIMARY KEY,
        count    INTEGER,
        year_min INTEGER,
        year_max INTEGER
    )
""")

# Read all categories and split them
BATCH = 100_000
offset = 0
cat_map = {}
done = 0

while True:
    rows = cur.execute(
        "SELECT categories, year FROM publications WHERE categories IS NOT NULL LIMIT ? OFFSET ?",
        (BATCH, offset)
    ).fetchall()
    if not rows: break

    for cats_str, year in rows:
        for cat in cats_str.split(","):
            cat = cat.strip().lower()
            if not cat: continue
            if cat not in cat_map:
                cat_map[cat] = {"count":0,"year_min":9999,"year_max":0}
            cat_map[cat]["count"] += 1
            if year:
                cat_map[cat]["year_min"] = min(cat_map[cat]["year_min"], year)
                cat_map[cat]["year_max"] = max(cat_map[cat]["year_max"], year)

    done   += len(rows)
    offset += BATCH
    elapsed = time.perf_counter()-t0
    print(f"    {done:>10,} / {total:,}  ({done/total*100:.1f}%)  {elapsed:.0f}s")

# Insert into summary table
conn.executemany(
    "INSERT OR REPLACE INTO summary_by_category (category,count,year_min,year_max) VALUES (?,?,?,?)",
    [(k, v["count"], v["year_min"] if v["year_min"]<9999 else None, v["year_max"] if v["year_max"]>0 else None)
     for k,v in cat_map.items()]
)
conn.execute("CREATE INDEX IF NOT EXISTS idx_sbc_count ON summary_by_category(count DESC)")
conn.commit()
print(f"    {len(cat_map)} unique categories stored")

# ── 3. Year+category cross table (top 50 categories × all years) ──────────────
print("[3] Year × category cross table...")
conn.execute("DROP TABLE IF EXISTS summary_year_cat")
conn.execute("""
    CREATE TABLE summary_year_cat (
        year     INTEGER,
        category TEXT,
        count    INTEGER,
        PRIMARY KEY (year, category)
    )
""")

# Get top 100 categories
top_cats = [r[0] for r in cur.execute(
    "SELECT category FROM summary_by_category ORDER BY count DESC LIMIT 100"
).fetchall()]

for cat in top_cats:
    rows = cur.execute(
        """SELECT year, COUNT(*) FROM publications
           WHERE categories LIKE ? AND year IS NOT NULL AND year > 0
           GROUP BY year""",
        (f"%{cat}%",)
    ).fetchall()
    conn.executemany(
        "INSERT OR REPLACE INTO summary_year_cat (year,category,count) VALUES (?,?,?)",
        [(r[0], cat, r[1]) for r in rows]
    )

conn.execute("CREATE INDEX IF NOT EXISTS idx_syc_cat  ON summary_year_cat(category)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_syc_year ON summary_year_cat(year)")
conn.commit()
print(f"    Year×category cross table built for {len(top_cats)} categories")

elapsed = time.perf_counter()-t0
print()
print("="*55)
print(f"  Summary tables built in {elapsed:.0f}s ({elapsed/60:.1f} min)")
print(f"  Analytics + category queries now instant")
print("="*55)
conn.close()