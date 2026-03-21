"""
build_orcid_cache.py
Fetches ORCID IDs for your top authors and stores them locally.
Run ONCE after ingestion: py -3.14 build_orcid_cache.py

Strategy:
  - Takes top N authors by paper count from your DB
  - Searches public ORCID API for each (no auth needed)
  - Stores results in a local orcid_cache table
  - server.py reads from cache first before calling ORCID API
"""

import sqlite3, urllib.request, urllib.parse, json, time
from pathlib import Path

DB_PATH    = "research_catalogue.db"
TOP_N      = 2000   # how many top authors to look up
SLEEP_S    = 0.3    # seconds between API calls (be polite to ORCID)
TIMEOUT    = 6      # seconds per request

if not Path(DB_PATH).exists():
    print(f"ERROR: {DB_PATH} not found")
    exit(1)

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
cur  = conn.cursor()

# ── Step 1: Create orcid_cache table ─────────────────────────────────────────
print("[1] Creating orcid_cache table...")
conn.execute("""
    CREATE TABLE IF NOT EXISTS orcid_cache (
        author_name  TEXT PRIMARY KEY,
        orcid_id     TEXT,
        orcid_url    TEXT,
        full_name    TEXT,
        fetched_at   TEXT DEFAULT (datetime('now')),
        status       TEXT  -- 'found' | 'not_found' | 'error'
    )
""")
conn.execute("CREATE INDEX IF NOT EXISTS idx_orcid_name ON orcid_cache(author_name)")
conn.commit()
print("    Done.")

# ── Step 2: Get top authors from DB ──────────────────────────────────────────
print(f"[2] Loading top {TOP_N} authors by paper count...")

# Use first_author column if available for cleaner names
has_first = cur.execute(
    "SELECT COUNT(*) FROM pragma_table_info('publications') WHERE name='first_author'"
).fetchone()[0]

if has_first:
    rows = cur.execute(
        """SELECT first_author, COUNT(*) as cnt
           FROM publications
           WHERE first_author IS NOT NULL AND first_author != ''
           GROUP BY first_author
           ORDER BY cnt DESC
           LIMIT ?""",
        (TOP_N,)
    ).fetchall()
else:
    # Fall back to parsing authors column
    rows = cur.execute(
        """SELECT authors, COUNT(*) as cnt
           FROM publications
           WHERE authors IS NOT NULL
           GROUP BY authors
           ORDER BY cnt DESC
           LIMIT ?""",
        (TOP_N,)
    ).fetchall()

# Get already-cached authors to skip them
already_cached = {r[0] for r in cur.execute(
    "SELECT author_name FROM orcid_cache"
).fetchall()}

authors_to_fetch = [
    (name, cnt) for name, cnt in rows
    if name and name not in already_cached
]

print(f"    Found {len(rows)} top authors")
print(f"    Already cached: {len(already_cached)}")
print(f"    Need to fetch : {len(authors_to_fetch)}")
print()

# ── Step 3: Fetch ORCID for each author ──────────────────────────────────────
print("[3] Fetching ORCID IDs from public API...")
print("    (This may take a while — being polite with rate limiting)")
print()

found = 0
not_found = 0
errors = 0

for i, (author_name, paper_count) in enumerate(authors_to_fetch, 1):
    # Search ORCID API
    try:
        query   = urllib.parse.quote(f'"{author_name}"')
        url     = f"https://pub.orcid.org/v3.0/search/?q={query}&rows=3"
        req     = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "NEXUS-Research-Engine/1.0"
        })
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            data = json.loads(resp.read())

        results = data.get("result") or []

        if not results:
            # No match found
            cur.execute(
                "INSERT OR REPLACE INTO orcid_cache (author_name,orcid_id,orcid_url,full_name,status) VALUES (?,?,?,?,?)",
                (author_name, None, None, None, "not_found")
            )
            not_found += 1
            status_str = "—"
        else:
            # Take first result
            item     = results[0]
            orcid_id = (item.get("orcid-identifier") or {}).get("path","")
            orcid_url = f"https://orcid.org/{orcid_id}" if orcid_id else None

            # Fetch full name from profile
            full_name = author_name  # default
            if orcid_id:
                try:
                    prof_req = urllib.request.Request(
                        f"https://pub.orcid.org/v3.0/{orcid_id}/person",
                        headers={"Accept":"application/json","User-Agent":"NEXUS-Research-Engine/1.0"}
                    )
                    with urllib.request.urlopen(prof_req, timeout=4) as pr:
                        pdata     = json.loads(pr.read())
                        name_data = pdata.get("name") or {}
                        given     = (name_data.get("given-names")  or {}).get("value","")
                        family    = (name_data.get("family-name")   or {}).get("value","")
                        if given or family:
                            full_name = f"{given} {family}".strip()
                except Exception:
                    pass

            cur.execute(
                "INSERT OR REPLACE INTO orcid_cache (author_name,orcid_id,orcid_url,full_name,status) VALUES (?,?,?,?,?)",
                (author_name, orcid_id, orcid_url, full_name, "found")
            )
            found += 1
            status_str = f"✓ {orcid_id}"

        conn.commit()

        # Progress
        if i % 10 == 0 or i <= 5:
            print(f"  [{i:>4}/{len(authors_to_fetch)}]  {author_name[:35]:<35}  {paper_count:>4} papers  {status_str}")

    except Exception as e:
        cur.execute(
            "INSERT OR REPLACE INTO orcid_cache (author_name,status) VALUES (?,?)",
            (author_name, "error")
        )
        conn.commit()
        errors += 1
        if i % 10 == 0:
            print(f"  [{i:>4}/{len(authors_to_fetch)}]  ERROR: {author_name[:35]}  {e}")

    time.sleep(SLEEP_S)

# ── Step 4: Summary ───────────────────────────────────────────────────────────
total_cached = cur.execute("SELECT COUNT(*) FROM orcid_cache").fetchone()[0]
total_found  = cur.execute("SELECT COUNT(*) FROM orcid_cache WHERE status='found'").fetchone()[0]

print()
print("=" * 55)
print("  ORCID cache complete")
print("=" * 55)
print(f"  Authors fetched : {len(authors_to_fetch):>6,}")
print(f"  ORCID found     : {found:>6,}  ({found/max(1,len(authors_to_fetch))*100:.1f}%)")
print(f"  Not found       : {not_found:>6,}")
print(f"  Errors          : {errors:>6,}")
print(f"  Total in cache  : {total_cached:>6,}")
print(f"  Total with ORCID: {total_found:>6,}")
print()

# Show sample results
print("  Sample cached entries:")
samples = cur.execute(
    "SELECT author_name, orcid_id, full_name FROM orcid_cache WHERE status='found' LIMIT 10"
).fetchall()
for s in samples:
    print(f"    {s[0]:<30} → {s[1]}  ({s[2]})")

conn.close()
print()
print("  Next: restart uvicorn — researcher profiles now load ORCID from cache")