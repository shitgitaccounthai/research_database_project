"""
NEXUS Research Discovery Engine — FastAPI Backend
Install: pip install fastapi uvicorn
Run:     uvicorn server:app --reload --port 8000
Then open: http://localhost:8000
"""
import os, sqlite3, time
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

DB   = "research_catalogue.db"
app  = FastAPI(title="NEXUS Research API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def cols():
    if not os.path.exists(DB):
        return []
    with get_conn() as c:
        return [r[1] for r in c.execute("PRAGMA table_info(publications)")]

def has_fts() -> bool:
    """Check if FTS5 index exists."""
    if "fts" not in _cache:
        with get_conn() as c:
            r = c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='publications_fts'"
            ).fetchone()
            _cache["fts"] = r is not None
    return _cache["fts"]

def build_fts_query(q: str) -> str:
    """
    Build an FTS5 query string from user input.

    Rules:
      - Multi-word input like "surjeet singh" → phrase search "surjeet singh"
        so it only matches those words appearing TOGETHER, not separately.
      - Single word "cosmology" → prefix search cosmology*
        so "cosmological" also matches.
      - Quoted input from user preserved as-is.
      - Special FTS characters escaped to prevent syntax errors.
    """
    q = q.strip()
    if not q:
        return None

    # If user already quoted it, use as-is (they know what they want)
    if q.startswith('"') and q.endswith('"'):
        return q

    # Escape FTS special characters except spaces and quotes
    # FTS5 special chars: . * ^ ( ) [ ] { } | & ~ : " '
    safe = q.replace('"', '').replace("'", "").strip()

    words = safe.split()
    if not words:
        return None

    if len(words) == 1:
        # Single word → prefix match (catches plurals, suffixes)
        return f'"{words[0]}"*'
    else:
        # Multiple words → phrase search first, then fallback to all-words
        # "surjeet singh" matches the exact phrase
        # Also try prefix on last word for partial typing
        phrase = " ".join(words)
        return f'"{phrase}"'

def fts_search(q: str, limit: int, offset: int, where_extra: str = "",
               extra_params: list = []) -> tuple[list, int]:
    """
    Fast FTS5 search. Returns (rows, total_count).
    Uses phrase matching for multi-word queries.
    """
    fts_q = build_fts_query(q)

    with get_conn() as c:
        available = [r[1] for r in c.execute("PRAGMA table_info(publications)")]
        has_cats  = "categories" in available
        has_arxiv = "arxiv_id"   in available

        select = "p.doi, p.title, p.authors, p.year"
        if has_cats:  select += ", p.categories"
        if has_arxiv: select += ", p.arxiv_id"

        if fts_q:
            base_sql = f"""
                SELECT {select}
                FROM publications_fts f
                JOIN publications p ON p.rowid = f.rowid
                WHERE publications_fts MATCH ?
                {where_extra}
            """
            count_sql = f"""
                SELECT COUNT(*)
                FROM publications_fts f
                JOIN publications p ON p.rowid = f.rowid
                WHERE publications_fts MATCH ?
                {where_extra}
            """
            base_params  = [fts_q] + extra_params
            count_params = [fts_q] + extra_params
        else:
            base_sql = f"""
                SELECT {select} FROM publications p
                WHERE 1=1 {where_extra}
            """
            count_sql = f"SELECT COUNT(*) FROM publications p WHERE 1=1 {where_extra}"
            base_params  = extra_params
            count_params = extra_params

        total = c.execute(count_sql, count_params).fetchone()[0]
        rows  = c.execute(
            base_sql + f" LIMIT ? OFFSET ?",
            base_params + [limit, offset]
        ).fetchall()
        return [dict(r) for r in rows], total

def order_sql(sort: str) -> str:
    return {"newest":"ORDER BY year DESC","oldest":"ORDER BY year ASC",
            "title_az":"ORDER BY title ASC","title_za":"ORDER BY title DESC"
            }.get(sort, "ORDER BY year DESC")

# ── In-memory cache for expensive queries ─────────────────────────────────────
_cache: dict = {}

def _cached(key: str, fn):
    if key not in _cache:
        _cache[key] = fn()
    return _cache[key]

# ── Startup info ──────────────────────────────────────────────────────────────
@app.get("/api/info")
def info():
    if not os.path.exists(DB):
        return {"error": f"DB not found at {os.path.abspath(DB)}",
                "total":0,"min_year":1991,"max_year":datetime.now().year,"columns":[]}

    def _load():
        c = get_conn()
        total    = c.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
        min_year = c.execute("SELECT MIN(year) FROM publications WHERE year>0").fetchone()[0] or 1991
        max_year = c.execute("SELECT MAX(year) FROM publications WHERE year>0").fetchone()[0] or datetime.now().year
        c.close()
        return {"total":total,"min_year":int(min_year),"max_year":int(max_year),"columns":cols()}

    return _cached("info", _load)

# ── Full search ───────────────────────────────────────────────────────────────
@app.get("/api/search")
def search(
    q:          Optional[str] = "",
    field:      str  = "all",
    year_from:  int  = 1991,
    year_to:    int  = 2026,
    month_from: int  = 1,
    month_to:   int  = 12,
    exact_year: int  = 0,      # if > 0, filter to this single year
    exact_month:int  = 0,      # if > 0, filter to this single month (requires exact_year)
    category:   str  = "",
    sort:       str  = "newest",
    limit:      int  = 50,     # page size — how many rows per page
    offset:     int  = 0,      # pagination offset
    page:       int  = 1,      # current page number (for response metadata)
):
    t0 = time.time()
    available = cols()
    has_cats  = "categories" in available
    has_absr  = "abstract_raw" in available
    has_abs   = "abstract" in available
    has_ingested = "ingested_at" in available

    params = []

    # ── Year / month filtering ────────────────────────────────────────────────
    if exact_year > 0:
        # Single year selected
        where = "WHERE year = ?"
        params.append(exact_year)
    else:
        # Year range
        where = "WHERE year BETWEEN ? AND ?"
        params += [year_from, year_to]

    # Month filter — uses ingested_at if available, otherwise approximated via year
    # Since arXiv update_date is stored in ingested_at as YYYY-MM-DD
    if exact_year > 0 and exact_month > 0 and has_ingested:
        where += " AND strftime('%m', ingested_at) = ?"
        params.append(f"{exact_month:02d}")
    elif (month_from > 1 or month_to < 12) and has_ingested:
        where += " AND CAST(strftime('%m', ingested_at) AS INTEGER) BETWEEN ? AND ?"
        params += [month_from, month_to]

    # ── Keyword filter ────────────────────────────────────────────────────────
    if q and q.strip():
        q = q.strip()
        if field == "title":
            where += " AND title LIKE ?";   params.append(f"%{q}%")
        elif field == "author":
            where += " AND authors LIKE ?"; params.append(f"%{q}%")
        elif field == "doi":
            where += " AND doi = ?";        params.append(q.lower())
        elif field == "category" and has_cats:
            where += " AND categories LIKE ?"; params.append(f"%{q}%")
        else:
            conds = ["title LIKE ?", "authors LIKE ?"]
            params += [f"%{q}%", f"%{q}%"]
            if has_cats:
                conds.append("categories LIKE ?"); params.append(f"%{q}%")
            if has_absr:
                conds.append("abstract_raw LIKE ?"); params.append(f"%{q}%")
            elif has_abs:
                conds.append("abstract LIKE ?"); params.append(f"%{q}%")
            where += f" AND ({' OR '.join(conds)})"

    # Category quick-filter (sidebar chip)
    if category and has_cats:
        where += " AND categories LIKE ?"; params.append(f"%{category}%")

    select_cols = "doi, title, authors, year"
    if "arxiv_id" in available: select_cols += ", arxiv_id"
    if has_cats: select_cols += ", categories"
    if "repository" in available: select_cols += ", repository"

    # ── Use FTS5 for keyword queries — falls back to LIKE if no FTS ────────────
    use_fts = (q and q.strip() and has_fts() and
               field in ("all", "title", "abstract") and
               not category)

    try:
        if use_fts:
            # Build extra WHERE for year/month filters (applied after FTS match)
            year_clause  = " AND p.year BETWEEN ? AND ?" if not exact_year else " AND p.year = ?"
            year_params  = [exact_year] if exact_year else [year_from, year_to]
            extra_where  = year_clause
            extra_params = year_params

            if exact_year and exact_month and has_ingested:
                extra_where  += " AND strftime('%m', p.ingested_at) = ?"
                extra_params += [f"{exact_month:02d}"]

            data, total_count = fts_search(
                q, limit, offset,
                where_extra  = extra_where,
                extra_params = extra_params,
            )
        else:
            # Original LIKE path (used for author/doi/category searches)
            count_sql    = f"SELECT COUNT(*) FROM publications {where}"
            sql          = f"SELECT {select_cols} FROM publications {where} {order_sql(sort)} LIMIT ? OFFSET ?"
            count_params = list(params)
            params.append(limit)
            params.append(offset)
            c            = get_conn()
            total_count  = c.execute(count_sql, count_params).fetchone()[0]
            rows         = c.execute(sql, params).fetchall()
            c.close()
            data = [dict(r) for r in rows]

        return {
            "results":     data,
            "count":       len(data),
            "total_count": total_count,
            "page":        page,
            "page_size":   limit,
            "total_pages": max(1, -(-total_count // limit)),
            "elapsed_ms":  round((time.time()-t0)*1000, 1),
            "engine":      "fts5" if use_fts else "like",
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Researcher profile ────────────────────────────────────────────────────────
@app.get("/api/researcher")
def researcher(name: str, year_from: int=1991, year_to: int=2026):
    t0 = time.time()
    available = cols()
    has_cats  = "categories" in available

    select = "doi,title,authors,year" + (",categories" if has_cats else "")
    try:
        c = get_conn()
        if has_fts():
            fts_q = build_fts_query(name)
            rows = c.execute(
                f"""SELECT p.doi,p.title,p.authors,p.year{',p.categories' if has_cats else ''}
                    FROM publications_fts f
                    JOIN publications p ON p.rowid = f.rowid
                    WHERE publications_fts MATCH ?
                    AND p.year BETWEEN ? AND ?
                    ORDER BY p.year DESC""",
                (f"authors:{fts_q}", year_from, year_to)
            ).fetchall()
            # fallback: search all columns if author-specific match returns nothing
            if not rows:
                rows = c.execute(
                    f"""SELECT p.doi,p.title,p.authors,p.year{',p.categories' if has_cats else ''}
                        FROM publications_fts f
                        JOIN publications p ON p.rowid = f.rowid
                        WHERE publications_fts MATCH ?
                        AND p.year BETWEEN ? AND ?
                        ORDER BY p.year DESC""",
                    (fts_q, year_from, year_to)
                ).fetchall()
        else:
            rows = c.execute(
                f"SELECT {select} FROM publications WHERE authors LIKE ? AND year BETWEEN ? AND ? ORDER BY year DESC",
                (f"%{name}%", year_from, year_to)
            ).fetchall()
        c.close()
        papers = [dict(r) for r in rows]

        # Build yearly breakdown
        yr_map = {}
        for p in papers:
            yr = p.get("year",0)
            yr_map[yr] = yr_map.get(yr,0)+1

        # Category breakdown
        cat_map = {}
        if has_cats:
            for p in papers:
                for cat in (p.get("categories") or "").split(","):
                    cat = cat.strip()
                    if cat: cat_map[cat] = cat_map.get(cat,0)+1
        cat_map = dict(sorted(cat_map.items(), key=lambda x:-x[1])[:8])

        return {
            "papers":   papers,
            "count":    len(papers),
            "by_year":  dict(sorted(yr_map.items())),
            "by_cat":   cat_map,
            "elapsed_ms": round((time.time()-t0)*1000,1)
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Author name cleaner ──────────────────────────────────────────────────────
import re as _re

# Patterns to strip from author strings
_AFF_PARENS = _re.compile(r'\([^)]*\)')          # anything in (parentheses)
_AFF_NUMS   = _re.compile(r'^\s*\(?\d+\)?\s*')   # leading numbers like (1) or 1)
_JUNK_WORDS = {
    'university','institute','department','college','center','centre',
    'laboratory','lab','school','faculty','division','research',
    'national','international','india','germany','france','china',
    'japan','usa','uk','italy','spain','brazil','canada','australia',
    'netherlands','switzerland','sweden','denmark','norway','poland',
    'russia','korea','taiwan','singapore','mexico','argentina',
    'the','of','and','for','at','in','von','van','de','del','le','la',
}

def _clean_author_name(raw: str) -> str | None:
    """
    Extract a clean personal name from a raw author string.
    Returns None if the string looks like an institution, not a person.
    """
    # Remove parenthetical affiliations
    name = _AFF_PARENS.sub('', raw)
    # Remove leading affiliation numbers  (1), 1)
    name = _AFF_NUMS.sub('', name)
    # Remove quotes, brackets
    name = name.replace('"','').replace("'",'').replace('[','').replace(']','')
    name = name.strip().strip(',').strip()

    if not name or len(name) < 3:
        return None

    # Reject if too long (institutions tend to be long)
    if len(name) > 60:
        return None

    # Reject if it contains junk institutional words
    words_lower = set(name.lower().split())
    if words_lower & _JUNK_WORDS:
        return None

    # Must contain at least one word that looks like a name
    # Allow non-latin names (Cyrillic, Arabic, Chinese etc.) — just check length
    words = name.split()
    if len(words) < 1:
        return None

    # Reject strings that are clearly not names:
    # - all uppercase abbreviations like "MEPhI" "CERN" "MIT" (unless short initials)
    # - strings with email addresses
    # - strings with URLs
    if '@' in name or 'http' in name.lower():
        return None

    # Reject all-caps words longer than 4 chars (institution abbreviations)
    non_initial_caps = [w for w in words if w.isupper() and len(w) > 4]
    if non_initial_caps:
        return None

    # At least one word should look like a name component
    # (starts with capital letter or is an initial like "A.")
    valid = [w for w in words
             if (w and (w[0].isupper() or (len(w)==2 and w[1]=='.')))
             and not any(c.isdigit() for c in w)]
    if not valid:
        return None

    return name

def _parse_authors(raw: str) -> list[str]:
    """
    Split a raw arXiv author string into individual clean names.

    Handles the messy reality of arXiv data:
      - "First Last, First Last, ..."          comma-separated full names
      - "Last, First; Last, First; ..."        semicolon-separated Last/First
      - "First Last (Affiliation), ..."        with parenthetical affiliations
      - "(1) First Last, (2) First Last ..."   with numbered affiliations
      - Mixed formats in the same string
    """
    if not raw:
        return []

    # Step 1 — strip everything in parentheses (affiliations, numbers)
    text = _AFF_PARENS.sub(' ', raw)

    # Step 2 — strip leading affiliation numbers like "(1)" "1)" "1."
    # Also strip trailing affiliation refs like "^1" "^{1,2}"
    text = _re.sub(r'\^\{?[\d,]+\}?', ' ', text)
    text = _re.sub(r'\b\d+\b', ' ', text)

    # Step 3 — normalise separators
    # Replace " and " with semicolon
    text = _re.sub(r'\s+and\s+', ';', text, flags=_re.IGNORECASE)

    # Step 4 — decide primary separator
    # If semicolons exist, split on semicolon (most reliable)
    if ';' in text:
        parts = text.split(';')
    else:
        # Comma-only: distinguish "First Last, First Last" from "Last, First"
        # Heuristic: if most comma-chunks look like "Word Word" (2 words, both capitalised)
        # then it's a "First Last" list. If they look like single words alternating,
        # it's likely "Last, First" pairs.
        chunks = [c.strip() for c in text.split(',') if c.strip()]
        two_word = sum(1 for c in chunks if len(c.split()) >= 2)
        if two_word >= len(chunks) * 0.5:
            # Mostly "First Last" format — comma is the name separator
            parts = chunks
        else:
            # "Last, First" format — pairs of chunks form one name
            parts = []
            i = 0
            while i < len(chunks):
                if i+1 < len(chunks):
                    combined = f"{chunks[i]}, {chunks[i+1]}"
                    # If combining makes a plausible name, use combined
                    c1 = chunks[i].strip()
                    c2 = chunks[i+1].strip()
                    # c1 is Last (single capitalised word), c2 is First
                    if (len(c1.split()) <= 2 and len(c2.split()) <= 3
                            and c1 and c1[0].isupper()):
                        parts.append(combined)
                        i += 2
                        continue
                parts.append(chunks[i])
                i += 1

    # Step 5 — clean each part
    results = []
    seen = set()
    for part in parts:
        cleaned = _clean_author_name(part)
        if cleaned:
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                results.append(cleaned)
    return results

# ── Author disambiguation — find all distinct authors matching a partial name ──
@app.get("/api/author_match")
def author_match(name: str, limit: int = 30):
    """
    Returns all distinct clean author names containing `name`.
    Strips affiliations, institutional words, numbered prefixes.
    """
    try:
        c = get_conn()
        has_first = "first_author" in cols()
        # Use FTS for author search if available — much faster
        if has_fts():
            # Build author-specific FTS query
            fts_author_q = build_fts_query(name)
            # Search in authors column specifically using column filter
            if fts_author_q:
                # Try exact phrase in authors column first
                rows = c.execute(
                    """SELECT p.authors
                       FROM publications_fts f
                       JOIN publications p ON p.rowid = f.rowid
                       WHERE publications_fts MATCH ?
                       LIMIT 50000""",
                    (f"authors:{fts_author_q}",)
                ).fetchall()
                # If nothing found, search across all columns
                if not rows:
                    rows = c.execute(
                        """SELECT p.authors
                           FROM publications_fts f
                           JOIN publications p ON p.rowid = f.rowid
                           WHERE publications_fts MATCH ?
                           LIMIT 50000""",
                        (fts_author_q,)
                    ).fetchall()
            else:
                rows = []
        elif has_first:
            rows = c.execute(
                """SELECT authors FROM publications
                   WHERE lower(authors) LIKE lower(?)
                      OR lower(first_author) LIKE lower(?)
                   LIMIT 300000""",
                (f"%{name}%", f"%{name}%")
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT authors FROM publications WHERE lower(authors) LIKE lower(?) LIMIT 300000",
                (f"%{name}%",)
            ).fetchall()
        c.close()

        counts: dict[str, int] = {}
        nl = name.lower()

        for row in rows:
            for author in _parse_authors(row[0] or ""):
                if nl in author.lower():
                    counts[author] = counts.get(author, 0) + 1

        # Sort by paper count descending
        results = sorted(
            [{"name": k, "count": v} for k, v in counts.items()],
            key=lambda x: -x["count"]
        )[:limit]

        return {"matches": results, "total": len(results)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── ORCID lookup — cache first, live API fallback ────────────────────────────
def _has_orcid_cache() -> bool:
    if "orcid_cache" not in _cache:
        with get_conn() as c:
            r = c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='orcid_cache'"
            ).fetchone()
            _cache["orcid_cache"] = r is not None
    return _cache["orcid_cache"]

@app.get("/api/orcid_lookup")
async def orcid_lookup(name: str):
    """
    Returns ORCID profile for a researcher.
    Checks local orcid_cache table first (instant).
    Falls back to live ORCID API if not cached.
    """
    import urllib.request, urllib.parse, json as _json

    # ── Try local cache first ─────────────────────────────────────────────────
    if _has_orcid_cache():
        with get_conn() as c:
            # Check columns available in orcid_cache
            oc_cols = {r[1] for r in c.execute("PRAGMA table_info(orcid_cache)").fetchall()}
            has_conf = "confidence" in oc_cols

            sel = "orcid_id, orcid_url, full_name, status" + (", confidence, match_reason" if has_conf else "")

            # Exact name match first
            row = c.execute(
                f"SELECT {sel} FROM orcid_cache WHERE author_name = ?", (name,)
            ).fetchone()
            # Partial match if no exact
            if not row:
                row = c.execute(
                    f"SELECT {sel} FROM orcid_cache WHERE lower(author_name) LIKE lower(?)",
                    (f"%{name}%",)
                ).fetchone()

        if row and row[3] == "found" and row[0]:
            result = {
                "orcid_id":  row[0],
                "orcid_url": row[1],
                "name":      row[2] or name,
                "source":    "cache",
            }
            if has_conf and len(row) > 4:
                result["confidence"]   = row[4]
                result["match_reason"] = row[5]
            return {"results": [result], "source": "cache"}

        if row and row[3] in ("not_found", "low_confidence"):
            return {"results": [], "source": "cache",
                    "note": "No confident ORCID match found"}

    # ── Live ORCID API fallback ───────────────────────────────────────────────
    try:
        query = urllib.parse.quote(f'"{name}"')
        url   = f"https://pub.orcid.org/v3.0/search/?q={query}&rows=5"
        req   = urllib.request.Request(url, headers={
            "Accept":"application/json","User-Agent":"NEXUS-Research-Engine/1.0"
        })
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = _json.loads(resp.read())

        results = []
        for item in (data.get("result") or []):
            orcid_id = (item.get("orcid-identifier") or {}).get("path","")
            if not orcid_id: continue
            try:
                preq = urllib.request.Request(
                    f"https://pub.orcid.org/v3.0/{orcid_id}/person",
                    headers={"Accept":"application/json","User-Agent":"NEXUS-Research-Engine/1.0"}
                )
                with urllib.request.urlopen(preq, timeout=4) as presp:
                    pdata     = _json.loads(presp.read())
                    name_data = pdata.get("name") or {}
                    given     = (name_data.get("given-names") or {}).get("value","")
                    family    = (name_data.get("family-name") or {}).get("value","")
                    full_name = f"{given} {family}".strip()
            except Exception:
                full_name = name
            results.append({
                "orcid_id":  orcid_id,
                "orcid_url": f"https://orcid.org/{orcid_id}",
                "name":      full_name,
                "source":    "live"
            })

        # Cache the result for next time
        if _has_orcid_cache() and results:
            with get_conn() as c:
                r = results[0]
                c.execute(
                    """INSERT OR REPLACE INTO orcid_cache
                       (author_name,orcid_id,orcid_url,full_name,status)
                       VALUES (?,?,?,?,?)""",
                    (name, r["orcid_id"], r["orcid_url"], r["name"], "found")
                )
        elif _has_orcid_cache() and not results:
            with get_conn() as c:
                c.execute(
                    "INSERT OR REPLACE INTO orcid_cache (author_name,status) VALUES (?,?)",
                    (name, "not_found")
                )

        return {"results": results, "source": "live"}
    except Exception as e:
        return {"results": [], "error": str(e), "source": "live"}


# ── Exact author profile — by exact name string ───────────────────────────────
@app.get("/api/author_profile")
def author_profile(exact_name: str, year_from: int=1991, year_to: int=2026):
    """
    Returns full profile for one exact author name.
    Used after disambiguation — caller passes the exact string chosen.
    """
    available = cols()
    has_cats  = "categories" in available
    t0 = time.time()
    try:
        c = get_conn()
        if has_fts():
            fts_q = build_fts_query(exact_name)
            rows = c.execute(
                f"""SELECT p.doi,p.title,p.authors,p.year{',p.categories' if has_cats else ''}
                    FROM publications_fts f
                    JOIN publications p ON p.rowid = f.rowid
                    WHERE publications_fts MATCH ?
                    AND p.year BETWEEN ? AND ?
                    ORDER BY p.year DESC""",
                (f"authors:{fts_q}", year_from, year_to)
            ).fetchall()
            if not rows:
                rows = c.execute(
                    f"""SELECT p.doi,p.title,p.authors,p.year{',p.categories' if has_cats else ''}
                        FROM publications_fts f
                        JOIN publications p ON p.rowid = f.rowid
                        WHERE publications_fts MATCH ?
                        AND p.year BETWEEN ? AND ?
                        ORDER BY p.year DESC""",
                    (fts_q, year_from, year_to)
                ).fetchall()
        else:
            rows = c.execute(
                f"SELECT doi,title,authors,year{',categories' if has_cats else ''} "
                f"FROM publications WHERE authors LIKE ? AND year BETWEEN ? AND ? ORDER BY year DESC",
                (f"%{exact_name}%", year_from, year_to)
            ).fetchall()
        c.close()

        papers  = [dict(r) for r in rows]
        yr_map  = {}
        cat_map = {}
        collab_map = {}

        for p in papers:
            # Year
            yr = p.get("year",0)
            yr_map[yr] = yr_map.get(yr,0) + 1
            # Categories
            if has_cats:
                for cat in (p.get("categories") or "").split(","):
                    cat = cat.strip()
                    if cat: cat_map[cat] = cat_map.get(cat,0)+1
            # Co-authors (collaborators)
            for part in _parse_authors(p.get("authors") or ""):
                if part.lower() != exact_name.lower():
                    collab_map[part] = collab_map.get(part,0)+1

        top_collabs = dict(sorted(collab_map.items(), key=lambda x:-x[1])[:10])

        return {
            "papers":      papers,
            "count":       len(papers),
            "by_year":     dict(sorted(yr_map.items())),
            "by_cat":      dict(sorted(cat_map.items(), key=lambda x:-x[1])[:8]),
            "top_collabs": top_collabs,
            "elapsed_ms":  round((time.time()-t0)*1000,1),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def _has_summary() -> bool:
    """Check if pre-computed summary tables exist."""
    if "summary" not in _cache:
        with get_conn() as c:
            r = c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='summary_by_year'"
            ).fetchone()
            _cache["summary"] = r is not None
    return _cache["summary"]

# ── Analytics ─────────────────────────────────────────────────────────────────
@app.get("/api/analytics")
def analytics(year_from: int=1991, year_to: int=2026):
    available = cols()
    has_cats  = "categories" in available
    cache_key = f"analytics_{year_from}_{year_to}"
    try:
        if cache_key in _cache:
            return _cache[cache_key]

        c = get_conn()

        if _has_summary():
            # ── Fast path: use pre-computed summary tables ─────────────────
            by_year = dict(c.execute(
                "SELECT year, count FROM summary_by_year WHERE year BETWEEN ? AND ? ORDER BY year",
                (year_from, year_to)
            ).fetchall())

            total    = sum(by_year.values())
            with_doi = c.execute(
                "SELECT COUNT(*) FROM publications WHERE doi IS NOT NULL AND year BETWEEN ? AND ?",
                (year_from, year_to)
            ).fetchone()[0]

            # Category counts from summary table
            cat_counts = {}
            if has_cats:
                TOP = ["cs.lg","cs.ai","cs.cv","cs.cl","stat.ml","cs.ro",
                       "astro-ph","quant-ph","hep-th","math.st","cs.ne","cs.cr"]
                rows = c.execute(
                    f"SELECT category, count FROM summary_by_category WHERE category IN ({','.join('?'*len(TOP))}) ORDER BY count DESC",
                    TOP
                ).fetchall()
                cat_counts = {r[0]: r[1] for r in rows}

        else:
            # ── Slow fallback: direct queries (no summary tables yet) ──────
            total    = c.execute("SELECT COUNT(*) FROM publications WHERE year BETWEEN ? AND ?",
                                 (year_from,year_to)).fetchone()[0]
            with_doi = c.execute("SELECT COUNT(*) FROM publications WHERE doi IS NOT NULL AND year BETWEEN ? AND ?",
                                 (year_from,year_to)).fetchone()[0]
            by_year  = dict(c.execute(
                "SELECT year,COUNT(*) FROM publications WHERE year BETWEEN ? AND ? GROUP BY year ORDER BY year",
                (year_from,year_to)).fetchall())
            cat_counts = {}
            if has_cats:
                TOP = ["cs.LG","cs.AI","cs.CV","cs.CL","stat.ML","cs.RO",
                       "astro-ph","quant-ph","hep-th","math.ST","cs.NE","cs.CR"]
                for cat in TOP:
                    n = c.execute(
                        "SELECT COUNT(*) FROM publications WHERE categories LIKE ? AND year BETWEEN ? AND ?",
                        (f"%{cat}%",year_from,year_to)).fetchone()[0]
                    if n: cat_counts[cat] = n

        c.close()
        result = {
            "total":       total,
            "with_doi":    with_doi,
            "by_year":     {str(k):v for k,v in by_year.items()},
            "by_category": cat_counts,
        }
        _cache[cache_key] = result
        return result
    except Exception as e:
        return JSONResponse({"error":str(e)}, status_code=500)

# ── Fast category counts (uses summary table if available) ───────────────────
@app.get("/api/category_counts")
def category_counts(year_from: int=1991, year_to: int=2026):
    """
    Returns paper counts for all categories.
    Uses pre-computed summary table for instant response.
    """
    cache_key = f"cat_counts_{year_from}_{year_to}"
    if cache_key in _cache:
        return _cache[cache_key]
    try:
        c = get_conn()
        if _has_summary():
            # Instant: read from summary table
            rows = c.execute(
                """SELECT category, SUM(count) as total
                   FROM summary_year_cat
                   WHERE year BETWEEN ? AND ?
                   GROUP BY category
                   ORDER BY total DESC
                   LIMIT 200""",
                (year_from, year_to)
            ).fetchall()
            if not rows:
                # Fallback to global summary
                rows = c.execute(
                    "SELECT category, count FROM summary_by_category ORDER BY count DESC LIMIT 200"
                ).fetchall()
        else:
            # Slow fallback
            rows = c.execute(
                """SELECT categories, COUNT(*) FROM publications
                   WHERE year BETWEEN ? AND ? AND categories IS NOT NULL
                   GROUP BY categories ORDER BY COUNT(*) DESC LIMIT 500""",
                (year_from, year_to)
            ).fetchall()
            # Parse and aggregate
            counts = {}
            for cats_str, cnt in rows:
                for cat in (cats_str or "").split(","):
                    cat = cat.strip().lower()
                    if cat: counts[cat] = counts.get(cat,0) + cnt
            rows = sorted(counts.items(), key=lambda x:-x[1])[:200]

        c.close()
        result = {"counts": {r[0]: r[1] for r in rows}}
        _cache[cache_key] = result
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Author list (paginated, searchable) ──────────────────────────────────────
@app.get("/api/authors")
def authors(search: str = "", limit: int = 50, offset: int = 0,
            year_from: int = 1991, year_to: int = 2026):
    """
    Returns authors with paper counts, sorted by productivity.
    Uses FTS5 when available for fast author search.
    Supports pagination and live search filtering.
    """
    try:
        c = get_conn()
        available = cols()
        has_first = "first_author" in available

        if search and has_fts():
            # Use FTS for fast author search
            fts_q = build_fts_query(search)
            rows = c.execute(
                """SELECT p.authors FROM publications_fts f
                   JOIN publications p ON p.rowid = f.rowid
                   WHERE publications_fts MATCH ?
                   AND p.year BETWEEN ? AND ?
                   AND p.authors IS NOT NULL
                   LIMIT 100000""",
                (f"authors:{fts_q}", year_from, year_to)
            ).fetchall()
            if not rows:
                rows = c.execute(
                    """SELECT p.authors FROM publications_fts f
                       JOIN publications p ON p.rowid = f.rowid
                       WHERE publications_fts MATCH ?
                       AND p.year BETWEEN ? AND ?
                       AND p.authors IS NOT NULL
                       LIMIT 100000""",
                    (fts_q, year_from, year_to)
                ).fetchall()
        elif search and has_first:
            # Fallback: search first_author index
            rows = c.execute(
                """SELECT authors FROM publications
                   WHERE lower(first_author) LIKE lower(?)
                   AND year BETWEEN ? AND ?
                   AND authors IS NOT NULL
                   LIMIT 100000""",
                (f"%{search}%", year_from, year_to)
            ).fetchall()
        elif search:
            rows = c.execute(
                """SELECT authors FROM publications
                   WHERE lower(authors) LIKE lower(?)
                   AND year BETWEEN ? AND ?
                   AND authors IS NOT NULL
                   LIMIT 100000""",
                (f"%{search}%", year_from, year_to)
            ).fetchall()
        else:
            # No search — load all authors for this year range
            rows = c.execute(
                """SELECT authors FROM publications
                   WHERE year BETWEEN ? AND ?
                   AND authors IS NOT NULL""",
                (year_from, year_to)
            ).fetchall()
        c.close()

        counts: dict[str, int] = {}
        sl = search.lower() if search else ""
        for row in rows:
            for name in _parse_authors(row[0] or ""):
                # Apply search filter on parsed names too
                if sl and sl not in name.lower():
                    continue
                counts[name] = counts.get(name, 0) + 1

        sorted_authors = sorted(counts.items(), key=lambda x: -x[1])
        total = len(sorted_authors)
        page  = sorted_authors[offset: offset + limit]
        return {
            "authors": [{"name": n, "count": c} for n, c in page],
            "total":   total,
            "limit":   limit,
            "offset":  offset,
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Collaboration network ─────────────────────────────────────────────────────
@app.get("/api/network")
def network(
    focus:      str = "",          # seed author name (empty = top authors)
    depth:      int = 1,           # hop depth from seed
    year_from:  int = 1991,
    year_to:    int = 2026,
    min_papers: int = 2,           # min shared papers to draw an edge
    max_nodes:  int = 80,          # cap to keep graph readable
    category:   str = "",
):
    """
    Builds a collaboration graph.
    Nodes = authors.  Edges = co-authored at least `min_papers` papers together.
    Returns {nodes:[{id,label,count,group}], edges:[{source,target,weight}]}
    """
    try:
        c = get_conn()
        has_cats = "categories" in cols()

        # If focused on a specific author, use FTS to get their papers first
        # then expand — much faster than scanning all 2.9M rows
        if focus and has_fts():
            fts_q = build_fts_query(focus)
            base_q = f"""
                SELECT p.authors FROM publications_fts f
                JOIN publications p ON p.rowid = f.rowid
                WHERE publications_fts MATCH ?
                AND p.year BETWEEN ? AND ?
                AND p.authors IS NOT NULL
            """
            params: list = [f"authors:{fts_q}", year_from, year_to]
            if category and has_cats:
                base_q += " AND p.categories LIKE ?"
                params.append(f"%{category}%")
            rows = c.execute(base_q, params).fetchall()
            # If FTS author search got nothing, try broader
            if not rows:
                params[0] = fts_q
                rows = c.execute(base_q, params).fetchall()
        else:
            base_q = "SELECT authors FROM publications WHERE year BETWEEN ? AND ? AND authors IS NOT NULL"
            params: list = [year_from, year_to]
            if category and has_cats:
                base_q += " AND categories LIKE ?"
                params.append(f"%{category}%")
            rows = c.execute(base_q, params).fetchall()
        c.close()

        # Parse every paper into a list of author sets
        paper_authors: list[list[str]] = []
        author_counts: dict[str, int] = {}

        for row in rows:
            parts = _parse_authors(row[0] or "")
            if len(parts) < 2:
                continue
            paper_authors.append(parts)
            for p in parts:
                author_counts[p] = author_counts.get(p, 0) + 1

        # Decide which authors to include
        if focus:
            # BFS from seed author
            fl  = focus.lower()
            # find exact/partial match
            seed_name = next((a for a in author_counts if fl in a.lower()), None)
            if not seed_name:
                return {"nodes": [], "edges": [], "error": f"Author '{focus}' not found"}

            included: set[str] = {seed_name}
            frontier: set[str] = {seed_name}
            for _ in range(depth):
                new_frontier: set[str] = set()
                for paper in paper_authors:
                    if any(a in frontier for a in paper):
                        for a in paper:
                            if a not in included:
                                new_frontier.add(a)
                included |= new_frontier
                frontier  = new_frontier
                if len(included) >= max_nodes:
                    break
            # Trim to max_nodes by paper count
            included_sorted = sorted(included, key=lambda a: -author_counts.get(a, 0))[:max_nodes]
            included = set(included_sorted)
        else:
            # Top authors by paper count
            top = sorted(author_counts.items(), key=lambda x: -x[1])[:max_nodes]
            included = {name for name, _ in top}

        # Build edge weights (co-authorship count)
        edge_map: dict[tuple[str,str], int] = {}
        for paper in paper_authors:
            members = [a for a in paper if a in included]
            for i in range(len(members)):
                for j in range(i+1, len(members)):
                    a, b = sorted([members[i], members[j]])
                    edge_map[(a,b)] = edge_map.get((a,b), 0) + 1

        # Filter by min_papers
        edges = [{"source": a, "target": b, "weight": w}
                 for (a,b), w in edge_map.items() if w >= min_papers]

        # Only keep nodes that have at least one edge
        connected = set()
        for e in edges:
            connected.add(e["source"])
            connected.add(e["target"])

        # Assign groups by top category (rough heuristic: find most common cat for this author)
        # Simple group = hash of first 2 chars of name → 5 groups
        def group(name: str) -> int:
            return (ord(name[0]) if name else 0) % 6

        nodes = [
            {
                "id":    a,
                "label": a,
                "count": author_counts.get(a, 0),
                "group": group(a),
                "is_seed": focus.lower() in a.lower() if focus else False,
            }
            for a in connected
        ]

        return {"nodes": nodes, "edges": edges,
                "total_authors": len(author_counts),
                "total_edges": len(edges)}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Serve the frontend ────────────────────────────────────────────────────────
from fastapi.responses import FileResponse

@app.get("/", response_class=HTMLResponse)
def root():
    with open("INDEX2.html", encoding="utf-8") as f:
        return f.read()

@app.get("/app.js")
def serve_js():
    return FileResponse("app.js", media_type="application/javascript",
        headers={"Cache-Control":"no-store, no-cache, must-revalidate",
                 "Pragma":"no-cache", "Expires":"0"})

@app.get("/INDEX2.html", response_class=HTMLResponse)
def serve_index():
    with open("INDEX2.html", encoding="utf-8") as f:
        return HTMLResponse(content=f.read(),
            headers={"Cache-Control":"no-store, no-cache, must-revalidate"})