"""
research_ingestion.py  —  FINAL CLEAN EDITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fixes over previous versions:
  ✅ Keeps ALL papers (DOI optional — arXiv ID used as fallback)
  ✅ Proper author cleaning (strips affiliations, numbers, institutions)
  ✅ Author order preserved (1st, 2nd, 3rd author tracked)
  ✅ No false rejections from messy author strings
  ✅ Python 3.14 compatible — stdlib only
  ✅ Fast — deferred indexes + 50k batch commits

Usage:  py -3.14 research_ingestion.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json, logging, re, sqlite3, unicodedata
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DATASET_PATH = r"data\datasets\Cornell-University\arxiv\versions\276\arxiv-metadata-oai-snapshot.json"
DB_PATH      = "research_catalogue.db"
LOG_PATH     = "ingestion_audit.log"
BATCH_SIZE   = 50_000

logging.basicConfig(filename=LOG_PATH, filemode="w", level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 0 — NULL / NA / NONE VALUE CLEANING  (runs first on every field)
# ─────────────────────────────────────────────────────────────────────────────
# All the ways "nothing" appears in arXiv raw data
_NULL_STRINGS = {
    'none','null','na','n/a','nan','nil','undefined','unknown',
    'not available','not applicable','no abstract','no title',
    'see paper','see above','see below','see article',
    '–','—','-','.',''
}

def _nullclean(value) -> str:
    """
    Convert any value to a clean string, returning empty string if it
    represents a null/missing/placeholder value.
    Handles: None, "None", "N/A", "null", "nan", 0, [], {}, etc.
    """
    if value is None:
        return ""
    # Convert to string
    text = str(value).strip()
    # Check against known null strings (case-insensitive)
    if text.lower() in _NULL_STRINGS:
        return ""
    # Reject strings that are just punctuation/whitespace repeated
    _JUNK = set(' \t\n\r.,;:-_*')
    if text and all(c in _JUNK for c in text):
        return ""
    return text

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 1 — ENCODING REPAIR
# ─────────────────────────────────────────────────────────────────────────────
_MOJIBAKE = [
    ("\u00e2\u0080\u0099","'"), ("\u00e2\u0080\u009c",'"'),
    ("\u00e2\u0080\u009d",'"'), ("\u00e2\u0080\u0093","\u2013"),
    ("\u00e2\u0080\u0094","\u2014"),
]
_HTML_ENT = [("&amp;","&"),("&lt;","<"),("&gt;",">"),("&quot;",'"'),("&#39;","'")]

def _repair(text):
    if not text: return ""
    for bad,good in _MOJIBAKE: text = text.replace(bad,good)
    for bad,good in _HTML_ENT:  text = text.replace(bad,good)
    return text

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 2 — UNICODE NORMALISATION
# ─────────────────────────────────────────────────────────────────────────────
_NOISE = {"Cc","Cf","Cs","Co","Cn"}
_PMAP  = str.maketrans({
    "\u2018":"'","\u2019":"'","\u201c":'"',"\u201d":'"',
    "\u2013":"-","\u2014":"-","\u2026":"...","\u00a0":" ",
})
def _uni(text):
    text = unicodedata.normalize("NFC", text).translate(_PMAP)
    return "".join(c for c in text if unicodedata.category(c) not in _NOISE or c in "\t\n\r")

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 3 — LATEX REMOVAL
# ─────────────────────────────────────────────────────────────────────────────
_L_KEEP = re.compile(r"\\(?:emph|textbf|textit|texttt|underline|mathrm|mathbf|mathit|mathcal|operatorname|text|mbox|hbox)\{([^{}]*)\}", re.DOTALL)
_L_ENV  = re.compile(r"\\begin\{[^}]+\}.*?\\end\{[^}]+\}", re.DOTALL)
_L_DISP = re.compile(r"\\\[.*?\\\]|\$\$.*?\$\$", re.DOTALL)
_L_INL  = re.compile(r"\\\(.*?\\\)|\$(?!\$)[^$\n]*?\$", re.DOTALL)
_L_CARG = re.compile(r"\\[a-zA-Z@]+\*?\{[^{}]*\}")
_L_BARE = re.compile(r"\\[a-zA-Z@]+\*?")
_L_HTML = re.compile(r"<[^>]{1,200}>")
_L_BR   = re.compile(r"[{}]")
_L_MF   = re.compile(r"(\[formula\]\s*){2,}")

def _latex(text):
    text = _L_KEEP.sub(r"\1",text)
    text = _L_ENV.sub(" [formula] ",text)
    text = _L_DISP.sub(" [formula] ",text)
    text = _L_INL.sub(" [formula] ",text)
    text = _L_CARG.sub(" ",text)
    text = _L_BARE.sub(" ",text)
    text = _L_HTML.sub(" ",text)
    text = _L_BR.sub("",text)
    return _L_MF.sub("[formula] ",text)

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 4 — WHITESPACE
# ─────────────────────────────────────────────────────────────────────────────
_WS  = re.compile(r"[ \t]+")
_MNL = re.compile(r"\n{3,}")

def _ws(text, para=False):
    text = text.replace("\r\n","\n").replace("\r","\n")
    text = _WS.sub(" ", text)
    if para: text = _MNL.sub("\n\n", text)
    else:    text = _WS.sub(" ", text.replace("\n"," "))
    return text.strip()

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 5 — AUTHOR CLEANING  (the main fix)
# ─────────────────────────────────────────────────────────────────────────────
# Institutional words that should never appear in a person's name
_INST_WORDS = {
    'university','institute','department','college','center','centre',
    'laboratory','lab','school','faculty','division','research',
    'national','international','physics','mathematics','chemistry',
    'biology','science','technology','engineering','computing',
    'india','germany','france','china','japan','usa','uk','italy',
    'spain','brazil','canada','australia','russia','korea','taiwan',
    'netherlands','switzerland','sweden','poland','singapore',
    'the','of','for','at','and','von','van','de','del','le','la','und',
}

# Patterns to strip from individual name tokens
_AFF_PAREN   = re.compile(r'\([^)]*\)')              # (affiliation text)
_LEAD_NUM    = re.compile(r'^\s*[\(\[]?\d+[\)\]]?\s*[,.]?\s*')  # (1) or 1. prefix
_SUPER_NUM   = re.compile(r'\^?\{?[\d,]+\}?$')       # trailing ^{1,2}
_EMAIL_PAT   = re.compile(r'\S+@\S+')
_URL_PAT     = re.compile(r'https?://\S+')
_MULTI_SP    = re.compile(r'\s+')

def _clean_one_name(raw: str) -> str | None:
    """
    Clean a single author name token.
    Returns None if it looks like an institution rather than a person.
    """
    name = raw

    # Strip emails and URLs
    name = _EMAIL_PAT.sub('', name)
    name = _URL_PAT.sub('', name)

    # Strip parenthetical affiliations
    name = _AFF_PAREN.sub('', name)

    # Strip leading affiliation numbers
    name = _LEAD_NUM.sub('', name)

    # Strip trailing superscript numbers
    name = _SUPER_NUM.sub('', name)

    # Strip quotes, brackets, special chars except . - '
    name = re.sub(r'["\[\]\\/*|<>]', '', name)
    name = _MULTI_SP.sub(' ', name).strip().strip(',').strip(';').strip()

    if not name or len(name) < 2:
        return None

    # Too long = probably an institution sentence
    if len(name) > 55:
        return None

    # Contains institutional words → reject
    words_lower = {w.lower().strip('.,;:') for w in name.split()}
    if words_lower & _INST_WORDS:
        return None

    # Must have at least one word that looks like a personal name
    # (starts with capital letter, not all-caps abbreviation > 4 chars)
    name_words = []
    for w in name.split():
        w_clean = w.strip('.,;:')
        if not w_clean: continue
        # Skip all-caps tokens longer than 4 chars (MEPhI, CERN, etc.)
        if w_clean.isupper() and len(w_clean) > 4: continue
        # Skip pure numbers
        if w_clean.isdigit(): continue
        # Valid name component: starts with capital, or is an initial "A."
        if w_clean and (w_clean[0].isupper() or
                        (len(w_clean) <= 3 and '.' in w_clean)):
            name_words.append(w_clean)

    if not name_words:
        return None

    # Reconstruct from valid words only
    return ' '.join(name_words)


def _parse_authors(raw: str) -> list[str]:
    """
    Parse a raw arXiv author string into a list of clean personal names.
    Handles all the messy formats found in arXiv data:
      - "First Last, First Last"
      - "Last, First; Last, First"
      - "(1) First Last, (2) First Last ((1) Affil, (2) Affil)"
      - "First Last (Affil1), First Last (Affil2)"
      - Mixed formats
    Returns ordered list — first author first.
    """
    if not raw: return []

    # Step 1: remove everything after double-paren affiliation blocks
    # Pattern: "... ((1) University of X, (2) MIT)" at end
    text = re.sub(r'\(\(.*', '', raw)

    # Step 2: strip remaining parenthetical content
    text = _AFF_PAREN.sub(' ', text)

    # Step 3: strip leading numbers like (1) (2) at start of tokens
    text = _LEAD_NUM.sub(' ', text)

    # Step 4: normalise "and" → semicolon
    text = re.sub(r'\s+and\s+', ';', text, flags=re.IGNORECASE)

    # Step 5: decide primary separator
    # Priority: semicolons > commas
    if ';' in text:
        parts = [p for p in text.split(';') if p.strip()]
    else:
        # Comma-only string — figure out if it's:
        # A) "First Last, First Last, ..."  (comma = name separator)
        # B) "Last, First, Last, First ..."  (comma = Last/First separator)
        chunks = [c.strip() for c in text.split(',') if c.strip()]

        # Heuristic: count how many chunks have 2+ words (Full Name) vs 1 word (Last or First)
        multi_word = sum(1 for c in chunks if len(c.split()) >= 2)
        single_word = len(chunks) - multi_word

        if multi_word >= single_word:
            # Mostly full names — comma separates people
            parts = chunks
        else:
            # Mostly single words — pair them as "Last, First"
            parts = []
            i = 0
            while i < len(chunks):
                if i + 1 < len(chunks):
                    c1, c2 = chunks[i], chunks[i+1]
                    # If c1 looks like a last name (1-2 words) and c2 looks like first name
                    if len(c1.split()) <= 2 and len(c2.split()) <= 2:
                        # Reassemble as "First Last" order
                        parts.append(f"{c2} {c1}")
                        i += 2
                        continue
                parts.append(chunks[i])
                i += 1

    # Step 6: clean each name
    results = []
    seen = set()
    for part in parts:
        cleaned = _clean_one_name(part.strip())
        if cleaned:
            key = cleaned.lower()
            if key not in seen:
                seen.add(key)
                results.append(cleaned)

    return results


def clean_authors(raw: str) -> tuple[str, str]:
    """
    Returns (authors_clean, first_author):
      - authors_clean : "First Last; First Last; ..." semicolon-separated
      - first_author  : just the first author name (for fast lookup)
    """
    names = _parse_authors(_repair(raw or ""))
    if not names:
        return ("", "")
    return ("; ".join(names), names[0])


# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 6 — TITLE CLEANING
# ─────────────────────────────────────────────────────────────────────────────
def clean_title(raw: str) -> str:
    text = _ws(_latex(_uni(_repair(raw or ""))))
    if not text: return ""
    # Fix all-lower or all-upper
    if text == text.lower() or text == text.upper():
        words = []
        for w in text.split():
            words.append(w.upper() if re.match(r'^[A-Z]{2,}\d*$', w.upper()) else w.capitalize())
        text = " ".join(words)
    return text

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 7 — ABSTRACT CLEANING
# ─────────────────────────────────────────────────────────────────────────────
def clean_abstract(raw: str) -> str:
    text = _ws(_latex(_uni(_repair(raw or ""))), para=True)
    return text

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 8 — CATEGORIES
# ─────────────────────────────────────────────────────────────────────────────
def clean_categories(raw) -> str | None:
    if not raw: return None
    cats = [c.strip().lower() for c in re.split(r'[\s,]+', raw) if c.strip()]
    return ", ".join(sorted(set(cats))) or None

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 9 — DOI (optional — never reject a paper because of missing DOI)
# ─────────────────────────────────────────────────────────────────────────────
_DOI_RE   = re.compile(r'^10\.\d{4,9}/.+$')
_DOI_PFXS = ("https://doi.org/","http://doi.org/",
             "https://dx.doi.org/","http://dx.doi.org/","doi:","DOI:")

def clean_doi(raw) -> str | None:
    if not raw or not raw.strip(): return None
    doi = raw.strip()
    for p in _DOI_PFXS:
        if doi.lower().startswith(p.lower()): doi = doi[len(p):]; break
    # Handle multiple DOIs concatenated — take only first
    doi = doi.split()[0].lower().rstrip(".")
    return doi if _DOI_RE.match(doi) else None

# ─────────────────────────────────────────────────────────────────────────────
#  LAYER 10 — YEAR
# ─────────────────────────────────────────────────────────────────────────────
_CUR_YEAR = datetime.now().year

def extract_year(paper: dict) -> int:
    for raw in [
        paper.get("update_date",""),
        ((paper.get("versions") or [{}])[-1]).get("created",""),
    ]:
        try:
            y = int(str(raw)[:4])
            if 1900 <= y <= _CUR_YEAR: return y
        except (ValueError, TypeError): pass
    return 0

# ─────────────────────────────────────────────────────────────────────────────
#  MASTER VALIDATION GATE
# ─────────────────────────────────────────────────────────────────────────────
def clean_and_validate(paper: dict) -> dict | None:
    # ── Layer 0: null-clean every raw field first ─────────────────────────────
    raw_id       = _nullclean(paper.get("id"))
    raw_doi      = _nullclean(paper.get("doi"))
    raw_title    = _nullclean(paper.get("title"))
    raw_abstract = _nullclean(paper.get("abstract"))
    raw_authors  = _nullclean(paper.get("authors"))
    raw_cats     = _nullclean(paper.get("categories"))
    raw_jref     = _nullclean(paper.get("journal-ref"))

    # Need at least one identifier
    arxiv_id = raw_id.strip()
    doi      = clean_doi(raw_doi)
    if not arxiv_id and not doi:
        return None

    # Title — must exist and be meaningful after null-cleaning
    title = clean_title(raw_title)
    if len(title) < 3:
        return None

    # Abstract — must have real content after null-cleaning
    abstract = clean_abstract(raw_abstract)
    if len(abstract.split()) < 5:
        return None

    # Authors — clean but do NOT reject if messy
    authors_clean, first_author = clean_authors(raw_authors)
    # Fallback: keep raw authors (stripped) if cleaning produced nothing
    if not authors_clean and raw_authors:
        authors_clean = raw_authors[:500]
        first_author  = raw_authors.split(",")[0].split(";")[0].strip()[:100]

    # Categories — None if empty after null-clean
    categories = clean_categories(raw_cats) if raw_cats else None

    # Journal ref — None if empty after null-clean
    journal_ref = _ws(_uni(_repair(raw_jref))) if raw_jref else None

    # Year — store as None instead of 0 when unknown
    year = extract_year(paper)
    if year == 0:
        year = None

    return {
        "id":           doi or f"arxiv:{arxiv_id}",
        "doi":          doi,
        "arxiv_id":     arxiv_id or None,
        "title":        title,
        "authors":      authors_clean or None,
        "first_author": first_author or None,
        "abstract":     abstract,
        "year":         year,
        "categories":   categories,
        "journal_ref":  journal_ref,
        "repository":   "arXiv",
    }

# ─────────────────────────────────────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def init_db(conn: sqlite3.Connection) -> None:
    # Drop old table if schema is outdated
    existing = {r[1] for r in conn.execute("PRAGMA table_info(publications)")}
    required = {"id","doi","arxiv_id","title","authors","first_author",
                "abstract","year","categories","journal_ref","repository"}
    if existing and not required.issubset(existing):
        print("  ⚠️  Schema outdated — dropping and rebuilding...")
        conn.execute("DROP TABLE IF EXISTS publications")
        conn.commit()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS publications (
            id           TEXT PRIMARY KEY,
            doi          TEXT UNIQUE,
            arxiv_id     TEXT UNIQUE,
            title        TEXT NOT NULL,
            authors      TEXT,
            first_author TEXT,
            abstract     TEXT,
            year         INTEGER,
            categories   TEXT,
            journal_ref  TEXT,
            repository   TEXT DEFAULT 'arXiv',
            ingested_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

def build_indexes(conn: sqlite3.Connection) -> None:
    print("  Building indexes...")
    for s in [
        "CREATE INDEX IF NOT EXISTS idx_year         ON publications(year)",
        "CREATE INDEX IF NOT EXISTS idx_title        ON publications(title)",
        "CREATE INDEX IF NOT EXISTS idx_authors      ON publications(authors)",
        "CREATE INDEX IF NOT EXISTS idx_first_author ON publications(first_author)",
        "CREATE INDEX IF NOT EXISTS idx_categories   ON publications(categories)",
        "CREATE INDEX IF NOT EXISTS idx_doi          ON publications(doi)",
        "CREATE INDEX IF NOT EXISTS idx_arxiv        ON publications(arxiv_id)",
    ]:
        conn.execute(s)
    conn.commit()
    print("  Indexes ready.")

# ─────────────────────────────────────────────────────────────────────────────
#  DEDUPLICATION
# ─────────────────────────────────────────────────────────────────────────────
def _title_key(title: str) -> str:
    """
    Create a normalised dedup key from a title.
    Lowercases, strips punctuation and extra spaces.
    Two papers with the same title_key are considered duplicates.
    """
    t = title.lower()
    t = re.sub(r'[^a-z0-9 ]', '', t)   # keep only letters, digits, spaces
    t = re.sub(r' +', ' ', t).strip()
    return t

class DedupSet:
    """
    In-memory set of seen title keys + DOIs + arXiv IDs.
    Catches three kinds of duplicates:
      1. Same DOI (e.g. published paper submitted twice)
      2. Same arXiv ID (exact same record in the JSON)
      3. Same normalised title (re-submission with corrections)
    """
    def __init__(self):
        self._doi_seen     : set[str] = set()
        self._arxiv_seen   : set[str] = set()
        self._title_seen   : set[str] = set()

    def is_duplicate(self, rec: dict) -> str | None:
        """Returns reason string if duplicate, None if clean."""
        doi      = rec.get("doi")
        arxiv_id = rec.get("arxiv_id")
        tkey     = _title_key(rec.get("title",""))

        if doi and doi in self._doi_seen:
            return f"duplicate DOI: {doi}"
        if arxiv_id and arxiv_id in self._arxiv_seen:
            return f"duplicate arXiv ID: {arxiv_id}"
        if tkey and tkey in self._title_seen:
            return f"duplicate title: {rec.get('title','')[:60]}"
        return None

    def add(self, rec: dict):
        doi      = rec.get("doi")
        arxiv_id = rec.get("arxiv_id")
        tkey     = _title_key(rec.get("title",""))
        if doi:      self._doi_seen.add(doi)
        if arxiv_id: self._arxiv_seen.add(arxiv_id)
        if tkey:     self._title_seen.add(tkey)


# ─────────────────────────────────────────────────────────────────────────────
#  INGESTION LOOP
# ─────────────────────────────────────────────────────────────────────────────
def _flush(cur, conn, batch, stats):
    if not batch: return
    try:
        cur.executemany("""
            INSERT OR IGNORE INTO publications
            (id,doi,arxiv_id,title,authors,first_author,abstract,
             year,categories,journal_ref,repository)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""", batch)
        conn.commit()
        stats["ingested"] += cur.rowcount
    except sqlite3.Error as e:
        logger.error("FLUSH: %s", e)
        conn.rollback()
        stats["errors"] += len(batch)

def start_bulk_ingestion() -> None:
    path = Path(DATASET_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"\nDataset not found: {path.resolve()}"
            f"\nEdit DATASET_PATH at the top of this script.")

    print("=" * 62)
    print("  NEXUS Final Ingestion — Full dataset, clean authors")
    print(f"  Strategy : keep ALL papers (DOI optional)")
    print(f"  Authors  : affiliations stripped, order preserved")
    print(f"  Batch    : {BATCH_SIZE:,} rows per commit")
    print("=" * 62)

    # Delete old DB so we start fresh with new schema
    db_path = Path(DB_PATH)
    if db_path.exists():
        print(f"\n  Deleting old database ({db_path.stat().st_size/1024/1024:.0f} MB)...")
        db_path.unlink()
        print("  Old database deleted. Starting fresh.\n")

    t0   = datetime.now()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-128000")
    conn.execute("PRAGMA temp_store=MEMORY")
    init_db(conn)
    cur  = conn.cursor()

    stats = {"ingested":0,"skipped":0,"errors":0,"duplicates":0}
    batch: list[tuple] = []
    dedup = DedupSet()   # in-memory deduplication across the full file

    with open(DATASET_PATH,"r",encoding="utf-8") as f:
        for line_no, line in enumerate(f,1):
            line = line.strip()
            if not line: continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                stats["errors"] += 1
                continue

            rec = clean_and_validate(raw)
            if rec is None:
                stats["skipped"] += 1
                continue

            # Deduplication check
            dup_reason = dedup.is_duplicate(rec)
            if dup_reason:
                stats["duplicates"] += 1
                logger.info("DUPLICATE | %s", dup_reason)
                continue
            dedup.add(rec)

            batch.append((
                rec["id"],          rec["doi"],        rec["arxiv_id"],
                rec["title"],       rec["authors"],    rec["first_author"],
                rec["abstract"],    rec["year"],       rec["categories"],
                rec["journal_ref"], rec["repository"],
            ))

            if len(batch) >= BATCH_SIZE:
                _flush(cur, conn, batch, stats)
                batch.clear()
                elapsed = (datetime.now()-t0).seconds
                total   = stats["ingested"]+stats["skipped"]+stats["errors"]
                pct     = stats["ingested"]/max(1,total)*100
                print(f"  ✅  {stats['ingested']:>10,} ingested  |  "
                      f"{stats['skipped']:>7,} skipped  |  "
                      f"{stats['duplicates']:>6,} dupes  |  "
                      f"yield {pct:.1f}%  |  {elapsed}s")

        _flush(cur, conn, batch, stats)

    build_indexes(conn)
    conn.close()

    elapsed = (datetime.now()-t0).seconds
    total   = stats["ingested"]+stats["skipped"]+stats["errors"]

    print("\n" + "="*62)
    print("  ✨  Ingestion complete")
    print("="*62)
    print(f"  Ingested   : {stats['ingested']:>10,}")
    print(f"  Skipped    : {stats['skipped']:>10,}  (no ID / short title / short abstract)")
    print(f"  Duplicates : {stats['duplicates']:>10,}  (same DOI / arXiv ID / title)")
    print(f"  Errors     : {stats['errors']:>10,}")
    print(f"  Yield     : {stats['ingested']/max(1,total)*100:.1f}%")
    print(f"  Time      : {elapsed//60}m {elapsed%60}s")
    print(f"  DB        : {Path(DB_PATH).resolve()}")
    print("="*62)
    print("\n  Next: py -3.14 -m uvicorn server:app --port 8000")


if __name__ == "__main__":
    start_bulk_ingestion()