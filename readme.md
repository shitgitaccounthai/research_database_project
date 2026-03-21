
# Research Dataset Catalogue & Bibliographic Metadata Manager

## Overview

This project is a system for processing, storing, and exploring large-scale research datasets (millions of records). It provides fast search, analytics, and researcher exploration through a web interface.

---

## Current Dataset

Note:

* The project currently includes only the arXiv dataset sourced from Kaggle.
* The dataset size is approximately 5GB with over 1.4 million entries.

Future updates will include support for additional datasets such as CrossRef, PubMed, and others.

---

## System Architecture

The system consists of the following components:

### Data Layer

* Raw JSON dataset (arXiv)

### Processing Scripts

* `research_ingestion.py` — cleans and inserts data into the database
* `add_fts.py` — builds full-text search index (FTS5)
* `buildsummary.py` — generates analytics tables
* `build_orcid_cache.py` — fetches and stores ORCID data

### Database

* `research_catalogue.db`
* Key tables:

  * `publications`
  * `publications_fts`
  * `summary_by_year`
  * `summary_by_category`
  * `orcid_cache`

### Backend

* `server.py` (FastAPI)

### Frontend

* `index.html`
* `app.js`

---

## Installation and Setup

### 1. Clone the repository

```
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
```

### 2. Install dependencies

```
pip install fastapi uvicorn sqlite-utils
```

(You may also need additional packages such as pandas or tqdm depending on the scripts.)

### 3. Prepare the dataset

Download the arXiv dataset from Kaggle and place it in the project directory.

### 4. Run data ingestion

```
python research_ingestion.py
```

This step cleans and inserts the dataset into the SQLite database.

### 5. Build full-text search index

```
python add_fts.py
```

This step is required for fast search functionality.

### 6. Build analytics tables

```
python build_summary.py
```

### 7. Build ORCID cache (optional)

```
python build_orcid_cache.py
```

### 8. Start the server

```
uvicorn server:app --port 8000
```

### 9. Open in browser

```
http://127.0.0.1:8000
```

---

## Features

### Search

* Full-text search using SQLite FTS5
* Phrase-based matching
* Filtering by year, pagination, and sorting

### Researcher Profiles

* Author search and disambiguation
* Publication listings
* ORCID integration

### Analytics

* Papers per year
* Category-based statistics
* Precomputed summaries for fast queries

### Network Exploration

* Author relationships and collaboration insights

---

## Performance

Full-text search significantly improves performance:

* Without FTS: several seconds per query
* With FTS: a few milliseconds per query

---

## How It Works

1. User enters a query in the interface
2. `app.js` sends a request to the backend
3. `server.py` processes the request
4. SQLite executes the search using FTS5
5. Results are returned as JSON
6. The frontend renders the results

---

## Future Improvements

* Support for multiple datasets beyond arXiv (Kaggle)
* Improved database scalability
* AI-based semantic search
* Recommendation systems
* Advanced network analysis

---

## Acknowledgement

This project was developed with the assistance of AI tools.

Primary AI used:

* Claude

AI was used for system design, implementation, and optimization.

---

## Disclaimer

This project is intended for educational and research purposes.
Dataset ownership belongs to the original providers (e.g., arXiv via Kaggle).

---

## Contributing

Contributions are welcome. You can fork the repository and submit pull requests or open issues.


