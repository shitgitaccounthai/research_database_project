import json
import sqlite3
import os

# Path where your Kaggle download is located

# data\datasets\Cornell-University\arxiv\versions\276\arxiv-metadata-oai-snapshot.json
DATASET_PATH = r"data\datasets\Cornell-University\arxiv\versions\276\arxiv-metadata-oai-snapshot.json"
DB_PATH = "research_catalogue.db"

def start_bulk_ingestion():
    # Connect to your relational database
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("🚀 Starting bulk ingestion...")

    # Open the massive file line-by-line to save memory
    with open(DATASET_PATH, 'r', encoding='utf-8') as f:
        count = 0
        for line in f:
            paper = json.loads(line)
            
            # --- VALIDATION & NORMALISATION (Task 33/36) ---
            doi = paper.get('doi')
            if not doi: continue  # Skip papers without a DOI fingerprint
            
            # Normalise the year from the update date
            year = int(paper.get('update_date', '0000-00-00')[:4])
            
            # --- DEDUPLICATION (Task 37) ---
            try:
                cur.execute('''
                    INSERT OR IGNORE INTO publications (doi, title, authors, year, repository)
                    VALUES (?, ?, ?, ?, ?)
                ''', (doi, paper['title'].strip(), paper['authors'], year, 'arXiv'))
                count += 1
            except Exception as e:
                pass # Skip if record is corrupted
            
            # Commit every 10,000 records to keep it fast
            if count % 10000 == 0:
                conn.commit()
                print(f"✅ Ingested {count} records...")

    conn.commit()
    conn.close()
    print("✨ Bulk ingestion complete!")

if __name__ == "__main__":
    start_bulk_ingestion()