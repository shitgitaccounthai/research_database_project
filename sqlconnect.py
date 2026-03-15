import json
import sqlite3

def connect_metadata_to_db(json_path, db_path):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Create the schema as per project requirements
    cur.execute('''CREATE TABLE IF NOT EXISTS catalogue (
        doi TEXT PRIMARY KEY, 
        arxiv_id TEXT, 
        title TEXT, 
        creators TEXT, 
        pub_year INTEGER, 
        repository TEXT DEFAULT 'arXiv'
    )''')

    # Automated ingestion via streaming
    with open(json_path, 'r') as f:
        for line in f:
            paper = json.loads(line)
            
            # Validation & Normalization
            doi = paper.get('doi')
            if not doi: continue  # Skip records without DOI
            
            title = paper.get('title').strip()
            year = int(paper.get('update_date')[:4]) # Extract year from string
            
            # Duplicate detection and storage
            try:
                cur.execute('''INSERT OR IGNORE INTO catalogue 
                            (doi, arxiv_id, title, creators, pub_year) 
                            VALUES (?, ?, ?, ?, ?)''', 
                            (doi, paper['id'], title, paper['authors'], year))
            except Exception as e:
                print(f"Error processing {doi}: {e}") # Log inconsistent data
        
    conn.commit()
    conn.close()