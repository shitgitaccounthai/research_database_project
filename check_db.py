import sqlite3

def check_record_count():
    # Connect to your research database
    conn = sqlite3.connect('research_catalogue.db')
    cursor = conn.cursor()

    # Core Task: Validate the corpus size
    cursor.execute("SELECT COUNT(*) FROM publications;")
    count = cursor.fetchone()[0]

    print(f"📊 Total records in database: {count:,}")
    
    # Check a few samples to ensure metadata normalization
    cursor.execute("SELECT title, year FROM publications LIMIT 5;")
    samples = cursor.fetchall()
    print("\n📝 Sample records:")
    for title, year in samples:
        print(f"- {title} ({year})")

    conn.close()

if __name__ == "__main__":
    check_record_count()