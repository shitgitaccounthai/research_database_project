import streamlit as st
import sqlite3
import pandas as pd
import time

# --- FUNCTION: MACHINE COMMUNICATION & SCALABILITY ANALYSIS ---
def get_scoped_results(search_term, search_type, year_filter, sort_order):
    conn = sqlite3.connect('research_catalogue.db')
    start_time = time.time()
    
    query = "SELECT doi, title, authors, year, repository FROM publications WHERE 1=1"
    params = []

    # Scoped Search Logic
    if search_term:
        if search_type == "Title":
            query += " AND title LIKE ?"
            params.append(f'%{search_term}%')
        elif search_type == "Author":
            query += " AND authors LIKE ?"
            params.append(f'%{search_term}%')
        elif search_type == "DOI":
            query += " AND doi = ?"
            params.append(search_term)
        else:  # Common Search (All Metadata)
            query += " AND (title LIKE ? OR authors LIKE ? OR doi LIKE ?)"
            params.extend([f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'])
    
    if year_filter != "All":
        query += " AND year = ?"
        params.append(year_filter)

    query += " ORDER BY year DESC" if sort_order == "Newest" else " ORDER BY year ASC"

    df = pd.read_sql_query(query, conn, params=params)
    duration = time.time() - start_time
    conn.close()
    return df, duration

# --- UI CONFIGURATION ---
st.set_page_config(page_title="Discovery Engine", layout="wide")

# Sidebar: THE TELESCOPE FILTERS
with st.sidebar:
    st.title("🔭 Knowledge Telescope")
    st.markdown("---")
    st.subheader("1. Basic Sieves")
    year_val = st.selectbox("Publication Year", ["All"] + list(range(2026, 1990, -1)))
    repo_val = st.multiselect("Repositories", ["arXiv", "Crossref", "Zenodo"], default=["arXiv"])
    
    st.subheader("2. Sorting & Impact")
    sort_val = st.radio("Sort Results By", ["Relevance", "Newest First", "Oldest First"])
    
    st.subheader("3. Access Filters")
    open_access = st.checkbox("Open Access Only", value=True)
    has_dataset = st.checkbox("Dataset Available")

# Main Interface Header
# --- SEARCH INTERFACE ---
st.title("⚛️ Research Discovery Engine")

# Scoped Search Selection
search_mode = st.radio(
    "Search Mode",
    ["Common Search", "Title", "Author", "DOI"],
    horizontal=True,
    help="Select 'Common' to search all metadata or pick a specific field."
)

search_input = st.text_input(f"Enter {search_mode}...")

if search_input or year_val != "All":
    # Call the scoped search function
    results, speed = get_scoped_results(search_input, search_mode, year_val, sort_val)

    # Performance Metrics
    m1, m2 = st.columns(2)
    m1.metric("Results Found", f"{len(results):,}")
    m2.metric("Search Speed", f"{speed:.4f}s")

    # The Unified Table with Auto-Download Links
    if not results.empty:
        results['Official_Access'] = "https://doi.org/" + results['doi']
        results['Direct_Download'] = "https://sci-hub.ru/" + results['doi']

        st.data_editor(
            results,
            column_config={
                "title": st.column_config.TextColumn("Title", width="large"),
                "authors": st.column_config.TextColumn("Authors"),
                "year": st.column_config.NumberColumn("Year", format="%d"),
                "Official_Access": st.column_config.LinkColumn("🔗 Open", display_text="Open"),
                "Direct_Download": st.column_config.LinkColumn("📥 Get Paper", display_text="Download"),
            },
            hide_index=True,
            use_container_width=True,
            disabled=True
        )
    else:
        st.warning("No papers found in this sector of the sky. Try broadening your filters.")
else:
    st.info("👋 Welcome, Researcher. Use the search bar or filters to begin your discovery.")