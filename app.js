/* NEXUS Research Discovery Engine — app.js */
const API = '';  // same origin
let currentMode  = 'search';
let activeCat    = '';
let searchTimer  = null;
let lastQuery    = {};

// ── Pagination state ──────────────────────────────────────────────────────────
let currentPage     = 1;
let currentPageSize = 50;    // default rows per page
let currentTotal    = 0;
let lastSearchParams = null; // cached params for page navigation

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const COLORS  = ['#00e0bc','#4d9eff','#9d7cf4','#f0c040','#ff6080'];
const CAT_COLORS = {
  'cs.LG':'#00e0bc','cs.AI':'#4d9eff','cs.CV':'#9d7cf4','stat.ML':'#f0c040',
  'astro-ph':'#ff6080','quant-ph':'#00e0bc','hep-th':'#4d9eff','cs.CL':'#9d7cf4'
};

// ── Init ────────────────────────────────────────────────────────────────────
async function init() {
  buildMonthGrid();
  try {
    const r = await fetch(`${API}/api/info`);
    const d = await r.json();
    if (d.error) {
      document.getElementById('sb-total').textContent = 'DB not found';
      document.getElementById('db-status').style.color = '#ff6080';
      showError('panel-search', d.error);
      return;
    }
    document.getElementById('sb-total').textContent = `${d.total.toLocaleString()} papers`;
    document.getElementById('corpus-count').textContent = d.total.toLocaleString();

    // Set bounds and default values — only on very first load
    const yrFrom = document.getElementById('yr-from');
    const yrTo   = document.getElementById('yr-to');
    yrFrom.min = d.min_year;
    yrTo.min   = d.min_year;
    yrTo.max   = d.max_year;
    yrFrom.max = d.max_year;
    // Only populate if still empty (first load)
    if (!yrFrom.value) yrFrom.value = d.min_year;
    if (!yrTo.value)   yrTo.value   = d.max_year;

    document.getElementById('db-status').style.color = '#00e0bc';
    updateLiveCount();
    showWelcome();
  } catch(e) {
    document.getElementById('sb-total').textContent = 'Cannot reach server';
    document.getElementById('db-status').style.color = '#ff6080';
  }
}

// ── Month grid ──────────────────────────────────────────────────────────────
function buildMonthGrid() {
  const g = document.getElementById('month-grid');
  const mf = parseInt(document.getElementById('mo-from').value);
  const mt = parseInt(document.getElementById('mo-to').value);
  g.innerHTML = MONTHS.map((m,i)=>{
    const n = i+1;
    let cls = 'month-btn';
    if(n>=mf && n<=mt) cls += ' range';
    return `<button class="${cls}" onclick="clickMonth(${n},this)">${m}</button>`;
  }).join('');
}

let monthStart = null;
function clickMonth(n, el) {
  if(!monthStart || monthStart > n) {
    monthStart = n;
    document.getElementById('mo-from').value = n;
    document.getElementById('mo-to').value   = n;
  } else {
    document.getElementById('mo-to').value = n;
    monthStart = null;
  }
  buildMonthGrid();
  onFilterChange();
}

// ── Mode switching ──────────────────────────────────────────────────────────
function setMode(mode, btn) {
  currentMode = mode;
  document.querySelectorAll('.mode-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.panel').forEach(p=>p.style.display='none');
  document.getElementById(`panel-${mode}`).style.display = 'block';

  const panel = document.getElementById(`panel-${mode}`);
  if(!panel.innerHTML.trim()) {
    if(mode==='search')     showWelcome();
    if(mode==='researcher') showResearcherInput();
    if(mode==='category')   showCategoryBrowse();
    if(mode==='analytics')  loadAnalytics();
    if(mode==='network')    showNetworkPage();
  }
}

// ── Filter helpers ──────────────────────────────────────────────────────────
function getFilters() {
  const exactYrEl  = document.getElementById('exact-yr');
  const exactMoEl  = document.getElementById('exact-mo');
  return {
    year_from:   parseInt(document.getElementById('yr-from').value)  || 1991,
    year_to:     parseInt(document.getElementById('yr-to').value)    || 2026,
    month_from:  parseInt(document.getElementById('mo-from').value)  || 1,
    month_to:    parseInt(document.getElementById('mo-to').value)    || 12,
    exact_year:  exactYrEl  ? (parseInt(exactYrEl.value)  || 0) : 0,
    exact_month: exactMoEl  ? (parseInt(exactMoEl.value)  || 0) : 0,
    sort:        document.getElementById('sort-sel').value,
    category:    activeCat,
  };
}

function toggleCat(el, cat) {
  if(activeCat === cat) {
    activeCat = '';
    el.classList.remove('active');
  } else {
    document.querySelectorAll('.cat-chip').forEach(c=>c.classList.remove('active'));
    activeCat = cat;
    el.classList.add('active');
  }
  onFilterChange();
}

function onFilterChange() {
  buildMonthGrid();
  updateLiveCount();
  if(currentMode === 'search')     debounceSearch();
  if(currentMode === 'analytics')  loadAnalytics();
}

// When user changes page size — re-run search from page 1
function onPageSizeChange() {
  const sel = document.getElementById('page-size-sel');
  if(sel) currentPageSize = parseInt(sel.value) || 50;
  if(lastSearchParams) {
    currentPage = 1;
    doSearch();
  }
}

// Navigate to a specific page of current search results
async function goToPage(page) {
  if(!lastSearchParams) return;
  currentPage = page;
  const panel = document.getElementById('panel-search');
  panel.innerHTML = `<div style="display:flex;align-items:center;gap:.6rem;color:var(--t2);font-size:.8rem;">
    <div class="spinner"></div> Loading page ${page}…</div>`;

  const params = new URLSearchParams({
    ...lastSearchParams,
    limit:  currentPageSize,
    offset: (page - 1) * currentPageSize,
    page:   page,
  });

  try {
    const r = await fetch(`${API}/api/search?${params}`);
    const d = await r.json();
    if(d.error) { showError('panel-search', d.error); return; }
    renderSearchResults(d, panel);
    // Scroll results into view
    panel.scrollIntoView({behavior:'smooth', block:'start'});
  } catch(e) { showError('panel-search', e.toString()); }
}

// Clear exact year/month fields
function clearExactFilter() {
  const ey = document.getElementById('exact-yr');
  const em = document.getElementById('exact-mo');
  if(ey) ey.value = '';
  if(em) em.value = '0';
  applyYearFilter();
}

// Called when user clicks "Apply Year Range" or presses Enter in year inputs
function applyYearFilter() {
  const yf = document.getElementById('yr-from');
  const yt = document.getElementById('yr-to');

  // Validate: from must be <= to
  let from = parseInt(yf.value);
  let to   = parseInt(yt.value);
  if (isNaN(from) || isNaN(to)) return;
  if (from > to) { [from, to] = [to, from]; yf.value = from; yt.value = to; }

  // Update live counter display
  document.getElementById('live-range').textContent = `${from} – ${to}`;
  updateLiveCount();

  // Re-run current mode with new year range
  if (currentMode === 'search')     debounceSearch();
  if (currentMode === 'analytics')  loadAnalytics();
  if (currentMode === 'researcher') {
    const name = document.getElementById('rname');
    if (name && name.value.trim()) loadResearcher();
  }
  if (currentMode === 'category') {
    const cat = document.getElementById('catin');
    if (cat && cat.value.trim()) searchByCategory();
  }
}

async function updateLiveCount() {
  const f = getFilters();
  try {
    const r = await fetch(`${API}/api/search?q=&year_from=${f.year_from}&year_to=${f.year_to}&limit=1`);
    const d = await r.json();
    document.getElementById('live-count').textContent = (d.count !== undefined ? '≈' + d.count.toLocaleString() : '—');
  } catch{ document.getElementById('live-count').textContent = '—'; }
  document.getElementById('live-range').textContent =
    `${document.getElementById('yr-from').value} – ${document.getElementById('yr-to').value}`;
}

// ── Search ──────────────────────────────────────────────────────────────────
function debounceSearch() {
  clearTimeout(searchTimer);
  const q = document.getElementById('search-box').value.trim();
  // Start searching after 2 chars, with 280ms debounce for live feel
  if(q.length > 0 && q.length < 2) return;
  searchTimer = setTimeout(doSearch, 280);
}

async function doSearch() {
  const q     = document.getElementById('search-box').value.trim();
  const field = document.getElementById('field-sel').value;
  const f     = getFilters();

  if(!q && !f.category) { showWelcome(); return; }

  const panel = document.getElementById('panel-search');
  panel.innerHTML = `<div style="display:flex;align-items:center;gap:.6rem;color:var(--t2);font-size:.8rem;">
    <div class="spinner"></div> Searching…</div>`;

  // Reset to page 1 on new search
  currentPage = 1;
  const pageSizeEl = document.getElementById('page-size-sel');
  if(pageSizeEl) currentPageSize = parseInt(pageSizeEl.value) || 50;

  // Cache params for page navigation (without page-specific values)
  lastSearchParams = {q, field, sort:f.sort,
    year_from:f.year_from, year_to:f.year_to,
    month_from:f.month_from, month_to:f.month_to,
    exact_year:f.exact_year, exact_month:f.exact_month,
    category:f.category};

  const params = new URLSearchParams({
    ...lastSearchParams,
    limit:  currentPageSize,
    offset: 0,
    page:   1,
  });

  try {
    const r = await fetch(`${API}/api/search?${params}`);
    const d = await r.json();
    if(d.error) { showError('panel-search', d.error); return; }
    renderSearchResults(d, panel);
  } catch(e) { showError('panel-search', e.toString()); }
}

function renderSearchResults(d, panel) {
  const results = d.results || [];
  const elapsed = d.elapsed_ms || 0;

  // Year distribution for mini chart
  const yrMap = {};
  results.forEach(r => { yrMap[r.year] = (yrMap[r.year]||0)+1; });
  const yrKeys = Object.keys(yrMap).map(Number).sort((a,b)=>a-b);

  const uniqueAuth = new Set(results.map(r=>r.authors)).size;

  // Read year range from sidebar — show exact year/month if set
  const f2         = getFilters();
  const exactYr    = f2.exact_year;
  const exactMo    = f2.exact_month;
  const filterFrom = document.getElementById('yr-from').value || '—';
  const filterTo   = document.getElementById('yr-to').value   || '—';
  let yearLabel;
  if (exactYr && exactMo) {
    const moName = ['Jan','Feb','Mar','Apr','May','Jun',
                    'Jul','Aug','Sep','Oct','Nov','Dec'][exactMo-1];
    yearLabel = `${moName} ${exactYr}`;
  } else if (exactYr) {
    yearLabel = `${exactYr}`;
  } else {
    yearLabel = `${filterFrom}–${filterTo}`;
  }

  // Pagination state
  const totalCount  = d.total_count  || results.length;
  const totalPages  = d.total_pages  || 1;
  const pageNum     = d.page         || currentPage;
  const pageSize    = d.page_size    || currentPageSize;
  currentTotal      = totalCount;

  // "Showing X–Y of Z papers"
  const rangeStart  = ((pageNum-1) * pageSize) + 1;
  const rangeEnd    = Math.min(pageNum * pageSize, totalCount);
  const countLabel  = totalCount > pageSize
    ? `${rangeStart.toLocaleString()}–${rangeEnd.toLocaleString()} of ${totalCount.toLocaleString()}`
    : totalCount.toLocaleString();

  let html = `<div class="metrics-row fade-in">
    ${metricCard('Papers in Range', totalCount.toLocaleString(), 'cyan')}
    ${metricCard('Query Time',      elapsed+'ms', 'blue')}
    ${metricCard('Year Filter',     yearLabel, 'gold')}
    ${metricCard('Unique Authors',  uniqueAuth.toLocaleString(), 'violet')}
  </div>`;

  if(yrKeys.length > 1) {
    html += buildBarChart(yrMap, yrKeys, 'Publication Trend');
  }

  if(results.length === 0) {
    html += `<div class="no-results fade-in">
      <div class="no-results-title">No papers found</div>
      <div class="no-results-sub">Try different keywords · check the year range in the sidebar · widen the month range</div>
    </div>`;
  } else {
    html += `<div class="section-label fade-in">Results · ${results.length.toLocaleString()} papers</div>`;
    html += buildTable(results);
    html += buildPagination(pageNum, totalPages, totalCount, rangeStart, rangeEnd, pageSize);
    html += `<div class="caption fade-in">↳ showing ${countLabel} · ${elapsed}ms · arXiv</div>`;
  }

  panel.innerHTML = html;
}

// ── Researcher mode ─────────────────────────────────────────────────────────

// ═══════════════════════════════════════════════════════════════════════
//  RESEARCHER MODE  — disambiguation + full profile + ORCID
// ═══════════════════════════════════════════════════════════════════════

function showResearcherInput() {
  const panel = document.getElementById('panel-researcher');
  panel.innerHTML = `
  <div id="r-search-row" style="margin-bottom:1.2rem;">
    <div class="section-label" style="margin-bottom:.6rem;">Researcher Name</div>
    <div style="display:flex;gap:.6rem;max-width:620px;flex-wrap:wrap;">
      <input type="text" id="r-input"
             placeholder="e.g.  Surjeet  or  LeCun, Yann  or  Hinton"
             oninput="debounceAuthorSearch()"
             onkeydown="if(event.key==='Enter')searchAuthors()"
             style="flex:1;min-width:220px;"/>
      <button onclick="searchAuthors()" style="
        background:rgba(0,224,188,.1);border:1px solid rgba(0,224,188,.3);
        border-radius:var(--radius);color:var(--cyan);font-family:var(--font-mono);
        font-size:.78rem;padding:.5rem 1rem;cursor:pointer;">Search</button>
    </div>
  </div>
  <div id="r-results">
    <div class="no-results">
      <div class="no-results-title">Researcher Profile</div>
      <div class="no-results-sub">Type a name — disambiguation list appears automatically.<br>
      Click any name to load their full profile + ORCID.</div>
    </div>
  </div>`;
}

let authorDebounceTimer = null;
function debounceAuthorSearch() {
  clearTimeout(authorDebounceTimer);
  const q = document.getElementById('r-input').value.trim();
  if(q.length < 2) return;
  authorDebounceTimer = setTimeout(searchAuthors, 320);
}

// ── Step 1: search and show disambiguation list ──────────────────────────────
async function searchAuthors() {
  const name = document.getElementById('r-input').value.trim();
  if(!name) return;
  const resultsDiv = document.getElementById('r-results');
  resultsDiv.innerHTML = `<div style="display:flex;align-items:center;gap:.6rem;color:var(--t2);font-size:.8rem;">
    <div class="spinner"></div> Finding researchers named "${esc(name)}"…</div>`;

  try {
    const r = await fetch(`${API}/api/author_match?name=${encodeURIComponent(name)}&limit=30`);
    const d = await r.json();
    if(d.error){ resultsDiv.innerHTML=`<div class="error-box">${esc(d.error)}</div>`; return; }
    renderDisambiguation(d.matches || [], name, resultsDiv);
  } catch(e){ resultsDiv.innerHTML=`<div class="error-box">${esc(e.toString())}</div>`; }
}

function renderDisambiguation(matches, query, container) {
  if(!matches.length) {
    container.innerHTML=`<div class="no-results">
      <div class="no-results-title">No researchers found for "${esc(query)}"</div>
      <div class="no-results-sub">Try a different spelling or partial last name</div>
    </div>`;
    return;
  }

  const rows = matches.map((m,i) => {
    const initials = m.name.split(/[\s,]+/).filter(Boolean).slice(0,2)
                      .map(w=>w[0]||'').join('').toUpperCase();
    const col = COLORS[i % COLORS.length];
    return `<div class="author-row" onclick="loadResearcherProfile('${esc(m.name).replace(/'/g,"\\'")}')">
      <div style="width:34px;height:34px;border-radius:50%;flex-shrink:0;
                  background:rgba(77,158,255,.1);border:1px solid ${col}33;
                  display:flex;align-items:center;justify-content:center;
                  font-family:var(--font-display);font-weight:700;font-size:.78rem;color:${col};">${initials}</div>
      <span class="ar-name">${esc(m.name)}</span>
      <span class="ar-count">${m.count} paper${m.count!==1?'s':''}</span>
    </div>`;
  }).join('');

  container.innerHTML = `
    <div class="section-label" style="margin-bottom:.8rem;">
      ${matches.length} researcher${matches.length!==1?'s':''} named "${esc(query)}" — click to view profile
    </div>
    <div style="background:var(--card);border:1px solid var(--b0);border-radius:var(--radius-lg);overflow:hidden;">
      ${rows}
    </div>`;
}

// ── Step 2: load full profile for exact name ─────────────────────────────────
async function loadResearcherProfile(exactName) {
  const f = getFilters();
  const resultsDiv = document.getElementById('r-results');
  resultsDiv.innerHTML = `<div style="display:flex;align-items:center;gap:.6rem;color:var(--t2);font-size:.8rem;">
    <div class="spinner"></div> Loading profile for ${esc(exactName)}…</div>`;

  try {
    const params = new URLSearchParams({name:exactName, year_from:f.year_from, year_to:f.year_to});
    const [papersResp, orcidResp] = await Promise.all([
      fetch(`${API}/api/researcher?${params}`),
      fetch(`${API}/api/orcid_lookup?name=${encodeURIComponent(exactName)}`),
    ]);
    const d = await papersResp.json();
    const o = await orcidResp.json();
    if(d.error){ resultsDiv.innerHTML=`<div class="error-box">${esc(d.error)}</div>`; return; }

    resultsDiv.innerHTML = renderFullProfile(exactName, d, o.results || []);
    resultsDiv.querySelectorAll('.tab-btn').forEach(b=>{
      b.onclick = ()=>switchTab(b.dataset.tab);
    });
  } catch(e){ resultsDiv.innerHTML=`<div class="error-box">${esc(e.toString())}</div>`; }
}

// ── Full profile renderer ────────────────────────────────────────────────────
function renderFullProfile(name, d, orcidResults) {
  const papers     = d.papers || [];
  if(!papers.length) return `<div class="no-results">
    <div class="no-results-title">No papers found for "${esc(name)}"</div>
    <div class="no-results-sub">Try widening the year range in the sidebar</div></div>`;

  const yrMap      = d.by_year || {};
  const catMap     = d.by_cat  || {};
  const yrs        = Object.keys(yrMap).map(Number).sort((a,b)=>a-b);
  const initials   = name.split(/[\s,]+/).filter(Boolean).slice(0,2).map(w=>w[0]||'').join('').toUpperCase();
  const topCat     = Object.keys(catMap)[0] || '—';
  const minYr      = yrs[0] || '—';
  const maxYr      = yrs[yrs.length-1] || '—';
  const peak       = yrs.reduce((a,b)=>yrMap[b]>yrMap[a]?b:a, yrs[0]);
  const catEntries = Object.entries(catMap);
  const maxC       = catEntries.reduce((m,[,v])=>Math.max(m,v),1);

  // ── ORCID section ─────────────────────────────────────────────────────────
  let orcidHtml = '';
  if(orcidResults.length > 0) {
    const links = orcidResults.map(o=>{
      const conf    = o.confidence || 0;
      const reason  = o.match_reason || '';
      const confCol = conf >= 60 ? '#00e0bc' : conf >= 30 ? '#f0c040' : '#ff6080';
      const confLbl = conf >= 60 ? 'High confidence' : conf >= 30 ? 'Medium confidence' : 'Low confidence';
      const confBadge = conf > 0
        ? `<span style="font-size:.6rem;padding:1px 6px;border-radius:8px;
                        background:${confCol}18;border:1px solid ${confCol}40;
                        color:${confCol};margin-left:5px;"
                 title="${esc(reason)}">${confLbl} (${conf})</span>`
        : '';
      return `
      <div style="display:inline-flex;align-items:center;gap:6px;
          padding:.28rem .7rem;border-radius:5px;
          background:rgba(166,215,85,.1);border:1px solid rgba(166,215,85,.3);
          font-family:var(--font-mono);font-size:.7rem;cursor:pointer;"
          onclick="window.open('${esc(o.orcid_url)}','_blank')"
          onmouseover="this.style.background='rgba(166,215,85,.18)'"
          onmouseout="this.style.background='rgba(166,215,85,.1)'">
        <svg width="12" height="12" viewBox="0 0 24 24" fill="#a6d755">
          <path d="M12 0C5.372 0 0 5.372 0 12s5.372 12 12 12 12-5.372 12-12S18.628 0 12 0zm-1.25 5.5a1.25 1.25 0 110 2.5 1.25 1.25 0 010-2.5zm-.75 4h1.5v9H10V9.5zm3 0h2c2.5 0 4 1.5 4 4.5s-1.5 4.5-4 4.5h-2V9.5zm1.5 1.5v6h.5c1.5 0 2.5-1 2.5-3s-1-3-2.5-3h-.5z"/>
        </svg>
        <span style="color:#a6d755;">${esc(o.name || name)} · ${esc(o.orcid_id)}</span>
        ${confBadge}
      </div>`;
    }).join('');
    orcidHtml = `
      <div style="margin-top:.6rem;">
        <div style="font-size:.58rem;letter-spacing:.16em;text-transform:uppercase;
                    color:var(--t3);margin-bottom:.4rem;">ORCID — verified by paper overlap</div>
        <div style="display:flex;flex-wrap:wrap;gap:.4rem;">${links}</div>
      </div>`;
  } else {
    orcidHtml = `<div style="margin-top:.5rem;font-size:.68rem;color:var(--t3);">
      No ORCID profile found — <span style="color:var(--blue);cursor:pointer;text-decoration:underline;"
      onclick="window.open('https://orcid.org/orcid-search/search?searchQuery='+encodeURIComponent('${esc(name)}'),'_blank')">Search manually on orcid.org</span></div>`;
  }

  // ── Co-authors ────────────────────────────────────────────────────────────
  const coMap = {};
  papers.forEach(p=>{
    (p.authors||'').split(/[;,]/).map(a=>a.trim()).filter(a=>a&&a!==name&&!name.toLowerCase().split(/\s+/).every(w=>a.toLowerCase().includes(w))).forEach(a=>{
      coMap[a] = (coMap[a]||0)+1;
    });
  });
  const topCo = Object.entries(coMap).sort((a,b)=>b[1]-a[1]).slice(0,8);

  const coHtml = topCo.length ? topCo.map(([a,c],i)=>`
    <div class="hbar-row" style="cursor:pointer;" onclick="loadResearcherProfile('${esc(a).replace(/'/g,"\\'")}')">
      <div class="hbar-top">
        <span class="hbar-label" style="color:var(--blue);">${esc(a)}</span>
        <span class="hbar-val" style="color:${COLORS[i%COLORS.length]};">${c} paper${c!==1?'s':''}</span>
      </div>
      <div class="hbar-track">
        <div class="hbar-fill" style="width:${(c/topCo[0][1]*100).toFixed(0)}%;background:${COLORS[i%COLORS.length]};"></div>
      </div>
    </div>`).join('') : '<div style="color:var(--t3);font-size:.75rem;">No co-author data</div>';

  // ── Category bars ─────────────────────────────────────────────────────────
  const catBars = catEntries.map(([cat,cnt],i)=>`
    <div class="hbar-row">
      <div class="hbar-top">
        <span class="hbar-label">${esc(cat)}</span>
        <span class="hbar-val" style="color:${COLORS[i%COLORS.length]};">${cnt} · ${(cnt/papers.length*100).toFixed(1)}%</span>
      </div>
      <div class="hbar-track">
        <div class="hbar-fill" style="width:${(cnt/maxC*100).toFixed(1)}%;background:${COLORS[i%COLORS.length]};"></div>
      </div>
    </div>`).join('');

  // ── Timeline ──────────────────────────────────────────────────────────────
  const maxY = Math.max(...Object.values(yrMap),1);
  const tBars = yrs.map(y=>`
    <div style="display:flex;align-items:center;gap:9px;margin-bottom:5px;">
      <div style="font-size:.68rem;color:var(--t3);width:32px;text-align:right;">${y}</div>
      <div style="width:${Math.max(3,Math.round(260*yrMap[y]/maxY))}px;height:10px;
                  background:linear-gradient(90deg,var(--blue),var(--cyan));
                  border-radius:2px;opacity:.85;"></div>
      <div style="font-size:.68rem;color:var(--blue);">${yrMap[y]}</div>
    </div>`).join('');

  let cum=0;
  const cumBars = yrs.map(y=>{cum+=yrMap[y]; return `
    <div style="display:flex;align-items:center;gap:9px;margin-bottom:5px;">
      <div style="font-size:.68rem;color:var(--t3);width:32px;text-align:right;">${y}</div>
      <div style="width:${Math.max(3,Math.round(260*cum/papers.length))}px;height:10px;
                  background:linear-gradient(90deg,var(--violet),var(--blue));
                  border-radius:2px;opacity:.85;"></div>
      <div style="font-size:.68rem;color:var(--violet);">${cum}</div>
    </div>`;}).join('');

  // ── Year pills ────────────────────────────────────────────────────────────
  const pills = yrs.slice().reverse().map(y=>
    `<span class="year-pill" style="cursor:pointer;" onclick="filterResearcherByYear(${y})">
      <span class="year-pill-yr">${y}</span>
      <span class="year-pill-cnt">${yrMap[y]}</span>
    </span>`).join('');

  return `
  <!-- Back button -->
  <button onclick="searchAuthors()" style="
    background:transparent;border:1px solid var(--b1);border-radius:6px;
    color:var(--t2);font-family:var(--font-mono);font-size:.7rem;
    padding:.35rem .8rem;cursor:pointer;margin-bottom:1rem;display:inline-flex;align-items:center;gap:5px;">
    ← Back to results
  </button>

  <!-- Profile card -->
  <div class="profile-card fade-in">
    <div class="profile-avatar">${initials}</div>
    <div class="profile-info">
      <div class="profile-name">${esc(name)}</div>
      <div class="profile-meta">
        Top field: <span>${esc(topCat)}</span> &nbsp;·&nbsp;
        Active: ${minYr}–${maxYr} &nbsp;·&nbsp;
        Peak: ${peak} (${yrMap[peak]} paper${yrMap[peak]!==1?'s':''})
      </div>
      ${orcidHtml}
    </div>
    <div class="profile-stats">
      <div class="pstat"><div class="pstat-val" style="color:var(--cyan);">${papers.length}</div><div class="pstat-lbl">Papers</div></div>
      <div class="pstat"><div class="pstat-val" style="color:var(--blue);">${yrs.length}</div><div class="pstat-lbl">Active Yrs</div></div>
      <div class="pstat"><div class="pstat-val" style="color:var(--gold);">${catEntries.length}</div><div class="pstat-lbl">Fields</div></div>
      <div class="pstat"><div class="pstat-val" style="color:var(--violet);">${topCo.length}</div><div class="pstat-lbl">Co-authors</div></div>
    </div>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <button class="tab-btn active" data-tab="rp-papers">📄 All Papers</button>
    <button class="tab-btn" data-tab="rp-timeline">📈 Timeline</button>
    <button class="tab-btn" data-tab="rp-fields">🏷 Fields</button>
    <button class="tab-btn" data-tab="rp-coauthors">🤝 Co-authors</button>
  </div>

  <div id="rp-papers" class="tab-panel active">
    <div class="year-pills fade-in" style="margin-bottom:1rem;">${pills}</div>
    ${buildTable(papers)}
    <div class="caption">↳ ${papers.length} papers · ${d.elapsed_ms}ms · click a year pill to filter</div>
  </div>

  <div id="rp-timeline" class="tab-panel">
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;">
      <div class="analytics-card">
        <div class="analytics-card-title">Papers Per Year</div>${tBars}
      </div>
      <div class="analytics-card">
        <div class="analytics-card-title">Cumulative Output</div>${cumBars}
      </div>
    </div>
  </div>

  <div id="rp-fields" class="tab-panel">
    <div class="analytics-card" style="max-width:560px;">
      <div class="analytics-card-title">Research Fields</div>${catBars}
    </div>
  </div>

  <div id="rp-coauthors" class="tab-panel">
    <div class="analytics-card" style="max-width:560px;">
      <div class="analytics-card-title">Top Co-authors — click any name to view their profile</div>
      ${coHtml}
    </div>
  </div>`;
}

// ── Filter researcher papers by clicking a year pill ────────────────────────
function filterResearcherByYear(yr) {
  const allRows = document.querySelectorAll('#rp-papers .results-table tbody tr');
  allRows.forEach(row => {
    const yearCell = row.querySelector('.td-year');
    if(!yearCell) return;
    row.style.display = (yearCell.textContent.trim() === String(yr)) ? '' : 'none';
  });
  // Show a reset button
  const pills = document.querySelector('.year-pills');
  if(pills && !document.getElementById('reset-yr-filter')) {
    const btn = document.createElement('button');
    btn.id = 'reset-yr-filter';
    btn.textContent = '✕ Show all years';
    btn.style.cssText = 'margin-left:8px;padding:3px 8px;border-radius:4px;background:rgba(255,96,128,.1);border:1px solid rgba(255,96,128,.3);color:#ff6080;font-family:var(--font-mono);font-size:.66rem;cursor:pointer;';
    btn.onclick = () => {
      allRows.forEach(r=>r.style.display='');
      btn.remove();
    };
    pills.appendChild(btn);
  }
}

function switchTab(id) {
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.toggle('active',b.dataset.tab===id));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.toggle('active',p.id===id));
}

// ── Category mode ────────────────────────────────────────────────────────────
async function showCategoryBrowse() {
  const panel = document.getElementById('panel-category');
  panel.innerHTML = `
    <div style="margin-bottom:1.2rem;">
      <div class="section-label" style="margin-bottom:.6rem;">Category Code</div>
      <div style="display:flex;gap:.6rem;max-width:560px;flex-wrap:wrap;">
        <input type="text" id="cat-input" placeholder="e.g.  astro-ph  or  cs.LG"
               onkeydown="if(event.key==='Enter')searchByCategory()"
               style="flex:1;min-width:200px;"/>
        <input type="text" id="cat-kw" placeholder="Optional keyword in title…"
               onkeydown="if(event.key==='Enter')searchByCategory()"
               style="flex:1;min-width:200px;"/>
        <button onclick="searchByCategory()" style="
          background:rgba(0,224,188,.1);border:1px solid rgba(0,224,188,.3);
          border-radius:var(--radius);color:var(--cyan);font-family:var(--font-mono);
          font-size:.78rem;padding:.5rem 1rem;cursor:pointer;">Search</button>
      </div>
    </div>
    <div id="cat-results"></div>`;

  // Load count cards for popular categories
  const KNOWN = [
    ['cs.LG','Machine Learning'],['cs.AI','Artificial Intelligence'],
    ['cs.CV','Computer Vision'],['cs.CL','Language & NLP'],
    ['stat.ML','Statistics ML'],['astro-ph','Astrophysics'],
    ['quant-ph','Quantum Physics'],['hep-th','High Energy Physics'],
    ['cs.RO','Robotics'],['math.ST','Math Statistics'],
    ['cs.NE','Neural & Evol.'],['cs.CR','Cryptography'],
  ];
  const f = getFilters();
  const colorsArr = ['#00e0bc','#4d9eff','#9d7cf4','#f0c040','#ff6080',
                     '#00e0bc','#4d9eff','#9d7cf4','#f0c040','#ff6080','#00e0bc','#4d9eff'];
  document.getElementById('cat-results').innerHTML =
    `<div class="section-label">Popular Categories</div><div class="cat-grid" id="cat-grid">
      ${KNOWN.map(([code,name],i)=>
        `<div class="cat-card" style="border-left-color:${colorsArr[i]};"
              onclick="document.getElementById('cat-input').value='${code}';searchByCategory()">
          <div class="cat-code" style="color:${colorsArr[i]};">${code}</div>
          <div class="cat-name">${name}</div>
          <div class="cat-count">Loading…</div>
        </div>`).join('')}
    </div>`;

  // Load all category counts in ONE fast request (uses summary table)
  try {
    const cr = await fetch(`${API}/api/category_counts?year_from=${f.year_from}&year_to=${f.year_to}`);
    const cd = await cr.json();
    const counts = cd.counts || {};
    const cards  = document.querySelectorAll('.cat-card');
    KNOWN.forEach(([code], i) => {
      const cnt = counts[code.toLowerCase()] || counts[code] || 0;
      if(cards[i]) cards[i].querySelector('.cat-count').textContent =
        cnt ? cnt.toLocaleString()+' papers' : '—';
    });
  } catch(e) {
    // silently fail — counts just stay as "loading"
  }
}

async function searchByCategory() {
  const cat = document.getElementById('cat-input').value.trim();
  const kw  = document.getElementById('cat-kw').value.trim();
  if(!cat) return;
  const f = getFilters();
  document.getElementById('cat-results').innerHTML =
    `<div style="display:flex;align-items:center;gap:.6rem;color:var(--t2);font-size:.8rem;">
      <div class="spinner"></div> Loading…</div>`;

  const q = kw || cat;
  const field = kw ? 'title' : 'category';
  const params = new URLSearchParams({
    q, field, sort:f.sort, limit:500,
    year_from:f.year_from, year_to:f.year_to,
    category: kw ? cat : ''
  });
  try {
    const r = await fetch(`${API}/api/search?${params}`);
    const d = await r.json();
    if(!d.results||!d.results.length){
      document.getElementById('cat-results').innerHTML =
        `<div class="no-results"><div class="no-results-title">No papers found in ${cat}</div></div>`;
      return;
    }
    // Top authors
    const authMap={};
    d.results.forEach(p=>{ (p.authors||'').split(';').forEach(a=>{a=a.trim();if(a)authMap[a]=(authMap[a]||0)+1;}); });
    const topAuth = Object.entries(authMap).sort((a,b)=>b[1]-a[1]).slice(0,8);
    const maxA = topAuth[0]?.[1]||1;
    const authBars = topAuth.map(([a,c],i)=>`
      <div class="hbar-row">
        <div class="hbar-top">
          <span class="hbar-label" style="font-size:.7rem;">${a.length>34?a.slice(0,34)+'…':a}</span>
          <span class="hbar-val" style="color:${COLORS[i%COLORS.length]};">${c}</span>
        </div>
        <div class="hbar-track">
          <div class="hbar-fill" style="width:${(c/maxA*100).toFixed(1)}%;background:${COLORS[i%COLORS.length]};"></div>
        </div>
      </div>`).join('');

    document.getElementById('cat-results').innerHTML = `
      <div class="metrics-row fade-in">
        ${metricCard('Papers',d.results.length.toLocaleString(),'cyan')}
        ${metricCard('Period',`${f.year_from}–${f.year_to}`,'blue')}
        ${metricCard('Latency',d.elapsed_ms+'ms','gold')}
        ${metricCard('Authors',new Set(d.results.map(r=>r.authors)).size.toLocaleString(),'violet')}
      </div>
      <div class="analytics-card fade-in" style="margin-bottom:1rem;max-width:580px;">
        <div class="analytics-card-title">Top Authors in ${cat}</div>${authBars}
      </div>
      <div class="section-label fade-in">Results · ${d.results.length.toLocaleString()} papers</div>
      ${buildTable(d.results)}`;
  } catch(e){ document.getElementById('cat-results').innerHTML=`<div class="error-box">${e}</div>`; }
}

// ── Analytics mode ───────────────────────────────────────────────────────────
async function loadAnalytics() {
  const panel = document.getElementById('panel-analytics');
  if(currentMode!=='analytics') return;
  const f = getFilters();
  panel.innerHTML = `<div style="display:flex;align-items:center;gap:.6rem;color:var(--t2);font-size:.8rem;">
    <div class="spinner"></div> Calculating…</div>`;
  try {
    const r = await fetch(`${API}/api/analytics?year_from=${f.year_from}&year_to=${f.year_to}`);
    const d = await r.json();
    if(d.error){ panel.innerHTML=`<div class="error-box">${d.error}</div>`; return; }
    renderAnalytics(d, panel, f);
  } catch(e){ panel.innerHTML=`<div class="error-box">${e}</div>`; }
}

function renderAnalytics(d, panel, f) {
  const span    = f.year_to - f.year_from + 1;
  const avg     = Math.round(d.total/Math.max(1,span));
  const doiPct  = d.total ? (d.with_doi/d.total*100).toFixed(1) : '0';

  const yrMap  = d.by_year||{};
  const yrKeys = Object.keys(yrMap).map(Number).sort((a,b)=>a-b);
  const peakYr = yrKeys.reduce((a,b)=>yrMap[b]>yrMap[a]?b:a,yrKeys[0]||0);

  const catEntries = Object.entries(d.by_category||{}).sort((a,b)=>b[1]-a[1]);
  const maxC = catEntries[0]?.[1]||1;
  const catBars = catEntries.map(([cat,cnt],i)=>`
    <div class="hbar-row">
      <div class="hbar-top">
        <span class="hbar-label">${cat}</span>
        <span class="hbar-val" style="color:${COLORS[i%COLORS.length]};">
          ${cnt.toLocaleString()} · ${(cnt/Math.max(1,d.total)*100).toFixed(1)}%</span>
      </div>
      <div class="hbar-track">
        <div class="hbar-fill" style="width:${(cnt/maxC*100).toFixed(1)}%;background:${COLORS[i%COLORS.length]};"></div>
      </div>
    </div>`).join('');

  panel.innerHTML = `
  <div class="metrics-row fade-in">
    ${metricCard('Papers in Period', d.total.toLocaleString(), 'cyan')}
    ${metricCard('With Valid DOI',   d.with_doi.toLocaleString(), 'blue')}
    ${metricCard('Avg / Year',       avg.toLocaleString(), 'gold')}
    ${metricCard('Years Covered',    span, 'violet')}
  </div>
  ${buildBarChart(yrMap, yrKeys, 'Annual Volume')}
  <div class="analytics-grid fade-in">
    <div class="analytics-card">
      <div class="analytics-card-title">Top Categories</div>${catBars}
    </div>
    <div class="analytics-card">
      <div class="analytics-card-title">Corpus Health</div>
      <div class="hbar-row">
        <div class="hbar-top"><span class="hbar-label">DOI Coverage</span>
          <span class="hbar-val" style="color:var(--cyan);">${doiPct}%</span></div>
        <div class="hbar-track"><div class="hbar-fill" style="width:${doiPct}%;background:var(--cyan);"></div></div>
      </div>
      <div class="hbar-row">
        <div class="hbar-top"><span class="hbar-label">Missing DOI</span>
          <span class="hbar-val" style="color:var(--rose);">${(100-doiPct).toFixed(1)}%</span></div>
        <div class="hbar-track"><div class="hbar-fill" style="width:${(100-doiPct).toFixed(1)}%;background:var(--rose);"></div></div>
      </div>
      <div style="margin-top:1.3rem;padding-top:1rem;border-top:1px solid var(--b0);">
        <div style="font-size:.56rem;letter-spacing:.18em;text-transform:uppercase;color:var(--t3);margin-bottom:3px;">Peak Year</div>
        <div style="font-family:var(--font-display);font-weight:700;font-size:1.1rem;color:var(--t1);">
          ${peakYr} <span style="font-size:.82rem;color:var(--t3);">(${(yrMap[peakYr]||0).toLocaleString()} papers)</span>
        </div>
      </div>
    </div>
  </div>`;
}

// ── Welcome ──────────────────────────────────────────────────────────────────
async function showWelcome() {
  const panel = document.getElementById('panel-search');

  // Quick-search chips data
  const SUGGESTIONS = [
    {label:'Large language models',   q:'large language models'},
    {label:'Quantum computing',        q:'quantum computing'},
    {label:'Dark matter detection',    q:'dark matter'},
    {label:'Transformer architecture', q:'transformer'},
    {label:'Black holes',              q:'black hole'},
    {label:'CRISPR gene editing',      q:'CRISPR'},
    {label:'Reinforcement learning',   q:'reinforcement learning'},
    {label:'Climate modelling',        q:'climate model'},
    {label:'Neural networks',          q:'neural network'},
    {label:'Gravitational waves',      q:'gravitational waves'},
    {label:'Protein folding',          q:'protein folding'},
    {label:'Diffusion models',         q:'diffusion model'},
  ];

  const CATEGORIES = [
    {code:'cs.LG',   name:'Machine Learning',      color:'var(--cyan)'},
    {code:'cs.AI',   name:'Artificial Intelligence',color:'var(--blue)'},
    {code:'cs.CV',   name:'Computer Vision',        color:'var(--violet)'},
    {code:'cs.CL',   name:'NLP',                   color:'var(--gold)'},
    {code:'astro-ph',name:'Astrophysics',           color:'#ff6080'},
    {code:'quant-ph',name:'Quantum Physics',        color:'var(--cyan)'},
    {code:'hep-th',  name:'High Energy Physics',    color:'var(--blue)'},
    {code:'math.ST', name:'Statistics',             color:'var(--violet)'},
  ];

  const chips = SUGGESTIONS.map(s =>
    `<button onclick="quickSearch('${s.q}')" style="
      padding:5px 13px;border-radius:20px;font-size:.7rem;cursor:pointer;
      border:1px solid var(--b1);background:var(--card);color:var(--t2);
      font-family:var(--font-mono);transition:all .18s;letter-spacing:.03em;"
      onmouseover="this.style.borderColor='var(--cyan)';this.style.color='var(--cyan)'"
      onmouseout="this.style.borderColor='var(--b1)';this.style.color='var(--t2)'">
      ${esc(s.label)}
    </button>`
  ).join('');

  const catCards = CATEGORIES.map(c =>
    `<div onclick="quickSearch('${c.code}')" style="
      padding:.6rem .9rem;border-radius:8px;cursor:pointer;
      border:1px solid var(--b0);background:var(--card);
      transition:all .18s;"
      onmouseover="this.style.borderColor='${c.color}';this.style.background='var(--hover)'"
      onmouseout="this.style.borderColor='var(--b0)';this.style.background='var(--card)'">
      <div style="font-size:.6rem;letter-spacing:.12em;text-transform:uppercase;color:${c.color};margin-bottom:2px;">${esc(c.code)}</div>
      <div style="font-size:.78rem;color:var(--t1);">${esc(c.name)}</div>
      <div class="cat-count-badge" data-code="${esc(c.code)}" style="font-size:.66rem;color:var(--t3);margin-top:2px;">loading…</div>
    </div>`
  ).join('');

  panel.innerHTML = `
  <div class="fade-in">

    <!-- Corpus stats row -->
    <div id="welcome-stats" style="
      display:grid;grid-template-columns:repeat(4,1fr);gap:10px;
      margin-bottom:1.8rem;">
      <div style="background:var(--card);border:1px solid var(--b0);border-radius:10px;padding:1rem 1.2rem;">
        <div style="font-size:.55rem;letter-spacing:.16em;text-transform:uppercase;color:var(--t3);margin-bottom:.4rem;">01 · Scope</div>
        <div style="font-family:var(--font-display);font-size:1.6rem;font-weight:700;color:var(--cyan);line-height:1;" id="ws-total">—</div>
        <div style="font-size:.66rem;color:var(--t3);margin-top:.3rem;">papers indexed</div>
      </div>
      <div style="background:var(--card);border:1px solid var(--b0);border-radius:10px;padding:1rem 1.2rem;">
        <div style="font-size:.55rem;letter-spacing:.16em;text-transform:uppercase;color:var(--t3);margin-bottom:.4rem;">02 · Speed</div>
        <div style="font-family:var(--font-display);font-size:1.6rem;font-weight:700;color:var(--blue);line-height:1;">Sub-50ms</div>
        <div style="font-size:.66rem;color:var(--t3);margin-top:.3rem;">indexed SQLite WAL</div>
      </div>
      <div style="background:var(--card);border:1px solid var(--b0);border-radius:10px;padding:1rem 1.2rem;">
        <div style="font-size:.55rem;letter-spacing:.16em;text-transform:uppercase;color:var(--t3);margin-bottom:.4rem;">03 · Year range</div>
        <div style="font-family:var(--font-display);font-size:1.6rem;font-weight:700;color:var(--gold);line-height:1;" id="ws-years">—</div>
        <div style="font-size:.66rem;color:var(--t3);margin-top:.3rem;">publication span</div>
      </div>
      <div style="background:var(--card);border:1px solid var(--b0);border-radius:10px;padding:1rem 1.2rem;">
        <div style="font-size:.55rem;letter-spacing:.16em;text-transform:uppercase;color:var(--t3);margin-bottom:.4rem;">04 · Quality</div>
        <div style="font-family:var(--font-display);font-size:1.6rem;font-weight:700;color:var(--violet);line-height:1;">10-Layer</div>
        <div style="font-size:.66rem;color:var(--t3);margin-top:.3rem;">cleaning pipeline</div>
      </div>
    </div>

    <!-- Quick search suggestions -->
    <div style="margin-bottom:1.6rem;">
      <div class="section-label" style="margin-bottom:.7rem;">Quick searches</div>
      <div style="display:flex;flex-wrap:wrap;gap:7px;">${chips}</div>
    </div>

    <!-- Category browser -->
    <div style="margin-bottom:1.6rem;">
      <div class="section-label" style="margin-bottom:.7rem;">Browse by field</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;">${catCards}</div>
    </div>

    <!-- Recent papers -->
    <div>
      <div class="section-label" style="margin-bottom:.7rem;">Most recent papers</div>
      <div id="welcome-recent">
        <div style="display:flex;align-items:center;gap:.5rem;color:var(--t2);font-size:.75rem;">
          <div class="spinner"></div> Loading recent papers…
        </div>
      </div>
    </div>

  </div>`;

  // Fetch corpus stats and fill live numbers
  try {
    const info = await fetch('/api/info').then(r=>r.json());
    if(info.total){
      const t = info.total;
      const label = t >= 1000000
        ? (t/1000000).toFixed(2)+'M'
        : t >= 1000 ? (t/1000).toFixed(0)+'K' : t.toString();
      document.getElementById('ws-total').textContent = label;
    }
    if(info.min_year && info.max_year)
      document.getElementById('ws-years').textContent = info.min_year+'–'+info.max_year;
  } catch(e){}

  // Fetch all category counts in ONE request (fast summary table)
  try {
    const f2  = getFilters();
    const cr  = await fetch(`/api/category_counts?year_from=${f2.year_from}&year_to=${f2.year_to}`);
    const cd  = await cr.json();
    const counts = cd.counts || {};
    CATEGORIES.forEach(c => {
      const badge = document.querySelector(`.cat-count-badge[data-code="${c.code}"]`);
      const cnt   = counts[c.code.toLowerCase()] || counts[c.code] || 0;
      if(badge) badge.textContent = cnt ? cnt.toLocaleString()+' papers' : '—';
    });
  } catch(e){}

  // Load 5 most recent papers
  try {
    const r = await fetch('/api/search?q=&sort=newest&limit=5');
    const d = await r.json();
    const recDiv = document.getElementById('welcome-recent');
    if(recDiv && d.results && d.results.length){
      recDiv.innerHTML = buildTable(d.results.slice(0,5));
    }
  } catch(e){}
}

function quickSearch(q) {
  const box = document.getElementById('search-box');
  if(box){ box.value = q; }
  doSearch();
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function metricCard(label, value, accent='cyan') {
  const colors={cyan:'var(--cyan)',blue:'var(--blue)',gold:'var(--gold)',violet:'var(--violet)'};
  return `<div class="metric-card">
    <div class="metric-label">${label}</div>
    <div class="metric-val" style="color:${colors[accent]||colors.cyan};">${value}</div>
  </div>`;
}

function buildBarChart(yrMap, yrKeys, title) {
  if(!yrKeys.length) return '';
  const maxV = Math.max(...yrKeys.map(k=>yrMap[k]),1);
  const bars = yrKeys.map(yr=>{
    const h = Math.max(4, Math.round(60 * yrMap[yr]/maxV));
    return `<div class="bar-col">
      <div class="bar-count">${yrMap[yr]}</div>
      <div class="bar-rect" style="height:${h}px;" title="${yr}: ${yrMap[yr]} papers"></div>
      <div class="bar-yr">${yr}</div>
    </div>`;}).join('');
  return `<div class="trend-card fade-in">
    <div class="trend-title">${title}</div>
    <div class="bar-wrap">${bars}</div>
  </div>`;
}

// ── Pagination UI builder ─────────────────────────────────────────────────────
function buildPagination(page, totalPages, totalCount, rangeStart, rangeEnd, pageSize) {
  if(totalPages <= 1) return '';

  // Build page number buttons — show window of 7 pages around current
  let pages = [];
  const window = 3;
  for(let i = Math.max(1, page-window); i <= Math.min(totalPages, page+window); i++) {
    pages.push(i);
  }

  const btnStyle = (active) => `
    display:inline-flex;align-items:center;justify-content:center;
    min-width:34px;height:30px;padding:0 8px;
    border-radius:5px;border:1px solid ${active ? 'rgba(0,224,188,.45)' : 'var(--b0)'};
    background:${active ? 'rgba(0,224,188,.1)' : 'var(--card)'};
    color:${active ? 'var(--cyan)' : 'var(--t2)'};
    font-family:var(--font-mono);font-size:.72rem;cursor:${active ? 'default' : 'pointer'};
    transition:all .15s;`;

  const hoverBtn = `onmouseover="this.style.borderColor='rgba(0,224,188,.35)';this.style.color='var(--cyan)'"
                    onmouseout="this.style.borderColor='var(--b0)';this.style.color='var(--t2)'"`;

  let btns = '';

  // Prev button
  if(page > 1) {
    btns += `<button style="${btnStyle(false)}" ${hoverBtn} onclick="goToPage(${page-1})">← Prev</button>`;
  }

  // First page + ellipsis
  if(pages[0] > 1) {
    btns += `<button style="${btnStyle(false)}" ${hoverBtn} onclick="goToPage(1)">1</button>`;
    if(pages[0] > 2) btns += `<span style="color:var(--t3);padding:0 4px;font-size:.72rem;">…</span>`;
  }

  // Page number buttons
  pages.forEach(p => {
    btns += `<button style="${btnStyle(p===page)}" ${p===page?'':''+hoverBtn} onclick="goToPage(${p})">${p}</button>`;
  });

  // Last page + ellipsis
  if(pages[pages.length-1] < totalPages) {
    if(pages[pages.length-1] < totalPages-1) btns += `<span style="color:var(--t3);padding:0 4px;font-size:.72rem;">…</span>`;
    btns += `<button style="${btnStyle(false)}" ${hoverBtn} onclick="goToPage(${totalPages})">${totalPages}</button>`;
  }

  // Next button
  if(page < totalPages) {
    btns += `<button style="${btnStyle(false)}" ${hoverBtn} onclick="goToPage(${page+1})">Next →</button>`;
  }

  // Jump to page input
  const jumpStyle = `
    width:52px;height:30px;text-align:center;
    background:var(--card);border:1px solid var(--b1);border-radius:5px;
    color:var(--t1);font-family:var(--font-mono);font-size:.72rem;`;

  return `
  <div class="fade-in" style="
    margin:1.1rem 0 .4rem;
    display:flex;align-items:center;gap:6px;flex-wrap:wrap;
  ">
    ${btns}
    <span style="margin-left:.6rem;font-size:.68rem;color:var(--t3);">
      Page ${page} of ${totalPages.toLocaleString()}
      &nbsp;·&nbsp; ${totalCount.toLocaleString()} total
    </span>
    <span style="margin-left:auto;display:flex;align-items:center;gap:6px;">
      <span style="font-size:.66rem;color:var(--t3);">Jump to</span>
      <input type="number" id="jump-input" min="1" max="${totalPages}" value="${page}"
             style="${jumpStyle}"
             onkeydown="if(event.key==='Enter'){const v=parseInt(this.value);if(v>=1&&v<=${totalPages})goToPage(v);}"/>
      <button style="${btnStyle(false)}" ${hoverBtn}
              onclick="const v=parseInt(document.getElementById('jump-input').value);if(v>=1&&v<=${totalPages})goToPage(v);">
        Go
      </button>
    </span>
  </div>`;
}

function buildTable(results) {
  if(!results.length) return '';
  const hasCat = results[0].hasOwnProperty('categories');
  const rows = results.slice(0,1000).map(r=>{
    const doi      = r.doi||'';
    const arxivId  = r.arxiv_id||'';

    // Build open URL: DOI preferred, arXiv abstract page as fallback
    const openUrl  = doi
      ? `https://doi.org/${doi}`
      : arxivId ? `https://arxiv.org/abs/${arxivId}` : '';

    // Build PDF URL: sci-hub for DOI, arXiv PDF for preprints
    const pdfUrl   = doi
      ? `https://sci-hub.ru/${doi}`
      : arxivId ? `https://arxiv.org/pdf/${arxivId}` : '';

    const title   = (r.title||'').slice(0,120);
    const authors = (r.authors||'').slice(0,55);
    const cats    = hasCat ? ((r.categories||'').slice(0,28)) : '';

    const openLabel = doi ? '🔗 Open' : arxivId ? '🔗 arXiv' : '🔗 Open';
    const pdfLabel  = doi ? '📥 PDF'  : arxivId ? '📥 PDF'  : '📥 PDF';

    const openClick = openUrl ? `onclick="window.open('${openUrl}','_blank')"` : '';
    const pdfClick  = pdfUrl  ? `onclick="window.open('${pdfUrl}','_blank')"` : '';

    return `<tr>
      <td class="td-title">${esc(title)}</td>
      <td class="td-authors">${esc(authors)}</td>
      <td class="td-year">${r.year||'—'}</td>
      ${hasCat?`<td class="td-cat">${esc(cats)}</td>`:''}
      <td class="td-links">
        <button class="link-btn" ${openClick} ${!openUrl?'disabled':''}>${openLabel}</button>
        <button class="link-btn" ${pdfClick}  ${!pdfUrl?'disabled':''}>${pdfLabel}</button>
      </td>
    </tr>`;}).join('');
  return `<div class="table-wrap fade-in">
    <table class="results-table">
      <thead><tr>
        <th>Title</th><th>Authors</th><th>Year</th>
        ${results[0].hasOwnProperty('categories')?'<th>Category</th>':''}
        <th>Links</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function showError(panelId, msg) {
  document.getElementById(panelId).innerHTML =
    `<div class="error-box">❌ ${esc(msg)}</div>`;
}


// ═══════════════════════════════════════════════════════════════════════════
//  AUTHOR NETWORK MODULE
// ═══════════════════════════════════════════════════════════════════════════

let netSimulation = null;
let selectedAuthor = null;
const NODE_COLORS = ['#00e0bc','#4d9eff','#9d7cf4','#f0c040','#ff6080','#4dffb8'];

// ── Init network page ────────────────────────────────────────────────────────
async function showNetworkPage() {
  const panel = document.getElementById('panel-network');
  panel.innerHTML = `
  <div class="network-layout" id="net-layout">

    <!-- Left: author list -->
    <div class="author-list-panel">
      <div class="alp-header">
        <input class="alp-search" id="author-search" type="text"
               placeholder="Search authors by name…"
               oninput="filterAuthorList()"
               onkeydown="if(event.key==='Enter'){clearTimeout(authorSearchTimer);loadAuthorList(this.value.trim());}"/>
        <div style="display:flex;align-items:center;gap:6px;margin-top:5px;">
          <span style="font-size:.62rem;color:var(--t3);">Show</span>
          <select id="author-limit-sel" onchange="onAuthorLimitChange()" style="
            background:var(--card);border:1px solid var(--b1);border-radius:5px;
            color:var(--t2);font-size:.65rem;padding:2px 5px;cursor:pointer;">
            <option value="25">25</option>
            <option value="50" selected>50</option>
            <option value="100">100</option>
            <option value="200">200</option>
            <option value="500">500</option>
          </select>
          <span style="font-size:.62rem;color:var(--t3);">authors</span>
        </div>
        <div class="alp-meta" id="alp-meta">Loading authors…</div>
      </div>
      <div class="alp-list" id="author-list"></div>
    </div>

    <!-- Right: graph -->
    <div class="graph-panel" id="graph-panel">
      <div class="graph-toolbar">
        <span class="graph-label">Focus Author</span>
        <input class="graph-input" id="net-focus" style="width:180px;"
               placeholder="Name or leave blank for top authors"
               onkeydown="if(event.key==='Enter')buildNetwork()"/>

        <span class="graph-label">Depth</span>
        <select class="graph-input" id="net-depth" style="width:64px;">
          <option value="1">1</option>
          <option value="2">2</option>
        </select>

        <span class="graph-label">Min shared papers</span>
        <select class="graph-input" id="net-minpapers" style="width:56px;">
          <option value="1">1</option>
          <option value="2" selected>2</option>
          <option value="3">3</option>
          <option value="5">5</option>
        </select>

        <span class="graph-label">Max nodes</span>
        <select class="graph-input" id="net-maxnodes" style="width:64px;">
          <option value="40">40</option>
          <option value="80" selected>80</option>
          <option value="120">120</option>
        </select>

        <button class="graph-btn" onclick="buildNetwork()">⚡ Build Graph</button>
        <button class="graph-btn secondary" onclick="resetNetwork()">↺ Reset</button>
        <button class="graph-btn secondary" onclick="downloadSVG()">↓ SVG</button>

        <span id="net-loading" style="display:none;">
          <span class="spinner"></span>
        </span>
      </div>

      <div id="graph-container" style="flex:1;display:flex;flex-direction:column;overflow:hidden;">
        <div class="graph-empty">
          <div class="graph-empty-icon">🕸</div>
          <div class="graph-empty-title">Collaboration Network</div>
          <div class="graph-empty-sub">
            Click an author from the list on the left,<br/>
            or type a name in the Focus field and press<br/>
            <strong style="color:var(--cyan);">Build Graph</strong>
          </div>
        </div>
      </div>

      <div class="graph-legend">
        <div class="legend-item"><div class="legend-dot" style="background:var(--cyan);box-shadow:0 0 6px var(--cyan);"></div>Seed / Focus author</div>
        <div class="legend-item"><div class="legend-dot" style="background:var(--blue);"></div>Direct collaborator</div>
        <div class="legend-item"><div class="legend-dot" style="background:var(--violet);"></div>2nd-degree connection</div>
        <div class="legend-item" style="margin-top:.2rem;border-top:1px solid var(--b0);padding-top:.3rem;">
          <div style="width:28px;height:1.5px;background:rgba(80,150,255,.6);"></div>Edge weight = shared papers
        </div>
      </div>

      <div class="net-stats" id="net-stats" style="display:none;"></div>
    </div>

  </div>`;

  await loadAuthorList();
}

// ── Author list ──────────────────────────────────────────────────────────────
let allAuthors      = [];
let authorListLimit = 50;    // default rows shown in author list
let authorSearchTimer = null;

async function loadAuthorList(searchQuery='') {
  const f    = getFilters();
  const meta = document.getElementById('alp-meta');
  const list = document.getElementById('author-list');
  if(!meta || !list) return;

  meta.textContent = 'Loading…';
  list.innerHTML   = '<div style="padding:1rem;color:var(--t3);font-size:.72rem;">Loading authors…</div>';

  try {
    const params = new URLSearchParams({
      limit:     authorListLimit,
      offset:    0,
      year_from: f.year_from,
      year_to:   f.year_to,
    });
    if(searchQuery) params.set('search', searchQuery);

    const r = await fetch(`${API}/api/authors?${params}`);
    const d = await r.json();
    if(d.error){ meta.textContent = d.error; return; }

    allAuthors = d.authors || [];
    const total = d.total || allAuthors.length;

    if(searchQuery) {
      meta.textContent = `${allAuthors.length} authors matching "${searchQuery}"`;
    } else {
      meta.textContent = `${total.toLocaleString()} unique authors · showing top ${allAuthors.length}`;
    }
    renderAuthorList(allAuthors);
  } catch(e) {
    meta.textContent = 'Could not load authors';
  }
}

function renderAuthorList(authors) {
  const list = document.getElementById('author-list');
  if(!list) return;
  if(!authors.length){
    list.innerHTML = '<div style="padding:1rem;color:var(--t3);font-size:.72rem;">No authors found</div>';
    return;
  }
  const maxC = authors[0]?.count || 1;
  list.innerHTML = authors.map((a, i) => {
    const sel = selectedAuthor === a.name ? ' selected' : '';
    return `<div class="author-row${sel}" onclick="selectAuthor('${esc(a.name).replace(/'/g,"\'")}')">
      <span class="ar-rank">${i+1}</span>
      <span class="ar-badge" style="background:${NODE_COLORS[i%NODE_COLORS.length]};opacity:.7;"></span>
      <span class="ar-name" title="${esc(a.name)}">${esc(a.name)}</span>
      <span class="ar-count">${a.count}</span>
    </div>`;
  }).join('');
}

// Debounced search — queries server, not just local filter
function filterAuthorList() {
  clearTimeout(authorSearchTimer);
  authorSearchTimer = setTimeout(() => {
    const q = document.getElementById('author-search')?.value?.trim() || '';
    loadAuthorList(q);
  }, 320);
}

// Change author list row limit
function onAuthorLimitChange() {
  const sel = document.getElementById('author-limit-sel');
  if(sel) authorListLimit = parseInt(sel.value) || 50;
  const q = document.getElementById('author-search')?.value?.trim() || '';
  loadAuthorList(q);
}

function selectAuthor(name) {
  selectedAuthor = name;
  const focusEl = document.getElementById('net-focus');
  if(focusEl) focusEl.value = name;
  // Highlight selected row
  document.querySelectorAll('.author-row').forEach(r => {
    r.classList.toggle('selected', r.querySelector('.ar-name')?.textContent === name);
  });
  buildNetwork();
}

// ── Build / render network ────────────────────────────────────────────────────
async function buildNetwork() {
  const focus     = document.getElementById('net-focus').value.trim();
  const depth     = document.getElementById('net-depth').value;
  const minP      = document.getElementById('net-minpapers').value;
  const maxN      = document.getElementById('net-maxnodes').value;
  const f         = getFilters();

  document.getElementById('net-loading').style.display = 'inline-flex';

  const params = new URLSearchParams({
    focus, depth, min_papers: minP, max_nodes: maxN,
    year_from: f.year_from, year_to: f.year_to, category: f.category
  });

  try {
    const r  = await fetch(`${API}/api/network?${params}`);
    const d  = await r.json();
    document.getElementById('net-loading').style.display = 'none';

    if(d.error) {
      showGraphError(d.error); return;
    }
    if(!d.nodes || d.nodes.length === 0) {
      showGraphEmpty('No collaboration network found.',
        'Try reducing "Min shared papers" to 1, or pick a different author.'); return;
    }
    renderForceGraph(d, focus);
  } catch(e) {
    document.getElementById('net-loading').style.display = 'none';
    showGraphError(e.toString());
  }
}

function resetNetwork() {
  selectedAuthor = null;
  document.getElementById('net-focus').value = '';
  document.querySelectorAll('.author-row').forEach(r => r.classList.remove('selected'));
  const gc = document.getElementById('graph-container');
  if(gc) gc.innerHTML = `<div class="graph-empty">
    <div class="graph-empty-icon">🕸</div>
    <div class="graph-empty-title">Collaboration Network</div>
    <div class="graph-empty-sub">Click an author from the list or type a name and press Build Graph</div>
  </div>`;
  const ns = document.getElementById('net-stats');
  if(ns) ns.style.display = 'none';
  if(netSimulation) { netSimulation.stop(); netSimulation = null; }
}

// ── D3 Force Graph ───────────────────────────────────────────────────────────
function renderForceGraph(data, focus) {
  const container = document.getElementById('graph-container');
  container.innerHTML = '';   // clear previous

  const W  = container.clientWidth  || 800;
  const H  = container.clientHeight || 520;

  // ── Scales ────────────────────────────────────────────────────────────────
  const maxCount = Math.max(...data.nodes.map(n => n.count), 1);
  const maxWeight= Math.max(...data.edges.map(e => e.weight), 1);

  const rScale = d3.scaleSqrt().domain([1, maxCount]).range([5, 22]);
  const wScale = d3.scaleLinear().domain([1, maxWeight]).range([0.8, 5]);
  const oScale = d3.scaleLinear().domain([1, maxWeight]).range([0.2, 0.75]);

  function nodeColor(n) {
    if(n.is_seed) return '#00e0bc';
    return NODE_COLORS[n.group % NODE_COLORS.length];
  }

  // ── SVG ───────────────────────────────────────────────────────────────────
  const svg = d3.select(container).append('svg')
    .attr('id', 'graph-svg')
    .attr('width', W).attr('height', H);

  // Background
  svg.append('rect').attr('width', W).attr('height', H)
     .attr('fill', 'transparent');

  // Zoom/pan
  const g = svg.append('g');
  svg.call(d3.zoom()
    .scaleExtent([0.15, 8])
    .on('zoom', e => g.attr('transform', e.transform)));

  // ── Simulation ────────────────────────────────────────────────────────────
  const nodes = data.nodes.map(n => ({...n}));
  const edges = data.edges.map(e => ({...e}));

  // Seed node starts in center
  nodes.forEach(n => {
    if(n.is_seed) { n.fx = W/2; n.fy = H/2; }
  });

  if(netSimulation) netSimulation.stop();
  netSimulation = d3.forceSimulation(nodes)
    .force('link',   d3.forceLink(edges).id(d => d.id)
                       .distance(d => Math.max(60, 130 - d.weight * 8))
                       .strength(0.55))
    .force('charge', d3.forceManyBody().strength(-260).distanceMax(380))
    .force('center', d3.forceCenter(W/2, H/2))
    .force('collide', d3.forceCollide().radius(d => rScale(d.count) + 8))
    .alphaDecay(0.025);

  // ── Edges ─────────────────────────────────────────────────────────────────
  const link = g.append('g').selectAll('line')
    .data(edges).join('line')
    .attr('stroke', '#4d9eff')
    .attr('stroke-width', d => wScale(d.weight))
    .attr('stroke-opacity', d => oScale(d.weight));

  // Edge weight labels (only for heavy edges to avoid clutter)
  const edgeLabel = g.append('g').selectAll('text')
    .data(edges.filter(e => e.weight >= 3)).join('text')
    .attr('fill', '#2e4060')
    .attr('font-size', 8)
    .attr('font-family', "'JetBrains Mono', monospace")
    .attr('text-anchor', 'middle')
    .text(d => d.weight);

  // ── Nodes ─────────────────────────────────────────────────────────────────
  const node = g.append('g').selectAll('g')
    .data(nodes).join('g')
    .attr('cursor', 'pointer')
    .call(d3.drag()
      .on('start', (event, d) => {
        if(!event.active) netSimulation.alphaTarget(0.3).restart();
        d.fx = d.x; d.fy = d.y;
      })
      .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
      .on('end', (event, d) => {
        if(!event.active) netSimulation.alphaTarget(0);
        if(!d.is_seed) { d.fx = null; d.fy = null; }
      }));

  // Glow filter for seed node
  const defs = svg.append('defs');
  const glow = defs.append('filter').attr('id','glow');
  glow.append('feGaussianBlur').attr('stdDeviation','3').attr('result','coloredBlur');
  const feMerge = glow.append('feMerge');
  feMerge.append('feMergeNode').attr('in','coloredBlur');
  feMerge.append('feMergeNode').attr('in','SourceGraphic');

  // Outer ring for seed
  node.filter(n => n.is_seed)
    .append('circle')
    .attr('r', d => rScale(d.count) + 6)
    .attr('fill', 'none')
    .attr('stroke', '#00e0bc')
    .attr('stroke-width', 1.5)
    .attr('stroke-opacity', 0.4)
    .attr('stroke-dasharray', '4 3');

  // Main circle
  node.append('circle')
    .attr('r', d => rScale(d.count))
    .attr('fill', d => nodeColor(d))
    .attr('fill-opacity', d => d.is_seed ? 0.9 : 0.65)
    .attr('stroke', d => nodeColor(d))
    .attr('stroke-width', d => d.is_seed ? 2.5 : 1)
    .attr('stroke-opacity', d => d.is_seed ? 1 : 0.4)
    .attr('filter', d => d.is_seed ? 'url(#glow)' : null);

  // Label — show for larger nodes + seed
  node.filter(d => rScale(d.count) >= 9 || d.is_seed)
    .append('text')
    .attr('dy', d => rScale(d.count) + 11)
    .attr('text-anchor', 'middle')
    .attr('fill', '#6a85a8')
    .attr('font-size', d => d.is_seed ? 11 : 9)
    .attr('font-family', "'JetBrains Mono', monospace")
    .text(d => d.label.length > 18 ? d.label.slice(0,16)+'…' : d.label);

  // ── Tooltip ───────────────────────────────────────────────────────────────
  const tooltip = document.getElementById('net-tooltip');

  node.on('mouseenter', (event, d) => {
    // Highlight connected edges/nodes
    const connected = new Set([d.id]);
    edges.forEach(e => {
      if(e.source.id===d.id || e.target.id===d.id) {
        connected.add(e.source.id || e.source);
        connected.add(e.target.id || e.target);
      }
    });
    link.attr('stroke-opacity', e =>
      (e.source.id===d.id||e.target.id===d.id) ? 0.95 : 0.06);
    node.selectAll('circle').attr('fill-opacity',
      n => connected.has(n.id) ? 0.95 : 0.15);

    tooltip.style.opacity = '1';
    tooltip.innerHTML = `
      <div style="font-family:'Syne',sans-serif;font-weight:700;font-size:.85rem;
                  color:${nodeColor(d)};margin-bottom:4px;">${d.label}</div>
      <div style="color:#6a85a8;">${d.count} paper${d.count!==1?'s':''}</div>
      ${d.is_seed?'<div style="color:#00e0bc;font-size:.65rem;margin-top:3px;">● Focus author</div>':''}`;
  })
  .on('mousemove', event => {
    tooltip.style.left = (event.clientX + 14)+'px';
    tooltip.style.top  = (event.clientY - 32)+'px';
  })
  .on('mouseleave', () => {
    link.attr('stroke-opacity', d => oScale(d.weight));
    node.selectAll('circle').attr('fill-opacity', d => d.is_seed ? 0.9 : 0.65);
    tooltip.style.opacity = '0';
  })
  .on('click', (event, d) => {
    event.stopPropagation();
    // Click any node → load that author's network immediately
    const name = d.label || d.id;
    selectedAuthor = name;
    const focusEl = document.getElementById('net-focus');
    if(focusEl) focusEl.value = name;
    // Sync highlight in the author list panel
    document.querySelectorAll('.author-row').forEach(r => {
      const nm = r.querySelector('.ar-name');
      r.classList.toggle('selected', nm && nm.textContent === name);
    });
    // Scroll highlighted row into view
    const selRow = document.querySelector('.author-row.selected');
    if(selRow) selRow.scrollIntoView({block:'nearest',behavior:'smooth'});
    buildNetwork();
  });

  // ── Tick ──────────────────────────────────────────────────────────────────
  netSimulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
    edgeLabel
      .attr('x', d => (d.source.x + d.target.x)/2)
      .attr('y', d => (d.source.y + d.target.y)/2);
    node.attr('transform', d => `translate(${d.x},${d.y})`);
  });

  // ── Stats bar ─────────────────────────────────────────────────────────────
  const ns = document.getElementById('net-stats');
  if(ns) {
    ns.style.display = 'flex';
    ns.innerHTML = `
      <div class="net-stat">Nodes <span>${nodes.length}</span></div>
      <div class="net-stat">Edges <span>${edges.length}</span></div>
      <div class="net-stat">Total authors in corpus <span>${(data.total_authors||0).toLocaleString()}</span></div>
      <div class="net-stat" style="margin-left:auto;color:var(--t3);font-size:.58rem;">
        Drag nodes · scroll to zoom · click node to re-focus
      </div>`;
  }
}

function showGraphEmpty(title, sub) {
  const gc = document.getElementById('graph-container');
  if(gc) gc.innerHTML = `<div class="graph-empty">
    <div class="graph-empty-icon">🔍</div>
    <div class="graph-empty-title">${esc(title)}</div>
    <div class="graph-empty-sub">${esc(sub)}</div>
  </div>`;
}

function showGraphError(msg) {
  const gc = document.getElementById('graph-container');
  if(gc) gc.innerHTML = `<div class="graph-empty">
    <div class="error-box" style="max-width:480px;">${esc(msg)}</div>
  </div>`;
}

function downloadSVG() {
  const svg = document.getElementById('graph-svg');
  if(!svg) return;
  const blob = new Blob([svg.outerHTML], {type:'image/svg+xml'});
  const a    = document.createElement('a');
  a.href     = URL.createObjectURL(blob);
  a.download = 'nexus-network.svg';
  a.click();
}

// ── Boot ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);