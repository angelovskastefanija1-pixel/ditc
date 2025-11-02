// ===============================
// FMCSA & DOT Public Data Viewer
// v5.0-Pro-UI+Columns
// ===============================

async function fetchJSON(url, opts) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

const dsSel = document.getElementById('dataset');
const statusEl = document.getElementById('status');
const qEl = document.getElementById('q');
const thead = document.getElementById('thead');
const tbody = document.getElementById('tbody');
const overlay = document.getElementById('overlay');
const pager = document.getElementById('pager');
const columnBox = document.getElementById('columnFilters');

function showOverlay(v) { overlay.classList.toggle('hidden', !v); }

let allRows = [];
let allHeaders = [];
let currentPage = 1;
const pageSize = 20;

// ===============================
// Load datasets list
// ===============================
async function loadDatasets() {
  const cfg = await fetchJSON('/api/datasets');
  dsSel.innerHTML = '';
  for (const ds of cfg) {
    const opt = document.createElement('option');
    opt.value = ds.key;
    opt.textContent = ds.label || ds.key;
    dsSel.appendChild(opt);
  }
}

// ===============================
// Render Column Filters (Checkboxes)
// ===============================
function renderColumnFilters(headers) {
  columnBox.innerHTML = '';
  if (!headers || !headers.length) return;

  headers.forEach(h => {
    const label = document.createElement('label');
    label.style.marginRight = '10px';
    label.style.display = 'flex';
    label.style.alignItems = 'center';
    label.style.gap = '6px';

    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    cb.value = h;
    cb.onchange = () => renderPage();

    label.append(cb, document.createTextNode(h));
    columnBox.append(label);
  });
}

// ===============================
// Pagination + Table Rendering
// ===============================
function renderPage() {
  const selectedCols = [...columnBox.querySelectorAll('input:checked')].map(cb => cb.value);
  const totalPages = Math.ceil(allRows.length / pageSize);
  const start = (currentPage - 1) * pageSize;
  const pageRows = allRows.slice(start, start + pageSize);

  thead.innerHTML = '';
  tbody.innerHTML = '';

  // Header
  const trh = document.createElement('tr');
  selectedCols.forEach(h => {
    const th = document.createElement('th');
    th.textContent = h;
    trh.appendChild(th);
  });
  thead.appendChild(trh);

  // Body
  for (const r of pageRows) {
    const trb = document.createElement('tr');
    selectedCols.forEach(h => {
      const td = document.createElement('td');
      td.textContent = r[h] ?? '';
      trb.appendChild(td);
    });
    tbody.appendChild(trb);
  }

  // Pager
  pager.innerHTML = '';
  const info = document.createElement('span');
  info.textContent = `Page ${currentPage} of ${totalPages}`;
  pager.appendChild(info);

  const prev = document.createElement('button');
  prev.textContent = '⬅ Prev';
  prev.disabled = currentPage === 1;
  prev.onclick = () => { currentPage--; renderPage(); };
  pager.appendChild(prev);

  const next = document.createElement('button');
  next.textContent = 'Next ➡';
  next.disabled = currentPage >= totalPages;
  next.onclick = () => { currentPage++; renderPage(); };
  pager.appendChild(next);
}

// ===============================
// Load CSV Data
// ===============================
async function loadData(file) {
  showOverlay(true);
  const q = qEl.value.trim();
  try {
    const r = await fetchJSON(`/api/data?file=${encodeURIComponent(file)}&limit=20000&offset=0&q=${encodeURIComponent(q)}`);
    allHeaders = r.headers || [];
    allRows = r.rows || [];
    currentPage = 1;

    renderColumnFilters(allHeaders);
    renderPage();

    statusEl.textContent = `Loaded ${r.rows.length} rows (matched ${r.totalMatched}) from ${file}`;
  } catch (e) {
    statusEl.textContent = 'Load failed: ' + e.message;
  } finally {
    showOverlay(false);
  }
}

// ===============================
// Refresh downloaded files
// ===============================
async function refreshFiles() {
  const files = await fetchJSON('/api/files');
  if (files.length) {
    const want = dsSel.value + '.csv';
    const pick = files.includes(want) ? want : files[0];
    if (pick) await loadData(pick);
  }
}

// ===============================
// Update Selected Dataset
// ===============================
async function updateSelected() {
  const key = dsSel.value;
  showOverlay(true);
  statusEl.textContent = 'Updating ' + key + ' ...';
  try {
    const r = await fetchJSON('/api/download-selected', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keys: [key] })
    });
    statusEl.textContent = (r.results || [])
      .map(x => `${x.key}: ${x.ok ? '✅ OK ' + (x.note || '') : '❌ FAIL ' + (x.note || '')}${x.source ? `\nsrc: ${x.source}` : ''}`)
      .join('\n');
    await refreshFiles();
  } catch (e) {
    statusEl.textContent = 'Update failed: ' + e.message;
  } finally {
    showOverlay(false);
  }
}

// ===============================
// Exporters
// ===============================
document.getElementById('csv').addEventListener('click', () => {
  const selectedCols = [...columnBox.querySelectorAll('input:checked')].map(cb => cb.value);
  if (!selectedCols.length) return;
  const esc = s => '"' + String(s).replace(/"/g, '""') + '"';
  const csv = [selectedCols.map(esc).join(',')]
    .concat(allRows.map(r => selectedCols.map(h => esc(r[h] ?? '')).join(',')))
    .join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'export.csv';
  a.click();
});

document.getElementById('xlsx').addEventListener('click', () => {
  const selectedCols = [...columnBox.querySelectorAll('input:checked')].map(cb => cb.value);
  if (!selectedCols.length) return;
  const ws = XLSX.utils.aoa_to_sheet([
    selectedCols,
    ...allRows.map(r => selectedCols.map(h => r[h] ?? ''))
  ]);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'Data');
  XLSX.writeFile(wb, 'export.xlsx');
});

document.getElementById('pdf').addEventListener('click', () => {
  const selectedCols = [...columnBox.querySelectorAll('input:checked')].map(cb => cb.value);
  if (!selectedCols.length) return;
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF({ orientation: 'l', unit: 'pt', format: 'a4' });
  doc.text('FMCSA & DOT Public Data — Export', 40, 40);
  const rows = allRows.map(r => selectedCols.map(h => r[h] ?? ''));
  doc.autoTable({ head: [selectedCols], body: rows, startY: 60, styles: { fontSize: 7 } });
  doc.save('export.pdf');
});

// ===============================
// Buttons + Events
// ===============================
document.getElementById('update').addEventListener('click', updateSelected);
document.getElementById('refresh').addEventListener('click', refreshFiles);
document.getElementById('load').addEventListener('click', refreshFiles);
qEl.addEventListener('input', () => { currentPage = 1; renderPage(); });

// ===============================
// Initialize App
// ===============================
loadDatasets().then(refreshFiles);
