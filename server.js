import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import fsp from 'fs/promises';
import { parse } from 'csv-parse';
import unzipper from 'unzipper';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// 🧩 Public и runtime директориуми
const PUBLIC_DIR = path.join(__dirname, 'public');
const STORAGE_DIR = process.env.RENDER ? '/tmp/storage' : path.join(__dirname, 'storage');
const OUT_DIR = process.env.RENDER ? '/tmp/out' : path.join(__dirname, 'out');
const DATA_DIR = path.join(__dirname, 'data');

// 🧠 datasets.json fallback логика
let DATASETS_FILE = path.join(DATA_DIR, 'datasets.render.json');
if (!fs.existsSync(DATASETS_FILE)) {
  console.warn('⚠️ datasets.render.json not found, falling back to datasets.json');
  DATASETS_FILE = path.join(DATA_DIR, 'datasets.json');
}

const MANIFEST = path.join(STORAGE_DIR, 'manifest.json');

app.use(express.static(PUBLIC_DIR));
app.use(express.json({ limit: '2mb' }));

function okDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}
okDir(STORAGE_DIR);
okDir(OUT_DIR);

const UA = 'RenderFetcher/1.0 (Node.js)';

// --- Utility functions ---
async function loadManifest() {
  try { return JSON.parse(await fsp.readFile(MANIFEST, 'utf8')); }
  catch { return {}; }
}

async function saveManifest(m) {
  await fsp.writeFile(MANIFEST, JSON.stringify(m, null, 2), 'utf-8');
}

async function fetchBuffer(url) {
  const fetch = (await import('node-fetch')).default;
  const ctl = new AbortController();
  const to = setTimeout(() => ctl.abort(), 30000);
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': '*/*' },
      redirect: 'follow',
      signal: ctl.signal
    });
    clearTimeout(to);
    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
    const ab = await res.arrayBuffer();
    return Buffer.from(ab);
  } catch (err) {
    console.error('❌ Fetch error:', url, err.message);
    throw err;
  }
}

async function fetchToFile(url, dest) {
  const buf = await fetchBuffer(url);
  await fsp.writeFile(dest, buf);
  return dest;
}

async function extractZip(zipPath, destDir) {
  await fs.createReadStream(zipPath).pipe(unzipper.Extract({ path: destDir })).promise();
}

function pickLargestCsv(files, baseDir) {
  const csvs = files.filter(f => f.toLowerCase().endsWith('.csv'));
  if (!csvs.length) return null;
  let best = csvs[0], size = 0;
  for (const c of csvs) {
    const s = fs.statSync(path.join(baseDir, c));
    if (s.size > size) { size = s.size; best = c; }
  }
  return best;
}

function toCSV(rows) {
  if (!Array.isArray(rows) || !rows.length) return '';
  const headers = [...new Set(rows.flatMap(o => Object.keys(o || {})))];
  const esc = v => `"${(v == null ? '' : String(v)).replace(/"/g, '""')}"`;
  const lines = [headers.map(esc).join(',')];
  for (const r of rows) lines.push(headers.map(h => esc(r[h])).join(','));
  return lines.join('\n');
}

// --- Core logic for downloading datasets ---
async function tryDownload(ds, manifest) {
  for (const src of (ds.sources || [])) {
    try {
      if (src.type === 'zip') {
        const stamp = Date.now();
        const zipDest = path.join(STORAGE_DIR, `${ds.key}-${stamp}.zip`);
        await fetchToFile(src.url, zipDest);
        const extractDir = path.join(STORAGE_DIR, `${ds.key}-${stamp}`);
        await fsp.mkdir(extractDir, { recursive: true });
        await extractZip(zipDest, extractDir);
        const files = await fsp.readdir(extractDir);
        const pick = pickLargestCsv(files, extractDir);
        if (!pick) throw new Error('No CSV inside ZIP');
        await fsp.copyFile(path.join(extractDir, pick), path.join(OUT_DIR, `${ds.key}.csv`));
        return { ok: true, note: 'ZIP extracted', source: src.url };
      } else if (src.type === 'csv') {
        await fetchToFile(src.url, path.join(OUT_DIR, `${ds.key}.csv`));
        return { ok: true, note: 'Downloaded CSV', source: src.url };
      } else if (src.type === 'json') {
        const buf = await fetchBuffer(src.url);
        const parsed = JSON.parse(buf.toString('utf-8'));
        const data = Array.isArray(parsed)
          ? parsed
          : parsed?.content || parsed?.data || parsed?.results || [parsed];
        const csv = toCSV(data);
        await fsp.writeFile(path.join(OUT_DIR, `${ds.key}.csv`), csv, 'utf-8');
        return { ok: true, note: 'JSON converted', source: src.url };
      }
    } catch (e) {
      console.warn('⚠️ Source failed:', src.url, e.message);
    }
  }
  return { ok: false, note: 'All sources failed' };
}

async function updateSelected(keys) {
  const cfg = JSON.parse(await fsp.readFile(DATASETS_FILE, 'utf8'));
  const manifest = await loadManifest();
  const results = [];
  for (const ds of cfg.filter(x => x.enabled && keys.includes(x.key))) {
    const r = await tryDownload(ds, manifest);
    results.push({ key: ds.key, ...r });
  }
  return results;
}

// --- API routes ---
app.get('/api/datasets', async (req, res) => {
  try {
    const raw = await fsp.readFile(DATASETS_FILE, 'utf8');
    const data = JSON.parse(raw);
    res.json(data);
  } catch (err) {
    console.error('❌ Error loading datasets:', err.message);
    res.json([]);
  }
});

app.post('/api/download-selected', async (req, res) => {
  try {
    const body = req.body || {};
    const keys = body.keys || [];
    if (!keys.length) return res.status(400).json({ error: 'No dataset keys provided.' });
    const results = await updateSelected(keys);
    res.json({ results });
  } catch (e) {
    console.error('❌ Update failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/files', async (req, res) => {
  const files = (await fsp.readdir(OUT_DIR)).filter(f => f.toLowerCase().endsWith('.csv'));
  res.json(files);
});

app.get('/api/data', async (req, res) => {
  const file = req.query.file;
  const limit = Math.min(parseInt(req.query.limit || '10000', 10), 20000);
  const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);
  const q = (req.query.q || '').toLowerCase();
  const filePath = path.join(OUT_DIR, file || '');
  if (!file || !fs.existsSync(filePath)) return res.status(404).json({ error: 'CSV not found. Run Update Selected first.' });

  const stream = fs.createReadStream(filePath).pipe(parse({ relax_quotes: true }));
  let headers = null, rows = [], total = 0;
  stream.on('data', row => {
    if (!headers) { headers = row; return; }
    const hay = row.join(' ').toLowerCase();
    if (q && !hay.includes(q)) return;
    total += 1;
    if (rows.length < limit && total > offset) {
      const obj = {}; headers.forEach((h, i) => obj[h] = row[i]); rows.push(obj);
    }
  });
  stream.on('end', () => res.json({ headers, rows, totalMatched: total }));
  stream.on('error', e => res.status(500).json({ error: e.message }));
});

app.listen(PORT, async () => {
  console.log(`✅ Server running at http://localhost:${PORT}`);
  console.log(`📦 Using datasets file: ${DATASETS_FILE}`);
  console.log(`📂 Output dir: ${OUT_DIR}`);
  console.log('ℹ️  Waiting for manual update ("Update Selected")...');
});
