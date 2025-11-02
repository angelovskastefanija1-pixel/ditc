
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

const PUBLIC_DIR = path.join(__dirname, 'public');
const OUT_DIR = path.join(__dirname, 'out');
const STORAGE_DIR = path.join(__dirname, 'storage');
const DATASETS_FILE = path.join(__dirname, 'data', 'datasets.json');
const MANIFEST = path.join(STORAGE_DIR, 'manifest.json');

app.use(express.static(PUBLIC_DIR));
app.use(express.json({ limit: '2mb' }));

function okDir(p){ if(!fs.existsSync(p)) fs.mkdirSync(p,{recursive:true}); }
okDir(OUT_DIR); okDir(STORAGE_DIR);

const UA='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36';

async function loadManifest(){ try{ return JSON.parse(await fsp.readFile(MANIFEST,'utf8')); } catch { return {}; } }
async function saveManifest(m){ await fsp.writeFile(MANIFEST, JSON.stringify(m,null,2),'utf-8'); }

async function head(url){
  const fetch=(await import('node-fetch')).default;
  try{
    const ctl = new AbortController();
    const to = setTimeout(()=>ctl.abort(), 20000);
    const res = await fetch(url,{method:'HEAD',redirect:'follow',headers:{'User-Agent':UA}, signal: ctl.signal});
    clearTimeout(to);
    return res;
  }catch{
    return null;
  }
}

async function fetchBuffer(url){
  const fetch=(await import('node-fetch')).default;
  const ctl = new AbortController();
  const to = setTimeout(()=>ctl.abort(), 60000);
  const res = await fetch(url,{headers:{'User-Agent':UA,'Accept':'*/*'},redirect:'follow', signal: ctl.signal});
  if(!res.ok) { clearTimeout(to); throw new Error(`HTTP ${res.status} ${res.statusText}`); }
  const ab = await res.arrayBuffer();
  clearTimeout(to);
  return Buffer.from(ab);
}

async function fetchToFile(url,dest){ const buf=await fetchBuffer(url); await fsp.writeFile(dest,buf); return dest; }
async function extractZip(zipPath,destDir){ await fs.createReadStream(zipPath).pipe(unzipper.Extract({path:destDir})).promise(); }

function pickLargestCsv(files,baseDir){
  const csvs=files.filter(f=>f.toLowerCase().endsWith('.csv'));
  if(!csvs.length) return null;
  let best=csvs[0], size=0;
  for(const c of csvs){
    const s=fs.statSync(path.join(baseDir,c));
    if(s.size>size){ size=s.size; best=c; }
  }
  return best;
}

function toCSV(rows){
  if(!Array.isArray(rows)||!rows.length) return '';
  const headers=[...new Set(rows.flatMap(o=>Object.keys(o||{})))];
  const esc=v=>{const s=(v==null?'':String(v)); return '"' + s.replace(/"/g,'""') + '"';};
  const lines=[headers.map(esc).join(',')];
  for(const r of rows) lines.push(headers.map(h=>esc(r[h])).join(','));
  return lines.join('\n');
}

async function tryDownload(ds, manifest){
  for(const src of (ds.sources||[])){
    try{
      let h = null;
      if (src.type !== 'json') {
        h = await head(src.url);
      }
      const etag = h && h.headers.get('etag');
      const lm   = h && h.headers.get('last-modified');
      const clen = h && h.headers.get('content-length');
      const last = manifest[src.url] || {};
      const changed = ((!last.etag && !last['last-modified'] && !last['content-length']) ||
                      (etag && etag !== last.etag) ||
                      (lm && lm !== last['last-modified']) ||
                      (clen && clen !== last['content-length']));

      if ((src.type === 'json') || !h || h.status >= 400 || changed) {
        if(src.type==='zip'){
          const stamp=Date.now();
          const zipDest=path.join(STORAGE_DIR,`${ds.key}-${stamp}.zip`);
          await fetchToFile(src.url,zipDest);
          const extractDir=path.join(STORAGE_DIR,`${ds.key}-${stamp}`);
          await fsp.mkdir(extractDir,{recursive:true});
          await extractZip(zipDest,extractDir);
          const files=await fsp.readdir(extractDir);
          const pick=pickLargestCsv(files,extractDir);
          if(!pick) throw new Error('No CSV inside ZIP');
          await fsp.copyFile(path.join(extractDir,pick), path.join(OUT_DIR,`${ds.key}.csv`));
        }else if(src.type==='csv'){
          await fetchToFile(src.url, path.join(OUT_DIR,`${ds.key}.csv`));
        }else if (src.type === 'json') {
          const buf = await fetchBuffer(src.url);
          let data = [];
          try {
            const parsed = JSON.parse(buf.toString('utf-8'));
            if (Array.isArray(parsed)) {
              data = parsed;
            } else if (parsed && parsed.content && Array.isArray(parsed.content)) {
              data = parsed.content;
            } else if (parsed && parsed.data && Array.isArray(parsed.data)) {
              data = parsed.data;
            } else if (parsed && parsed.results && Array.isArray(parsed.results)) {
              data = parsed.results;
            } else {
              data = [parsed];
            }
          } catch (err) {
            console.error('JSON parse error:', err.message);
            throw err;
          }
          const csv = toCSV(data);
          await fsp.writeFile(path.join(OUT_DIR, `${ds.key}.csv`), csv, 'utf-8');
        }

        manifest[src.url]={ etag, 'last-modified': lm, 'content-length': clen, savedAs:`${ds.key}.csv`, ts:Date.now() };
        await saveManifest(manifest);
        return { ok:true, note:(src.type==='zip'?'ZIP extracted':'Downloaded'), source: src.url };
      }else{
        const out=path.join(OUT_DIR,`${ds.key}.csv`);
        if(fs.existsSync(out)) return { ok:true, note:'Up-to-date', source: src.url };
        if(src.type==='csv'){ await fetchToFile(src.url,out); return { ok:true, note:'Downloaded (recovered)', source: src.url }; }
      }
    }catch(e){
      console.warn('Source failed:', src.url, e.message);
    }
  }
  return { ok:false, note:'All sources failed' };
}

async function updateSelected(keys){
  const cfg=JSON.parse(await fsp.readFile(DATASETS_FILE,'utf8'));
  const manifest=await loadManifest();
  const results=[];
  for(const ds of cfg.filter(x=>x.enabled && keys.includes(x.key))){
    const r=await tryDownload(ds,manifest);
    results.push({ key: ds.key, ...r });
  }
  return results;
}

app.get('/api/datasets', async (req,res)=>{
  const raw=await fsp.readFile(DATASETS_FILE,'utf8');
  res.json(JSON.parse(raw));
});

app.post('/api/download-selected', async (req,res)=>{
  try{
    const body=req.body||{};
    const keys=body.keys||[];
    if(!keys.length) return res.status(400).json({error:'No dataset keys provided.'});
    const results=await updateSelected(keys);
    res.json({results});
  }catch(e){
    res.status(500).json({error:e.message});
  }
});

app.get('/api/files', async (req,res)=>{
  const files=(await fsp.readdir(OUT_DIR)).filter(f=>f.toLowerCase().endsWith('.csv'));
  res.json(files);
});

app.get('/api/data', async (req,res)=>{
  const file=req.query.file;
  const limit=Math.min(parseInt(req.query.limit||'10000',10),20000);
  const offset=Math.max(parseInt(req.query.offset||'0',10),0);
  const q=(req.query.q||'').toLowerCase();
  const filePath=path.join(OUT_DIR,file||'');
  if(!file||!fs.existsSync(filePath)) return res.status(404).json({error:'CSV not found. Run Update Selected first.'});
  const stream=fs.createReadStream(filePath).pipe(parse({relax_quotes:true}));
  let headers=null, rows=[], total=0;
  stream.on('data', row=>{
    if(!headers){ headers=row; return; }
    const hay=row.join(' ').toLowerCase();
    if(q && !hay.includes(q)) return;
    total+=1;
    if(rows.length<limit && total>offset){
      const obj={}; headers.forEach((h,i)=>obj[h]=row[i]); rows.push(obj);
    }
  });
  stream.on('end', ()=>res.json({headers,rows,totalMatched:total}));
  stream.on('error', e=>res.status(500).json({error:e.message}));
});

app.listen(PORT, async ()=>{
  console.log(`✅ Server running at http://localhost:${PORT}`);
  console.log('ℹ️  Waiting for manual update ("Update Selected")...');
});
