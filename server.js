import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import fsp from "fs/promises";
import { parse } from "csv-parse/sync";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_DIR = path.join(__dirname, "data");

// 🧠 автоматски избери datasets фајл
let DATASETS_FILE = path.join(DATA_DIR, "datasets.render.json");
if (!fs.existsSync(DATASETS_FILE)) {
  console.warn("⚠️ datasets.render.json not found, falling back to datasets.json");
  DATASETS_FILE = path.join(DATA_DIR, "datasets.json");
}

app.use(express.static(PUBLIC_DIR));
app.use(express.json({ limit: "2mb" }));

const UA = "RenderDirectReader/1.0";

// ✅ Читање datasets
app.get("/api/datasets", async (req, res) => {
  try {
    const raw = await fsp.readFile(DATASETS_FILE, "utf8");
    const data = JSON.parse(raw);
    res.json(data);
  } catch (err) {
    console.error("❌ Error loading datasets:", err.message);
    res.json([]);
  }
});

// ✅ Download-selected dummy (не се користи, но спречува 404)
app.get("/api/download-selected", (req, res) => {
  res.json({
    message:
      "This endpoint is not used in Render version. Data is fetched live from CSV URLs.",
  });
});

// ✅ Главна рута: чита CSV податоци директно од интернет
app.get("/api/data", async (req, res) => {
  const file = req.query.file;
  if (!file) return res.status(400).json({ error: "Missing file query param" });

  try {
    const cfg = JSON.parse(await fsp.readFile(DATASETS_FILE, "utf8"));
    const datasetKey = file.replace(/\.csv$/, "");
    const ds = cfg.find((x) => x.key === datasetKey);
    if (!ds) return res.status(404).json({ error: "Dataset not found." });

    const src = ds.sources.find((s) => s.type === "csv" || s.type === "json");
    if (!src) return res.status(404).json({ error: "No valid data source." });

    const fetch = (await import("node-fetch")).default;
    const resp = await fetch(src.url, {
      headers: { "User-Agent": UA, Accept: "*/*" },
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status} ${resp.statusText}`);

    let text = await resp.text();
    if (src.type === "json" || src.url.endsWith(".json")) {
      const json = JSON.parse(text);
      const rows = Array.isArray(json)
        ? json
        : json.content || json.data || json.results || [json];
      // convert JSON to CSV in memory
      const headers = [...new Set(rows.flatMap((r) => Object.keys(r)))];
      const csv = [headers.join(",")].concat(
        rows.map((r) => headers.map((h) => JSON.stringify(r[h] ?? "")).join(","))
      );
      text = csv.join("\n");
    }

    // parse CSV
    const records = parse(text, {
      columns: true,
      skip_empty_lines: true,
    });
    const headers = Object.keys(records[0] || {});
    const q = (req.query.q || "").toLowerCase();

    const filtered = q
      ? records.filter((r) =>
          Object.values(r).some((v) =>
            String(v).toLowerCase().includes(q)
          )
        )
      : records;

    res.json({
      headers,
      rows: filtered.slice(0, 200), // лимит 200 редови за побрзо вчитување
      totalMatched: filtered.length,
    });
  } catch (err) {
    console.error("❌ Error reading data:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ✅ тест API за да се види дека серверот е активен
app.get("/api/status", (req, res) => {
  res.json({
    ok: true,
    message: "Render live data server running.",
    datasetsFile: DATASETS_FILE,
  });
});

app.listen(PORT, () => {
  console.log(`✅ Server running at http://localhost:${PORT}`);
  console.log(`📦 Using datasets file: ${DATASETS_FILE}`);
  console.log("⚡ Live CSV reading mode enabled (no file storage).");
});
