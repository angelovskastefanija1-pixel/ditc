# FMCSA & DOT Public Data — v4.8 Dark (On‑Demand)

**Run**
```
npm install
npm start
```
Open http://localhost:3000

- Click **Update Selected** to fetch the chosen dataset (on-demand).
- Then click **Refresh Data** or **Search** to load the CSV into the table.

**Sources**
- Socrata FMCSA carrier census JSON (no `$select`): limited but works without token.
- FMCSA SMS ZIP snapshots via Wayback mirrors for historic data.
- Notes: FMCSA does not publish personal driver emails/phones in open data.
