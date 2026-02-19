# SDOH Risk Dashboard

Interactive dashboard to analyze Social Determinants of Health (SDOH)
impact on member-level and ZIP-level risk.

## Features
- Member & ZIP views
- SDOH lift analysis
- Explainable model drivers
- Interactive filtering, including contract-level cohort refinement

## Tech
- HTML / CSS / Vanilla JS
- CSV-based data input
- Designed for Databricks & local development

## Databricks SQL API
This repo includes a small Node API that queries a Databricks SQL warehouse
and serves JSON for the dashboard.

1. Configure environment variables (see `server/.env.example`).
2. Install dependencies: `cd server && npm install`
3. Run the API: `npm start`
4. Open `http://localhost:8787/index.html`

The dashboard will use `/api/members` automatically when served by the API.
