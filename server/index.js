const path = require("path");
require("dotenv").config();
const express = require("express");
const fetch = (...args) => import("node-fetch").then(({ default: fetchFn }) => fetchFn(...args));

const app = express();
const PORT = process.env.PORT || 8787;

const DATABRICKS_HOST = process.env.DATABRICKS_HOST;
const DATABRICKS_TOKEN = process.env.DATABRICKS_TOKEN;
const DATABRICKS_WAREHOUSE_ID = process.env.DATABRICKS_WAREHOUSE_ID;
const DATABRICKS_TABLE =
  process.env.DATABRICKS_TABLE ||
  "medicare.default.member_sdoh_clinical_enriched_v1";

const DEFAULT_LIMIT = Number(process.env.DATABRICKS_LIMIT || 0);
const SQL_TIMEOUT_MS = Number(process.env.DATABRICKS_TIMEOUT_MS || 30000);

app.use(express.json());
app.use(express.static(path.join(__dirname, "..")));

function requireEnv(name, value) {
  if (!value) {
    const err = new Error(`${name} is required`);
    err.status = 500;
    throw err;
  }
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchStatementResult(statementId) {
  const resp = await fetch(`${DATABRICKS_HOST}/api/2.0/sql/statements/${statementId}`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${DATABRICKS_TOKEN}`
    }
  });
  const body = await resp.json();
  if (!resp.ok) {
    const err = new Error(body.message || "Failed to fetch statement result");
    err.status = resp.status;
    throw err;
  }
  return body;
}

function rowsFromStatementResult(body) {
  const result = body && body.result;
  const schema = result && result.schema;
  const columns = schema && schema.columns ? schema.columns : [];
  const dataArray = result && result.data_array ? result.data_array : null;
  if (!dataArray || !columns.length) return [];
  return dataArray.map((row) => {
    const obj = {};
    columns.forEach((col, idx) => {
      obj[col.name] = row[idx];
    });
    return obj;
  });
}

async function runDatabricksQuery(sql) {
  requireEnv("DATABRICKS_HOST", DATABRICKS_HOST);
  requireEnv("DATABRICKS_TOKEN", DATABRICKS_TOKEN);
  requireEnv("DATABRICKS_WAREHOUSE_ID", DATABRICKS_WAREHOUSE_ID);

  const resp = await fetch(`${DATABRICKS_HOST}/api/2.0/sql/statements`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${DATABRICKS_TOKEN}`
    },
    body: JSON.stringify({
      warehouse_id: DATABRICKS_WAREHOUSE_ID,
      statement: sql,
      disposition: "INLINE",
      wait_timeout: `${Math.ceil(SQL_TIMEOUT_MS / 1000)}s`
    })
  });

  const body = await resp.json();
  if (!resp.ok) {
    const err = new Error(body.message || "Failed to execute statement");
    err.status = resp.status;
    throw err;
  }

  if (body.status && body.status.state && body.status.state !== "SUCCEEDED") {
    const statementId = body.statement_id;
    const deadline = Date.now() + SQL_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await sleep(1000);
      const latest = await fetchStatementResult(statementId);
      if (latest.status && latest.status.state === "SUCCEEDED") {
        return rowsFromStatementResult(latest);
      }
      if (latest.status && ["FAILED", "CANCELED"].includes(latest.status.state)) {
        const err = new Error("Statement failed");
        err.status = 500;
        throw err;
      }
    }
    const err = new Error("Statement timed out");
    err.status = 504;
    throw err;
  }

  return rowsFromStatementResult(body);
}

app.get("/api/members", async (req, res) => {
  try {
    const limit = Number(req.query.limit || DEFAULT_LIMIT || 0);
    const sql = `SELECT * FROM ${DATABRICKS_TABLE}` + (limit > 0 ? ` LIMIT ${limit}` : "");
    const rows = await runDatabricksQuery(sql);
    res.json({ rows });
  } catch (err) {
    res.status(err.status || 500).json({
      error: err.message || "Server error"
    });
  }
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log(`API server running on http://localhost:${PORT}`);
});
