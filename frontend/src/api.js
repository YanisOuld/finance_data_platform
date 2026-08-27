export async function api(path, apiKey, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (apiKey) headers["X-API-Key"] = apiKey;
  const res = await fetch(path, { ...options, headers });
  let body = null;
  try {
    body = await res.json();
  } catch (_) {
    /* empty body */
  }
  if (!res.ok) {
    const detail = body && body.detail ? JSON.stringify(body.detail) : res.statusText;
    throw new Error(`${res.status}: ${detail}`);
  }
  return body;
}

export function fmtNum(v, digits = 2) {
  return v === null || v === undefined ? "" : Number(v).toFixed(digits);
}

// Known FRED series codes (src/core/constants.py::FRED_COLUMN_SERIES) -- a
// static, rarely-changing list, so it's just mirrored here rather than
// adding an endpoint purely to enumerate it.
export const FRED_SERIES = [
  "cpi", "inflation", "gdp", "unemployment", "yield_curve", "fed_funds",
  "interest_rate_10y", "usd/eur", "usd/cad", "usd/jpy", "usd/gbp",
];
