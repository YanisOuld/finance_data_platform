// Shapes returned by the FastAPI backend (src/api/schemas.py). Kept in sync by
// hand -- these mirror the Pydantic response models the API serves.

export interface Instrument {
  id: number;
  ticker: string;
  name: string | null;
  exchange: string | null;
  currency: string;
  timezone: string;
  is_active: boolean;
  is_scheduled: boolean;
}

export interface Price {
  symbol: string;
  ts: string; // ISO date, e.g. "2026-01-02"
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
  close_returns: number | null;
}

export interface Fundamental {
  ticker: string;
  concept: string;
  unit: string;
  period_end: string;
  fy: number | null;
  fp: string;
  form: string;
  val: number;
}

export interface MacroPoint {
  series: string;
  ts: string;
  value: number | null;
}

export interface MacroCatalogEntry {
  code: string;
  label: string;
}

// Grouped FRED/FX catalog, e.g. { US: [...], Canada: [...], FX: [...] }.
export type MacroCatalog = Record<string, MacroCatalogEntry[]>;

export interface ApiKeyInfo {
  id: number;
  label: string;
  prefix: string;
  scopes: string; // comma-separated, e.g. "read" or "read,write"
  is_active: boolean;
  created_at: string | null;
  last_used_at: string | null;
}

// The create response additionally carries the plaintext token, shown once.
export interface ApiKeyCreated extends ApiKeyInfo {
  key: string;
}

export interface NavPage {
  key: string;
  label: string;
}
