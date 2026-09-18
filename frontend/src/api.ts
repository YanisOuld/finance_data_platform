// Thin fetch wrappers around the platform API.

interface JsonError {
  detail?: unknown;
}

const API_V1_PREFIX = "/v1";

function withBase(path: string): string {
  if (path.startsWith("/admin") || path.startsWith("/health") || path.startsWith(API_V1_PREFIX)) {
    return path;
  }
  return `${API_V1_PREFIX}${path}`;
}

function buildHeaders(apiKey: string | undefined, extra?: HeadersInit): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...((extra as Record<string, string>) || {}),
  };
  if (apiKey) headers["X-API-Key"] = apiKey;
  return headers;
}

async function parseJson(res: Response): Promise<unknown> {
  try {
    return await res.json();
  } catch {
    return null; // empty body
  }
}

function raiseForStatus(res: Response, body: unknown): void {
  if (res.ok) return;
  const err = body as JsonError | null;
  const detail = err && err.detail ? JSON.stringify(err.detail) : res.statusText;
  throw new Error(`${res.status}: ${detail}`);
}

export async function api<T = unknown>(
  path: string,
  apiKey?: string,
  options: RequestInit = {},
): Promise<T> {
  const res = await fetch(withBase(path), { ...options, headers: buildHeaders(apiKey, options.headers) });
  const body = await parseJson(res);
  raiseForStatus(res, body);
  return body as T;
}

export async function apiWithTotal<T = unknown>(
  path: string,
  apiKey?: string,
  options: RequestInit = {},
): Promise<{ data: T; total: number }> {
  const res = await fetch(withBase(path), { ...options, headers: buildHeaders(apiKey, options.headers) });
  const body = await parseJson(res);
  raiseForStatus(res, body);
  const total = Number(res.headers.get("X-Total-Count"));
  const data = body as T;
  const fallback = Array.isArray(data) ? data.length : 0;
  return { data, total: Number.isFinite(total) ? total : fallback };
}

export function fmtNum(v: number | string | null | undefined, digits = 2): string {
  return v === null || v === undefined ? "" : Number(v).toFixed(digits);
}

export function errMsg(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export const FRED_SERIES: string[] = [
  "cpi", "inflation", "gdp", "unemployment", "yield_curve", "fed_funds",
  "interest_rate_10y", "usd/eur", "usd/cad", "usd/jpy", "usd/gbp",
];
