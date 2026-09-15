import { useEffect, useState } from "react";
import { apiWithTotal, fmtNum } from "../api.js";

// Columns the server is allowed to sort by (must match the Literal in
// src/api/routes/prices.py). `key` is what we send as sort_by; `num` right-
// aligns numeric cells; `sortable=false` marks derived cells with no server
// column (Return is computed from close_returns, which IS sortable, so it maps
// to that key rather than being disabled).
const COLUMNS = [
  { key: "ts", label: "Date" },
  { key: "open", label: "Open", num: true },
  { key: "high", label: "High", num: true },
  { key: "low", label: "Low", num: true },
  { key: "close", label: "Close", num: true },
  { key: "volume", label: "Volume", num: true },
  { key: "close_returns", label: "Return", num: true },
];

const PAGE_SIZES = [25, 50, 100, 250];

export default function PricesTab({ apiKey, ticker }) {
  const [rows, setRows] = useState(null);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState(null);

  // Filters / sort / pagination state.
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [sortBy, setSortBy] = useState("ts");
  const [order, setOrder] = useState("desc");
  const [pageSize, setPageSize] = useState(50);
  const [page, setPage] = useState(0); // zero-based

  const offset = page * pageSize;

  // Re-fetch whenever any query input changes. Every filter/sort/page lives in
  // the URL query, so the server does the work -- sorting and paging stay
  // consistent across the whole result set, not just the rows on screen.
  useEffect(() => {
    if (!ticker) {
      setRows([]);
      setTotal(0);
      return;
    }
    setRows(null);
    setError(null);

    const params = new URLSearchParams({
      limit: String(pageSize),
      offset: String(offset),
      sort_by: sortBy,
      order,
    });
    if (start) params.set("start", start);
    if (end) params.set("end", end);

    apiWithTotal(`/prices/${ticker}?${params.toString()}`, apiKey)
      .then(({ data, total }) => {
        setRows(data);
        setTotal(total);
      })
      .catch((e) => setError(e.message));
  }, [ticker, apiKey, start, end, sortBy, order, pageSize, offset]);

  // Any filter/sort change should send us back to page 1 -- otherwise you can
  // land on an out-of-range offset (e.g. page 5 of a now 2-page result).
  function changeSort(key) {
    if (key === sortBy) {
      setOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(key);
      setOrder("desc");
    }
    setPage(0);
  }

  function onFilter(setter) {
    return (e) => {
      setter(e.target.value);
      setPage(0);
    };
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <>
      <div className="row">
        <div className="field">
          <label>From</label>
          <input type="date" value={start} onChange={onFilter(setStart)} />
        </div>
        <div className="field">
          <label>To</label>
          <input type="date" value={end} onChange={onFilter(setEnd)} />
        </div>
        {(start || end) && (
          <button
            onClick={() => {
              setStart("");
              setEnd("");
              setPage(0);
            }}
          >
            Clear
          </button>
        )}
        <div className="field" style={{ marginLeft: "auto" }}>
          <label>Rows</label>
          <select
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setPage(0);
            }}
          >
            {PAGE_SIZES.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </div>
      </div>

      {error && <div className="status err">Error: {error}</div>}
      {rows === null && !error && <div className="empty">Loading...</div>}

      {rows !== null && !error && (
        <>
          <table>
            <thead>
              <tr>
                {COLUMNS.map((c) => {
                  const active = c.key === sortBy;
                  return (
                    <th
                      key={c.key}
                      onClick={() => changeSort(c.key)}
                      style={{ cursor: "pointer", userSelect: "none", textAlign: c.num ? "right" : "left" }}
                      title="Click to sort"
                    >
                      {c.label}
                      {active ? (order === "asc" ? " ▲" : " ▼") : ""}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.ts}>
                  <td>{row.ts}</td>
                  <td style={{ textAlign: "right" }}>{fmtNum(row.open)}</td>
                  <td style={{ textAlign: "right" }}>{fmtNum(row.high)}</td>
                  <td style={{ textAlign: "right" }}>{fmtNum(row.low)}</td>
                  <td style={{ textAlign: "right" }}>{fmtNum(row.close)}</td>
                  <td style={{ textAlign: "right" }}>{row.volume ?? ""}</td>
                  <td
                    style={{
                      textAlign: "right",
                      color:
                        row.close_returns > 0
                          ? "var(--ok)"
                          : row.close_returns < 0
                          ? "var(--bad)"
                          : undefined,
                    }}
                  >
                    {row.close_returns !== null && row.close_returns !== undefined
                      ? `${fmtNum(row.close_returns * 100, 2)}%`
                      : ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {rows.length === 0 ? (
            <div className="empty">
              No price data for this filter -- widen the date range, or the backfill may still be running.
            </div>
          ) : (
            <div className="row" style={{ marginTop: 12, marginBottom: 0 }}>
              <span className="muted">
                {offset + 1}-{offset + rows.length} of {total}
              </span>
              <div className="field" style={{ marginLeft: "auto", gap: 8 }}>
                <button disabled={page === 0} onClick={() => setPage(0)}>
                  « First
                </button>
                <button disabled={page === 0} onClick={() => setPage((p) => p - 1)}>
                  ‹ Prev
                </button>
                <span className="muted">
                  Page {page + 1} / {totalPages}
                </span>
                <button disabled={page + 1 >= totalPages} onClick={() => setPage((p) => p + 1)}>
                  Next ›
                </button>
                <button disabled={page + 1 >= totalPages} onClick={() => setPage(totalPages - 1)}>
                  Last »
                </button>
              </div>
            </div>
          )}
        </>
      )}
    </>
  );
}
