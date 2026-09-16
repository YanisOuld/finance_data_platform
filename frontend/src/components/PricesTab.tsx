import { useEffect, useState } from "react";
import { apiWithTotal, errMsg, fmtNum } from "../api";
import type { Price } from "../types";

// Sortable columns; `key` is sent as sort_by, `num` right-aligns.
interface Column {
  key: string;
  label: string;
  num?: boolean;
}

const COLUMNS: Column[] = [
  { key: "ts", label: "Date" },
  { key: "open", label: "Open", num: true },
  { key: "high", label: "High", num: true },
  { key: "low", label: "Low", num: true },
  { key: "close", label: "Close", num: true },
  { key: "volume", label: "Volume", num: true },
  { key: "close_returns", label: "Return", num: true },
];

const PAGE_SIZES = [25, 50, 100, 250];

type Order = "asc" | "desc";

interface PricesTabProps {
  apiKey: string;
  ticker: string;
}

export default function PricesTab({ apiKey, ticker }: PricesTabProps) {
  const [rows, setRows] = useState<Price[] | null>(null);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [sortBy, setSortBy] = useState("ts");
  const [order, setOrder] = useState<Order>("desc");
  const [pageSize, setPageSize] = useState(50);
  const [page, setPage] = useState(0); // zero-based

  const offset = page * pageSize;

  // Server-side filter/sort/paginate so results stay consistent across pages.
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

    apiWithTotal<Price[]>(`/prices/${ticker}?${params.toString()}`, apiKey)
      .then(({ data, total }) => {
        setRows(data);
        setTotal(total);
      })
      .catch((e) => setError(errMsg(e)));
  }, [ticker, apiKey, start, end, sortBy, order, pageSize, offset]);

  function changeSort(key: string) {
    if (key === sortBy) {
      setOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(key);
      setOrder("desc");
    }
    setPage(0);
  }

  function onFilter(setter: (v: string) => void) {
    return (e: React.ChangeEvent<HTMLInputElement>) => {
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
              {rows.map((row) => {
                const ret = row.close_returns;
                const retColor =
                  ret != null && ret > 0 ? "var(--ok)" : ret != null && ret < 0 ? "var(--bad)" : undefined;
                return (
                  <tr key={row.ts}>
                    <td>{row.ts}</td>
                    <td style={{ textAlign: "right" }}>{fmtNum(row.open)}</td>
                    <td style={{ textAlign: "right" }}>{fmtNum(row.high)}</td>
                    <td style={{ textAlign: "right" }}>{fmtNum(row.low)}</td>
                    <td style={{ textAlign: "right" }}>{fmtNum(row.close)}</td>
                    <td style={{ textAlign: "right" }}>{row.volume ?? ""}</td>
                    <td style={{ textAlign: "right", color: retColor }}>
                      {ret != null ? `${fmtNum(ret * 100, 2)}%` : ""}
                    </td>
                  </tr>
                );
              })}
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
                <button className="pagebtn" disabled={page === 0} onClick={() => setPage(0)}>
                  « First
                </button>
                <button className="pagebtn" disabled={page === 0} onClick={() => setPage((p) => p - 1)}>
                  ‹ Prev
                </button>
                <span className="muted">
                  Page {page + 1} / {totalPages}
                </span>
                <button
                  className="pagebtn"
                  disabled={page + 1 >= totalPages}
                  onClick={() => setPage((p) => p + 1)}
                >
                  Next ›
                </button>
                <button
                  className="pagebtn"
                  disabled={page + 1 >= totalPages}
                  onClick={() => setPage(totalPages - 1)}
                >
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
