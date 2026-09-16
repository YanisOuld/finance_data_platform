import { useEffect, useState } from "react";
import { api, apiWithTotal, errMsg, fmtNum } from "../api";
import type { MacroCatalog, MacroPoint } from "../types";

const PAGE_SIZES = [25, 50, 100, 250];

interface MacroSectionProps {
  apiKey: string;
}

export default function MacroSection({ apiKey }: MacroSectionProps) {
  const [catalog, setCatalog] = useState<MacroCatalog | null>(null);
  const [group, setGroup] = useState("US");
  const [series, setSeries] = useState("");
  const [rows, setRows] = useState<MacroPoint[] | null>(null);
  const [total, setTotal] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [page, setPage] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const offset = page * pageSize;

  // Load the grouped catalog once.
  useEffect(() => {
    api<MacroCatalog>("/macro/catalog", apiKey)
      .then((cat) => {
        setCatalog(cat);
        const groups = Object.keys(cat);
        const g = groups.includes("US") ? "US" : groups[0];
        setGroup(g);
        setSeries(cat[g]?.[0]?.code || "");
      })
      .catch((e) => setError(errMsg(e)));
  }, [apiKey]);

  // Fetch the selected series' observations (newest first, paginated server-side).
  useEffect(() => {
    if (!series) return;
    setRows(null);
    setError(null);
    const qs = new URLSearchParams({
      limit: String(pageSize),
      offset: String(offset),
      order: "desc",
    });
    apiWithTotal<MacroPoint[]>(`/macro/${series}?${qs}`, apiKey)
      .then(({ data, total }) => {
        setRows(data);
        setTotal(total);
      })
      .catch((e) => setError(errMsg(e)));
  }, [series, apiKey, pageSize, offset]);

  function pickGroup(g: string) {
    setGroup(g);
    setSeries(catalog?.[g]?.[0]?.code || "");
    setPage(0);
  }

  const options = catalog?.[group] || [];
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <section>
      <h2>
        Macro / FX <span className="muted">FRED series — US &amp; Canada</span>
      </h2>

      {catalog && (
        <div className="tabs">
          {Object.keys(catalog).map((g) => (
            <button key={g} className={group === g ? "active" : ""} onClick={() => pickGroup(g)}>
              {g}
            </button>
          ))}
        </div>
      )}

      <div className="row">
        <select
          value={series}
          onChange={(e) => {
            setSeries(e.target.value);
            setPage(0);
          }}
        >
          {options.map((s) => (
            <option key={s.code} value={s.code}>
              {s.label}
            </option>
          ))}
        </select>
        {series && (
          <span className="muted">
            series code: <code>{series}</code>
          </span>
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

      {error && <div className="empty">Error: {error}</div>}
      {!error && rows === null && <div className="empty">Loading...</div>}
      {!error && rows !== null && (
        <>
          <div style={{ overflowX: "auto" }}>
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th style={{ textAlign: "right" }}>Value</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.ts}>
                    <td>{row.ts}</td>
                    <td style={{ textAlign: "right" }}>{fmtNum(row.value, 3)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {rows.length === 0 ? (
            <div className="empty">No data for this series yet — run the FRED macro DAG for it.</div>
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
    </section>
  );
}
