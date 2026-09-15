import { useEffect, useMemo, useRef, useState } from "react";
import { api, apiWithTotal } from "../api.js";
import { KEY_METRICS, KEY_METRIC_CONCEPTS, prettyConcept, fmtCompact } from "../fundamentalsMeta.js";

const EXPLORER_COLUMNS = [
  { key: "concept", label: "Concept" },
  { key: "period_end", label: "Period end" },
  { key: "fy", label: "FY", num: true },
  { key: "fp", label: "FP" },
  { key: "form", label: "Form" },
  { key: "val", label: "Value", num: true },
];

const PAGE_SIZES = [25, 50, 100, 250];

// ---- Curated "Key financials" -------------------------------------------------
function KeyFinancials({ apiKey, ticker }) {
  const [form, setForm] = useState("10-Q");
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!ticker) return;
    setRows(null);
    setError(null);
    const qs = new URLSearchParams({
      concepts: KEY_METRIC_CONCEPTS.join(","),
      form,
      limit: "3000",
      sort_by: "period_end",
      order: "desc",
    });
    api(`/fundamentals/${ticker}?${qs}`, apiKey)
      .then(setRows)
      .catch((e) => setError(e.message));
  }, [ticker, apiKey, form]);

  const { periods, metricRows } = useMemo(() => {
    if (!rows || rows.length === 0) return { periods: [], metricRows: [] };
    const periods = [...new Set(rows.map((r) => r.period_end))].sort().reverse().slice(0, 5);

    const metricRows = KEY_METRICS.map((m) => {
      const concept = m.concepts.find((c) => rows.some((r) => r.concept === c));
      const byPeriod = {};
      if (concept) {
        for (const r of rows) {
          if (r.concept === concept) byPeriod[r.period_end] = r.val;
        }
      }
      return { label: m.label, kind: m.kind, concept, byPeriod };
    }).filter((m) => m.concept);

    return { periods, metricRows };
  }, [rows]);

  return (
    <section style={{ marginBottom: 20 }}>
      <div className="row">
        <strong>Key financials</strong>
        <div className="tabs" style={{ marginBottom: 0, marginLeft: "auto" }}>
          {["10-Q", "10-K"].map((f) => (
            <button key={f} className={form === f ? "active" : ""} onClick={() => setForm(f)}>
              {f === "10-Q" ? "Quarterly" : "Annual"}
            </button>
          ))}
        </div>
      </div>

      {error && <div className="empty">Error: {error}</div>}
      {!error && rows === null && <div className="empty">Loading...</div>}
      {!error && rows !== null && metricRows.length === 0 && (
        <div className="empty">No key financials for {form} filings.</div>
      )}
      {!error && metricRows.length > 0 && (
        <div style={{ overflowX: "auto" }}>
          <table>
            <thead>
              <tr>
                <th>Metric</th>
                {periods.map((p) => (
                  <th key={p} style={{ textAlign: "right" }}>
                    {p}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {metricRows.map((m) => (
                <tr key={m.label}>
                  <td title={m.concept}>{m.label}</td>
                  {periods.map((p) => (
                    <td key={p} style={{ textAlign: "right" }}>
                      {fmtCompact(m.byPeriod[p], m.kind)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

// ---- Multi-concept picker -----------------------------------------------------
// Lets the user choose exactly which concepts to view (search + checkboxes), so
// the explorer can show just those, grouped together. Empty selection = all.
function ConceptPicker({ concepts, selected, setSelected }) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const boxRef = useRef(null);

  // Close when clicking outside the picker.
  useEffect(() => {
    function onClick(e) {
      if (boxRef.current && !boxRef.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, []);

  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const base = needle
      ? concepts.filter(
          (c) => c.toLowerCase().includes(needle) || prettyConcept(c).toLowerCase().includes(needle)
        )
      : concepts;
    return base.slice(0, 300); // cap rendered rows for performance
  }, [concepts, q]);

  function toggle(c) {
    setSelected((prev) => (prev.includes(c) ? prev.filter((x) => x !== c) : [...prev, c]));
  }

  return (
    <div ref={boxRef} style={{ position: "relative" }}>
      <button onClick={() => setOpen((o) => !o)}>
        Choose concepts{selected.length ? ` (${selected.length})` : ""} ▾
      </button>
      {open && (
        <div
          style={{
            position: "absolute",
            zIndex: 20,
            top: "calc(100% + 4px)",
            left: 0,
            width: 380,
            background: "var(--panel)",
            border: "1px solid var(--border)",
            borderRadius: 8,
            padding: 8,
            boxShadow: "0 8px 24px rgba(0,0,0,0.4)",
          }}
        >
          <input
            type="text"
            autoFocus
            placeholder="search concept..."
            value={q}
            onChange={(e) => setQ(e.target.value)}
            style={{ width: "100%", marginBottom: 6 }}
          />
          <div style={{ maxHeight: 280, overflowY: "auto" }}>
            {filtered.length === 0 && <div className="empty" style={{ padding: 6 }}>No match.</div>}
            {filtered.map((c) => (
              <label
                key={c}
                style={{ display: "flex", alignItems: "center", gap: 8, padding: "3px 4px", cursor: "pointer" }}
                title={c}
              >
                <input type="checkbox" checked={selected.includes(c)} onChange={() => toggle(c)} />
                <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {prettyConcept(c)}
                </span>
              </label>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ---- Full explorer ------------------------------------------------------------
function Explorer({ apiKey, ticker }) {
  const [concepts, setConcepts] = useState([]);
  const [selected, setSelected] = useState([]);
  const [form, setForm] = useState("");
  const [sortBy, setSortBy] = useState("period_end");
  const [order, setOrder] = useState("desc");
  const [pageSize, setPageSize] = useState(50);
  const [page, setPage] = useState(0);
  const [rows, setRows] = useState(null);
  const [total, setTotal] = useState(0);
  const [error, setError] = useState(null);

  const offset = page * pageSize;

  useEffect(() => {
    if (!ticker) return;
    setSelected([]);
    api(`/fundamentals/${ticker}/concepts`, apiKey)
      .then(setConcepts)
      .catch(() => setConcepts([]));
  }, [ticker, apiKey]);

  // Any concept selection change: back to page 1, and group rows by concept so
  // the chosen concepts sit together (user can still re-sort by clicking a header).
  function updateSelected(updater) {
    setSelected((prev) => {
      const next = typeof updater === "function" ? updater(prev) : updater;
      if (next.length > 0) {
        setSortBy("concept");
        setOrder("asc");
      } else {
        setSortBy("period_end");
        setOrder("desc");
      }
      return next;
    });
    setPage(0);
  }

  useEffect(() => {
    if (!ticker) return;
    setRows(null);
    setError(null);
    const qs = new URLSearchParams({
      limit: String(pageSize),
      offset: String(offset),
      sort_by: sortBy,
      order,
    });
    if (selected.length) qs.set("concepts", selected.join(","));
    if (form) qs.set("form", form);
    apiWithTotal(`/fundamentals/${ticker}?${qs}`, apiKey)
      .then(({ data, total }) => {
        setRows(data);
        setTotal(total);
      })
      .catch((e) => setError(e.message));
  }, [ticker, apiKey, selected, form, sortBy, order, pageSize, offset]);

  function changeSort(key) {
    if (key === sortBy) setOrder((o) => (o === "asc" ? "desc" : "asc"));
    else {
      setSortBy(key);
      setOrder("desc");
    }
    setPage(0);
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <section>
      <div className="row">
        <strong>Explore concepts</strong>
      </div>
      <div className="row">
        <ConceptPicker concepts={concepts} selected={selected} setSelected={updateSelected} />
        <select
          value={form}
          onChange={(e) => {
            setForm(e.target.value);
            setPage(0);
          }}
        >
          <option value="">All forms</option>
          <option value="10-K">10-K (annual)</option>
          <option value="10-Q">10-Q (quarterly)</option>
          <option value="8-K">8-K</option>
        </select>
        {(selected.length > 0 || form) && (
          <button
            onClick={() => {
              updateSelected([]);
              setForm("");
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

      {/* Selected concepts as removable chips */}
      {selected.length > 0 && (
        <div className="row" style={{ gap: 6 }}>
          {selected.map((c) => (
            <span
              key={c}
              className="pill on"
              style={{ cursor: "pointer" }}
              title={c}
              onClick={() => updateSelected((prev) => prev.filter((x) => x !== c))}
            >
              {prettyConcept(c)} ✕
            </span>
          ))}
        </div>
      )}

      {error && <div className="status err">Error: {error}</div>}
      {!error && rows === null && <div className="empty">Loading...</div>}

      {!error && rows !== null && (
        <>
          <div style={{ overflowX: "auto" }}>
            <table>
              <thead>
                <tr>
                  {EXPLORER_COLUMNS.map((c) => {
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
                {rows.map((row, i) => (
                  <tr key={i}>
                    <td title={row.concept}>{prettyConcept(row.concept)}</td>
                    <td>{row.period_end}</td>
                    <td style={{ textAlign: "right" }}>{row.fy ?? ""}</td>
                    <td>{row.fp}</td>
                    <td>{row.form}</td>
                    <td style={{ textAlign: "right" }}>
                      {fmtCompact(row.val, row.unit === "USD/shares" ? "perShare" : "money")}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {rows.length === 0 ? (
            <div className="empty">No facts match this filter.</div>
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
    </section>
  );
}

export default function FundamentalsTab({ apiKey, ticker }) {
  if (!ticker) return <div className="empty">Select a ticker.</div>;
  return (
    <>
      <KeyFinancials apiKey={apiKey} ticker={ticker} />
      <Explorer apiKey={apiKey} ticker={ticker} />
    </>
  );
}
