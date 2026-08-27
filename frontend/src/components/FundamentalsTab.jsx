import { useEffect, useState } from "react";
import { api, fmtNum } from "../api.js";

export default function FundamentalsTab({ apiKey, ticker }) {
  const [concept, setConcept] = useState("");
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!ticker) {
      setRows([]);
      return;
    }
    setRows(null);
    setError(null);
    const qs = new URLSearchParams({ limit: "100" });
    if (concept.trim()) qs.set("concept", concept.trim());
    api(`/fundamentals/${ticker}?${qs}`, apiKey)
      .then(setRows)
      .catch((e) => setError(e.message));
  }, [ticker, apiKey, concept]);

  return (
    <>
      <div className="row">
        <input
          type="text"
          style={{ width: 280 }}
          placeholder="filter by concept, e.g. us-gaap:Revenues"
          value={concept}
          onChange={(e) => setConcept(e.target.value)}
        />
      </div>
      {error && <div className="empty">Error: {error}</div>}
      {!error && rows === null && <div className="empty">Loading...</div>}
      {!error && rows !== null && (
        <>
          <table>
            <thead>
              <tr>
                <th>Concept</th>
                <th>Unit</th>
                <th>Period end</th>
                <th>FY</th>
                <th>FP</th>
                <th>Form</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  <td>{row.concept}</td>
                  <td>{row.unit}</td>
                  <td>{row.period_end}</td>
                  <td>{row.fy ?? ""}</td>
                  <td>{row.fp}</td>
                  <td>{row.form}</td>
                  <td>{fmtNum(row.val, 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length === 0 && <div className="empty">No fundamentals data yet.</div>}
        </>
      )}
    </>
  );
}
