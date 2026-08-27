import { useEffect, useState } from "react";
import { api, fmtNum, FRED_SERIES } from "../api.js";

export default function MacroSection({ apiKey }) {
  const [series, setSeries] = useState(FRED_SERIES[0]);
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    setRows(null);
    setError(null);
    api(`/macro/${series}?limit=100`, apiKey)
      .then((body) => setRows(body.slice().reverse()))
      .catch((e) => setError(e.message));
  }, [series, apiKey]);

  return (
    <section>
      <h2>
        Macro / FX <span className="muted">FRED series</span>
      </h2>
      <div className="row">
        <select value={series} onChange={(e) => setSeries(e.target.value)}>
          {FRED_SERIES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </div>
      {error && <div className="empty">Error: {error}</div>}
      {!error && rows === null && <div className="empty">Loading...</div>}
      {!error && rows !== null && (
        <>
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.ts}>
                  <td>{row.ts}</td>
                  <td>{fmtNum(row.value, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length === 0 && <div className="empty">No data for this series yet.</div>}
        </>
      )}
    </section>
  );
}
