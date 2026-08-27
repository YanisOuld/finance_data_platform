import { useEffect, useState } from "react";
import { api, fmtNum } from "../api.js";

export default function PricesTab({ apiKey, ticker }) {
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!ticker) {
      setRows([]);
      return;
    }
    setRows(null);
    setError(null);
    api(`/prices/${ticker}?limit=100&offset=0`, apiKey)
      .then((body) => setRows(body.slice().reverse()))
      .catch((e) => setError(e.message));
  }, [ticker, apiKey]);

  if (error) return <div className="empty">Error: {error}</div>;
  if (rows === null) return <div className="empty">Loading...</div>;

  return (
    <>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Open</th>
            <th>High</th>
            <th>Low</th>
            <th>Close</th>
            <th>Volume</th>
            <th>Return</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.ts}>
              <td>{row.ts}</td>
              <td>{fmtNum(row.open)}</td>
              <td>{fmtNum(row.high)}</td>
              <td>{fmtNum(row.low)}</td>
              <td>{fmtNum(row.close)}</td>
              <td>{row.volume ?? ""}</td>
              <td>
                {row.close_returns !== null && row.close_returns !== undefined
                  ? `${fmtNum(row.close_returns * 100, 2)}%`
                  : ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && (
        <div className="empty">
          No price data yet -- select a ticker, or the backfill may still be running.
        </div>
      )}
    </>
  );
}
