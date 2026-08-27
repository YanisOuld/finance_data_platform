import { useEffect, useState } from "react";
import { api } from "../api.js";

export default function FigiTab({ apiKey, ticker }) {
  const [rows, setRows] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!ticker) {
      setRows([]);
      return;
    }
    setRows(null);
    setError(null);
    api(`/instruments/${ticker}/figi`, apiKey)
      .then(setRows)
      .catch((e) => setError(e.message));
  }, [ticker, apiKey]);

  if (error) return <div className="empty">Error: {error}</div>;
  if (rows === null) return <div className="empty">Loading...</div>;

  return (
    <>
      <table>
        <thead>
          <tr>
            <th>FIGI</th>
            <th>Composite</th>
            <th>Share class</th>
            <th>Type</th>
            <th>Exchange</th>
            <th>Name</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.figi}>
              <td>
                <code>{row.figi}</code>
              </td>
              <td>{row.composite_figi || ""}</td>
              <td>{row.share_class_figi || ""}</td>
              <td>{row.security_type || ""}</td>
              <td>{row.exch_code || ""}</td>
              <td>{row.name || ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && <div className="empty">No FIGI mapping yet.</div>}
    </>
  );
}
