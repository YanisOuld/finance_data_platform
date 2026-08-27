import { useState } from "react";
import { api } from "../api.js";

export default function InstrumentsSection({ apiKey, instruments, reload }) {
  const [newTicker, setNewTicker] = useState("");
  const [newScheduled, setNewScheduled] = useState(true);
  const [adding, setAdding] = useState(false);
  const [status, setStatus] = useState(null); // {ok, msg}
  const [togglingTicker, setTogglingTicker] = useState(null);

  async function handleAdd() {
    const ticker = newTicker.trim().toUpperCase();
    if (!ticker) return;
    setAdding(true);
    setStatus(null);
    try {
      await api("/instruments", apiKey, {
        method: "POST",
        body: JSON.stringify({ ticker, is_scheduled: newScheduled }),
      });
      setStatus({
        ok: true,
        msg: `${ticker} registered. Initial price backfill is running in the background -- check back in a bit, or hit Refresh.`,
      });
      setNewTicker("");
      await reload();
    } catch (e) {
      setStatus({ ok: false, msg: `Failed to register ${ticker}: ${e.message}` });
    } finally {
      setAdding(false);
    }
  }

  async function handleToggle(inst) {
    setTogglingTicker(inst.ticker);
    try {
      await api(`/instruments/${inst.ticker}/scheduled`, apiKey, {
        method: "PATCH",
        body: JSON.stringify({ is_scheduled: !inst.is_scheduled }),
      });
      await reload();
    } catch (e) {
      alert(`Failed to toggle ${inst.ticker}: ${e.message}`);
    } finally {
      setTogglingTicker(null);
    }
  }

  return (
    <section>
      <h2>
        Instruments <span className="muted">registered tickers &amp; ETL scheduling</span>
      </h2>
      {status && <div className={`status ${status.ok ? "ok" : "err"}`}>{status.msg}</div>}
      <div className="row">
        <input
          type="text"
          style={{ width: 120 }}
          placeholder="e.g. AAPL"
          value={newTicker}
          onChange={(e) => setNewTicker(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleAdd()}
        />
        <label className="muted">
          <input
            type="checkbox"
            checked={newScheduled}
            onChange={(e) => setNewScheduled(e.target.checked)}
          />{" "}
          auto-schedule
        </label>
        <button onClick={handleAdd} disabled={adding}>
          + Add ticker
        </button>
        {adding && <span className="muted">validating + upserting (a few seconds)...</span>}
      </div>
      <table>
        <thead>
          <tr>
            <th>Ticker</th>
            <th>Name</th>
            <th>Exchange</th>
            <th>Currency</th>
            <th>Active</th>
            <th>Scheduled</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {instruments.map((inst) => (
            <tr key={inst.ticker}>
              <td>
                <code>{inst.ticker}</code>
              </td>
              <td>{inst.name || ""}</td>
              <td>{inst.exchange || ""}</td>
              <td>{inst.currency}</td>
              <td>
                {inst.is_active ? (
                  <span className="pill on">active</span>
                ) : (
                  <span className="pill off">inactive</span>
                )}
              </td>
              <td>
                {inst.is_scheduled ? (
                  <span className="pill on">scheduled</span>
                ) : (
                  <span className="pill off">manual</span>
                )}
              </td>
              <td>
                <button onClick={() => handleToggle(inst)} disabled={togglingTicker === inst.ticker}>
                  {inst.is_scheduled ? "Pause ETL" : "Resume ETL"}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {instruments.length === 0 && <div className="empty">No instruments registered yet.</div>}
    </section>
  );
}
