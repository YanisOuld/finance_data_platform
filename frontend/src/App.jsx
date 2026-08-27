import { useCallback, useEffect, useState } from "react";
import { api } from "./api.js";
import Header from "./components/Header.jsx";
import InstrumentsSection from "./components/InstrumentsSection.jsx";
import TickerDetailSection from "./components/TickerDetailSection.jsx";
import MacroSection from "./components/MacroSection.jsx";

export default function App() {
  const [apiKey, setApiKey] = useState(() => localStorage.getItem("apiKey") || "");
  const [instruments, setInstruments] = useState([]);
  const [refreshing, setRefreshing] = useState(false);
  const [loadError, setLoadError] = useState(null);

  useEffect(() => {
    localStorage.setItem("apiKey", apiKey);
  }, [apiKey]);

  const reload = useCallback(async () => {
    setRefreshing(true);
    try {
      const body = await api("/instruments", apiKey);
      setInstruments(body);
      setLoadError(null);
    } catch (e) {
      setLoadError(`Could not reach the API: ${e.message}`);
    } finally {
      setRefreshing(false);
    }
  }, [apiKey]);

  useEffect(() => {
    reload();
  }, [reload]);

  return (
    <>
      <Header apiKey={apiKey} setApiKey={setApiKey} onRefresh={reload} refreshing={refreshing} />
      <main>
        {loadError && <div className="status err">{loadError}</div>}
        <InstrumentsSection apiKey={apiKey} instruments={instruments} reload={reload} />
        <TickerDetailSection apiKey={apiKey} instruments={instruments} />
        <MacroSection apiKey={apiKey} />
      </main>
    </>
  );
}
