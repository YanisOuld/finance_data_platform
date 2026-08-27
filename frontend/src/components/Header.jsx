export default function Header({ apiKey, setApiKey, onRefresh, refreshing }) {
  return (
    <header>
      <h1>Finance Data Platform</h1>
      <div className="field">
        <label htmlFor="apiKey">X-API-Key</label>
        <input
          type="password"
          id="apiKey"
          placeholder="only needed if API_KEY is set server-side"
          value={apiKey}
          onChange={(e) => setApiKey(e.target.value)}
        />
      </div>
      <button onClick={onRefresh} disabled={refreshing}>
        Refresh
      </button>
    </header>
  );
}
