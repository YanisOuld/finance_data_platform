import type { NavPage } from "../types";

interface HeaderProps {
  apiKey: string;
  setApiKey: (v: string) => void;
  onRefresh: () => void;
  refreshing: boolean;
  page: string;
  setPage: (p: string) => void;
  pages: NavPage[];
}

export default function Header({
  apiKey,
  setApiKey,
  onRefresh,
  refreshing,
  page,
  setPage,
  pages,
}: HeaderProps) {
  return (
    <header>
      <div style={{ display: "flex", alignItems: "center", gap: 16, flexWrap: "wrap", width: "100%" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, flex: 1 }}>
          {/* Logo lives in public/, referenced via the Vite base so it resolves
              under /app/ in both dev and the built app. */}
          <img
            src={`${import.meta.env.BASE_URL}logo.png`}
            alt="Finance Data Platform logo"
            width={28}
            height={28}
            style={{ display: "block" }}
          />
          <h1>Finance Data Platform</h1>
        </div>
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
      </div>

      <nav className="tabs" style={{ marginBottom: 0, flexBasis: "100%" }}>
        {pages.map((p) => (
          <button key={p.key} className={page === p.key ? "active" : ""} onClick={() => setPage(p.key)}>
            {p.label}
          </button>
        ))}
      </nav>
    </header>
  );
}
