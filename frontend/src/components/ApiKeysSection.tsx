import { useCallback, useEffect, useState } from "react";
import { api, errMsg } from "../api";
import type { ApiKeyInfo, ApiKeyCreated } from "../types";

interface ApiKeysSectionProps {
  apiKey: string;
}

interface FreshKey {
  label: string;
  key: string;
}

// Manage the platform's API keys. Needs the admin (env master) key.
export default function ApiKeysSection({ apiKey }: ApiKeysSectionProps) {
  const [keys, setKeys] = useState<ApiKeyInfo[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [label, setLabel] = useState("");
  const [creating, setCreating] = useState(false);
  const [freshKey, setFreshKey] = useState<FreshKey | null>(null); // shown once
  const [copied, setCopied] = useState(false);

  const reload = useCallback(async () => {
    setError(null);
    try {
      const rows = await api<ApiKeyInfo[]>("/admin/api-keys", apiKey);
      setKeys(rows);
    } catch (e) {
      setKeys([]);
      setError(errMsg(e));
    }
  }, [apiKey]);

  useEffect(() => {
    reload();
  }, [reload]);

  async function onCreate(e: React.FormEvent) {
    e.preventDefault();
    if (!label.trim()) return;
    setCreating(true);
    setError(null);
    try {
      const created = await api<ApiKeyCreated>("/admin/api-keys", apiKey, {
        method: "POST",
        body: JSON.stringify({ label: label.trim() }),
      });
      setFreshKey({ label: created.label, key: created.key });
      setCopied(false);
      setLabel("");
      await reload();
    } catch (e) {
      setError(errMsg(e));
    } finally {
      setCreating(false);
    }
  }

  async function onRevoke(id: number, keyLabel: string) {
    if (!window.confirm(`Revoke key "${keyLabel}"? Systems using it will stop working.`)) return;
    setError(null);
    try {
      await api(`/admin/api-keys/${id}`, apiKey, { method: "DELETE" });
      await reload();
    } catch (e) {
      setError(errMsg(e));
    }
  }

  async function copyKey() {
    if (!freshKey) return;
    try {
      await navigator.clipboard.writeText(freshKey.key);
      setCopied(true);
    } catch {
      /* clipboard blocked -- the value is still selectable on screen */
    }
  }

  return (
    <section>
      <h2>
        API keys <span className="muted">— for external systems to read the platform</span>
      </h2>

      {error && <div className="status err">{error}</div>}

      <form className="row" onSubmit={onCreate}>
        <input
          type="text"
          style={{ width: 260 }}
          placeholder="label, e.g. reporting-service"
          value={label}
          onChange={(e) => setLabel(e.target.value)}
        />
        <button type="submit" disabled={creating || !label.trim()}>
          {creating ? "Creating..." : "Create key"}
        </button>
      </form>

      {freshKey && (
        <div className="status ok" style={{ marginBottom: 12 }}>
          <div style={{ marginBottom: 6 }}>
            New key <strong>{freshKey.label}</strong> — copy it now, it won't be shown again:
          </div>
          <div className="row" style={{ marginBottom: 0 }}>
            <code style={{ userSelect: "all", wordBreak: "break-all" }}>{freshKey.key}</code>
            <button type="button" onClick={copyKey}>
              {copied ? "Copied ✓" : "Copy"}
            </button>
            <button type="button" onClick={() => setFreshKey(null)}>
              Dismiss
            </button>
          </div>
        </div>
      )}

      {keys === null ? (
        <div className="empty">Loading...</div>
      ) : keys.length === 0 ? (
        <div className="empty">No keys yet. Create one above.</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table>
            <thead>
              <tr>
                <th>Label</th>
                <th>Prefix</th>
                <th>Status</th>
                <th>Created</th>
                <th>Last used</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {keys.map((k) => (
                <tr key={k.id}>
                  <td>{k.label}</td>
                  <td>
                    <code>{k.prefix}…</code>
                  </td>
                  <td>
                    <span className={`pill ${k.is_active ? "on" : "off"}`}>
                      {k.is_active ? "active" : "revoked"}
                    </span>
                  </td>
                  <td className="muted">{k.created_at ? k.created_at.slice(0, 10) : ""}</td>
                  <td className="muted">{k.last_used_at ? k.last_used_at.replace("T", " ").slice(0, 16) : "never"}</td>
                  <td>
                    {k.is_active && (
                      <button onClick={() => onRevoke(k.id, k.label)}>Revoke</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
