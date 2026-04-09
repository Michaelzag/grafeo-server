import { useEffect, useState, useCallback } from "react";
import { api } from "../../api/client";
import type { TokenResponse, DatabaseSummary } from "../../types/api";
import CreateTokenDialog from "./CreateTokenDialog";
import styles from "./TokenPanel.module.css";

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

export default function TokenPanel() {
  const [tokens, setTokens] = useState<TokenResponse[]>([]);
  const [databases, setDatabases] = useState<DatabaseSummary[]>([]);
  const [creating, setCreating] = useState(false);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(() => {
    setLoading(true);
    api.tokens
      .list()
      .then(setTokens)
      .catch(() => setTokens([]))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    refresh();
    api.db.list().then((r) => setDatabases(r.databases)).catch(() => {});
  }, [refresh]);

  const handleDelete = useCallback(
    async (id: string, name: string) => {
      if (!confirm(`Revoke token "${name}"? This takes effect immediately.`)) return;
      try {
        await api.tokens.delete(id);
        refresh();
      } catch (err) {
        console.error("Delete token failed:", err);
      }
    },
    [refresh],
  );

  const handleCreated = useCallback(() => {
    setCreating(false);
    refresh();
  }, [refresh]);

  return (
    <div>
      <div className={styles.header}>
        <h2 className={styles.heading}>API Tokens</h2>
        <button className={styles.createButton} onClick={() => setCreating(true)}>
          + New Token
        </button>
      </div>

      {loading ? (
        <div className={styles.empty}>Loading...</div>
      ) : tokens.length === 0 ? (
        <div className={styles.empty}>No tokens. Create one to get started.</div>
      ) : (
        <table className={styles.table}>
          <thead>
            <tr>
              <th>Name</th>
              <th>Role</th>
              <th>Databases</th>
              <th>Created</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {tokens.map((t) => (
              <tr key={t.id}>
                <td className={styles.tokenName}>{t.name}</td>
                <td>
                  <span className={`${styles.roleBadge} ${styles[`role_${t.scope.role.replace("-", "_")}`] || ""}`}>
                    {t.scope.role}
                  </span>
                </td>
                <td>
                  {t.scope.databases.length === 0 ? (
                    <span className={styles.allDbs}>all</span>
                  ) : (
                    <span className={styles.dbList}>
                      {t.scope.databases.join(", ")}
                    </span>
                  )}
                </td>
                <td className={styles.date}>{formatDate(t.created_at)}</td>
                <td>
                  <button
                    className={styles.deleteButton}
                    onClick={() => handleDelete(t.id, t.name)}
                  >
                    Revoke
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {creating && (
        <CreateTokenDialog
          databases={databases.map((d) => d.name)}
          onCreated={handleCreated}
          onCancel={() => setCreating(false)}
        />
      )}
    </div>
  );
}
