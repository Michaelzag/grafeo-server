import { useEffect, useState, useCallback } from "react";
import { api } from "../../api/client";
import type { DatabaseSummary, DatabaseStatsResponse } from "../../types/api";
import styles from "./DatabaseCards.module.css";

interface Props {
  selectedDb: string | null;
  onSelectDb: (name: string | null) => void;
}

interface DbCardData extends DatabaseSummary {
  stats?: DatabaseStatsResponse;
  backing_up?: boolean;
}

export default function DatabaseCards({ selectedDb, onSelectDb }: Props) {
  const [databases, setDatabases] = useState<DbCardData[]>([]);

  const refresh = useCallback(() => {
    api.db.list().then(async (r) => {
      const withStats = await Promise.all(
        r.databases.map(async (db) => {
          try {
            const stats = await api.admin.stats(db.name);
            return { ...db, stats };
          } catch {
            return { ...db };
          }
        }),
      );
      setDatabases(withStats);
    }).catch(() => {});
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  const handleBackup = useCallback(async (name: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setDatabases((prev) =>
      prev.map((d) => (d.name === name ? { ...d, backing_up: true } : d)),
    );
    try {
      await api.backup.create(name);
    } catch (err) {
      console.error("Backup failed:", err);
    }
    setDatabases((prev) =>
      prev.map((d) => (d.name === name ? { ...d, backing_up: false } : d)),
    );
  }, []);

  const handleCheckpoint = useCallback(async (name: string, e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      await api.admin.checkpoint(name);
    } catch (err) {
      console.error("Checkpoint failed:", err);
    }
  }, []);

  const handleValidate = useCallback(async (name: string, e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      const result = await api.admin.validate(name);
      if (result.valid) {
        alert(`${name}: valid`);
      } else {
        alert(`${name}: ${result.errors.length} error(s)\n${result.errors.map((e) => e.message).join("\n")}`);
      }
    } catch (err) {
      console.error("Validation failed:", err);
    }
  }, []);

  return (
    <div>
      <h2 className={styles.heading}>Databases</h2>
      <div className={styles.grid}>
        {databases.map((db) => (
          <button
            key={db.name}
            className={`${styles.card} ${selectedDb === db.name ? styles.selected : ""}`}
            onClick={() => onSelectDb(selectedDb === db.name ? null : db.name)}
          >
            <div className={styles.cardHeader}>
              <span className={styles.dbName}>{db.name}</span>
              <span className={styles.badge}>{db.database_type.toUpperCase()}</span>
            </div>
            <div className={styles.stats}>
              <span>{(db.stats?.node_count ?? db.node_count).toLocaleString()} nodes</span>
              <span>{(db.stats?.edge_count ?? db.edge_count).toLocaleString()} edges</span>
            </div>
            <div className={styles.meta}>
              {db.persistent ? "Persistent" : "In-memory"}
              {db.stats?.memory_bytes != null && ` \u00B7 ${formatBytes(db.stats.memory_bytes)}`}
            </div>
            <div className={styles.actions}>
              <button
                className={styles.actionButton}
                onClick={(e) => handleBackup(db.name, e)}
                disabled={db.backing_up}
              >
                {db.backing_up ? "..." : "Backup"}
              </button>
              <button
                className={styles.actionButton}
                onClick={(e) => handleCheckpoint(db.name, e)}
              >
                Checkpoint
              </button>
              <button
                className={styles.actionButton}
                onClick={(e) => handleValidate(db.name, e)}
              >
                Validate
              </button>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
