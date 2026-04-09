import { useEffect, useState, useCallback } from "react";
import { api } from "../../api/client";
import type { BackupEntry, DatabaseSummary } from "../../types/api";
import RestoreDialog from "./RestoreDialog";
import styles from "./BackupPanel.module.css";

interface Props {
  database: string;
  onClose: () => void;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

export default function BackupPanel({ database, onClose }: Props) {
  const [backups, setBackups] = useState<BackupEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [restoring, setRestoring] = useState<BackupEntry | null>(null);
  const [databases, setDatabases] = useState<DatabaseSummary[]>([]);

  const refresh = useCallback(() => {
    setLoading(true);
    api.backup.list(database).then(setBackups).catch(() => {}).finally(() => setLoading(false));
  }, [database]);

  useEffect(() => { refresh(); }, [refresh]);

  useEffect(() => {
    api.db.list().then((r) => setDatabases(r.databases)).catch(() => {});
  }, []);

  const handleDelete = useCallback(async (filename: string) => {
    if (!confirm(`Delete backup ${filename}?`)) return;
    try {
      await api.backup.remove(filename);
      refresh();
    } catch (err) {
      console.error("Delete failed:", err);
    }
  }, [refresh]);

  const handleRestore = useCallback(async (targetDb: string, backup: string) => {
    try {
      await api.backup.restore(targetDb, backup);
      setRestoring(null);
      refresh();
    } catch (err) {
      alert(`Restore failed: ${err}`);
    }
  }, [refresh]);

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <h2 className={styles.heading}>Backups: {database}</h2>
        <button className={styles.closeButton} onClick={onClose}>x</button>
      </div>

      {loading ? (
        <div className={styles.empty}>Loading...</div>
      ) : backups.length === 0 ? (
        <div className={styles.empty}>No backups. Use the Backup button on the database card to create one.</div>
      ) : (
        <table className={styles.table}>
          <thead>
            <tr>
              <th>Filename</th>
              <th>Created</th>
              <th>Size</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {backups.map((b) => (
              <tr key={b.filename}>
                <td className={styles.filename}>{b.filename}</td>
                <td>{formatDate(b.created_at)}</td>
                <td>{formatBytes(b.size_bytes)}</td>
                <td className={styles.rowActions}>
                  <a
                    href={api.backup.downloadUrl(b.filename)}
                    className={styles.link}
                    download
                  >
                    Download
                  </a>
                  <button
                    className={styles.link}
                    onClick={() => setRestoring(b)}
                  >
                    Restore
                  </button>
                  <button
                    className={`${styles.link} ${styles.danger}`}
                    onClick={() => handleDelete(b.filename)}
                  >
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {restoring && (
        <RestoreDialog
          backup={restoring}
          databases={databases.map((d) => d.name)}
          defaultTarget={database}
          onRestore={handleRestore}
          onCancel={() => setRestoring(null)}
        />
      )}
    </div>
  );
}
