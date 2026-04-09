import { useEffect, useState } from "react";
import { api } from "../../api/client";
import type { HealthResponse, DatabaseSummary } from "../../types/api";
import styles from "./ServerOverview.module.css";

function formatUptime(seconds: number): string {
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

export default function ServerOverview() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [databases, setDatabases] = useState<DatabaseSummary[]>([]);

  useEffect(() => {
    api.health().then(setHealth).catch(() => {});
    api.db.list().then((r) => setDatabases(r.databases)).catch(() => {});
  }, []);

  if (!health) return <div className={styles.loading}>Loading server info...</div>;

  const totalNodes = databases.reduce((n, d) => n + d.node_count, 0);
  const totalEdges = databases.reduce((n, d) => n + d.edge_count, 0);

  return (
    <div className={styles.grid}>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Server</span>
        <span className={styles.cardValue}>v{health.version}</span>
      </div>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Engine</span>
        <span className={styles.cardValue}>v{health.engine_version}</span>
      </div>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Uptime</span>
        <span className={styles.cardValue}>{formatUptime(health.uptime_seconds)}</span>
      </div>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Storage</span>
        <span className={styles.cardValue}>{health.persistent ? "Persistent" : "In-memory"}</span>
      </div>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Databases</span>
        <span className={styles.cardValue}>{databases.length}</span>
      </div>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Total Nodes</span>
        <span className={styles.cardValue}>{totalNodes.toLocaleString()}</span>
      </div>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Total Edges</span>
        <span className={styles.cardValue}>{totalEdges.toLocaleString()}</span>
      </div>
      <div className={styles.card}>
        <span className={styles.cardLabel}>Sessions</span>
        <span className={styles.cardValue}>{health.active_sessions}</span>
      </div>
    </div>
  );
}
