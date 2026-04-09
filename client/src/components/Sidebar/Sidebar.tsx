import { useContext, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../../api/client";
import type { HealthResponse } from "../../types/api";
import DatabasePanel from "./DatabasePanel";
import { WorkbenchContext } from "../../views/WorkbenchView";
import styles from "./Sidebar.module.css";

interface SidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  currentDatabase: string;
  onSelectDatabase: (name: string, dbType?: string) => void;
  isAdmin?: boolean;
}

const EXAMPLES = [
  { name: "All nodes", query: "MATCH (n) RETURN n LIMIT 25" },
  { name: "All edges", query: "MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 25" },
  { name: "Count nodes", query: "MATCH (n) RETURN count(n)" },
  { name: "Node labels", query: "MATCH (n) RETURN DISTINCT labels(n)" },
  { name: "Insert person", query: "INSERT (:Person {name: 'Alice', age: 30})" },
  { name: "Find by name", query: "MATCH (p:Person {name: 'Alice'}) RETURN p" },
];

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

function formatTimeAgo(timestamp: number): string {
  const diff = Math.floor((Date.now() - timestamp) / 1000);
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

const MAX_VISIBLE_HISTORY = 10;

export default function Sidebar({
  collapsed,
  onToggle,
  currentDatabase,
  onSelectDatabase,
  isAdmin = false,
}: SidebarProps) {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [showAllHistory, setShowAllHistory] = useState(false);
  const navigate = useNavigate();

  // Workbench-specific data (null when on admin view)
  const workbench = useContext(WorkbenchContext);

  useEffect(() => {
    api.health().then(setHealth).catch(() => {});
  }, []);

  const visibleHistory = workbench
    ? (showAllHistory ? workbench.historyEntries : workbench.historyEntries.slice(0, MAX_VISIBLE_HISTORY))
    : [];

  return (
    <aside className={`${styles.container} ${collapsed ? styles.collapsed : ""}`}>
      <div className={styles.brand}>
        <img
          src={import.meta.env.BASE_URL + "favicon.png"}
          alt="Grafeo"
          className={styles.logo}
          onClick={collapsed ? onToggle : undefined}
          style={collapsed ? { cursor: "pointer" } : undefined}
          title={collapsed ? "Expand sidebar" : "Grafeo"}
        />
        {!collapsed && <span className={styles.brandName}>Grafeo Studio</span>}
        <button
          className={styles.collapseButton}
          onClick={onToggle}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {collapsed ? "\u00BB" : "\u00AB"}
        </button>
      </div>

      {!collapsed && (
        <>
          {/* View switcher */}
          <div className={styles.viewSwitcher}>
            <button
              className={`${styles.viewButton} ${!isAdmin ? styles.viewActive : ""}`}
              onClick={() => navigate("/")}
            >
              Query
            </button>
            <button
              className={`${styles.viewButton} ${isAdmin ? styles.viewActive : ""}`}
              onClick={() => navigate("/admin")}
            >
              Admin
            </button>
          </div>

          {/* Server info — always shown */}
          <div className={styles.section}>
            <h3 className={styles.heading}>Server</h3>
            {health ? (
              <div className={styles.serverInfo}>
                <div>
                  <span className={styles.label}>Status</span>
                  <span className={styles.value} style={{ color: "var(--success)" }}>Connected</span>
                </div>
                <div>
                  <span className={styles.label}>Server</span>
                  <span className={styles.value}>v{health.version}</span>
                </div>
                <div>
                  <span className={styles.label}>Engine</span>
                  <span className={styles.value}>v{health.engine_version}</span>
                </div>
                <div>
                  <span className={styles.label}>Storage</span>
                  <span className={styles.value}>{health.persistent ? "Persistent" : "In-memory"}</span>
                </div>
                {health.uptime_seconds != null && (
                  <div>
                    <span className={styles.label}>Uptime</span>
                    <span className={styles.value}>{formatUptime(health.uptime_seconds)}</span>
                  </div>
                )}
                {health.active_sessions != null && (
                  <div>
                    <span className={styles.label}>Sessions</span>
                    <span className={styles.value}>{health.active_sessions}</span>
                  </div>
                )}
              </div>
            ) : (
              <span className={styles.muted}>Connecting...</span>
            )}
          </div>

          {/* Databases — always shown */}
          <div className={styles.section}>
            <h3 className={styles.heading}>Databases</h3>
            <DatabasePanel
              currentDatabase={currentDatabase}
              onSelectDatabase={onSelectDatabase}
            />
          </div>

          {/* Workbench-only sections */}
          {!isAdmin && workbench && (
            <>
              {workbench.savedQueries.length > 0 && (
                <div className={styles.section}>
                  <h3 className={styles.heading}>Saved Queries</h3>
                  <ul className={styles.list}>
                    {workbench.savedQueries.map((q, i) => (
                      <li key={i} className={styles.item}>
                        <button
                          className={styles.itemButton}
                          onClick={() => workbench.onQuerySelect(q.query)}
                          title={q.query}
                        >
                          {q.name}
                        </button>
                        <button
                          className={styles.removeButton}
                          onClick={() => workbench.onRemoveSaved(i)}
                        >
                          x
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {workbench.historyEntries.length > 0 && (
                <div className={styles.section}>
                  <h3 className={styles.heading}>History</h3>
                  <ul className={styles.list}>
                    {visibleHistory.map((entry, i) => (
                      <li key={i} className={styles.item}>
                        <button
                          className={styles.itemButton}
                          onClick={() => workbench.onQuerySelect(entry.query)}
                          title={entry.query}
                        >
                          <span className={styles.historyQuery}>
                            {entry.query.length > 40 ? entry.query.slice(0, 40) + "..." : entry.query}
                          </span>
                          <span className={styles.historyTime}>{formatTimeAgo(entry.timestamp)}</span>
                        </button>
                      </li>
                    ))}
                  </ul>
                  {workbench.historyEntries.length > MAX_VISIBLE_HISTORY && (
                    <button
                      className={styles.showMoreButton}
                      onClick={() => setShowAllHistory(!showAllHistory)}
                    >
                      {showAllHistory ? "Show less" : `Show all (${workbench.historyEntries.length})`}
                    </button>
                  )}
                </div>
              )}

              <div className={styles.section}>
                <h3 className={styles.heading}>Examples</h3>
                <ul className={styles.list}>
                  {EXAMPLES.map((q) => (
                    <li key={q.name} className={styles.item}>
                      <button
                        className={styles.itemButton}
                        onClick={() => workbench.onQuerySelect(q.query)}
                        title={q.query}
                      >
                        {q.name}
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            </>
          )}
        </>
      )}
    </aside>
  );
}
