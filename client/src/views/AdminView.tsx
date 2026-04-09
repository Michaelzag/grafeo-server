import { useState, useEffect } from "react";
import { api } from "../api/client";
import ServerOverview from "../components/Admin/ServerOverview";
import DatabaseCards from "../components/Admin/DatabaseCards";
import BackupPanel from "../components/Admin/BackupPanel";
import TokenPanel from "../components/Admin/TokenPanel";
import styles from "./AdminView.module.css";

export default function AdminView() {
  const [selectedDb, setSelectedDb] = useState<string | null>(null);
  const [hasAuth, setHasAuth] = useState(false);

  useEffect(() => {
    api.health().then((h) => {
      const features = h as unknown as Record<string, unknown>;
      const serverFeatures = features.enabled_server_features;
      if (Array.isArray(serverFeatures) && serverFeatures.includes("auth")) {
        setHasAuth(true);
      }
    }).catch(() => {});
  }, []);

  return (
    <div className={styles.container}>
      <ServerOverview />
      <DatabaseCards selectedDb={selectedDb} onSelectDb={setSelectedDb} />
      {selectedDb && (
        <BackupPanel database={selectedDb} onClose={() => setSelectedDb(null)} />
      )}
      {hasAuth && <TokenPanel />}
    </div>
  );
}
