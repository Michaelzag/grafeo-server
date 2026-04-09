import { useState } from "react";
import ServerOverview from "../components/Admin/ServerOverview";
import DatabaseCards from "../components/Admin/DatabaseCards";
import BackupPanel from "../components/Admin/BackupPanel";
import styles from "./AdminView.module.css";

export default function AdminView() {
  const [selectedDb, setSelectedDb] = useState<string | null>(null);

  return (
    <div className={styles.container}>
      <ServerOverview />
      <DatabaseCards selectedDb={selectedDb} onSelectDb={setSelectedDb} />
      {selectedDb && (
        <BackupPanel database={selectedDb} onClose={() => setSelectedDb(null)} />
      )}
    </div>
  );
}
