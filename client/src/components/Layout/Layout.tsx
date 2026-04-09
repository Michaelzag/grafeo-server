import { Outlet, useLocation } from "react-router-dom";
import Sidebar from "../Sidebar/Sidebar";
import { useApp } from "../../context/AppContext";
import styles from "./Layout.module.css";

export default function Layout() {
  const { sidebarOpen, toggleSidebar, currentDatabase, selectDatabase } = useApp();
  const location = useLocation();
  const isAdmin = location.pathname.startsWith("/admin");

  return (
    <div className={styles.layout}>
      <Sidebar
        collapsed={!sidebarOpen}
        onToggle={toggleSidebar}
        currentDatabase={currentDatabase}
        onSelectDatabase={selectDatabase}
        isAdmin={isAdmin}
      />
      <div className={styles.main}>
        <Outlet />
      </div>
    </div>
  );
}
