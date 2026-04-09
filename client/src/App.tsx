import { BrowserRouter, Routes, Route } from "react-router-dom";
import { AppProvider } from "./context/AppContext";
import Layout from "./components/Layout/Layout";
import WorkbenchView from "./views/WorkbenchView";
import AdminView from "./views/AdminView";

export default function App() {
  return (
    <BrowserRouter basename="/studio">
      <AppProvider>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<WorkbenchView />} />
            <Route path="/admin" element={<AdminView />} />
          </Route>
        </Routes>
      </AppProvider>
    </BrowserRouter>
  );
}
