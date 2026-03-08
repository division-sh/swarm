import { useCallback, useRef, useState } from "react";
import { getEmpireKey } from "../api/client.js";
import { useHashTab } from "../hooks/useHashTab.js";
import { usePersistentState } from "../hooks/usePersistentState.js";
import { VALID_TABS } from "./dashboardTabs.js";

export function useDashboardUIState() {
  const [activeView, setActiveView] = useHashTab(VALID_TABS, "agents");
  const [statusText, setStatusText] = useState("Loading...");
  const [apiKey, setApiKey] = usePersistentState("empire_api_key", getEmpireKey());
  const [initialLoading, setInitialLoading] = useState(true);
  const [agentSearch, setAgentSearch] = useState("");
  const [selectedMailboxItem, setSelectedMailboxItem] = useState("");
  const [modalContent, setModalContent] = useState(null);
  const [toasts, setToasts] = useState([]);
  const toastSeq = useRef(0);

  const addToast = useCallback((msg, type) => {
    const id = ++toastSeq.current;
    setToasts((prev) => [...prev, { id, msg, type: type || "info" }]);
    setTimeout(() => setToasts((prev) => prev.filter((toast) => toast.id !== id)), 4000);
  }, []);

  return {
    activeView,
    setActiveView,
    statusText,
    setStatusText,
    apiKey,
    setApiKey,
    initialLoading,
    setInitialLoading,
    agentSearch,
    setAgentSearch,
    selectedMailboxItem,
    setSelectedMailboxItem,
    modalContent,
    setModalContent,
    toasts,
    addToast,
  };
}
