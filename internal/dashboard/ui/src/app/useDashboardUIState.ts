import { useCallback, useMemo, useRef, useState } from "react";
import { getEmpireKey } from "../api/client.ts";
import { useHashTab } from "../hooks/useHashTab.ts";
import { usePersistentState } from "../hooks/usePersistentState.ts";
import { VALID_TABS } from "./dashboardTabs.ts";

type ToastItem = {
  id: number;
  msg: string;
  type: string;
};

export function useDashboardUIState() {
  const {
    activeTab: activeView,
    activeSubview,
    setActiveTab: setActiveView,
    setRoute: setViewRoute,
  } = useHashTab(VALID_TABS, "overview");
  const [statusText, setStatusText] = useState("Loading...");
  const [apiKey, setApiKey] = usePersistentState("empire_api_key", getEmpireKey());
  const [initialLoading, setInitialLoading] = useState(true);
  const [agentSearch, setAgentSearch] = useState("");
  const [selectedMailboxItem, setSelectedMailboxItem] = useState("");
  const [modalContent, setModalContent] = useState<Record<string, any> | null>(null);
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const toastSeq = useRef(0);

  const addToast = useCallback((msg: string, type?: string) => {
    const id = ++toastSeq.current;
    setToasts((prev) => [...prev, { id, msg, type: type || "info" }]);
    setTimeout(() => setToasts((prev) => prev.filter((toast) => toast.id !== id)), 4000);
  }, []);

  return useMemo(() => ({
    activeView,
    activeSubview,
    setActiveView,
    setViewRoute,
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
  }), [
    activeView,
    activeSubview,
    statusText,
    apiKey,
    initialLoading,
    agentSearch,
    selectedMailboxItem,
    modalContent,
    toasts,
    addToast,
    setActiveView,
    setViewRoute,
    setApiKey,
  ]);
}
