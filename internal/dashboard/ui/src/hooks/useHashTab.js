import { useEffect, useState } from "react";

function normalizeHashRoute(hash) {
  return (hash || "").replace("#", "").trim().toLowerCase();
}

function splitRoute(route) {
  const [tab = "", subview = ""] = String(route || "").split("/", 2);
  return { tab, subview };
}

function readHashRoute(validTabs, defaultTab) {
  const route = normalizeHashRoute(location.hash);
  const { tab, subview } = splitRoute(route);
  if (!validTabs.includes(tab)) return defaultTab;
  return subview ? `${tab}/${subview}` : tab;
}

export function useHashTab(validTabs, defaultTab) {
  const [route, setRoute] = useState(() => readHashRoute(validTabs, defaultTab));
  const { tab: activeTab, subview: activeSubview } = splitRoute(route);

  useEffect(() => {
    location.hash = route;
  }, [route]);

  useEffect(() => {
    const handler = () => setRoute(readHashRoute(validTabs, defaultTab));
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, [defaultTab, validTabs]);

  return {
    activeTab,
    activeSubview,
    route,
    setActiveTab: (nextTab) => {
      const next = String(nextTab || "").trim().toLowerCase();
      setRoute(validTabs.includes(next) ? next : defaultTab);
    },
    setRoute: (nextTab, nextSubview = "") => {
      const tab = String(nextTab || "").trim().toLowerCase();
      if (!validTabs.includes(tab)) {
        setRoute(defaultTab);
        return;
      }
      const subview = String(nextSubview || "").trim().toLowerCase();
      setRoute(subview ? `${tab}/${subview}` : tab);
    },
  };
}
