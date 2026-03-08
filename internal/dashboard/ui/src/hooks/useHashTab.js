import { useEffect, useState } from "react";

function readHashTab(validTabs, defaultTab) {
  const h = (location.hash || "").replace("#", "").toLowerCase();
  return validTabs.includes(h) ? h : defaultTab;
}

export function useHashTab(validTabs, defaultTab) {
  const [activeTab, setActiveTab] = useState(() => readHashTab(validTabs, defaultTab));

  useEffect(() => {
    location.hash = activeTab;
  }, [activeTab]);

  useEffect(() => {
    const handler = () => setActiveTab(readHashTab(validTabs, defaultTab));
    window.addEventListener("hashchange", handler);
    return () => window.removeEventListener("hashchange", handler);
  }, [defaultTab, validTabs]);

  return [activeTab, setActiveTab];
}
