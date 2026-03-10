import React from "react";
import LiveClock from "../components/LiveClock.tsx";

type DashboardHeaderProps = {
  initialLoading: boolean;
  statusText: string;
  apiKey: string;
  setApiKey: (value: string) => void;
  tabs: ReadonlyArray<readonly [string, string]>;
  tabBadges: Record<string, { n: number; type: string } | undefined>;
  activeView: string;
  setActiveView: (value: string) => void;
};

export default function DashboardHeader({
  initialLoading,
  statusText,
  apiKey,
  setApiKey,
  tabs,
  tabBadges,
  activeView,
  setActiveView,
}: DashboardHeaderProps) {
  return (
    <>
      {initialLoading ? (
        <div className="loading-overlay">
          <div className="loading-content">
            <div className="title">EMPIREAI</div>
            <div className="loading-spinner" />
            <div className="tiny">Connecting to command center…</div>
          </div>
        </div>
      ) : null}
      <header>
        <div className="header-top">
          <div className="header-brand">
            <h1 className="title">EMPIREAI</h1>
            <div className="sub">{statusText}</div>
            <LiveClock />
          </div>
          <details className="header-key">
            <summary className="tiny" style={{ cursor: "pointer", userSelect: "none" }}>Connection</summary>
            <input
              type="password"
              className="mono"
              placeholder="Enter key..."
              aria-label="Dashboard API key"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              style={{ marginTop: 6 }}
            />
          </details>
        </div>
        <div className="view-nav">
          {tabs.map(([id, label]) => {
            const badge = tabBadges[id];
            return (
              <button key={id} className={`view-btn ${activeView === id ? "active" : ""}`} onClick={() => setActiveView(id)}>
                {label}{badge ? <span className={`tab-badge tab-badge-${badge.type}`}>{badge.n}</span> : null}
              </button>
            );
          })}
        </div>
      </header>
    </>
  );
}
