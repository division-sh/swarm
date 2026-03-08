import React from "react";
import LiveClock from "../components/LiveClock.jsx";

export default function DashboardHeader({
  initialLoading,
  statusText,
  apiKey,
  setApiKey,
  overview,
  stuckAgents,
  tabs,
  tabBadges,
  activeView,
  setActiveView,
}) {
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
            <div className="title">EMPIREAI</div>
            <div className="sub">{statusText}</div>
            <LiveClock />
          </div>
          <div className="header-key">
            <span className="tiny">API Key</span>
            <input
              type="password"
              className="mono"
              placeholder="Enter key..."
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
            />
          </div>
        </div>
        <div className="overview">
          <div className="kpi"><div className="label">Agents Active</div><div className="value">{overview.agents_active || 0}</div></div>
          <div className="kpi"><div className="label">Events 24h</div><div className="value">{overview.events_24h || 0}</div></div>
          <div className="kpi"><div className="label">Mailbox Pending</div><div className="value">{overview.mailbox_pending || 0}</div></div>
          <div className="kpi"><div className="label">Verticals</div><div className="value">{overview.verticals_total || 0}</div></div>
          <div className={`kpi ${stuckAgents > 0 ? "kpi-alert" : ""}`}><div className="label">Stuck Agents</div><div className="value">{stuckAgents}</div></div>
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
