import React from "react";
import OverviewView from "../features/overview/OverviewView.jsx";
import DashboardOpsViews from "./DashboardOpsViews.jsx";
import DashboardRuntimeViews from "./DashboardRuntimeViews.jsx";

export default function DashboardViewRouter({ activeView, activeSubview, setActiveView, setViewRoute, overview, runtime, pipeline, ops }) {
  return (
    <main>
      {activeView === "overview" ? (
        <OverviewView state={overview.state} actions={overview.actions} />
      ) : null}
      <DashboardRuntimeViews activeView={activeView} activeSubview={activeSubview} setActiveView={setActiveView} setViewRoute={setViewRoute} runtime={runtime} pipeline={pipeline} />
      <DashboardOpsViews activeView={activeView} activeSubview={activeSubview} setActiveView={setActiveView} setViewRoute={setViewRoute} ops={ops} pipeline={pipeline} />
    </main>
  );
}
