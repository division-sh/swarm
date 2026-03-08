import React from "react";
import OverviewView from "../features/overview/OverviewView.jsx";
import DashboardOpsViews from "./DashboardOpsViews.jsx";
import DashboardRuntimeViews from "./DashboardRuntimeViews.jsx";

export default function DashboardViewRouter({ activeView, setActiveView, overview, runtime, pipeline, ops }) {
  return (
    <main>
      {activeView === "overview" ? (
        <OverviewView state={overview.state} actions={overview.actions} />
      ) : null}
      <DashboardRuntimeViews activeView={activeView} setActiveView={setActiveView} runtime={runtime} pipeline={pipeline} />
      <DashboardOpsViews activeView={activeView} setActiveView={setActiveView} ops={ops} pipeline={pipeline} />
    </main>
  );
}
