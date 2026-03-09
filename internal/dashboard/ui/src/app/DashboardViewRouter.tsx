import React from "react";
import OverviewView from "../features/overview/OverviewView.tsx";
import DashboardOpsViews from "./DashboardOpsViews.tsx";
import DashboardRuntimeViews from "./DashboardRuntimeViews.tsx";

type DashboardViewRouterProps = {
  activeView: string;
  activeSubview: string;
  setActiveView: (value: string) => void;
  setViewRoute: (view: string, subview?: string) => void;
  overview: Record<string, any>;
  runtime: Record<string, any>;
  pipeline: Record<string, any>;
  ops: Record<string, any>;
};

export default function DashboardViewRouter({
  activeView,
  activeSubview,
  setActiveView,
  setViewRoute,
  overview,
  runtime,
  pipeline,
  ops,
}: DashboardViewRouterProps) {
  return (
    <main>
      {activeView === "overview" ? (
        <OverviewView state={overview.state} actions={overview.actions} />
      ) : null}
      <DashboardRuntimeViews activeView={activeView} activeSubview={activeSubview} setActiveView={setActiveView} setViewRoute={setViewRoute} runtime={runtime} pipeline={pipeline} />
      <DashboardOpsViews activeView={activeView} activeSubview={activeSubview} setActiveView={setActiveView} setViewRoute={setViewRoute} runtime={runtime} ops={ops} pipeline={pipeline} />
    </main>
  );
}
