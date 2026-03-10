import React from "react";
import OverviewView from "../features/overview/OverviewView.tsx";
import DashboardOpsViews from "./DashboardOpsViews.tsx";
import DashboardRuntimeViews from "./DashboardRuntimeViews.tsx";

type OverviewControllerShape = Parameters<typeof OverviewView>[0];

type DashboardViewRouterProps = {
  activeView: string;
  activeSubview: string;
  setActiveView: (value: string) => void;
  setViewRoute: (view: string, subview?: string) => void;
  overview: OverviewControllerShape["state"] extends never ? never : {
    state: OverviewControllerShape["state"];
    actions: OverviewControllerShape["actions"];
  };
  runtime: Parameters<typeof DashboardRuntimeViews>[0]["runtime"];
  pipeline: Parameters<typeof DashboardRuntimeViews>[0]["pipeline"] & Parameters<typeof DashboardOpsViews>[0]["pipeline"];
  ops: Parameters<typeof DashboardOpsViews>[0]["ops"];
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
