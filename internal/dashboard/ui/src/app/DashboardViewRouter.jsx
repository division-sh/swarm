import React from "react";
import DashboardOpsViews from "./DashboardOpsViews.jsx";
import DashboardRuntimeViews from "./DashboardRuntimeViews.jsx";

export default function DashboardViewRouter({ activeView, runtime, pipeline, ops }) {
  return (
    <main>
      <DashboardRuntimeViews activeView={activeView} runtime={runtime} pipeline={pipeline} />
      <DashboardOpsViews activeView={activeView} ops={ops} pipeline={pipeline} />
    </main>
  );
}
