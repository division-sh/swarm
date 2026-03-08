import React from "react";
import DashboardOpsViews from "./DashboardOpsViews.jsx";
import DashboardRuntimeViews from "./DashboardRuntimeViews.jsx";

export default function DashboardViewRouter({ app }) {
  return (
    <main>
      <DashboardRuntimeViews app={app} />
      <DashboardOpsViews app={app} />
    </main>
  );
}
