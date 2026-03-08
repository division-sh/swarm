import React from "react";
import Toasts from "../components/Toasts.jsx";
import DashboardHeader from "./DashboardHeader.jsx";
import DashboardModals from "./DashboardModals.jsx";
import DashboardViewRouter from "./DashboardViewRouter.jsx";
import { useDashboardCoordinator } from "./useDashboardCoordinator.js";

export default function AppShell() {
  const app = useDashboardCoordinator();

  return (
    <>
      <DashboardHeader
        initialLoading={app.initialLoading}
        statusText={app.statusText}
        apiKey={app.apiKey}
        setApiKey={app.setApiKey}
        overview={app.overview}
        stuckAgents={app.agentsResp.states.stuck || 0}
        tabs={app.tabs}
        tabBadges={app.tabBadges}
        activeView={app.activeView}
        setActiveView={app.setActiveView}
      />
      <DashboardViewRouter app={app} />
      <DashboardModals
        modalContent={app.modalContent}
        setModalContent={app.setModalContent}
        holdingDetailModal={app.holdingDetailModal}
        setHoldingDetailModal={app.setHoldingDetailModal}
      />
      <Toasts items={app.toasts} />
    </>
  );
}
