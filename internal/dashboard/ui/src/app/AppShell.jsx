import React from "react";
import Toasts from "../components/Toasts.jsx";
import DashboardHeader from "./DashboardHeader.jsx";
import DashboardModals from "./DashboardModals.jsx";
import DashboardViewRouter from "./DashboardViewRouter.jsx";
import { useDashboardCoordinator } from "./useDashboardCoordinator.js";

export default function AppShell() {
  const { header, views, modals, toasts } = useDashboardCoordinator();

  return (
    <>
      <DashboardHeader {...header} />
      <DashboardViewRouter
        activeView={header.activeView}
        setActiveView={header.setActiveView}
        overview={views.overview}
        runtime={views.runtime}
        pipeline={views.pipeline}
        ops={views.ops}
      />
      <DashboardModals
        modalContent={modals.modalContent}
        setModalContent={modals.setModalContent}
        holdingDetailModal={modals.holdingDetailModal}
        setHoldingDetailModal={modals.setHoldingDetailModal}
      />
      <Toasts items={toasts} />
    </>
  );
}
