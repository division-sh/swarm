import React from "react";
import Toasts from "../components/Toasts.tsx";
import DashboardHeader from "./DashboardHeader.tsx";
import DashboardModals from "./DashboardModals.tsx";
import DashboardViewRouter from "./DashboardViewRouter.tsx";
import { useDashboardCoordinator } from "./useDashboardCoordinator.ts";

export default function AppShell() {
  const { header, views, modals, toasts } = useDashboardCoordinator();

  return (
    <>
      <DashboardHeader {...header} />
      <DashboardViewRouter
        activeView={header.activeView}
        activeSubview={header.activeSubview}
        setActiveView={header.setActiveView}
        setViewRoute={header.setViewRoute}
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
