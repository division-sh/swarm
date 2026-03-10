import { DockviewReact, themeAbyssSpaced } from "dockview";
import React, { useCallback, useEffect, useMemo, useRef } from "react";
import useDockviewPanelVisibility from "../../hooks/useDockviewPanelVisibility.ts";
import HoldingView from "../holding/HoldingView.tsx";
import PipelineView from "../pipeline/PipelineView.tsx";
import PortfolioDownstreamCard from "./PortfolioDownstreamCard.tsx";
import PortfolioFocusCard from "./PortfolioFocusCard.tsx";
import PortfolioPresetBar from "./PortfolioPresetBar.tsx";
import PortfolioTriageSummary from "./PortfolioTriageSummary.tsx";

function PortfolioOverviewPanel(props) {
  const params = props.params || {};
  return (
    <div className="portfolio-dock-panel">
      <PortfolioPresetBar
        presetCounts={params.presets.presetCounts}
        savedViews={params.presets.savedViews}
        onApplyPreset={params.presets.applyPreset}
        onSaveView={params.presets.saveView}
        onApplySavedView={params.presets.applySavedView}
      />
      <PortfolioFocusCard
        focusSummary={params.focusSummary}
        subview={params.subview}
        onSelectSubview={params.selectSubview}
        onOpenHoldingDetail={params.onOpenHoldingDetail}
        onOpenWorkflowTrace={params.onOpenWorkflowTrace}
        onOpenWorkflowTopology={params.onOpenWorkflowTopology}
        onOpenFunnelTrace={params.onOpenFunnelTrace}
        onClearFocus={params.onClearFocus}
      />
      <PortfolioDownstreamCard
        focusSummary={params.focusSummary}
        downstream={params.downstream}
        onOpenOperations={params.onOpenOperations}
        onOpenTask={params.onOpenTask}
        onOpenAgent={params.onOpenAgent}
      />
    </div>
  );
}

function PortfolioTriagePanel(props) {
  const params = props.params || {};
  return (
    <div className="portfolio-dock-panel">
      <PortfolioTriageSummary
        triage={params.triage}
        onFocusVertical={params.onFocusVertical}
        onOpenHolding={params.onOpenHolding}
        onOpenPipeline={params.onOpenPipeline}
        onOpenWorkflowTrace={params.onOpenWorkflowTraceForVertical}
        onOpenOperations={params.onOpenOperationsForVertical}
      />
    </div>
  );
}

function PortfolioHoldingPanel(props) {
  const params = props.params || {};
  const { hasRendered } = useDockviewPanelVisibility(props.api);
  if (!hasRendered) return <div className="workflow-dock-placeholder tiny">Board panel loads on first open.</div>;
  return (
    <div className="portfolio-dock-panel">
      <HoldingView
        state={params.holding.state}
        actions={params.holding.actions}
        portfolioFocusKey={params.portfolioFocusKey}
        portfolioDownstreamByKey={params.downstream.byKey}
        onFocusVertical={params.onFocusVertical}
        onOpenFunnelTrace={params.onOpenFunnelTraceForVertical}
        onOpenWorkflowTrace={params.onOpenWorkflowTraceForVertical}
        onOpenWorkflowTopology={params.onOpenWorkflowTopologyForVertical}
        onOpenOperations={params.onOpenOperationsForVertical}
        onOpenAgent={params.onOpenAgent}
      />
    </div>
  );
}

function PortfolioPipelinePanel(props) {
  const params = props.params || {};
  const { hasRendered } = useDockviewPanelVisibility(props.api);
  if (!hasRendered) return <div className="workflow-dock-placeholder tiny">Funnel panel loads on first open.</div>;
  return (
    <div className="portfolio-dock-panel">
      <PipelineView
        state={params.pipeline.state}
        actions={params.pipeline.actions}
        portfolioFocusKey={params.portfolioFocusKey}
        portfolioDownstreamByKey={params.downstream.byKey}
        onFocusVertical={params.onFocusVertical}
        onOpenHolding={params.onOpenHoldingDetailForVertical}
        onOpenWorkflow={params.onOpenWorkflowTraceForVertical}
        onOpenOperations={params.onOpenOperationsForVertical}
      />
    </div>
  );
}

export default function PortfolioWorkbench({
  subview,
  setViewRoute,
  metrics,
  presets,
  focusSummary,
  downstream,
  triage,
  portfolioFocusKey,
  setPortfolioFocusKey,
  holding,
  pipeline,
  actions,
}) {
  const dockApiRef = useRef(null);
  const dockInitRef = useRef(false);
  const dockDisposerRef = useRef(null);

  const selectSubview = useCallback((next) => {
    setViewRoute("portfolio", next);
    dockApiRef.current?.getPanel(next)?.api?.setActive();
  }, [setViewRoute]);

  const dockComponents = useMemo(() => ({
    overview: PortfolioOverviewPanel,
    triage: PortfolioTriagePanel,
    holding: PortfolioHoldingPanel,
    pipeline: PortfolioPipelinePanel,
  }), []);

  const dockParams = useMemo(() => ({
    metrics,
    presets,
    focusSummary,
    downstream,
    triage,
    subview,
    portfolioFocusKey,
    onFocusVertical: setPortfolioFocusKey,
    holding,
    pipeline,
    selectSubview,
    ...actions,
  }), [
    actions,
    downstream,
    focusSummary,
    holding,
    metrics,
    pipeline,
    portfolioFocusKey,
    presets,
    selectSubview,
    setPortfolioFocusKey,
    subview,
    triage,
  ]);

  useEffect(() => {
    const api = dockApiRef.current;
    if (!api) return;
    ["overview", "triage", "holding", "pipeline"].forEach((panelID) => {
      api.getPanel(panelID)?.api?.updateParameters(dockParams);
    });
  }, [dockParams]);

  useEffect(() => {
    const api = dockApiRef.current;
    if (!api) return;
    const activeTarget = subview || "overview";
    api.getPanel(activeTarget)?.api?.setActive();
  }, [subview]);

  useEffect(() => () => {
    dockDisposerRef.current?.dispose?.();
  }, []);

  const handleReady = useCallback((event) => {
    const api = event.api;
    dockApiRef.current = api;
    if (!dockInitRef.current) {
      dockInitRef.current = true;
      const overviewPanel = api.addPanel({
        id: "overview",
        component: "overview",
        title: "Overview",
        params: dockParams,
      });
      const triagePanel = api.addPanel({
        id: "triage",
        component: "triage",
        title: "Triage",
        params: dockParams,
        position: {
          referencePanel: overviewPanel,
          direction: "below",
        },
      });
      api.addPanel({
        id: "holding",
        component: "holding",
        title: "Board",
        params: dockParams,
        position: {
          referencePanel: overviewPanel,
          direction: "right",
        },
      });
      api.addPanel({
        id: "pipeline",
        component: "pipeline",
        title: "Funnel",
        params: dockParams,
        position: {
          referencePanel: triagePanel,
          direction: "right",
        },
      });
    }
    dockDisposerRef.current?.dispose?.();
    dockDisposerRef.current = api.onDidActivePanelChange((panel) => {
      const next = panel?.id || "overview";
      setViewRoute("portfolio", next);
    });
    api.getPanel(subview || "overview")?.api?.setActive();
  }, [dockParams, setViewRoute, subview]);

  return (
    <section className="portfolio-workbench-shell">
      <div className="head">
        <h2>Workbench</h2>
        <div className="stack">
          <button className={subview === "overview" ? "active" : ""} onClick={() => selectSubview("overview")}>Overview</button>
          <button className={subview === "triage" ? "active" : ""} onClick={() => selectSubview("triage")}>Triage</button>
          <button className={subview === "holding" ? "active" : ""} onClick={() => selectSubview("holding")}>Board</button>
          <button className={subview === "pipeline" ? "active" : ""} onClick={() => selectSubview("pipeline")}>Funnel</button>
        </div>
      </div>
      <div className="tiny" style={{ marginBottom: 10 }}>
        Portfolio workspace with shared focus, downstream handoff, triage, board, and funnel surfaces. {metrics.holdingCount} visible verticals, {metrics.shardCount} shard scans, {metrics.stuckCount} stuck funnel items.
      </div>
      <div className="body portfolio-workbench-body">
        <DockviewReact
          className="portfolio-dockview"
          theme={themeAbyssSpaced}
          components={dockComponents}
          onReady={handleReady}
          disableFloatingGroups
          dndEdges={false}
          tabComponents={{}}
        />
      </div>
    </section>
  );
}
