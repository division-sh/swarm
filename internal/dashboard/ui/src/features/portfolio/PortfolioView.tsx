import React, { useEffect } from "react";
import { usePortfolioDerivedState } from "./usePortfolioDerivedState.ts";
import { usePortfolioFocusState } from "./usePortfolioFocusState.ts";
import { usePortfolioDownstreamState } from "./usePortfolioDownstreamState.ts";
import { normalizePortfolioSubview, usePortfolioPresets } from "./usePortfolioPresets.ts";
import { usePersistentState } from "../../hooks/usePersistentState.ts";
import PortfolioWorkbench from "./PortfolioWorkbench.tsx";

function routeToSubview(activeView) {
  if (activeView === "pipeline" || activeView === "holding" || activeView === "overview" || activeView === "triage") return activeView;
  return "";
}

export default function PortfolioView({
  activeView,
  activeSubview,
  setViewRoute,
  runtime,
  ops,
  pipeline,
  holding,
  flow,
  graph,
}) {
  const routeSubview = routeToSubview(activeView) || activeSubview;
  const [storedSubview, setStoredSubview] = usePersistentState("dashboard_portfolio_subview", routeSubview || "overview");
  const subview = normalizePortfolioSubview(storedSubview);
  const { portfolioFocusKey, setPortfolioFocusKey, focusSummary } = usePortfolioFocusState({
    holdingData: holding.state.holdingData,
    traceRows: pipeline.state.traceRows,
    traceVertical: pipeline.state.traceVertical,
  });
  const triage = usePortfolioDerivedState({
    holdingData: holding.state.holdingData,
    funnel: pipeline.state.funnel,
    shardScans: pipeline.state.shardScans,
  });
  const downstream = usePortfolioDownstreamState({
    verticals: holding.state.holdingData?.verticals,
    focusSummary,
    tasks: ops.tasks.state.tasksResp?.tasks,
    mailboxItems: ops.control.state.mailbox?.items,
    targets: ops.control.state.targets,
    agents: runtime.agents.state.agentsResp?.agents,
  });

  function updateSubview(next) {
    setStoredSubview(next);
    setViewRoute("portfolio", next);
  }

  const presets = usePortfolioPresets({
    triage,
    subview,
    setSubview: updateSubview,
    focusSummary,
    setPortfolioFocusKey,
    holdingState: holding.state,
    holdingActions: holding.actions,
    pipelineState: pipeline.state,
    pipelineActions: pipeline.actions,
  });

  useEffect(() => {
    if (!routeSubview) return;
    setStoredSubview(routeSubview);
    if (activeView === "pipeline" || activeView === "holding") {
      setViewRoute("portfolio", routeSubview);
    }
  }, [activeView, routeSubview, setStoredSubview, setViewRoute]);

  function selectSubview(next) {
    updateSubview(next);
  }

  function openFunnelTrace() {
    if (!portfolioFocusKey) return;
    pipeline.actions.setTraceVertical(portfolioFocusKey);
    pipeline.actions.traceVerticalFlow(portfolioFocusKey).catch(() => {});
    selectSubview("pipeline");
  }

  function openFunnelTraceForVertical(verticalKey) {
    const next = String(verticalKey || "").trim();
    if (!next) return;
    setPortfolioFocusKey(next);
    pipeline.actions.setTraceVertical(next);
    pipeline.actions.traceVerticalFlow(next).catch(() => {});
    selectSubview("pipeline");
  }

  function openHoldingDetailForVertical(vertical) {
    const next = vertical?.slug || vertical?.id || "";
    if (!next) return;
    setPortfolioFocusKey(next);
    if (vertical?.id) holding.actions.openHoldingVerticalDetail(vertical.id);
    selectSubview("holding");
  }

  function openWorkflowTraceForVertical(vertical) {
    const next = vertical?.slug || vertical?.id || "";
    if (!next) return;
    setPortfolioFocusKey(next);
    flow.actions.setFlowView("runtime");
    flow.actions.setFlowVertical(next);
    flow.actions.setSelectedFlowNodeID("");
    flow.actions.setSelectedFlowEdgeID("");
    setViewRoute("workflow", "flow");
  }

  function openWorkflowTrace() {
    if (!portfolioFocusKey) return;
    flow.actions.setFlowView("runtime");
    flow.actions.setFlowVertical(portfolioFocusKey);
    flow.actions.setSelectedFlowNodeID("");
    flow.actions.setSelectedFlowEdgeID("");
    setViewRoute("workflow", "flow");
  }

  function openWorkflowTopology() {
    if (!portfolioFocusKey) return;
    graph.actions.setGraphMode("opco");
    graph.actions.setGraphVertical(portfolioFocusKey);
    graph.actions.setSelectedGraphNodeID("");
    graph.actions.setSelectedGraphEdgeID("");
    setViewRoute("workflow", "graph");
  }

  function openOperationsForVertical(vertical) {
    const next = typeof vertical === "string" ? vertical : (vertical?.slug || vertical?.id || "");
    if (!next) return;
    setPortfolioFocusKey(next);
    const match = downstream.byKey[next];
    if (match?.primaryTask?.id) {
      ops.tasks.actions.setTaskStatus("all");
      ops.tasks.actions.setSelectedTaskID(match.primaryTask.id);
      setViewRoute("operations", "tasks");
      return;
    }
    if (match?.primaryMailbox?.id) {
      ops.control.actions.setMailStatus("all");
      ops.control.actions.setMailboxID(match.primaryMailbox.id);
      ops.control.actions.setSelectedMailboxItem(match.primaryMailbox.id);
      if (match.primaryTarget?.agent_id) {
        ops.control.actions.setControlTarget(match.primaryTarget.agent_id);
      }
      setViewRoute("operations", "control");
      return;
    }
    if (match?.primaryTarget?.agent_id) {
      ops.control.actions.setControlTarget(match.primaryTarget.agent_id);
    }
    setViewRoute("operations", "control");
  }

  function openTask(task) {
    if (!task?.id) return;
    if (task.vertical_slug || task.vertical_id) {
      setPortfolioFocusKey(task.vertical_slug || task.vertical_id);
    }
    ops.tasks.actions.setTaskStatus("all");
    ops.tasks.actions.setSelectedTaskID(task.id);
    setViewRoute("operations", "tasks");
  }

  function openAgent(agentID) {
    const next = String(agentID || "").trim();
    if (!next) return;
    runtime.agents.actions.setSelectedAgentID(next);
    setViewRoute("agents");
  }

  const holdingCount = Array.isArray(holding.state.holdingVisibleVerticals) ? holding.state.holdingVisibleVerticals.length : 0;
  const shardCount = Array.isArray(pipeline.state.shardScans) ? pipeline.state.shardScans.length : 0;
  const stuckCount = Array.isArray(pipeline.state.funnel?.stuck) ? pipeline.state.funnel.stuck.length : 0;

  return (
    <div>
      <div className="head">
        <h2>Portfolio</h2>
        <div className="tiny">Operator workspace for board execution, funnel pressure, and downstream handoffs.</div>
      </div>
      <PortfolioWorkbench
        subview={subview}
        setViewRoute={setViewRoute}
        metrics={{ holdingCount, shardCount, stuckCount }}
        presets={presets}
        focusSummary={focusSummary}
        downstream={downstream}
        triage={triage}
        portfolioFocusKey={portfolioFocusKey}
        setPortfolioFocusKey={setPortfolioFocusKey}
        holding={holding}
        pipeline={pipeline}
        actions={{
          selectSubview,
          onOpenHoldingDetail: () => focusSummary.vertical?.id && holding.actions.openHoldingVerticalDetail(focusSummary.vertical.id),
          onOpenWorkflowTrace: openWorkflowTrace,
          onOpenWorkflowTopology: openWorkflowTopology,
          onOpenFunnelTrace: openFunnelTrace,
          onClearFocus: () => setPortfolioFocusKey(""),
          onOpenOperations: () => openOperationsForVertical(focusSummary.vertical || portfolioFocusKey),
          onOpenTask: openTask,
          onOpenAgent: openAgent,
          onOpenHoldingDetailForVertical: openHoldingDetailForVertical,
          onOpenFunnelTraceForVertical: openFunnelTraceForVertical,
          onOpenWorkflowTraceForVertical: openWorkflowTraceForVertical,
          onOpenWorkflowTopologyForVertical: (vertical) => {
            const next = vertical?.slug || vertical?.id || "";
            if (!next) return;
            setPortfolioFocusKey(next);
            graph.actions.setGraphMode("opco");
            graph.actions.setGraphVertical(next);
            graph.actions.setSelectedGraphNodeID("");
            graph.actions.setSelectedGraphEdgeID("");
            setViewRoute("workflow", "graph");
          },
          onOpenOperationsForVertical: openOperationsForVertical,
        }}
      />
    </div>
  );
}
