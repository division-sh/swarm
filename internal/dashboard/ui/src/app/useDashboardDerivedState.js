import { useEffect, useMemo } from "react";
import { validationGateModel } from "../components/GateIndicator.jsx";
import {
  getFlowActiveEdgeKeys,
  getFlowEventStageMap,
  getFlowRubricOptions,
  getFlowStageOptions,
  getVisibleFlowEvents,
  summarizeFlowEvents,
} from "../features/flow/helpers.js";
import { useHoldingViewState } from "../features/holding/useHoldingViewState.js";
import { buildTabBadges, DASHBOARD_TABS } from "./dashboardTabs.js";

function hasRuntimeError(item) {
  if (!item || typeof item !== "object") return false;
  if ((item.error_code || "").trim() !== "") return true;
  const level = (item.level || "").toLowerCase();
  if (level === "error") return true;
  return (item.error || "").trim() !== "";
}

function hasEventError(item) {
  if (!item || typeof item !== "object") return false;
  const errors = Number(item.error_count || 0);
  const dead = Number(item.dead_count || 0);
  return errors > 0 || dead > 0;
}

export function useDashboardDerivedState({
  agentSearch,
  agentsResp,
  selectedAgentID,
  setSelectedAgentID,
  events,
  eventsRuntimeErrorsOnly,
  runtimeLogs,
  logsData,
  logsRuntimeErrorsOnly,
  selectedLogID,
  setSelectedLogID,
  incidentsData,
  selectedIncidentCode,
  graph,
  graphViewGraph,
  selectedGraphNodeID,
  setSelectedGraphNodeID,
  selectedGraphEdgeID,
  setSelectedGraphEdgeID,
  flowGraphMeta,
  flowEvents,
  flowView,
  flowReplayIndex,
  flowStage,
  flowRubric,
  flowGraph,
  flowViewGraph,
  selectedFlowNodeID,
  setSelectedFlowNodeID,
  selectedFlowEdgeID,
  setSelectedFlowEdgeID,
  tasksResp,
  selectedTaskID,
  health,
  holdingData,
  mailbox,
  funnel,
}) {
  const groupedAgents = useMemo(() => {
    const query = (agentSearch || "").trim().toLowerCase();
    const all = (agentsResp.agents || []).filter((agent) => {
      if (!query) return true;
      return `${agent.id} ${agent.role || ""} ${agent.state || ""} ${agent.vertical_slug || ""}`.toLowerCase().includes(query);
    });
    const holding = [];
    const opco = new Map();
    for (const agent of all) {
      const isHolding = !(agent.vertical_slug || agent.vertical_id) || agent.mode !== "operating";
      if (isHolding) {
        holding.push(agent);
      } else {
        const key = agent.vertical_slug || agent.vertical_id || "unknown";
        if (!opco.has(key)) opco.set(key, []);
        opco.get(key).push(agent);
      }
    }
    const opcos = Array.from(opco.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([slug, grouped]) => ({ slug, agents: grouped }));
    return { holding, opcos };
  }, [agentsResp.agents, agentSearch]);

  const filteredEvents = useMemo(
    () => (eventsRuntimeErrorsOnly ? (events || []).filter(hasEventError) : (events || [])),
    [events, eventsRuntimeErrorsOnly],
  );

  const filteredRuntimeLogs = useMemo(
    () => (eventsRuntimeErrorsOnly ? (runtimeLogs || []).filter(hasRuntimeError) : (runtimeLogs || [])),
    [runtimeLogs, eventsRuntimeErrorsOnly],
  );

  const filteredLogsData = useMemo(
    () => (logsRuntimeErrorsOnly ? (logsData || []).filter(hasRuntimeError) : (logsData || [])),
    [logsData, logsRuntimeErrorsOnly],
  );

  const selectedAgent = useMemo(
    () => (agentsResp.agents || []).find((agent) => agent.id === selectedAgentID) || null,
    [agentsResp.agents, selectedAgentID],
  );

  useEffect(() => {
    if (!selectedAgentID) return;
    if (!selectedAgent) setSelectedAgentID("");
  }, [selectedAgentID, selectedAgent, setSelectedAgentID]);

  useEffect(() => {
    if (!selectedLogID) return;
    const exists = (filteredLogsData || []).some((log) => log.id === selectedLogID);
    if (!exists) setSelectedLogID(null);
  }, [selectedLogID, filteredLogsData, setSelectedLogID]);

  const selectedLog = useMemo(
    () => (filteredLogsData || []).find((log) => log.id === selectedLogID) || null,
    [filteredLogsData, selectedLogID],
  );

  const selectedIncident = useMemo(
    () => (incidentsData || []).find((item) => item.code === selectedIncidentCode) || null,
    [incidentsData, selectedIncidentCode],
  );

  const flowStageOptions = useMemo(() => getFlowStageOptions(flowGraphMeta), [flowGraphMeta]);
  const flowRubricOptions = useMemo(() => getFlowRubricOptions(flowGraphMeta), [flowGraphMeta]);
  const flowEventStageMap = useMemo(() => getFlowEventStageMap(flowGraphMeta), [flowGraphMeta]);
  const visibleFlowEvents = useMemo(
    () => getVisibleFlowEvents(flowEvents, flowView, flowReplayIndex, flowStage, flowRubric, flowEventStageMap),
    [flowEvents, flowView, flowReplayIndex, flowStage, flowRubric, flowEventStageMap],
  );
  const selectedFlowSummary = useMemo(
    () => summarizeFlowEvents(visibleFlowEvents, flowEventStageMap),
    [flowEventStageMap, visibleFlowEvents],
  );
  const flowActiveEdgeKeys = useMemo(() => getFlowActiveEdgeKeys(visibleFlowEvents), [visibleFlowEvents]);

  useEffect(() => {
    if (!selectedGraphNodeID) return;
    const currentGraph = graphViewGraph || graph;
    const exists = ((currentGraph && currentGraph.nodes) || []).some((node) => node.id === selectedGraphNodeID);
    if (!exists) setSelectedGraphNodeID("");
  }, [selectedGraphNodeID, graph, graphViewGraph, setSelectedGraphNodeID]);

  useEffect(() => {
    if (!selectedGraphEdgeID) return;
    const currentGraph = graphViewGraph || graph;
    const exists = ((currentGraph && currentGraph.edges) || []).some((edge, index) => `${edge.kind}:${edge.from}->${edge.to}:${index}` === selectedGraphEdgeID);
    if (!exists) setSelectedGraphEdgeID("");
  }, [selectedGraphEdgeID, graph, graphViewGraph, setSelectedGraphEdgeID]);

  useEffect(() => {
    if (!selectedFlowNodeID) return;
    const currentGraph = flowViewGraph || flowGraph;
    const exists = ((currentGraph && currentGraph.nodes) || []).some((node) => node.id === selectedFlowNodeID);
    if (!exists) setSelectedFlowNodeID("");
  }, [selectedFlowNodeID, flowGraph, flowViewGraph, setSelectedFlowNodeID]);

  useEffect(() => {
    if (!selectedFlowEdgeID) return;
    const currentGraph = flowViewGraph || flowGraph;
    const exists = ((currentGraph && currentGraph.edges) || []).some((edge, index) => `${edge.kind}:${edge.from}->${edge.to}:${index}` === selectedFlowEdgeID);
    if (!exists) setSelectedFlowEdgeID("");
  }, [selectedFlowEdgeID, flowGraph, flowViewGraph, setSelectedFlowEdgeID]);

  const selectedTask = useMemo(
    () => (tasksResp.tasks || []).find((task) => task.id === selectedTaskID) || null,
    [tasksResp.tasks, selectedTaskID],
  );

  const contractsData = health && typeof health === "object" ? health.contracts || {} : {};
  const contractWorkflow = contractsData.workflow || {};
  const contractPlatform = contractsData.platform || {};
  const contractVerification = contractsData.verification_gates || {};
  const validationGateData = useMemo(() => validationGateModel(contractsData), [contractsData]);
  const holdingViewState = useHoldingViewState({ holdingData, validationGateData, contractWorkflow });

  const tabBadges = useMemo(() => buildTabBadges({
    agentsResp,
    mailbox,
    funnel,
    holdingData,
    incidentsData,
    flowEvents,
  }), [agentsResp, mailbox, funnel, holdingData, incidentsData, flowEvents]);

  return {
    groupedAgents,
    filteredEvents,
    filteredRuntimeLogs,
    filteredLogsData,
    selectedLog,
    selectedIncident,
    flowStageOptions,
    flowRubricOptions,
    visibleFlowEvents,
    selectedFlowSummary,
    flowActiveEdgeKeys,
    selectedTask,
    contractsData,
    contractWorkflow,
    contractPlatform,
    contractVerification,
    validationGateData,
    holdingViewState,
    tabBadges,
    tabs: DASHBOARD_TABS,
  };
}
