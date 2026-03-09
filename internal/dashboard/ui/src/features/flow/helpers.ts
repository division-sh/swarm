const DEFAULT_FLOW_STAGE_OPTIONS = ["all", "discovery", "scoring", "validation", "mailbox", "opco", "system"];
const DEFAULT_FLOW_RUBRIC_OPTIONS = ["all", "universal"];

function flowStageForEvent(eventType, eventStageMap) {
  const t = String(eventType || "").toLowerCase().trim();
  const contractStages = eventStageMap && typeof eventStageMap === "object" ? eventStageMap[t] || eventStageMap[eventType] : null;
  if (Array.isArray(contractStages) && contractStages.length > 0) return contractStages[0];
  if (!t) return "system";
  if (
    t.startsWith("scan.") ||
    t.startsWith("market_research.") ||
    t.startsWith("trend_research.") ||
    t.startsWith("scanner.") ||
    t.startsWith("category.") ||
    t.startsWith("trend.") ||
    t.startsWith("source.") ||
    t === "campaign.completed"
  ) return "discovery";
  if (
    t.startsWith("score.") ||
    t.startsWith("scoring.") ||
    t === "vertical.discovered" ||
    t === "vertical.scored" ||
    t === "vertical.shortlisted" ||
    t === "vertical.marginal" ||
    t === "vertical.rejected" ||
    t === "timer.marginal_review" ||
    t === "timer.marginal_kill" ||
    t === "timer.portfolio_digest"
  ) return "scoring";
  if (
    t.startsWith("validation.") ||
    t.startsWith("research.") ||
    t.startsWith("spec.") ||
    t.startsWith("cto.") ||
    t.startsWith("brand.") ||
    t === "vertical.ready_for_review" ||
    t === "vertical.resumed"
  ) return "validation";
  if (
    t === "vertical.approved" ||
    t === "vertical.killed" ||
    t === "vertical.needs_more_data" ||
    t.startsWith("human_task.") ||
    t === "mailbox.item_decided"
  ) return "mailbox";
  if (
    t.startsWith("opco.") ||
    t.startsWith("build.") ||
    t.startsWith("deploy.") ||
    t.startsWith("devops.") ||
    t.startsWith("qa.") ||
    t.startsWith("product.") ||
    t.startsWith("growth.") ||
    t.startsWith("support.") ||
    t.startsWith("launch.") ||
    t === "mandate_updated"
  ) return "opco";
  if (t === "timer.scan_timeout" || t === "timer.campaign_deadline") return "discovery";
  return "system";
}

function flowEventMatchesFilters(eventType, stageFilter, rubricFilter, eventStageMap) {
  const stage = flowStageForEvent(eventType, eventStageMap);
  if (stageFilter && stageFilter !== "all" && stage !== stageFilter) return false;
  if (rubricFilter && rubricFilter !== "all") {
    const t = String(eventType || "").toLowerCase().trim();
    const rubricAware =
      t.startsWith("score.") ||
      t.startsWith("scoring.") ||
      t === "vertical.discovered" ||
      t === "vertical.scored" ||
      t === "vertical.shortlisted" ||
      t === "vertical.marginal" ||
      t === "vertical.rejected";
    if (!rubricAware) return false;
  }
  return true;
}

export function getFlowStageOptions(flowGraphMeta) {
  const fromMeta = Array.isArray(flowGraphMeta && flowGraphMeta.stages) ? flowGraphMeta.stages : [];
  return Array.from(new Set(["all", ...DEFAULT_FLOW_STAGE_OPTIONS, ...fromMeta]));
}

export function getFlowRubricOptions(flowGraphMeta) {
  const fromMeta = Array.isArray(flowGraphMeta && flowGraphMeta.rubrics) ? flowGraphMeta.rubrics : [];
  return Array.from(new Set(["all", ...DEFAULT_FLOW_RUBRIC_OPTIONS, ...fromMeta]));
}

export function getFlowEventStageMap(flowGraphMeta) {
  if (!flowGraphMeta || typeof flowGraphMeta !== "object") return {};
  const raw = flowGraphMeta.event_stage_map;
  return raw && typeof raw === "object" ? raw : {};
}

export function getVisibleFlowEvents(flowEvents, flowView, flowReplayIndex, flowStage, flowRubric, flowEventStageMap) {
  const rows = (flowEvents || []).filter((ev) => flowEventMatchesFilters(ev && ev.event_type, flowStage, flowRubric, flowEventStageMap));
  if (flowView === "replay") {
    const n = Math.max(0, Math.min(rows.length, flowReplayIndex));
    return rows.slice(0, n);
  }
  return rows;
}

export function summarizeFlowEvents(visibleFlowEvents, flowEventStageMap) {
  const rows = visibleFlowEvents || [];
  const stageCounts = {};
  for (const ev of rows) {
    const stage = flowStageForEvent(ev && ev.event_type, flowEventStageMap);
    stageCounts[stage] = (stageCounts[stage] || 0) + 1;
  }
  return {
    total: rows.length,
    first: rows.length > 0 ? rows[rows.length - 1] : null,
    last: rows.length > 0 ? rows[0] : null,
    byStage: stageCounts,
    recent: rows.slice(0, 12),
  };
}

export function getFlowActiveEdgeKeys(visibleFlowEvents) {
  const rows = (visibleFlowEvents || []).slice(0, 150);
  const out = new Set();
  for (const ev of rows) {
    const source = ev && ev.source_node ? String(ev.source_node).trim() : "";
    const eventType = ev && ev.event_type ? String(ev.event_type).trim() : "";
    const targets = Array.isArray(ev && ev.target_nodes) ? ev.target_nodes : [];
    if (!source || !eventType) continue;
    for (const t of targets) {
      const target = String(t || "").trim();
      if (!target) continue;
      out.add(`${source}->${target}|${eventType}`);
    }
  }
  return out;
}
