import { useMemo, useState } from "react";

export function useHoldingViewState({ holdingData, validationGateData, contractWorkflow }) {
  const [holdingSearch, setHoldingSearch] = useState("");
  const [holdingWorkflowFilter, setHoldingWorkflowFilter] = useState("all");
  const [holdingSort, setHoldingSort] = useState("updated_desc");

  const holdingVisibleVerticals = useMemo(() => {
    const query = String(holdingSearch || "").trim().toLowerCase();
    let rows = [...(holdingData.verticals || [])];
    if (query) {
      rows = rows.filter((v) => `${v.slug || ""} ${v.name || ""} ${v.geography || ""} ${v.stage || ""} ${v.workflow_current_state || ""}`.toLowerCase().includes(query));
    }
    rows = rows.filter((v) => {
      switch (holdingWorkflowFilter) {
      case "drift":
        return !!(v.workflow_current_state && v.workflow_current_state !== v.stage);
      case "timers":
        return Number(v.active_timer_count || 0) > 0;
      case "revisions":
        return Number(v.revision_count || 0) > 0;
      case "stale":
        return !!v.stage_entered_at;
      default:
        return true;
      }
    });
    rows.sort((a, b) => {
      switch (holdingSort) {
      case "stage_age_desc":
        return new Date(a.stage_entered_at || 0).getTime() - new Date(b.stage_entered_at || 0).getTime();
      case "revisions_desc":
        return Number(b.revision_count || 0) - Number(a.revision_count || 0);
      case "timers_desc":
        return Number(b.active_timer_count || 0) - Number(a.active_timer_count || 0);
      case "score_desc":
        return Number(b.composite_score || 0) - Number(a.composite_score || 0);
      default:
        return new Date(b.updated_at || 0).getTime() - new Date(a.updated_at || 0).getTime();
      }
    });
    return rows;
  }, [holdingData.verticals, holdingSearch, holdingSort, holdingWorkflowFilter]);

  const holdingWorkflowSummary = useMemo(() => ({
    drift: holdingVisibleVerticals.filter((v) => v.workflow_current_state && v.workflow_current_state !== v.stage).length,
    timers: holdingVisibleVerticals.filter((v) => Number(v.active_timer_count || 0) > 0).length,
    revisions: holdingVisibleVerticals.filter((v) => Number(v.revision_count || 0) > 0).length,
  }), [holdingVisibleVerticals]);

  const approvedHoldingStages = useMemo(() => {
    const phaseMap = contractWorkflow && typeof contractWorkflow.stage_phase_map === "object" ? contractWorkflow.stage_phase_map : {};
    const stageIDs = Array.isArray(contractWorkflow.stage_ids) ? contractWorkflow.stage_ids : [];
    const derived = ["approved", ...stageIDs.filter((stage) => phaseMap[stage] === "operating")];
    return [...new Set(derived.filter(Boolean))];
  }, [contractWorkflow]);

  const holdingColumns = useMemo(() => {
    const cols = [
      { key: "discovery", label: "Discovery", stages: ["discovered"], items: [] },
      { key: "scoring", label: "Scoring", stages: ["scoring", "shortlisted", "marginal_review"], items: [] },
      { key: "validation", label: "Validation", stages: validationGateData.stages, items: [] },
      { key: "mailbox", label: "Mailbox", stages: ["ready_for_review"], items: [] },
      { key: "approved", label: "Approved", stages: approvedHoldingStages.length > 0 ? approvedHoldingStages : ["approved", "full_speccing", "building", "pre_launch", "launched", "operating", "expanding"], items: [] },
      { key: "killed", label: "Killed", stages: ["killed"], items: [] },
    ];
    const stageMap = {};
    for (const c of cols) {
      for (const s of c.stages) stageMap[s] = c;
    }
    for (const v of holdingVisibleVerticals) {
      const col = stageMap[v.stage];
      if (col) col.items.push(v);
    }
    return cols;
  }, [approvedHoldingStages, holdingVisibleVerticals, validationGateData.stages]);

  return {
    domain: {
      holdingData,
      holdingVisibleVerticals,
      holdingWorkflowSummary,
      holdingColumns,
      validationGateData,
    },
    controls: {
      holdingSearch,
      setHoldingSearch,
      holdingWorkflowFilter,
      setHoldingWorkflowFilter,
      holdingSort,
      setHoldingSort,
    },
  };
}
