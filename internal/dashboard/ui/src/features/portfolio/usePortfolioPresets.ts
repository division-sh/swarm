import { useMemo } from "react";
import { usePersistentState } from "../../hooks/usePersistentState.ts";
import type { ShardScanRecord, VerticalRecord } from "../../types/portfolio.ts";

const SLOT_COUNT = 3;

export function normalizePortfolioSubview(value: unknown) {
  return value === "pipeline" || value === "holding" || value === "overview" || value === "triage" ? value : "overview";
}

function normalizeKey(value: unknown) {
  return String(value || "").trim();
}

function safeParse<T>(raw: string, fallback: T) {
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export type PortfolioSubview = "overview" | "triage" | "holding" | "pipeline";

export type SavedPortfolioView = {
  label: string;
  subview: PortfolioSubview;
  focusKey: string;
  holdingFilter: string;
  holdingSort: string;
  traceVertical: string;
  selectedShardScanID: string;
};

export function normalizePortfolioViews(raw: unknown): Array<SavedPortfolioView | null> {
  const items = Array.isArray(raw) ? raw : [];
  return Array.from({ length: SLOT_COUNT }, (_, index) => {
    const item = items[index] && typeof items[index] === "object" ? items[index] : null;
    if (!item) return null;
    return {
      label: String(item.label || `View ${index + 1}`),
      subview: normalizePortfolioSubview(item.subview),
      focusKey: normalizeKey(item.focusKey),
      holdingFilter: String(item.holdingFilter || "all"),
      holdingSort: String(item.holdingSort || "updated_desc"),
      traceVertical: normalizeKey(item.traceVertical),
      selectedShardScanID: normalizeKey(item.selectedShardScanID),
    };
  });
}

type PortfolioPresetsInput = {
  triage: {
    summary?: {
      drift?: number;
      timers?: number;
      revisions?: number;
      stale?: number;
      humanNeeded?: number;
      retryScans?: number;
    };
    lists?: {
      driftedVerticals?: VerticalRecord[];
      timerHeavyVerticals?: VerticalRecord[];
      revisionedVerticals?: VerticalRecord[];
      staleVerticals?: VerticalRecord[];
      humanNeededVerticals?: VerticalRecord[];
      retryShardScans?: ShardScanRecord[];
    };
  };
  subview: string;
  setSubview: (value: PortfolioSubview) => void;
  focusSummary: {
    key?: string;
    vertical?: VerticalRecord | null;
  };
  setPortfolioFocusKey: (value: string) => void;
  holdingState: {
    holdingWorkflowFilter?: string;
    holdingSort?: string;
  };
  holdingActions: {
    setHoldingSearch?: (value: string) => void;
    setHoldingWorkflowFilter?: (value: string) => void;
    setHoldingSort?: (value: string) => void;
  };
  pipelineState: {
    traceVertical?: string;
    selectedShardScanID?: string;
  };
  pipelineActions: {
    setTraceVertical?: (value: string) => void;
    setSelectedShardScanID?: (value: string) => void;
    traceVerticalFlow?: (vertical?: string) => Promise<unknown>;
    loadShardScanDetail?: (scanID?: string) => Promise<unknown>;
  };
};

export function usePortfolioPresets({
  triage,
  subview,
  setSubview,
  focusSummary,
  setPortfolioFocusKey,
  holdingState,
  holdingActions,
  pipelineState,
  pipelineActions,
}: PortfolioPresetsInput) {
  const [savedViewsRaw, setSavedViewsRaw] = usePersistentState("dashboard_portfolio_saved_views", "[]");
  const savedViews = useMemo(() => normalizePortfolioViews(safeParse(savedViewsRaw, [])), [savedViewsRaw]);

  const presetCounts = useMemo(() => ({
    drift: triage.summary?.drift || 0,
    timers: triage.summary?.timers || 0,
    revisions: triage.summary?.revisions || 0,
    stale: triage.summary?.stale || 0,
    humanNeeded: triage.summary?.humanNeeded || 0,
    shardFailures: triage.summary?.retryScans || 0,
  }), [triage.summary]);

  function applyState(next: Partial<SavedPortfolioView> & { subview: PortfolioSubview }) {
    const targetSubview = normalizePortfolioSubview(next.subview);
    setSubview(targetSubview);

    holdingActions?.setHoldingSearch?.("");
    holdingActions?.setHoldingWorkflowFilter?.(next.holdingFilter || "all");
    holdingActions?.setHoldingSort?.(next.holdingSort || "updated_desc");

    const focusKey = normalizeKey(next.focusKey);
    setPortfolioFocusKey(focusKey);

    pipelineActions?.setTraceVertical?.(normalizeKey(next.traceVertical || focusKey));
    pipelineActions?.setSelectedShardScanID?.(normalizeKey(next.selectedShardScanID));

    if (targetSubview === "pipeline") {
      const traceTarget = normalizeKey(next.traceVertical || focusKey);
      if (traceTarget) {
        pipelineActions?.traceVerticalFlow?.(traceTarget).catch(() => {});
      }
      if (next.selectedShardScanID) {
        pipelineActions?.loadShardScanDetail?.(next.selectedShardScanID).catch(() => {});
      }
    }
  }

  function applyPreset(name: string) {
    switch (name) {
    case "drift": {
      const vertical = triage.lists?.driftedVerticals?.[0];
      applyState({ subview: "holding", focusKey: vertical?.slug || vertical?.id || "", holdingFilter: "drift", holdingSort: "updated_desc" });
      break;
    }
    case "timers": {
      const vertical = triage.lists?.timerHeavyVerticals?.[0];
      applyState({ subview: "holding", focusKey: vertical?.slug || vertical?.id || "", holdingFilter: "timers", holdingSort: "timers_desc" });
      break;
    }
    case "revisions": {
      const vertical = triage.lists?.revisionedVerticals?.[0];
      applyState({ subview: "holding", focusKey: vertical?.slug || vertical?.id || "", holdingFilter: "revisions", holdingSort: "revisions_desc" });
      break;
    }
    case "stale": {
      const vertical = triage.lists?.staleVerticals?.[0];
      applyState({ subview: "holding", focusKey: vertical?.slug || vertical?.id || "", holdingFilter: "stale", holdingSort: "stage_age_desc" });
      break;
    }
    case "humanNeeded": {
      const vertical = triage.lists?.humanNeededVerticals?.[0];
      applyState({ subview: "holding", focusKey: vertical?.slug || vertical?.id || "", holdingFilter: "all", holdingSort: "updated_desc" });
      break;
    }
    case "shardFailures": {
      const scan = triage.lists?.retryShardScans?.[0];
      applyState({
        subview: "pipeline",
        focusKey: "",
        holdingFilter: holdingState?.holdingWorkflowFilter || "all",
        holdingSort: holdingState?.holdingSort || "updated_desc",
        traceVertical: pipelineState?.traceVertical || "",
        selectedShardScanID: scan?.scan_id || "",
      });
      break;
    }
    default:
      break;
    }
  }

  function saveView(slot: number) {
    const index = Number(slot);
    if (!Number.isInteger(index) || index < 0 || index >= SLOT_COUNT) return;
    const next = [...savedViews];
    next[index] = {
      label: focusSummary?.vertical?.slug || focusSummary?.vertical?.name || focusSummary?.key || `View ${index + 1}`,
      subview: normalizePortfolioSubview(subview),
      focusKey: focusSummary?.key || "",
      holdingFilter: holdingState?.holdingWorkflowFilter || "all",
      holdingSort: holdingState?.holdingSort || "updated_desc",
      traceVertical: pipelineState?.traceVertical || "",
      selectedShardScanID: pipelineState?.selectedShardScanID || "",
    };
    setSavedViewsRaw(JSON.stringify(next));
  }

  function applySavedView(slot: number) {
    const index = Number(slot);
    if (!Number.isInteger(index) || index < 0 || index >= SLOT_COUNT) return;
    const view = savedViews[index];
    if (!view) return;
    applyState(view);
  }

  return {
    presetCounts,
    savedViews,
    applyPreset,
    saveView,
    applySavedView,
  };
}
