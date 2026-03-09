import { useEffect, useMemo } from "react";
import { usePersistentState } from "../../hooks/usePersistentState.ts";
import type { HoldingResponse } from "../../types/portfolio.ts";

function normalizeKey(value: unknown) {
  return String(value || "").trim();
}

function matchesVertical(vertical: Record<string, any>, key: string) {
  if (!vertical || !key) return false;
  return normalizeKey(vertical.slug) === key || normalizeKey(vertical.id) === key;
}

type PortfolioFocusStateInput = {
  holdingData: HoldingResponse;
  traceRows: Record<string, any>[];
  traceVertical: string;
};

export function usePortfolioFocusState({ holdingData, traceRows, traceVertical }: PortfolioFocusStateInput) {
  const [portfolioFocusKey, setPortfolioFocusKey] = usePersistentState("dashboard_portfolio_focus", "");
  const fallbackKey = normalizeKey(traceVertical);
  const allVerticals = useMemo(
    () => (Array.isArray(holdingData?.verticals) ? holdingData.verticals : []),
    [holdingData],
  );

  const focusedVertical = useMemo(() => {
    const wanted = normalizeKey(portfolioFocusKey) || fallbackKey;
    if (!wanted) return null;
    return allVerticals.find((vertical) => matchesVertical(vertical, wanted)) || null;
  }, [allVerticals, fallbackKey, portfolioFocusKey]);

  const resolvedFocusKey = useMemo(
    () => normalizeKey(focusedVertical?.slug || focusedVertical?.id || portfolioFocusKey || fallbackKey),
    [fallbackKey, focusedVertical, portfolioFocusKey],
  );

  const focusedTraceRows = useMemo(() => {
    if (!resolvedFocusKey) return traceRows || [];
    return (traceRows || []).filter((row) => normalizeKey(row.vertical_id || row.vertical_slug || row.slug) === resolvedFocusKey);
  }, [resolvedFocusKey, traceRows]);

  const focusSummary = useMemo(() => ({
    key: resolvedFocusKey,
    vertical: focusedVertical,
    latestTraceRow: focusedTraceRows.length > 0 ? focusedTraceRows[focusedTraceRows.length - 1] : (traceRows || []).length > 0 ? traceRows[traceRows.length - 1] : null,
    traceCount: focusedTraceRows.length,
    drift: !!(focusedVertical?.workflow_current_stage && focusedVertical.workflow_current_stage !== focusedVertical.stage),
    activeTimers: Number(focusedVertical?.active_timer_count || 0),
    revisions: Number(focusedVertical?.revision_count || 0),
  }), [focusedTraceRows, focusedVertical, resolvedFocusKey, traceRows]);

  useEffect(() => {
    if (!portfolioFocusKey || focusedVertical) return;
    setPortfolioFocusKey(fallbackKey || "");
  }, [fallbackKey, focusedVertical, portfolioFocusKey, setPortfolioFocusKey]);

  return {
    portfolioFocusKey: resolvedFocusKey,
    setPortfolioFocusKey: (next) => setPortfolioFocusKey(normalizeKey(next)),
    focusSummary,
  };
}
