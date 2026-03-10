import { useMemo } from "react";
import type { HoldingResponse, VerticalRecord } from "../../types/portfolio.ts";

type HoldingViewDomain = {
  holdingData: HoldingResponse;
  holdingVisibleVerticals: VerticalRecord[];
  holdingWorkflowSummary: {
    drift: number;
    timers: number;
    revisions: number;
  };
  holdingColumns: Array<{
    key: string;
    label: string;
    stages: string[];
    items: VerticalRecord[];
  }>;
  validationGateData: {
    stages: string[];
  };
};

type HoldingViewControls = {
  holdingSearch: string;
  setHoldingSearch: (value: string) => void;
  holdingWorkflowFilter: string;
  setHoldingWorkflowFilter: (value: string) => void;
  holdingSort: string;
  setHoldingSort: (value: string) => void;
};

type HoldingControllerInput = {
  domain: HoldingViewDomain;
  controls: HoldingViewControls;
  openHoldingVerticalDetail: (verticalID: string) => Promise<void> | void;
};

export function useHoldingController({
  domain,
  controls,
  openHoldingVerticalDetail,
}: HoldingControllerInput) {
  return useMemo(() => ({
    state: { ...domain, ...controls },
    actions: { ...controls, openHoldingVerticalDetail },
  }), [controls, domain, openHoldingVerticalDetail]);
}
