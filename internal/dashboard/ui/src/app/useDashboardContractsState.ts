import type { HealthResponse } from "../types/core.ts";
import type { HoldingResponse } from "../types/portfolio.ts";
import { useHealthContracts } from "../features/health/useHealthContracts.ts";
import { useHoldingViewState } from "../features/holding/useHoldingViewState.ts";

type ContractsStateInput = {
  health: HealthResponse;
  holdingData: HoldingResponse;
};

export function useDashboardContractsState({ health, holdingData }: ContractsStateInput) {
  const healthContracts = useHealthContracts({ health });
  const holdingViewState = useHoldingViewState({
    holdingData,
    validationGateData: healthContracts.validationGateData,
    contractWorkflow: healthContracts.contractWorkflow,
  });

  return { healthContracts, holdingViewState };
}
