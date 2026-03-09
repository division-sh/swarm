import { useHealthContracts } from "../features/health/useHealthContracts.ts";
import { useHoldingViewState } from "../features/holding/useHoldingViewState.ts";

type ContractsStateInput = {
  health: Record<string, any>;
  holdingData: Record<string, any>;
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
