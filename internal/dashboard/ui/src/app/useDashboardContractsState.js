import { useHealthContracts } from "../features/health/useHealthContracts.js";
import { useHoldingViewState } from "../features/holding/useHoldingViewState.js";

export function useDashboardContractsState({ health, holdingData }) {
  const healthContracts = useHealthContracts({ health });
  const holdingViewState = useHoldingViewState({
    holdingData,
    validationGateData: healthContracts.validationGateData,
    contractWorkflow: healthContracts.contractWorkflow,
  });

  return { healthContracts, holdingViewState };
}
