import { useMemo } from "react";
import { deriveHealthState } from "./useHealthDerivedState.js";

export function useHealthController({
  health,
  contractsData,
  contractWorkflow,
  contractPlatform,
  contractVerification,
  openView,
}) {
  const derived = deriveHealthState({
    health,
    contractWorkflow,
    contractPlatform,
    contractVerification,
  });

  return useMemo(() => ({
    state: {
      health,
      contractsData,
      contractWorkflow,
      contractPlatform,
      contractVerification,
      derived,
    },
    actions: {
      openView,
    },
  }), [contractPlatform, contractVerification, contractWorkflow, contractsData, derived, health, openView]);
}
