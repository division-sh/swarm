import { useMemo } from "react";
import type { HealthResponse } from "../../types/core.ts";
import { deriveHealthState } from "./useHealthDerivedState.ts";

type OpenView = (view: string, subview?: string) => void;

type HealthControllerInput = {
  health: HealthResponse;
  contractsData: Record<string, any>;
  contractWorkflow: Record<string, any>;
  contractPlatform: Record<string, any>;
  contractVerification: Record<string, any>;
  openView: OpenView;
};

export function useHealthController({
  health,
  contractsData,
  contractWorkflow,
  contractPlatform,
  contractVerification,
  openView,
}: HealthControllerInput) {
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
