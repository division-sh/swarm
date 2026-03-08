import { useMemo } from "react";

export function useHealthController({
  health,
  contractsData,
  contractWorkflow,
  contractPlatform,
  contractVerification,
}) {
  return useMemo(() => ({
    state: {
      health,
      contractsData,
      contractWorkflow,
      contractPlatform,
      contractVerification,
    },
    actions: {},
  }), [contractPlatform, contractVerification, contractWorkflow, contractsData, health]);
}
