import { useMemo } from "react";
import { validationGateModel } from "../../components/GateIndicator.jsx";

export function useHealthContracts({ health }) {
  const contractsData = health && typeof health === "object" ? health.contracts || {} : {};
  const contractWorkflow = contractsData.workflow || {};
  const contractPlatform = contractsData.platform || {};
  const contractVerification = contractsData.verification_gates || {};
  const validationGateData = useMemo(() => validationGateModel(contractsData), [contractsData]);

  return {
    contractsData,
    contractWorkflow,
    contractPlatform,
    contractVerification,
    validationGateData,
  };
}
