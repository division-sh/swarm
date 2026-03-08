import React from "react";

export const DEFAULT_VALIDATION_GATES = ["researching", "mvp_speccing", "cto_spec_review", "branding"];

export const VALIDATION_GATE_LABELS = {
  researching: "G1 Research",
  mvp_speccing: "G2 Spec",
  cto_spec_review: "G3 CTO",
  branding: "G4 Brand",
};

export function validationGateModel(contractsData) {
  const workflow = contractsData && typeof contractsData === "object" ? contractsData.workflow || {} : {};
  const fromContracts = Array.isArray(workflow.validation_stages) ? workflow.validation_stages.filter(Boolean) : [];
  const stages = fromContracts.length > 0 ? fromContracts : DEFAULT_VALIDATION_GATES;
  const labels = stages.map((stage, idx) => {
    if (VALIDATION_GATE_LABELS[stage]) return VALIDATION_GATE_LABELS[stage];
    return `G${idx + 1} ${String(stage || "").replaceAll("_", " ")}`;
  });
  return { stages, labels };
}

function validationGateIndex(stage, stages) {
  const current = String(stage || "").trim();
  if (!current) return -1;
  if (current === "ready_for_review") return stages.length;
  return stages.indexOf(current);
}

export default function GateIndicator({ stage, stages, labels }) {
  const gateStages = Array.isArray(stages) && stages.length > 0 ? stages : DEFAULT_VALIDATION_GATES;
  const gateLabels = Array.isArray(labels) && labels.length === gateStages.length
    ? labels
    : gateStages.map((item, idx) => VALIDATION_GATE_LABELS[item] || `G${idx + 1}`);
  const idx = validationGateIndex(stage, gateStages);
  const allDone = idx >= gateLabels.length;
  return (
    <div className="gate-row">
      {gateLabels.map((label, i) => {
        const cls = allDone || i < idx ? "gate gate-done" : i === idx ? "gate gate-active" : "gate gate-pending";
        return <span key={i} className={cls}><span className="gate-dot" /><span className="gate-label">{label}</span></span>;
      })}
    </div>
  );
}
