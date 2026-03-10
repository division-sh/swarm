import type { HealthResponse } from "../../types/core.ts";

type Route = {
  view: string;
  subview: string;
};

type Hotspot = {
  kind: string;
  title: string;
  detail: string;
  route: Route;
};

type ContractSummary = {
  workflowVersion: string;
  platformVersion: string;
  verificationCount: number;
  mustPass: number;
};

type HealthStateResult = {
  hotspots: Hotspot[];
  unhealthyVerticals: Record<string, unknown>[];
  warnings: string[];
  authErrors1h: number;
  authErrors24h: number;
  contractSummary: ContractSummary;
};

function readNumber(value: unknown): number {
  return Number(value || 0);
}

function readString(value: unknown): string {
  return typeof value === "string" && value.trim() ? value : "-";
}

export function deriveHealthState({
  health,
  contractWorkflow,
  contractPlatform,
  contractVerification,
}: {
  health: HealthResponse;
  contractWorkflow: Record<string, unknown>;
  contractPlatform: Record<string, unknown>;
  contractVerification: Record<string, unknown>;
}): HealthStateResult {
  const warnings = Array.isArray(health?.workflow_audit?.warnings) ? health.workflow_audit.warnings : [];
  const verticalHealth = Array.isArray(health?.vertical_health) ? health.vertical_health : [];
  const unhealthyVerticals = verticalHealth.filter((item) => item.health_status !== "healthy" || item.deploy_status !== "live");
  const authErrors1h = readNumber(health?.auth?.auth_errors_1h);
  const authErrors24h = readNumber(health?.auth?.auth_errors_24h);
  const priorityCounts = contractVerification?.priority_counts;
  const mustPass = readNumber(
    priorityCounts && typeof priorityCounts === "object"
      ? (priorityCounts as Record<string, unknown>).must_pass
      : 0,
  );

  const hotspots = [
    ...(authErrors1h > 0 || authErrors24h > 0 ? [{
      kind: "auth",
      title: "Authentication errors detected",
      detail: `${authErrors1h} in 1h · ${authErrors24h} in 24h`,
      route: { view: "observability", subview: "logs" },
    }] : []),
    ...(warnings.length > 0 ? [{
      kind: "workflow",
      title: "Workflow audit warnings",
      detail: `${warnings.length} active warnings`,
      route: { view: "workflow", subview: "issues" },
    }] : []),
    ...(unhealthyVerticals.length > 0 ? [{
      kind: "verticals",
      title: "Vertical deploy health issues",
      detail: `${unhealthyVerticals.length} unhealthy or not live`,
      route: { view: "portfolio", subview: "holding" },
    }] : []),
    ...(readNumber(contractVerification?.count) > 0 ? [{
      kind: "contracts",
      title: "Contract verification diagnostics",
      detail: `${mustPass} must-pass gates · ${readNumber(contractPlatform?.compliance_rule_count)} rules`,
      route: { view: "workflow", subview: "artifacts" },
    }] : []),
  ];

  return {
    hotspots,
    unhealthyVerticals,
    warnings,
    authErrors1h,
    authErrors24h,
    contractSummary: {
      workflowVersion: readString(contractWorkflow?.version),
      platformVersion: readString(contractPlatform?.version),
      verificationCount: readNumber(contractVerification?.count),
      mustPass,
    },
  };
}
