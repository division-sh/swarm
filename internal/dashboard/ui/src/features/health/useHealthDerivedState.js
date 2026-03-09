export function deriveHealthState({
  health,
  contractWorkflow,
  contractPlatform,
  contractVerification,
}) {
  const warnings = Array.isArray(health?.workflow_audit?.warnings) ? health.workflow_audit.warnings : [];
  const verticalHealth = Array.isArray(health?.vertical_health) ? health.vertical_health : [];
  const unhealthyVerticals = verticalHealth.filter((item) => item.health_status !== "healthy" || item.deploy_status !== "live");
  const authErrors1h = Number(health?.auth?.auth_errors_1h || 0);
  const authErrors24h = Number(health?.auth?.auth_errors_24h || 0);
  const mustPass = Number(contractVerification?.priority_counts?.must_pass || 0);

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
    ...(Number(contractVerification?.count || 0) > 0 ? [{
      kind: "contracts",
      title: "Contract verification diagnostics",
      detail: `${mustPass} must-pass gates · ${contractPlatform?.compliance_rule_count || 0} rules`,
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
      workflowVersion: contractWorkflow?.version || "-",
      platformVersion: contractPlatform?.version || "-",
      verificationCount: Number(contractVerification?.count || 0),
      mustPass,
    },
  };
}
