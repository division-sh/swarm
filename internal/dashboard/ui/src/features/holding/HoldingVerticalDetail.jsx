import React from "react";
import { firstNonEmptyText, readPath } from "../../lib/format.ts";
import HoldingArtifactsPanel from "./HoldingArtifactsPanel.jsx";
import HoldingEventsMailboxPanel from "./HoldingEventsMailboxPanel.jsx";
import HoldingSummaryPanel from "./HoldingSummaryPanel.jsx";
import HoldingTeamPanel from "./HoldingTeamPanel.jsx";
import HoldingWorkflowPanel from "./HoldingWorkflowPanel.jsx";

export default function HoldingVerticalDetail({ detail }) {
  if (!detail || typeof detail !== "object") return <div className="empty-state">No detail available</div>;

  const vertical = detail.vertical || {};
  const workflowState = detail.workflow_state && typeof detail.workflow_state === "object" ? detail.workflow_state : null;
  const businessModel = firstNonEmptyText([
    readPath(vertical, ["business_brief", "business_model"]),
    readPath(vertical, ["business_brief", "revenue_model"]),
    readPath(vertical, ["mvp_spec", "business_model"]),
    readPath(vertical, ["validation_kit", "business_model"]),
    readPath(vertical, ["full_spec", "business_model"]),
    readPath(vertical, ["raw_signals", "business_model"]),
  ]);
  const opportunity = firstNonEmptyText([
    readPath(vertical, ["raw_signals", "opportunity_hypothesis"]),
    readPath(vertical, ["business_brief", "opportunity_hypothesis"]),
    readPath(vertical, ["mvp_spec", "opportunity"]),
    readPath(vertical, ["validation_kit", "opportunity_hypothesis"]),
  ]);

  const artifacts = [
    ["Raw Signals", vertical.raw_signals],
    ["Scores", vertical.scores],
    ["Business Brief", vertical.business_brief],
    ["MVP Spec", vertical.mvp_spec],
    ["Spec Review", vertical.spec_review],
    ["CTO Feasibility", vertical.cto_feasibility],
    ["Brand", vertical.brand],
    ["Validation Kit", vertical.validation_kit],
    ["Full Spec", vertical.full_spec],
    ["Deploy Config", vertical.deploy_config],
    ["Launch Targets", vertical.launch_targets],
  ];

  return (
    <div className="holding-detail">
      <HoldingSummaryPanel vertical={vertical} businessModel={businessModel} opportunity={opportunity} />
      <HoldingWorkflowPanel workflowState={workflowState} workflowStateError={detail.workflow_state_error} />
      <HoldingTeamPanel agents={detail.agents || []} />
      <HoldingArtifactsPanel artifacts={artifacts} />
      <HoldingEventsMailboxPanel events={detail.events || []} mailbox={detail.mailbox || []} spend={detail.spend} />
    </div>
  );
}
