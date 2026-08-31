package runforkexecution

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type SelectedContractAgentDeliveryMaterializationRequest struct {
	RunID             string
	RecipientPlanning runfork.RunForkSelectedContractRecipientPlanning
	AgentRuntime      SelectedContractAgentRuntimeMaterialization
}

type SelectedContractAgentDeliveryMaterialization struct {
	Owner                    string                              `json:"owner"`
	RecipientPlanningOwner   string                              `json:"recipient_planning_owner"`
	ExecutionOwner           string                              `json:"execution_owner"`
	AgentRecipients          []agentidentity.Identity            `json:"agent_recipients,omitempty"`
	MaterializationRequired  bool                                `json:"materialization_required"`
	MaterializationSupported bool                                `json:"materialization_supported"`
	UnsupportedBlockers      []runfork.RunForkUnsupportedBlocker `json:"unsupported_blockers,omitempty"`
}

func RequireSelectedContractAgentDeliveryMaterialization(ctx context.Context, req SelectedContractAgentDeliveryMaterializationRequest) (SelectedContractAgentDeliveryMaterialization, error) {
	planning := req.RecipientPlanning
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return SelectedContractAgentDeliveryMaterialization{}, fmt.Errorf("selected-contract authoritative agent delivery materialization requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	agents, err := selectedContractPlannedAgentRecipients(req.RunID, planning)
	if err != nil {
		return SelectedContractAgentDeliveryMaterialization{}, err
	}
	result := SelectedContractAgentDeliveryMaterialization{
		Owner:                    runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
		RecipientPlanningOwner:   planning.Owner,
		ExecutionOwner:           runfork.RunForkSelectedContractExecutionOwner,
		AgentRecipients:          agents,
		MaterializationRequired:  len(agents) > 0,
		MaterializationSupported: len(agents) == 0,
	}
	if len(agents) == 0 {
		return result, nil
	}
	if selectedContractAgentRuntimeCoversRecipients(req.AgentRuntime, agents) {
		result.MaterializationSupported = true
		return result, nil
	}
	blocker := runfork.RunForkUnsupportedBlocker{
		Code:    runfork.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported,
		Message: fmt.Sprintf("%s requires selected-fork handler materialization for authoritative agent recipients before fork mutation; missing selected-fork handler materializer for %s", runfork.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner, strings.Join(agentIdentityDescriptions(agents), ",")),
	}
	result.UnsupportedBlockers = []runfork.RunForkUnsupportedBlocker{blocker}
	return result, fmt.Errorf("%s: %s", blocker.Code, blocker.Message)
}

func selectedContractAgentRuntimeCoversRecipients(runtime SelectedContractAgentRuntimeMaterialization, agents []agentidentity.Identity) bool {
	if strings.TrimSpace(runtime.Owner) != runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner ||
		!runtime.MaterializationSupported {
		return false
	}
	seen := map[agentidentity.Identity]struct{}{}
	for _, identity := range runtime.ConfiguredAgentIdentities {
		seen[identity.Normalize()] = struct{}{}
	}
	for _, identity := range runtime.AgentRecipients {
		seen[identity.Normalize()] = struct{}{}
	}
	for _, identity := range agents {
		if _, ok := seen[identity.Normalize()]; !ok {
			return false
		}
	}
	return true
}

func selectedContractPlannedAgentRecipients(runID string, planning runfork.RunForkSelectedContractRecipientPlanning) ([]agentidentity.Identity, error) {
	plans, err := selectedContractPlannedAgentRecipientPlans(planning)
	if err != nil {
		return nil, err
	}
	out := make([]agentidentity.Identity, 0, len(plans))
	for _, plan := range plans {
		identity, err := plan.Live(runID)
		if err != nil {
			return nil, fmt.Errorf("selected-contract agent recipient %q requires exact run-scoped identity: %w", plan.AgentID(), err)
		}
		out = append(out, identity)
	}
	sortAgentIdentities(out)
	return out, nil
}

func selectedContractPlannedAgentRecipientPlans(planning runfork.RunForkSelectedContractRecipientPlanning) ([]agentidentity.Plan, error) {
	seen := map[agentidentity.Plan]struct{}{}
	for _, event := range planning.RecipientPlanEvents {
		for _, recipient := range event.Recipients {
			if !recipient.Recipient.IsAgent() {
				continue
			}
			plan := recipient.AgentPlan.Normalize()
			if err := plan.Validate(); err != nil {
				return nil, fmt.Errorf("selected-contract agent recipient %q requires exact declaration plan: %w", recipient.Recipient.ID(), err)
			}
			if plan.AgentID() != recipient.Recipient.ID() {
				return nil, fmt.Errorf("selected-contract agent recipient %q conflicts with declaration plan %s", recipient.Recipient.ID(), plan.Description())
			}
			seen[plan] = struct{}{}
		}
	}
	out := make([]agentidentity.Plan, 0, len(seen))
	for plan := range seen {
		out = append(out, plan)
	}
	sort.Slice(out, func(i, j int) bool { return agentidentity.LessPlan(out[i], out[j]) })
	return out, nil
}
