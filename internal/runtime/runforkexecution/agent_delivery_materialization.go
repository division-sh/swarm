package runforkexecution

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type SelectedContractAgentDeliveryMaterializationRequest struct {
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
	_ = ctx
	planning := req.RecipientPlanning
	if strings.TrimSpace(planning.Owner) != runfork.RunForkSelectedContractRecipientPlanningOwner {
		return SelectedContractAgentDeliveryMaterialization{}, fmt.Errorf("selected-contract authoritative agent delivery materialization requires %s; got %q", runfork.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	agents, err := selectedContractPlannedAgentRecipients(planning)
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

func selectedContractPlannedAgentRecipients(planning runfork.RunForkSelectedContractRecipientPlanning) ([]agentidentity.Identity, error) {
	seen := map[agentidentity.Identity]struct{}{}
	for _, event := range planning.RecipientPlanEvents {
		for _, recipient := range event.Recipients {
			if !recipient.Recipient.IsAgent() {
				continue
			}
			identity := recipient.AgentIdentity.Normalize()
			if err := identity.Validate(); err != nil {
				return nil, fmt.Errorf("selected-contract agent recipient %q requires exact concrete identity: %w", recipient.Recipient.ID(), err)
			}
			if identity.AgentID() != recipient.Recipient.ID() {
				return nil, fmt.Errorf("selected-contract agent recipient %q conflicts with concrete identity %s", recipient.Recipient.ID(), identity.Description())
			}
			seen[identity] = struct{}{}
		}
	}
	out := make([]agentidentity.Identity, 0, len(seen))
	for identity := range seen {
		out = append(out, identity)
	}
	sortAgentIdentities(out)
	return out, nil
}
