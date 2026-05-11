package runforkexecution

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"swarm/internal/store"
)

type SelectedContractAgentDeliveryMaterializationRequest struct {
	RecipientPlanning store.RunForkSelectedContractRecipientPlanning
}

type SelectedContractAgentDeliveryMaterialization struct {
	Owner                    string                            `json:"owner"`
	RecipientPlanningOwner   string                            `json:"recipient_planning_owner"`
	ExecutionOwner           string                            `json:"execution_owner"`
	AgentRecipients          []string                          `json:"agent_recipients,omitempty"`
	MaterializationRequired  bool                              `json:"materialization_required"`
	MaterializationSupported bool                              `json:"materialization_supported"`
	UnsupportedBlockers      []store.RunForkUnsupportedBlocker `json:"unsupported_blockers,omitempty"`
}

func RequireSelectedContractAgentDeliveryMaterialization(ctx context.Context, req SelectedContractAgentDeliveryMaterializationRequest) (SelectedContractAgentDeliveryMaterialization, error) {
	_ = ctx
	planning := req.RecipientPlanning
	if strings.TrimSpace(planning.Owner) != store.RunForkSelectedContractRecipientPlanningOwner {
		return SelectedContractAgentDeliveryMaterialization{}, fmt.Errorf("selected-contract authoritative agent delivery materialization requires %s; got %q", store.RunForkSelectedContractRecipientPlanningOwner, planning.Owner)
	}
	agents := selectedContractPlannedAgentRecipients(planning)
	result := SelectedContractAgentDeliveryMaterialization{
		Owner:                    store.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner,
		RecipientPlanningOwner:   planning.Owner,
		ExecutionOwner:           store.RunForkSelectedContractExecutionOwner,
		AgentRecipients:          agents,
		MaterializationRequired:  len(agents) > 0,
		MaterializationSupported: len(agents) == 0,
	}
	if len(agents) == 0 {
		return result, nil
	}
	blocker := store.RunForkUnsupportedBlocker{
		Code:    store.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported,
		Message: fmt.Sprintf("%s requires selected-fork handler materialization for authoritative agent recipients before fork mutation; missing selected-fork handler materializer for %s", store.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner, strings.Join(agents, ",")),
	}
	result.UnsupportedBlockers = []store.RunForkUnsupportedBlocker{blocker}
	return result, fmt.Errorf("%s: %s", blocker.Code, blocker.Message)
}

func selectedContractPlannedAgentRecipients(planning store.RunForkSelectedContractRecipientPlanning) []string {
	seen := map[string]struct{}{}
	for _, event := range planning.RecipientPlanEvents {
		for _, recipient := range event.Recipients {
			if strings.TrimSpace(recipient.SubscriberType) != "agent" {
				continue
			}
			id := strings.TrimSpace(recipient.SubscriberID)
			if id == "" {
				continue
			}
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
