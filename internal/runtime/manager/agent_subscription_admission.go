package manager

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func admitAgentConfigSubscriptions(source semanticview.Source, cfg *runtimeactors.AgentConfig, localEvents map[string]struct{}) (semanticview.FlowOwnedAgentSubscriptionAdmission, error) {
	if cfg == nil {
		return semanticview.FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf("agent config is required")
	}
	cfg.NormalizeRuntimeDescriptor()
	admission, err := semanticview.AdmitFlowOwnedAgentSubscriptions(source, semanticview.FlowOwnedAgentSubscriptionRequest{
		AgentID:       cfg.ID,
		FlowID:        cfg.FlowID,
		FlowPath:      cfg.CanonicalFlowPath(),
		LocalEvents:   localEvents,
		Subscriptions: cfg.Subscriptions,
	})
	if err != nil {
		return semanticview.FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf("agent subscription admission failed: %w", err)
	}
	cfg.Subscriptions = admission.PersistedSubscriptions()
	return admission, nil
}

func admittedSubscriptionEventTypes(admission semanticview.FlowOwnedAgentSubscriptionAdmission) []events.EventType {
	patterns := admission.RoutePatterns()
	if len(patterns) == 0 {
		return nil
	}
	out := make([]events.EventType, 0, len(patterns))
	for _, pattern := range patterns {
		out = append(out, events.EventType(pattern))
	}
	return out
}
