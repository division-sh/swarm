package manager

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/cel-go/cel"
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
	if err := prepareAgentContent(source, cfg); err != nil {
		return semanticview.FlowOwnedAgentSubscriptionAdmission{}, err
	}
	return admission, nil
}

func prepareAgentContent(source semanticview.Source, cfg *runtimeactors.AgentConfig) error {
	cfg.ContentPrograms = nil
	if cfg.Content == "" {
		return nil
	}
	if source == nil {
		return fmt.Errorf("agent content requires its admitted source")
	}
	declared := false
	for _, declaration := range semanticview.AgentDeclarations(source) {
		if declaration.OwnerFlowID != cfg.FlowID || declaration.Entry.ResolvedIntent != cfg.Intent {
			continue
		}
		if declaration.Entry.Content != cfg.Content {
			return fmt.Errorf("agent %s content differs from its captured declaration", cfg.ID)
		}
		declared = true
	}
	if !declared {
		return fmt.Errorf("agent %s content requires its captured declaration", cfg.ID)
	}
	scope, found := source.FlowScopeByID(cfg.FlowID)
	if !found {
		return fmt.Errorf("agent content requires its exact declaring flow")
	}
	keys := append([]string(nil), scope.InputEvents...)
	for key := range scope.Events {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	localEvents := localEventSet(keys)
	programs := make(map[string]cel.Program, len(keys))
	for _, key := range keys {
		if len(cfg.Subscriptions) > 0 && !agentContentSubscriptionMatches(source, cfg, key, localEvents) {
			continue
		}
		typ, found := source.ResolveFlowEventStructuralType(cfg.FlowID, key)
		options := workflowexpr.ValueExpressionOptions{AllowEventAt: true}
		if found {
			options.PayloadType = &typ
		}
		program, err := workflowexpr.PrepareAgentContentExpression(cfg.Content, options.PayloadType)
		if err != nil {
			return fmt.Errorf("agent %s content for %s: %w", cfg.ID, key, err)
		}
		local := eventidentity.LocalizeForFlow(cfg.CanonicalFlowPath(), keys, key)
		programs[local] = program
	}
	if len(programs) == 0 {
		return fmt.Errorf("agent %s content requires a declared input schema", cfg.ID)
	}
	cfg.ContentPrograms = programs
	return nil
}

func agentContentSubscriptionMatches(source semanticview.Source, cfg *runtimeactors.AgentConfig, event string, localEvents map[string]struct{}) bool {
	for _, subscription := range cfg.Subscriptions {
		admission := semanticview.ClassifyAuthoredSubscription(source, semanticview.AuthoredSubscriptionRequest{
			ConsumerKind: semanticview.AuthoredSubscriptionConsumerAgent, ConsumerID: cfg.ID,
			FlowID: cfg.FlowID, FlowPath: cfg.CanonicalFlowPath(), LocalEvents: localEvents, Authored: subscription,
		})
		if admission.Matches(event) {
			return true
		}
	}
	return false
}

func localEventSet(keys []string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		out[key] = struct{}{}
	}
	return out
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
