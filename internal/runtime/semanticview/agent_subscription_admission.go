package semanticview

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

// FlowOwnedAgentSubscriptionRequest is the complete semantic context needed to
// admit one flow-owned agent's authored or recovered subscriptions.
type FlowOwnedAgentSubscriptionRequest struct {
	AgentID       string
	FlowID        string
	FlowPath      string
	LocalEvents   map[string]struct{}
	Subscriptions []string
}

// FlowOwnedAgentSubscriptionAdmission is the only capability that may install
// a flow-owned agent route. Its fields are private so callers cannot turn raw
// strings into route authority without running the canonical admission owner.
type FlowOwnedAgentSubscriptionAdmission struct {
	agentID                string
	flowPath               string
	persistedSubscriptions []string
	routePatterns          []string
}

func (a FlowOwnedAgentSubscriptionAdmission) ValidForAgent(agentID string) bool {
	return a.agentID != "" && a.agentID == strings.TrimSpace(agentID)
}

func (a FlowOwnedAgentSubscriptionAdmission) AgentID() string {
	return a.agentID
}

func (a FlowOwnedAgentSubscriptionAdmission) PersistedSubscriptions() []string {
	return append([]string(nil), a.persistedSubscriptions...)
}

func (a FlowOwnedAgentSubscriptionAdmission) RoutePatterns() []string {
	return append([]string(nil), a.routePatterns...)
}

func (a FlowOwnedAgentSubscriptionAdmission) FlowPath() string {
	return a.flowPath
}

// CarrierOnly preserves the admitted agent identity while installing no
// subscription route. Typed connect/direct delivery may still target its live
// channel.
func (a FlowOwnedAgentSubscriptionAdmission) CarrierOnly() FlowOwnedAgentSubscriptionAdmission {
	a.routePatterns = nil
	return a
}

// AdmitFlowOwnedAgentSubscriptions derives route identities from the owning
// flow and its filesystem descendants.
func AdmitFlowOwnedAgentSubscriptions(source Source, req FlowOwnedAgentSubscriptionRequest) (FlowOwnedAgentSubscriptionAdmission, error) {
	req.AgentID = strings.TrimSpace(req.AgentID)
	req.FlowID = strings.TrimSpace(req.FlowID)
	req.FlowPath = eventidentity.Normalize(req.FlowPath)
	if req.AgentID == "" {
		return FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf("agent subscription admission requires agent id")
	}

	localEvents := cloneSubscriptionEventSet(req.LocalEvents)
	if source != nil && req.FlowID != "" {
		if scope, ok := source.FlowScopeByID(req.FlowID); ok {
			if req.FlowPath == "" && req.FlowID != "." {
				req.FlowPath = eventidentity.Normalize(scope.Path)
				if req.FlowPath == "" {
					req.FlowPath = eventidentity.Normalize(source.FlowPath(req.FlowID))
				}
			}
			if len(localEvents) == 0 {
				localEvents = agentSubscriptionLocalEvents(scope)
			}
		}
	}

	persisted := make([]string, 0, len(req.Subscriptions))
	routes := make([]string, 0, len(req.Subscriptions))
	for _, authored := range req.Subscriptions {
		admission := ClassifyAuthoredSubscription(source, AuthoredSubscriptionRequest{
			ConsumerKind: AuthoredSubscriptionConsumerAgent,
			ConsumerID:   req.AgentID,
			FlowID:       req.FlowID,
			FlowPath:     req.FlowPath,
			LocalEvents:  localEvents,
			Authored:     authored,
		})
		if admission.Authored() == "" {
			continue
		}
		if !admission.Admitted() {
			return FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf("%s; declare a receiver-local event or use schema.yaml connect for cross-flow delivery", admission.Message())
		}
		persisted = append(persisted, admission.PersistedValue())
		routes = append(routes, admission.RoutePatterns()...)
	}

	return FlowOwnedAgentSubscriptionAdmission{
		agentID:                req.AgentID,
		flowPath:               req.FlowPath,
		persistedSubscriptions: normalizedAgentSubscriptionValues(persisted),
		routePatterns:          normalizedAgentSubscriptionValues(routes),
	}, nil
}

func cloneSubscriptionEventSet(in map[string]struct{}) map[string]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}

func agentSubscriptionLocalEvents(scope FlowScope) map[string]struct{} {
	out := make(map[string]struct{}, len(scope.Events)+len(scope.InputEvents)+len(scope.OutputEvents)+1)
	for eventType := range scope.Events {
		if eventType = eventidentity.Normalize(eventType); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, values := range [][]string{scope.InputEvents, scope.OutputEvents} {
		for _, eventType := range values {
			if eventType = eventidentity.Normalize(eventType); eventType != "" {
				out[eventType] = struct{}{}
			}
		}
	}
	if eventType := eventidentity.Normalize(scope.AutoEmitEvent); eventType != "" {
		out[eventType] = struct{}{}
	}
	return out
}

func admitSameScopeAgentExact(flowPath, raw string) (string, error) {
	flowPath = eventidentity.Normalize(flowPath)
	raw = eventidentity.Normalize(raw)
	if raw == "" {
		return "", nil
	}
	if !strings.Contains(raw, "/") {
		if flowPath == "" {
			return raw, nil
		}
		return flowPath + "/" + raw, nil
	}
	if flowPath == "" || !strings.HasPrefix(raw, flowPath+"/") {
		return "", fmt.Errorf("qualified exact subscriptions cannot cross a flow boundary; declare connect in the nearest common ancestor schema.yaml")
	}
	local := strings.TrimPrefix(raw, flowPath+"/")
	if local == "" || strings.Contains(local, "/") {
		return "", fmt.Errorf("qualified exact subscriptions cannot address a descendant flow; declare connect in the nearest common ancestor schema.yaml")
	}
	return raw, nil
}

func admitNonImportAgentPattern(flowPath, raw string) (string, error) {
	flowPath = eventidentity.Normalize(flowPath)
	raw = eventidentity.Normalize(raw)
	if raw == "" {
		return "", nil
	}
	if strings.Contains(raw, "/") {
		return "", fmt.Errorf("wildcard subscriptions must use a flow-local event pattern; declare output/input pins and connect in the nearest common ancestor schema.yaml for cross-flow delivery")
	}
	if flowPath == "" {
		return raw, nil
	}
	return flowPath + "/" + raw, nil
}

func normalizedAgentSubscriptionValues(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value = eventidentity.Normalize(value); value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
