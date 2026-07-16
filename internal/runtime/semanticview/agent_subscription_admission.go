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
	PackageKey    string
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

// AdmitFlowOwnedAgentSubscriptions derives same-scope route identities and
// consumes the imported-package wildcard/grant owner when a wildcard crosses a
// package boundary. Qualified exact subscriptions never create boundary edges.
func AdmitFlowOwnedAgentSubscriptions(source Source, req FlowOwnedAgentSubscriptionRequest) (FlowOwnedAgentSubscriptionAdmission, error) {
	req.AgentID = strings.TrimSpace(req.AgentID)
	req.FlowID = strings.TrimSpace(req.FlowID)
	req.FlowPath = eventidentity.Normalize(req.FlowPath)
	req.PackageKey = strings.TrimSpace(req.PackageKey)
	if req.AgentID == "" {
		return FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf("agent subscription admission requires agent id")
	}

	localEvents := cloneImportBoundaryEventSet(req.LocalEvents)
	if source != nil && req.FlowID != "" {
		if scope, ok := source.FlowScopeByID(req.FlowID); ok {
			if req.FlowPath == "" {
				req.FlowPath = eventidentity.Normalize(scope.Path)
				if req.FlowPath == "" {
					req.FlowPath = eventidentity.Normalize(source.FlowPath(req.FlowID))
				}
			}
			if req.PackageKey == "" {
				req.PackageKey = normalizeImportPackageKey(scope.PackageKey)
			}
			if len(localEvents) == 0 {
				localEvents = agentSubscriptionLocalEvents(scope)
			}
		}
	}

	if req.PackageKey != "" {
		req.PackageKey = normalizeImportPackageKey(req.PackageKey)
	}
	imported := source != nil && req.PackageKey != "" && importBoundaryPackageImported(source, req.PackageKey)
	persisted := make([]string, 0, len(req.Subscriptions))
	routes := make([]string, 0, len(req.Subscriptions))
	for _, authored := range req.Subscriptions {
		authored = eventidentity.Normalize(authored)
		if authored == "" {
			continue
		}
		if strings.Contains(authored, "*") {
			if imported {
				resolution := ResolveImportBoundaryWildcardSubscription(source, req.PackageKey, req.FlowID, req.FlowPath, localEvents, authored)
				if !resolution.Scoped || len(resolution.Patterns) == 0 {
					return FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf(
						"agent %q subscription %q has no imported-package subtree candidate or bind.observe grant; declare a narrow observe grant or use package.yaml connect",
						req.AgentID,
						authored,
					)
				}
				persisted = append(persisted, authored)
				for _, pattern := range resolution.Patterns {
					routes = append(routes, pattern.EventPattern)
				}
				continue
			}
			resolved, err := admitNonImportAgentPattern(req.FlowPath, authored)
			if err != nil {
				return FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf("agent %q subscription %q: %w", req.AgentID, authored, err)
			}
			persisted = append(persisted, resolved)
			routes = append(routes, resolved)
			continue
		}

		resolved, err := admitSameScopeAgentExact(req.FlowPath, authored)
		if err != nil {
			return FlowOwnedAgentSubscriptionAdmission{}, fmt.Errorf("agent %q subscription %q: %w", req.AgentID, authored, err)
		}
		persisted = append(persisted, resolved)
		routes = append(routes, resolved)
	}

	return FlowOwnedAgentSubscriptionAdmission{
		agentID:                req.AgentID,
		flowPath:               req.FlowPath,
		persistedSubscriptions: normalizedAgentSubscriptionValues(persisted),
		routePatterns:          normalizedAgentSubscriptionValues(routes),
	}, nil
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
		return "", fmt.Errorf("qualified exact subscriptions cannot cross a flow boundary; declare package.yaml connect")
	}
	local := strings.TrimPrefix(raw, flowPath+"/")
	if local == "" || strings.Contains(local, "/") {
		return "", fmt.Errorf("qualified exact subscriptions cannot address a descendant flow; declare package.yaml connect")
	}
	return raw, nil
}

func admitNonImportAgentPattern(flowPath, raw string) (string, error) {
	flowPath = eventidentity.Normalize(flowPath)
	raw = eventidentity.Normalize(raw)
	if raw == "" {
		return "", nil
	}
	if flowPath == "" {
		return raw, nil
	}
	if strings.HasPrefix(raw, flowPath+"/") {
		return raw, nil
	}
	if !strings.Contains(raw, "/") || strings.HasPrefix(raw, "*/") || strings.HasPrefix(raw, "**/") {
		return flowPath + "/" + raw, nil
	}
	return "", fmt.Errorf("qualified wildcard subscriptions cannot cross a flow boundary without typed wildcard/grant authority")
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
