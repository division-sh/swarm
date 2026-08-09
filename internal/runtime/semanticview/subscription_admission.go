package semanticview

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

type AuthoredSubscriptionConsumerKind string

const (
	AuthoredSubscriptionConsumerNode  AuthoredSubscriptionConsumerKind = "node"
	AuthoredSubscriptionConsumerAgent AuthoredSubscriptionConsumerKind = "agent"
	AuthoredSubscriptionConsumerTimer AuthoredSubscriptionConsumerKind = "timer"
)

type AuthoredSubscriptionAdmissionClass string

const (
	AuthoredSubscriptionLocalExact          AuthoredSubscriptionAdmissionClass = "local_exact"
	AuthoredSubscriptionSameScopeAgentExact AuthoredSubscriptionAdmissionClass = "same_scope_agent_exact"
	AuthoredSubscriptionLocalPattern        AuthoredSubscriptionAdmissionClass = "local_pattern"
	AuthoredSubscriptionImportedPattern     AuthoredSubscriptionAdmissionClass = "imported_pattern"
)

type AuthoredSubscriptionFailure string

const (
	AuthoredSubscriptionFailureQualifiedExact        AuthoredSubscriptionFailure = "qualified_exact_forbidden"
	AuthoredSubscriptionFailurePatternUnauthorized   AuthoredSubscriptionFailure = "pattern_unauthorized"
	AuthoredSubscriptionFailureTimerPatternForbidden AuthoredSubscriptionFailure = "timer_pattern_forbidden"
)

// AuthoredSubscriptionRequest carries the complete scope needed to classify
// one authored subscription before it may become validation or route authority.
type AuthoredSubscriptionRequest struct {
	ConsumerKind AuthoredSubscriptionConsumerKind
	ConsumerID   string
	FlowID       string
	FlowPath     string
	PackageKey   string
	LocalEvents  map[string]struct{}
	InputEvents  []string
	Authored     string
}

// AuthoredSubscriptionAdmission is the closed result consumed by validation,
// typed relation, route materialization, and handler execution. Route patterns
// remain private so raw authored strings cannot bypass classification.
type AuthoredSubscriptionAdmission struct {
	consumerKind   AuthoredSubscriptionConsumerKind
	consumerID     string
	authored       string
	localEvent     string
	persistedValue string
	routePatterns  []string
	class          AuthoredSubscriptionAdmissionClass
	failure        AuthoredSubscriptionFailure
	message        string
}

func (a AuthoredSubscriptionAdmission) Admitted() bool {
	return a.failure == "" && a.class != ""
}

func (a AuthoredSubscriptionAdmission) Authored() string {
	return a.authored
}

func (a AuthoredSubscriptionAdmission) LocalEvent() string {
	return a.localEvent
}

func (a AuthoredSubscriptionAdmission) PersistedValue() string {
	return a.persistedValue
}

func (a AuthoredSubscriptionAdmission) RoutePatterns() []string {
	return append([]string(nil), a.routePatterns...)
}

func (a AuthoredSubscriptionAdmission) Class() AuthoredSubscriptionAdmissionClass {
	return a.class
}

func (a AuthoredSubscriptionAdmission) Failure() AuthoredSubscriptionFailure {
	return a.failure
}

func (a AuthoredSubscriptionAdmission) Message() string {
	return a.message
}

func (a AuthoredSubscriptionAdmission) Pattern() bool {
	return a.class == AuthoredSubscriptionLocalPattern || a.class == AuthoredSubscriptionImportedPattern
}

func (a AuthoredSubscriptionAdmission) Matches(eventType string) bool {
	if !a.Admitted() {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	candidates := a.routePatterns
	if !a.Pattern() {
		candidates = append([]string{a.authored, a.localEvent}, candidates...)
	}
	for _, candidate := range candidates {
		candidate = eventidentity.Normalize(candidate)
		if candidate == "" {
			continue
		}
		if a.Pattern() {
			if eventidentity.MatchPattern(candidate, eventType) {
				return true
			}
			continue
		}
		if candidate == eventType {
			return true
		}
	}
	return false
}

func (a AuthoredSubscriptionAdmission) MatchesReceiverInput(eventType, flowPath string, inputEvents []string) bool {
	if !a.Admitted() || a.class == AuthoredSubscriptionImportedPattern {
		return false
	}
	localized := eventidentity.LocalizeForFlow(flowPath, inputEvents, eventType)
	if localized == "" {
		return false
	}
	if a.Pattern() {
		return eventidentity.MatchPattern(a.authored, localized)
	}
	return localized == a.localEvent
}

func ClassifyAuthoredSubscription(source Source, req AuthoredSubscriptionRequest) AuthoredSubscriptionAdmission {
	req.ConsumerID = strings.TrimSpace(req.ConsumerID)
	req.FlowID = strings.TrimSpace(req.FlowID)
	req.FlowPath = eventidentity.Normalize(req.FlowPath)
	req.PackageKey = strings.TrimSpace(req.PackageKey)
	req.Authored = eventidentity.Normalize(req.Authored)
	result := AuthoredSubscriptionAdmission{
		consumerKind: req.ConsumerKind,
		consumerID:   req.ConsumerID,
		authored:     req.Authored,
	}
	if req.Authored == "" {
		return result
	}
	fillAuthoredSubscriptionScope(source, &req)

	if strings.Contains(req.Authored, "*") {
		if req.ConsumerKind == AuthoredSubscriptionConsumerTimer {
			return failedAuthoredSubscription(result, AuthoredSubscriptionFailureTimerPatternForbidden,
				fmt.Sprintf("timer %q event reference %q must be an exact local event name", req.ConsumerID, req.Authored))
		}
		if source != nil {
			resolution := ResolveImportBoundaryWildcardSubscription(source, req.PackageKey, req.FlowID, req.FlowPath, req.LocalEvents, req.Authored)
			if resolution.Scoped && len(resolution.Patterns) == 0 {
				return failedAuthoredSubscription(result, AuthoredSubscriptionFailurePatternUnauthorized,
					fmt.Sprintf("%s %q subscription %q has no imported-package subtree candidate or bind.observe grant", req.ConsumerKind, req.ConsumerID, req.Authored))
			}
			if resolution.Scoped {
				patterns := make([]string, 0, len(resolution.Patterns))
				for _, pattern := range resolution.Patterns {
					patterns = append(patterns, pattern.EventPattern)
				}
				result.class = AuthoredSubscriptionImportedPattern
				result.persistedValue = req.Authored
				result.routePatterns = normalizedSubscriptionValues(patterns)
				return result
			}
		}

		pattern := ""
		if req.ConsumerKind == AuthoredSubscriptionConsumerAgent {
			resolved, err := admitNonImportAgentPattern(req.FlowPath, req.Authored)
			if err != nil {
				return failedAuthoredSubscription(result, AuthoredSubscriptionFailurePatternUnauthorized,
					fmt.Sprintf("agent %q subscription %q: %v", req.ConsumerID, req.Authored, err))
			}
			pattern = resolved
		} else {
			scope := eventidentity.Scope{
				Path:        req.FlowPath,
				LocalEvents: sortedSubscriptionEventSet(req.LocalEvents),
				InputEvents: append([]string(nil), req.InputEvents...),
			}
			pattern = scope.ResolveSubscriptionPattern(req.Authored, authoredSubscriptionDescendants(source, req.FlowID))
		}
		if pattern == "" {
			return failedAuthoredSubscription(result, AuthoredSubscriptionFailurePatternUnauthorized,
				fmt.Sprintf("%s %q subscription %q cannot be resolved in its declaring scope", req.ConsumerKind, req.ConsumerID, req.Authored))
		}
		result.class = AuthoredSubscriptionLocalPattern
		result.persistedValue = pattern
		result.routePatterns = []string{pattern}
		return result
	}

	if req.ConsumerKind == AuthoredSubscriptionConsumerAgent {
		resolved, err := admitSameScopeAgentExact(req.FlowPath, req.Authored)
		if err != nil {
			return failedAuthoredSubscription(result, AuthoredSubscriptionFailureQualifiedExact,
				fmt.Sprintf("agent %q subscription %q: %v", req.ConsumerID, req.Authored, err))
		}
		result.class = AuthoredSubscriptionLocalExact
		if strings.Contains(req.Authored, "/") {
			result.class = AuthoredSubscriptionSameScopeAgentExact
		}
		result.localEvent = req.Authored
		if req.FlowPath != "" && strings.HasPrefix(resolved, req.FlowPath+"/") {
			result.localEvent = strings.TrimPrefix(resolved, req.FlowPath+"/")
		}
		result.persistedValue = resolved
		result.routePatterns = []string{resolved}
		return result
	}

	if strings.Contains(req.Authored, "/") {
		return failedAuthoredSubscription(result, AuthoredSubscriptionFailureQualifiedExact,
			fmt.Sprintf("%s %q exact subscription %q must use a local event name", req.ConsumerKind, req.ConsumerID, req.Authored))
	}
	resolved := req.Authored
	if req.FlowPath != "" {
		resolved = req.FlowPath + "/" + req.Authored
	}
	result.class = AuthoredSubscriptionLocalExact
	result.localEvent = req.Authored
	result.persistedValue = req.Authored
	result.routePatterns = []string{resolved}
	return result
}

func ClassifyNodeSubscription(source Source, nodeID, authored string) AuthoredSubscriptionAdmission {
	request := AuthoredSubscriptionRequest{
		ConsumerKind: AuthoredSubscriptionConsumerNode,
		ConsumerID:   strings.TrimSpace(nodeID),
		Authored:     authored,
	}
	if source != nil {
		if owner, ok := source.NodeContractSource(nodeID); ok {
			request.FlowID = owner.FlowID
			request.PackageKey = owner.PackageKey
		}
		request.FlowPath = source.FlowPath(request.FlowID)
		request.LocalEvents, request.InputEvents = authoredSubscriptionFlowEvents(source, request.FlowID)
	}
	return ClassifyAuthoredSubscription(source, request)
}

func ClassifyTimerSubscription(source Source, timerID, flowID, authored string) AuthoredSubscriptionAdmission {
	localEvents, inputEvents := authoredSubscriptionFlowEvents(source, flowID)
	return ClassifyAuthoredSubscription(source, AuthoredSubscriptionRequest{
		ConsumerKind: AuthoredSubscriptionConsumerTimer,
		ConsumerID:   strings.TrimSpace(timerID),
		FlowID:       strings.TrimSpace(flowID),
		FlowPath:     sourceFlowPath(source, flowID),
		LocalEvents:  localEvents,
		InputEvents:  inputEvents,
		Authored:     authored,
	})
}

type NodeSubscriptionHandlerResolution struct {
	Handler         runtimecontracts.SystemNodeEventHandler
	HandlerEventKey string
	Admission       AuthoredSubscriptionAdmission
	Matched         bool
}

// ResolveNodeSubscriptionHandler prevents raw handler maps from becoming a
// second exact-subscription interpreter at execution time.
func ResolveNodeSubscriptionHandler(source Source, nodeID, eventType string) NodeSubscriptionHandlerResolution {
	if source == nil {
		return NodeSubscriptionHandlerResolution{}
	}
	handlers := source.NodeEventHandlers(nodeID)
	keys := make([]string, 0, len(handlers))
	for key := range handlers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	owner, _ := source.NodeContractSource(nodeID)
	flowPath := source.FlowPath(owner.FlowID)
	inputEvents := source.FlowInputEvents(owner.FlowID)
	for _, key := range keys {
		admission := ClassifyNodeSubscription(source, nodeID, key)
		if !admission.Matches(eventType) && !admission.MatchesReceiverInput(eventType, flowPath, inputEvents) {
			continue
		}
		handler := handlers[key]
		if bundle, ok := Bundle(source); ok && bundle != nil {
			if externalized, matched := bundle.NodeEventHandler(nodeID, key); matched {
				handler = externalized
			}
		}
		return NodeSubscriptionHandlerResolution{
			Handler:         handler,
			HandlerEventKey: strings.TrimSpace(key),
			Admission:       admission,
			Matched:         true,
		}
	}
	return NodeSubscriptionHandlerResolution{}
}

func failedAuthoredSubscription(result AuthoredSubscriptionAdmission, failure AuthoredSubscriptionFailure, message string) AuthoredSubscriptionAdmission {
	result.failure = failure
	result.message = strings.TrimSpace(message)
	return result
}

func fillAuthoredSubscriptionScope(source Source, req *AuthoredSubscriptionRequest) {
	if req == nil || source == nil {
		return
	}
	if req.FlowPath == "" {
		req.FlowPath = eventidentity.Normalize(source.FlowPath(req.FlowID))
	}
	if scope, ok := source.FlowScopeByID(req.FlowID); ok {
		if req.PackageKey == "" {
			req.PackageKey = strings.TrimSpace(scope.PackageKey)
		}
		if len(req.LocalEvents) == 0 || len(req.InputEvents) == 0 {
			localEvents, inputEvents := authoredSubscriptionScopeEvents(scope)
			if len(req.LocalEvents) == 0 {
				req.LocalEvents = localEvents
			}
			if len(req.InputEvents) == 0 {
				req.InputEvents = inputEvents
			}
		}
	}
}

func authoredSubscriptionFlowEvents(source Source, flowID string) (map[string]struct{}, []string) {
	if source == nil {
		return nil, nil
	}
	if scope, ok := source.FlowScopeByID(strings.TrimSpace(flowID)); ok {
		return authoredSubscriptionScopeEvents(scope)
	}
	local := map[string]struct{}{}
	for eventType := range source.AuthoredEventEntries() {
		if eventType = eventidentity.Normalize(eventType); eventType != "" {
			local[eventType] = struct{}{}
		}
	}
	return local, nil
}

func authoredSubscriptionScopeEvents(scope FlowScope) (map[string]struct{}, []string) {
	local := make(map[string]struct{}, len(scope.Events)+len(scope.InputEvents)+len(scope.OutputEvents)+1)
	for eventType := range scope.Events {
		if eventType = eventidentity.Normalize(eventType); eventType != "" {
			local[eventType] = struct{}{}
		}
	}
	for _, events := range [][]string{scope.InputEvents, scope.OutputEvents} {
		for _, eventType := range events {
			if eventType = eventidentity.Normalize(eventType); eventType != "" {
				local[eventType] = struct{}{}
			}
		}
	}
	if eventType := eventidentity.Normalize(scope.AutoEmitEvent); eventType != "" {
		local[eventType] = struct{}{}
	}
	return local, append([]string(nil), scope.InputEvents...)
}

func authoredSubscriptionDescendants(source Source, flowID string) []eventidentity.DescendantScope {
	if source == nil || strings.TrimSpace(flowID) == "" {
		return nil
	}
	parentPath := eventidentity.Normalize(source.FlowPath(flowID))
	if parentPath == "" {
		return nil
	}
	out := make([]eventidentity.DescendantScope, 0)
	for _, scope := range source.FlowScopes() {
		path := eventidentity.Normalize(scope.Path)
		if path == "" || !strings.HasPrefix(path, parentPath+"/") {
			continue
		}
		local, _ := authoredSubscriptionScopeEvents(scope)
		out = append(out, eventidentity.DescendantScope{Path: path, LocalEvents: sortedSubscriptionEventSet(local)})
	}
	return out
}

func sourceFlowPath(source Source, flowID string) string {
	if source == nil {
		return ""
	}
	return source.FlowPath(strings.TrimSpace(flowID))
}

func sortedSubscriptionEventSet(events map[string]struct{}) []string {
	out := make([]string, 0, len(events))
	for eventType := range events {
		if eventType = eventidentity.Normalize(eventType); eventType != "" {
			out = append(out, eventType)
		}
	}
	sort.Strings(out)
	return out
}

func normalizedSubscriptionValues(values []string) []string {
	seen := map[string]struct{}{}
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
