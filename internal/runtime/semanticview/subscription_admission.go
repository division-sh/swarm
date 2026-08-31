package semanticview

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
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
)

type AuthoredSubscriptionFailure string

const (
	AuthoredSubscriptionFailureQualifiedExact        AuthoredSubscriptionFailure = "qualified_exact_forbidden"
	AuthoredSubscriptionFailurePatternUnauthorized   AuthoredSubscriptionFailure = "pattern_unauthorized"
	AuthoredSubscriptionFailureTimerPatternForbidden AuthoredSubscriptionFailure = "timer_pattern_forbidden"
	AuthoredSubscriptionFailureSemanticScopeInvalid  AuthoredSubscriptionFailure = "semantic_scope_invalid"
)

// AuthoredSubscriptionRequest carries the complete scope needed to classify
// one authored subscription before it may become validation or route authority.
type AuthoredSubscriptionRequest struct {
	ConsumerKind AuthoredSubscriptionConsumerKind
	ConsumerID   string
	FlowID       string
	FlowPath     string
	LocalEvents  map[string]struct{}
	InputEvents  []string
	Authored     string
}

// AuthoredSubscriptionAdmission is the closed result consumed by validation,
// typed relation, route materialization, and handler execution. Route patterns
// remain private so raw authored strings cannot bypass classification.
type AuthoredSubscriptionAdmission struct {
	consumerKind    AuthoredSubscriptionConsumerKind
	consumerID      string
	authored        string
	localEvent      string
	persistedValue  string
	routePatterns   []string
	localizedEvents []string
	class           AuthoredSubscriptionAdmissionClass
	failure         AuthoredSubscriptionFailure
	message         string
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
	return a.class == AuthoredSubscriptionLocalPattern
}

func (a AuthoredSubscriptionAdmission) Matches(eventType string) bool {
	if !a.Admitted() {
		return false
	}
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return false
	}
	for _, localized := range a.localizedEvents {
		if eventidentity.Normalize(localized) == eventType {
			return true
		}
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
	if !a.Admitted() {
		return false
	}
	localized := eventidentity.LocalizeForFlow(flowPath, inputEvents, eventType)
	if localized == "" {
		return false
	}
	declaredInput := false
	for _, input := range inputEvents {
		if eventidentity.Normalize(input) == localized {
			declaredInput = true
			break
		}
	}
	if !declaredInput {
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
		if strings.Contains(req.Authored, "/") {
			return failedAuthoredSubscription(result, AuthoredSubscriptionFailurePatternUnauthorized,
				fmt.Sprintf("%s %q wildcard subscription %q must use a flow-local event pattern; declare output/input pins and connect in the nearest common ancestor schema.yaml for cross-flow delivery", req.ConsumerKind, req.ConsumerID, req.Authored))
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
			pattern = admitNonImportNodePattern(req)
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

func admitNonImportNodePattern(req AuthoredSubscriptionRequest) string {
	if req.FlowPath != "" {
		return req.FlowPath + "/" + req.Authored
	}
	return req.Authored
}

func ClassifyExecutableNodeSubscription(source Source, node runtimeidentity.ExecutableNode, authored string) AuthoredSubscriptionAdmission {
	if source == nil || !node.Valid() {
		return AuthoredSubscriptionAdmission{}
	}
	flowPath := ""
	localEvents := map[string]struct{}{}
	var inputEvents []string
	semanticScope, scopeErr := ResolveExecutableNodeSemanticScope(source, node)
	if scopeErr != nil {
		return failedAuthoredSubscription(AuthoredSubscriptionAdmission{}, AuthoredSubscriptionFailureSemanticScopeInvalid,
			fmt.Sprintf("node %q semantic scope is invalid: %v", node.Key(), scopeErr))
	}
	if scope, ok := semanticScope.OwningFlow(); ok {
		flowPath = sourceFlowPath(source, node.FlowPath())
		localEvents, inputEvents = authoredSubscriptionScopeEvents(scope)
	}
	return ClassifyAuthoredSubscription(source, AuthoredSubscriptionRequest{
		ConsumerKind: AuthoredSubscriptionConsumerNode,
		ConsumerID:   node.Key(),
		FlowID:       node.FlowPath(),
		FlowPath:     flowPath,
		LocalEvents:  localEvents,
		InputEvents:  inputEvents,
		Authored:     authored,
	})
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

func ResolveExecutableNodeSubscriptionHandler(source Source, node runtimeidentity.ExecutableNode, eventType string) NodeSubscriptionHandlerResolution {
	if source == nil || !node.Valid() {
		return NodeSubscriptionHandlerResolution{}
	}
	handlers := source.ExecutableNodeEventHandlers(node)
	if len(handlers) == 0 {
		return NodeSubscriptionHandlerResolution{}
	}
	keys := make([]string, 0, len(handlers))
	for key := range handlers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	type handlerCandidate struct {
		key       string
		admission AuthoredSubscriptionAdmission
	}
	exact := make([]handlerCandidate, 0, len(keys))
	patterns := make([]handlerCandidate, 0, len(keys))
	for _, key := range keys {
		admission := ClassifyExecutableNodeSubscription(source, node, key)
		if !admission.Admitted() {
			continue
		}
		candidate := handlerCandidate{key: key, admission: admission}
		if admission.Pattern() {
			patterns = append(patterns, candidate)
		} else {
			exact = append(exact, candidate)
		}
	}
	flowPath := ""
	var inputEvents []string
	if semanticScope, err := ResolveExecutableNodeSemanticScope(source, node); err == nil {
		if scope, ok := semanticScope.OwningFlow(); ok {
			flowPath = sourceFlowPath(source, node.FlowPath())
			inputEvents = append([]string(nil), scope.InputEvents...)
		}
	}
	for _, candidate := range append(exact, patterns...) {
		admission := candidate.admission
		if eventidentity.Normalize(eventType) != eventidentity.Normalize(admission.LocalEvent()) &&
			!admission.Matches(eventType) && !admission.MatchesReceiverInput(eventType, flowPath, inputEvents) {
			continue
		}
		return NodeSubscriptionHandlerResolution{
			Handler: handlers[candidate.key], HandlerEventKey: strings.TrimSpace(candidate.key),
			Admission: admission, Matched: true,
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
	if req.FlowPath == "" && strings.TrimSpace(req.FlowID) != "." {
		req.FlowPath = eventidentity.Normalize(source.FlowPath(req.FlowID))
	}
	if scope, ok := source.FlowScopeByID(req.FlowID); ok {
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

func sourceFlowPath(source Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "." {
		return ""
	}
	return source.FlowPath(flowID)
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
