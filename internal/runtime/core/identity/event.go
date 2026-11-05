package identity

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	runtimepaths "github.com/division-sh/swarm/internal/runtime/core/paths"
)

var eventTypeTokenPattern = regexp.MustCompile("^[a-z0-9_]+$")
var eventPathSegmentPattern = regexp.MustCompile("^[a-z0-9_-]+$")

type EventScope struct {
	Path         string
	LocalEvents  []string
	InputEvents  []string
	OutputEvents []string
}

type DescendantEventScope struct {
	Path        string
	LocalEvents []string
}

func NormalizeEventName(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func IsValidEventName(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	for _, segment := range strings.Split(raw, "/") {
		segment = strings.TrimSpace(segment)
		if segment == "" || segment == "*" {
			return false
		}
		if strings.Contains(segment, ".") {
			parts := strings.Split(segment, ".")
			for index := range parts {
				if !validDottedEventToken(parts, index, false) {
					return false
				}
			}
			continue
		}
		if !eventPathSegmentPattern.MatchString(segment) {
			return false
		}
	}
	return true
}

// IsCanonicalEventName reports whether raw is an exact concrete event identity,
// without accepting whitespace or slash normalization aliases.
func IsCanonicalEventName(raw string) bool {
	return raw == NormalizeEventName(raw) && IsValidEventName(raw)
}

// IsCanonicalEventPattern reports whether raw uses the closed authored wildcard
// grammar: exact path/name tokens plus whole-token * and whole-segment **.
func IsCanonicalEventPattern(raw string) bool {
	if raw == "" || raw != NormalizeEventName(raw) || !strings.Contains(raw, "*") {
		return false
	}
	for _, segment := range strings.Split(raw, "/") {
		if segment == "*" || segment == "**" {
			continue
		}
		if segment == "" || strings.Contains(segment, "**") {
			return false
		}
		if !strings.Contains(segment, ".") {
			if !eventPathSegmentPattern.MatchString(segment) {
				return false
			}
			continue
		}
		parts := strings.Split(segment, ".")
		for index := range parts {
			if !validDottedEventToken(parts, index, true) {
				return false
			}
		}
	}
	return true
}

func validDottedEventToken(parts []string, index int, allowWildcard bool) bool {
	token := parts[index]
	if allowWildcard && token == "*" {
		return true
	}
	if isProviderDirectoryEventPosition(parts, index) {
		return runtimepaths.ValidateFlowSegment(token) == nil
	}
	if !allowWildcard {
		token = strings.TrimSpace(token)
	}
	return eventTypeTokenPattern.MatchString(token)
}

// Recognize the position before validation so no ordinary-token acceptance or
// whitespace repair can bypass the directory owner.
func isProviderDirectoryEventPosition(parts []string, index int) bool {
	return len(parts) >= 3 && parts[0] == "inbound" && index == 1
}

// ProviderEventKeyError supplies the directory owner's teaching error. It diagnoses
// only concrete provider keys, not general event validity or wildcard admission.
func ProviderEventKeyError(raw string) error {
	for _, segment := range strings.Split(raw, "/") {
		parts := strings.Split(segment, ".")
		if !isProviderDirectoryEventPosition(parts, 1) || parts[1] == "*" {
			continue
		}
		if err := runtimepaths.ValidateFlowSegment(parts[1]); err != nil {
			return fmt.Errorf("provider event %q must use inbound.<provider-directory>.<local-event> with an exact portable directory key: %w", raw, err)
		}
	}
	return nil
}

func IsCanonicalEventDeclaration(raw string) bool {
	return IsCanonicalEventName(raw) || IsCanonicalEventPattern(raw)
}

func EventLeafName(raw string) string {
	raw = NormalizeEventName(raw)
	if raw == "" {
		return ""
	}
	if idx := strings.LastIndex(raw, "/"); idx >= 0 && idx+1 < len(raw) {
		return strings.TrimSpace(raw[idx+1:])
	}
	return raw
}

func SplitEventRouteSegments(raw string) []string {
	raw = NormalizeEventName(raw)
	if raw == "" {
		return nil
	}
	return strings.Split(raw, "/")
}

func NormalizeEventNames(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = NormalizeEventName(item)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}

func IsLocalEvent(localEvents map[string]struct{}, raw string) bool {
	if len(localEvents) == 0 {
		return false
	}
	_, ok := localEvents[NormalizeEventName(raw)]
	return ok
}

func (s EventScope) ResolveEvent(raw string, descendants []DescendantEventScope) string {
	raw = NormalizeEventName(raw)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "/") {
		if absolute, ok := ExternalizeDescendantEventForFlow(s.Path, raw, descendantLocalEventSets(descendants)); ok {
			return absolute
		}
		return raw
	}
	return ExternalizeEventForFlow(s.Path, s.LocalEvents, raw)
}

func (s EventScope) ResolveSubscriptionPattern(raw string, descendants []DescendantEventScope) string {
	raw = NormalizeEventName(raw)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "/") && !strings.Contains(raw, "://") {
		if absolute, ok := ExternalizeDescendantEventForFlow(s.Path, raw, descendantLocalEventSets(descendants)); ok {
			return absolute
		}
	}
	return ResolveEventPattern(s.Path, localEventSet(s.LocalEvents), raw)
}

func (s EventScope) LocalizeInput(raw string) string {
	return LocalizeEventForFlow(s.Path, s.InputEvents, raw)
}

func (s EventScope) LocalizeOutput(raw string) string {
	return LocalizeEventForFlow(s.Path, s.OutputEvents, raw)
}

func (s EventScope) HasInput(raw string) bool {
	local := NormalizeEventName(s.LocalizeInput(raw))
	if local == "" {
		return false
	}
	for _, input := range NormalizeEventNames(s.InputEvents) {
		if input == local {
			return true
		}
	}
	return false
}

func (s EventScope) HasOutput(raw string) bool {
	local := NormalizeEventName(s.LocalizeOutput(raw))
	if local == "" {
		return false
	}
	for _, output := range NormalizeEventNames(s.OutputEvents) {
		if output == local {
			return true
		}
	}
	return false
}

func (s EventScope) Matches(subscription, eventName string, descendants []DescendantEventScope) bool {
	subscription = NormalizeEventName(subscription)
	eventName = NormalizeEventName(eventName)
	if subscription == "" || eventName == "" {
		return false
	}
	if strings.Contains(subscription, "*") {
		return MatchEventPattern(s.ResolveSubscriptionPattern(subscription, descendants), eventName)
	}
	if eventName == subscription || eventName == s.ResolveEvent(subscription, descendants) {
		return true
	}
	return NormalizeEventName(s.LocalizeInput(eventName)) == subscription
}

func ResolveEventPattern(basePath string, localEvents map[string]struct{}, raw string) string {
	raw = NormalizeEventName(raw)
	basePath = NormalizeEventName(basePath)
	switch {
	case raw == "":
		return ""
	case strings.Contains(raw, "://"):
		return raw
	case strings.HasPrefix(raw, "*/"), strings.HasPrefix(raw, "**/"):
		if basePath == "" {
			return raw
		}
		return basePath + "/" + raw
	case strings.Contains(raw, "/"):
		return raw
	case IsLocalEvent(localEvents, raw):
		if basePath == "" {
			return raw
		}
		return basePath + "/" + raw
	default:
		return raw
	}
}

func MatchEventPattern(pattern, eventName string) bool {
	pattern = NormalizeEventName(pattern)
	eventName = NormalizeEventName(eventName)
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		return routeSegmentsMatch(SplitEventRouteSegments(pattern), SplitEventRouteSegments(eventName))
	}
}

func routeSegmentsMatch(pattern, event []string) bool {
	if len(pattern) == 0 {
		return len(event) == 0
	}
	head := strings.TrimSpace(pattern[0])
	switch head {
	case "**":
		if len(pattern) == 1 {
			return true
		}
		for i := 0; i <= len(event); i++ {
			if routeSegmentsMatch(pattern[1:], event[i:]) {
				return true
			}
		}
		return false
	case "*":
		if len(event) == 0 {
			return false
		}
		return routeSegmentsMatch(pattern[1:], event[1:])
	default:
		if len(event) == 0 {
			return false
		}
		ok, err := path.Match(head, event[0])
		if err != nil || !ok {
			return false
		}
		return routeSegmentsMatch(pattern[1:], event[1:])
	}
}

func LocalizeEventForFlow(flowPath string, inputEvents []string, eventName string) string {
	flowPath = NormalizeEventName(flowPath)
	eventName = NormalizeEventName(eventName)
	if flowPath == "" || eventName == "" {
		return eventName
	}
	prefix := flowPath + "/"
	if strings.HasPrefix(eventName, prefix) {
		return strings.TrimPrefix(eventName, prefix)
	}
	for _, input := range NormalizeEventNames(inputEvents) {
		if eventName == input {
			return input
		}
	}
	return eventName
}

func ExternalizeEventForFlow(flowPath string, localEvents []string, eventName string) string {
	flowPath = NormalizeEventName(flowPath)
	eventName = NormalizeEventName(eventName)
	if flowPath == "" || eventName == "" {
		return eventName
	}
	if strings.Contains(eventName, "/") {
		return eventName
	}
	for _, localEvent := range NormalizeEventNames(localEvents) {
		if eventName != localEvent {
			continue
		}
		return flowPath + "/" + eventName
	}
	return eventName
}

func ExternalizeDescendantEventForFlow(flowPath, eventName string, descendantLocalEvents map[string]map[string]struct{}) (string, bool) {
	flowPath = NormalizeEventName(flowPath)
	eventName = NormalizeEventName(eventName)
	if flowPath == "" || eventName == "" || len(descendantLocalEvents) == 0 {
		return "", false
	}
	if strings.HasPrefix(eventName, flowPath+"/") {
		return "", false
	}
	for descendantPath, localEvents := range descendantLocalEvents {
		descendantPath = NormalizeEventName(descendantPath)
		if descendantPath == "" || !strings.HasPrefix(descendantPath, flowPath+"/") {
			continue
		}
		relativePath := strings.TrimPrefix(descendantPath, flowPath+"/")
		if relativePath == "" || !strings.HasPrefix(eventName, relativePath+"/") {
			continue
		}
		localEvent := strings.TrimPrefix(eventName, relativePath+"/")
		if !IsLocalEvent(localEvents, localEvent) {
			continue
		}
		return flowPath + "/" + eventName, true
	}
	return "", false
}

func localEventSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range NormalizeEventNames(values) {
		out[value] = struct{}{}
	}
	return out
}

func descendantLocalEventSets(descendants []DescendantEventScope) map[string]map[string]struct{} {
	if len(descendants) == 0 {
		return nil
	}
	out := make(map[string]map[string]struct{}, len(descendants))
	for _, descendant := range descendants {
		path := NormalizeEventName(descendant.Path)
		if path == "" {
			continue
		}
		local := localEventSet(descendant.LocalEvents)
		if len(local) == 0 {
			continue
		}
		out[path] = local
	}
	return out
}
