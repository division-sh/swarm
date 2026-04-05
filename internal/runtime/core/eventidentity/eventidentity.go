package eventidentity

import (
	"path"
	"strings"
)

type Scope struct {
	Path         string
	LocalEvents  []string
	InputEvents  []string
	OutputEvents []string
}

type DescendantScope struct {
	Path        string
	LocalEvents []string
}

func Normalize(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}

func LeafName(raw string) string {
	raw = Normalize(raw)
	if raw == "" {
		return ""
	}
	if idx := strings.LastIndex(raw, "/"); idx >= 0 && idx+1 < len(raw) {
		return strings.TrimSpace(raw[idx+1:])
	}
	return raw
}

func SplitRouteSegments(raw string) []string {
	raw = Normalize(raw)
	if raw == "" {
		return nil
	}
	return strings.Split(raw, "/")
}

func NormalizeList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = Normalize(item)
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
	_, ok := localEvents[Normalize(raw)]
	return ok
}

func (s Scope) ResolveEvent(raw string, descendants []DescendantScope) string {
	raw = Normalize(raw)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "/") {
		if absolute, ok := ExternalizeDescendantForFlow(s.Path, raw, descendantLocalEventSets(descendants)); ok {
			return absolute
		}
		return raw
	}
	return ExternalizeForFlow(s.Path, s.LocalEvents, raw)
}

func (s Scope) ResolveSubscriptionPattern(raw string, descendants []DescendantScope) string {
	raw = Normalize(raw)
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "/") && !strings.Contains(raw, "://") {
		if absolute, ok := ExternalizeDescendantForFlow(s.Path, raw, descendantLocalEventSets(descendants)); ok {
			return absolute
		}
	}
	return ResolvePattern(s.Path, localEventSet(s.LocalEvents), raw)
}

func (s Scope) LocalizeInput(raw string) string {
	return LocalizeForFlow(s.Path, s.InputEvents, raw)
}

func (s Scope) LocalizeOutput(raw string) string {
	return LocalizeForFlow(s.Path, s.OutputEvents, raw)
}

func (s Scope) HasInput(raw string) bool {
	local := Normalize(s.LocalizeInput(raw))
	if local == "" {
		return false
	}
	for _, input := range NormalizeList(s.InputEvents) {
		if input == local {
			return true
		}
	}
	return false
}

func (s Scope) HasOutput(raw string) bool {
	local := Normalize(s.LocalizeOutput(raw))
	if local == "" {
		return false
	}
	for _, output := range NormalizeList(s.OutputEvents) {
		if output == local {
			return true
		}
	}
	return false
}

func (s Scope) Matches(subscription, eventName string, descendants []DescendantScope) bool {
	subscription = Normalize(subscription)
	eventName = Normalize(eventName)
	if subscription == "" || eventName == "" {
		return false
	}
	if strings.Contains(subscription, "*") {
		return MatchPattern(s.ResolveSubscriptionPattern(subscription, descendants), eventName)
	}
	if eventName == subscription || eventName == s.ResolveEvent(subscription, descendants) {
		return true
	}
	return Normalize(s.LocalizeInput(eventName)) == subscription
}

func ResolvePattern(basePath string, localEvents map[string]struct{}, raw string) string {
	raw = Normalize(raw)
	basePath = Normalize(basePath)
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

func MatchPattern(pattern, eventName string) bool {
	pattern = Normalize(pattern)
	eventName = Normalize(eventName)
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		return routeSegmentsMatch(SplitRouteSegments(pattern), SplitRouteSegments(eventName))
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

func LocalizeForFlow(flowPath string, inputEvents []string, eventName string) string {
	flowPath = Normalize(flowPath)
	eventName = Normalize(eventName)
	if flowPath == "" || eventName == "" {
		return eventName
	}
	prefix := flowPath + "/"
	if strings.HasPrefix(eventName, prefix) {
		return strings.TrimPrefix(eventName, prefix)
	}
	for _, input := range NormalizeList(inputEvents) {
		if eventName == input || strings.HasSuffix(eventName, "/"+input) {
			return input
		}
	}
	return eventName
}

func ExternalizeForFlow(flowPath string, localEvents []string, eventName string) string {
	flowPath = Normalize(flowPath)
	eventName = Normalize(eventName)
	if flowPath == "" || eventName == "" {
		return eventName
	}
	if strings.Contains(eventName, "/") {
		return eventName
	}
	for _, localEvent := range NormalizeList(localEvents) {
		if eventName != localEvent {
			continue
		}
		return flowPath + "/" + eventName
	}
	return eventName
}

func ExternalizeDescendantForFlow(flowPath, eventName string, descendantLocalEvents map[string]map[string]struct{}) (string, bool) {
	flowPath = Normalize(flowPath)
	eventName = Normalize(eventName)
	if flowPath == "" || eventName == "" || len(descendantLocalEvents) == 0 {
		return "", false
	}
	if strings.HasPrefix(eventName, flowPath+"/") {
		return "", false
	}
	for descendantPath, localEvents := range descendantLocalEvents {
		descendantPath = Normalize(descendantPath)
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
	for _, value := range NormalizeList(values) {
		out[value] = struct{}{}
	}
	return out
}

func descendantLocalEventSets(descendants []DescendantScope) map[string]map[string]struct{} {
	if len(descendants) == 0 {
		return nil
	}
	out := make(map[string]map[string]struct{}, len(descendants))
	for _, descendant := range descendants {
		path := Normalize(descendant.Path)
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
