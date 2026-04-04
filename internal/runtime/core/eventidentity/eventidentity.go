package eventidentity

import (
	"path"
	"strings"
)

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
