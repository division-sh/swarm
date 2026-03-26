package bus

import (
	"path"
	"regexp"
	"strings"

	"swarm/internal/events"
)

var eventTypeTokenPattern = regexp.MustCompile(`^[a-z0-9_]+$`)
var eventPathSegmentPattern = regexp.MustCompile(`^[a-z0-9_-]+$`)

func RouteMatches(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		return routeSegmentsMatch(splitRouteSegments(pattern), splitRouteSegments(eventType))
	}
}

func splitRouteSegments(raw string) []string {
	raw = strings.Trim(strings.TrimSpace(raw), "/")
	if raw == "" {
		return nil
	}
	return strings.Split(raw, "/")
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

func AppendUniqueEventType(in []events.EventType, v events.EventType) []events.EventType {
	if v == "" {
		return in
	}
	for _, x := range in {
		if x == v {
			return in
		}
	}
	return append(in, v)
}

func IsValidEventTypeName(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	segments := strings.Split(raw, "/")
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" || segment == "*" {
			return false
		}
		if strings.Contains(segment, ".") {
			parts := strings.Split(segment, ".")
			if len(parts) == 0 {
				return false
			}
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" || !eventTypeTokenPattern.MatchString(p) {
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

func UniqueStrings(in []string) []string {
	if len(in) <= 1 {
		return in
	}
	set := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := set[v]; ok {
			continue
		}
		set[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func FilterOutAgentIDs(in []string, disallow []string) []string {
	if len(in) == 0 || len(disallow) == 0 {
		return in
	}
	set := make(map[string]struct{}, len(disallow))
	for _, v := range disallow {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		set[v] = struct{}{}
	}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, blocked := set[v]; blocked {
			continue
		}
		out = append(out, v)
	}
	return out
}
