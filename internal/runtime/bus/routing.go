package bus

import (
	"path"
	"regexp"
	"strings"

	"empireai/internal/events"
)

type RoutingTable struct {
	VerticalID string
	Routes     []Route
}

type Route struct {
	EventPattern string
	SubscriberID string
	Status       string // active | proposed | deactivated
}

var eventTypeTokenPattern = regexp.MustCompile(`^[a-z0-9_]+$`)

// Spec v2.0.4: factory vs OpCo-internal routing classification is based on
// event type prefix, never on vertical_id presence.
var FactoryEventPrefixes = []string{
	"agent.",
	"runtime.",
	"ops.",
	"system.",
	"timer.",
	"heartbeat.",
	"scan.",
	"scanner.",
	"campaign.",
	"dedup.",
	"synthesis.",
	"vertical.",
	"scoring.",
	"market_research.",
	"trend_research.",
	"validation.",
	"research.",
	"spec.",
	"spec_review.",
	"cto.",
	"brand.",
	"template.",
	"budget.",
	"human_task.",
	"analyst.",
	"portfolio.",
	"mailbox.",
	"board.",
	"review.",
	"founder_input.",
	"spend.",
	"source.",
	"score.",
	"category.",
	"trend.",
	"devops.",
	"opco.",
}

func RouteMatches(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		if strings.Contains(pattern, "*") {
			if ok, err := path.Match(pattern, eventType); err == nil && ok {
				return true
			}
		}
		if strings.HasSuffix(pattern, "*") {
			return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
		}
		return pattern == eventType
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
	parts := strings.Split(raw, ".")
	if len(parts) == 0 {
		return false
	}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || !eventTypeTokenPattern.MatchString(p) {
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
