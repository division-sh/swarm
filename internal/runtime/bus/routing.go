package bus

import (
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

func RouteMatches(pattern, eventType string) bool {
	return eventidentity.MatchPattern(pattern, eventType)
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
	return eventidentity.IsValidName(raw)
}

// UniqueStrings returns in with entries trimmed, empty entries removed, and
// duplicates removed, preserving first-occurrence order.
//
// This helper is intentionally kept local rather than consolidated into
// stringsutil.Unique: for inputs of length <= 1 it returns the input slice
// verbatim, so a single element is returned untrimmed (and the input slice is
// never reallocated). stringsutil.Unique always trims every element and
// returns a fresh, non-nil slice.
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
