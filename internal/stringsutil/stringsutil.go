// Package stringsutil provides shared string helpers that were previously
// duplicated across the repository. All helpers are pure functions with
// byte-identical semantics to the copies they consolidate.
package stringsutil

import "strings"

// FirstNonEmpty returns the first value whose trimmed form is non-empty,
// trimmed. If every value is empty after trimming, it returns "".
//
// This consolidates the firstNonEmpty/firstNonEmptyString/coalesce family.
// Callers that previously received the untrimmed original must trim the
// result themselves; callers of the trimmed variants are unchanged.
func FirstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// Contains reports whether values contains target exactly.
func Contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// ContainsTrimmed reports whether values contains target when both the
// candidate and target are trimmed of surrounding whitespace.
func ContainsTrimmed(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

// Unique returns values with every entry trimmed, empty entries removed, and
// duplicates removed, preserving first-occurrence order.
//
// This consolidates the UniqueStrings/uniqueStrings/uniqueOrderedStrings/
// uniqueTrimmedStrings/dedupeStrings/uniquePromptStrings/normalizedUniqueStrings
// and unsorted normalizeStrings family. It returns an empty (non-nil) slice
// for empty input.
func Unique(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
