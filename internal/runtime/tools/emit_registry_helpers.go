package tools

import (
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func SnapshotEmitSchemas(registry map[string]EmitSchema) map[string]EmitSchema {
	out := make(map[string]EmitSchema, len(registry))
	for eventType, entry := range registry {
		schemaCopy, _ := deepCloneJSONValue(entry.Schema).(map[string]any)
		if schemaCopy == nil {
			schemaCopy = map[string]any{}
		}
		out[eventType] = EmitSchema{
			Description:    entry.Description,
			Schema:         schemaCopy,
			CitationFields: cloneEmitCriteriaCitationMap(entry.CitationFields),
		}
	}
	return out
}

func cloneEmitCriteriaCitationMap(in map[string]runtimecontracts.CriteriaCitation) map[string]runtimecontracts.CriteriaCitation {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]runtimecontracts.CriteriaCitation, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = runtimecontracts.CriteriaCitation{
			Criteria:       strings.TrimSpace(value.Criteria),
			AllowedClasses: UniqueNonEmpty(value.AllowedClasses),
		}
	}
	return out
}

func UniqueNonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
