package pipeline

import (
	"path/filepath"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func stringValue(v any) string {
	if typed, ok := v.(string); ok {
		return typed
	}
	return ""
}

func workflowEntitySchemaFields(source semanticview.Source) map[string]struct{} {
	out := map[string]struct{}{}
	if source == nil {
		return out
	}
	collectEntitySchemaFields(source.WorkflowEntitySchema(), out)
	return out
}

func collectEntitySchemaFields(raw any, out map[string]struct{}) {
	switch typed := raw.(type) {
	case runtimecontracts.EntitySchema:
		for _, group := range typed.Groups {
			for _, field := range group.Fields {
				name := strings.TrimSpace(field.Name)
				if name != "" {
					out[name] = struct{}{}
				}
			}
		}
		return
	case *runtimecontracts.EntitySchema:
		if typed != nil {
			collectEntitySchemaFields(*typed, out)
		}
		return
	}
	obj, ok := asObject(raw)
	if !ok {
		return
	}
	if groups, ok := obj["groups"]; ok {
		switch typed := groups.(type) {
		case []any:
			for _, item := range typed {
				group, ok := asObject(item)
				if !ok {
					continue
				}
				fields, ok := group["fields"]
				if !ok {
					continue
				}
				collectEntitySchemaFields(fields, out)
			}
		}
	}
	if fields, ok := obj["fields"]; ok {
		collectEntitySchemaFields(fields, out)
		return
	}
	if items, ok := raw.([]any); ok {
		for _, item := range items {
			field, ok := asObject(item)
			if !ok {
				continue
			}
			name := strings.TrimSpace(asString(field["name"]))
			if name != "" {
				out[name] = struct{}{}
			}
		}
	}
}

func workflowNodeFlowID(source semanticview.Source, nodeID string) string {
	if source == nil {
		return ""
	}
	contractSource, ok := source.NodeContractSource(nodeID)
	if !ok {
		return ""
	}
	return strings.TrimSpace(contractSource.FlowID)
}

func runtimecontractsHandlerPatternMatches(pattern, eventType string) bool {
	pattern = strings.TrimSpace(pattern)
	eventType = strings.TrimSpace(eventType)
	if pattern == "" || eventType == "" {
		return false
	}
	if pattern == eventType {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	return workflowRouteMatches(pattern, eventType)
}

func workflowRouteMatches(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		return workflowRouteSegmentsMatch(workflowSplitRouteSegments(pattern), workflowSplitRouteSegments(eventType))
	}
}

func workflowSplitRouteSegments(raw string) []string {
	raw = strings.Trim(strings.TrimSpace(raw), "/")
	if raw == "" {
		return nil
	}
	return strings.Split(raw, "/")
}

func workflowRouteSegmentsMatch(pattern, event []string) bool {
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
			if workflowRouteSegmentsMatch(pattern[1:], event[i:]) {
				return true
			}
		}
		return false
	case "*":
		if len(event) == 0 {
			return false
		}
		return workflowRouteSegmentsMatch(pattern[1:], event[1:])
	default:
		if len(event) == 0 {
			return false
		}
		matched, err := filepath.Match(head, event[0])
		if err != nil || !matched {
			return false
		}
		return workflowRouteSegmentsMatch(pattern[1:], event[1:])
	}
}

func normalizeStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func sortedWorkflowValidationKeys[T any](m map[string]T) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
