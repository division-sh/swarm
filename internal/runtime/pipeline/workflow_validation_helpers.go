package pipeline

import (
	"encoding/json"
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/eventidentity"
	"swarm/internal/runtime/entityruntime"
	"swarm/internal/runtime/semanticview"
)

func stringValue(v any) string {
	if typed, ok := v.(string); ok {
		return typed
	}
	return ""
}

func asBool(v any) bool {
	typed, ok := v.(bool)
	return ok && typed
}

func workflowEntityContract(source semanticview.Source, flowID string) (entityruntime.Contract, bool) {
	return entityruntime.ResolveForFlow(source, flowID)
}

func workflowEntitySchemaFields(source semanticview.Source, flowID string) map[string]struct{} {
	contract, ok := workflowEntityContract(source, flowID)
	if !ok {
		return map[string]struct{}{}
	}
	out := make(map[string]struct{}, len(contract.Entity.Fields))
	for _, field := range entityruntime.FieldNames(contract) {
		out[field] = struct{}{}
	}
	return out
}

func workflowEntitySchemaInitialValues(source semanticview.Source, flowID string) map[string]any {
	contract, ok := workflowEntityContract(source, flowID)
	if !ok {
		return nil
	}
	values, err := entityruntime.Materialize(contract, nil)
	if err != nil {
		return nil
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

func WorkflowEntitySchemaInitialValueFields(source semanticview.Source) map[string]struct{} {
	if source == nil {
		return nil
	}
	return workflowEntitySchemaInitialValueFieldsFromRaw(source.WorkflowEntitySchema())
}

func workflowEntitySchemaInitialValueFieldsForFlow(source semanticview.Source, flowID string) map[string]struct{} {
	values := workflowEntitySchemaInitialValues(source, flowID)
	if len(values) == 0 {
		return nil
	}
	out := map[string]struct{}{}
	for field := range values {
		out[field] = struct{}{}
	}
	return out
}

func workflowEntitySchemaInitialValueFieldsFromRaw(raw any) map[string]struct{} {
	fields := workflowEntitySchemaFieldDefinitions(raw)
	if len(fields) == 0 {
		return nil
	}
	out := map[string]struct{}{}
	for name, field := range fields {
		if field.Initial != nil {
			out[name] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowEntitySchemaFieldDefinitions(raw any) map[string]runtimecontracts.EntitySchemaField {
	out := map[string]runtimecontracts.EntitySchemaField{}
	switch typed := raw.(type) {
	case runtimecontracts.EntitySchema:
		for _, group := range typed.Groups {
			for _, field := range group.Fields {
				name := strings.TrimSpace(field.Name)
				if name != "" {
					field.Name = name
					out[name] = field
				}
			}
		}
		return out
	case *runtimecontracts.EntitySchema:
		if typed != nil {
			return workflowEntitySchemaFieldDefinitions(*typed)
		}
		return out
	}
	obj, ok := asObject(raw)
	if !ok {
		return out
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
				for name, field := range workflowEntitySchemaFieldDefinitions(fields) {
					out[name] = field
				}
			}
		}
	}
	if fields, ok := obj["fields"]; ok {
		for name, field := range workflowEntitySchemaFieldDefinitions(fields) {
			out[name] = field
		}
		return out
	}
	if items, ok := raw.([]any); ok {
		for _, item := range items {
			field, ok := asObject(item)
			if !ok {
				continue
			}
			name := strings.TrimSpace(asString(field["name"]))
			if name != "" {
				out[name] = runtimecontracts.EntitySchemaField{
					Name:        name,
					Type:        strings.TrimSpace(asString(field["type"])),
					Initial:     cloneWorkflowSchemaValue(field["initial"]),
					Nullable:    asBool(field["nullable"]),
					Description: strings.TrimSpace(asString(field["description"])),
				}
			}
		}
	}
	return out
}

func workflowMaterializeEntityMetadata(source semanticview.Source, flowID string, metadata map[string]any) map[string]any {
	contract, ok := workflowEntityContract(source, flowID)
	if !ok {
		return cloneStringAnyMap(metadata)
	}
	entityFields := map[string]any{}
	for name := range contract.Entity.Fields {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if value, exists := metadata[name]; exists {
			entityFields[name] = cloneWorkflowSchemaValue(value)
		}
	}
	materialized, err := entityruntime.Materialize(contract, entityFields)
	if err != nil {
		return cloneStringAnyMap(metadata)
	}
	out := cloneStringAnyMap(metadata)
	if out == nil {
		out = map[string]any{}
	}
	for key, value := range materialized {
		out[key] = value
	}
	if entityType := strings.TrimSpace(contract.EntityType); entityType != "" {
		out["entity_type"] = entityType
	}
	return out
}

func cloneWorkflowSchemaValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = cloneWorkflowSchemaValue(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, cloneWorkflowSchemaValue(item))
		}
		return out
	case json.RawMessage:
		return append(json.RawMessage(nil), typed...)
	default:
		return typed
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
	return eventidentity.MatchPattern(pattern, eventType)
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
