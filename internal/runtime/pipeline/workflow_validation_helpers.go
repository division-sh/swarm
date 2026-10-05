package pipeline

import (
	"encoding/json"
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func asBool(v any) bool {
	typed, ok := v.(bool)
	return ok && typed
}

func workflowEntityContract(source semanticview.Source, flowID string) (entityruntime.Contract, bool) {
	return entityruntime.ResolveForFlow(source, flowID)
}

func WorkflowEntitySchemaInitialValueFields(source semanticview.Source) map[string]struct{} {
	if source == nil {
		return nil
	}
	return workflowEntitySchemaInitialValueFieldsFromRaw(source.WorkflowEntitySchema())
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

func workflowNormalizeEntityFields(source semanticview.Source, flowID string, fields map[string]any) (map[string]any, error) {
	contract, ok := workflowEntityContract(source, flowID)
	if !ok {
		if len(fields) == 0 {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("flow %s has entity fields without a declared contract", flowID)
	}
	return entityruntime.NormalizeState(contract, fields)
}

func requireWorkflowEntityType(source semanticview.Source, flowID string) (string, error) {
	contract, ok := workflowEntityContract(source, flowID)
	if !ok {
		return "", fmt.Errorf("flow %s requires one canonical entity contract", strings.TrimSpace(flowID))
	}
	entityType := strings.TrimSpace(contract.EntityType)
	if entityType == "" {
		return "", fmt.Errorf("flow %s canonical entity contract has empty entity_type", strings.TrimSpace(flowID))
	}
	return entityType, nil
}

func workflowEntityTypeForFlow(source semanticview.Source, flowID string) (string, error) {
	if _, found := workflowEntityContract(source, flowID); found {
		return requireWorkflowEntityType(source, flowID)
	}
	if source == nil {
		return "", fmt.Errorf("flow %s requires an admitted source", flowID)
	}
	if _, found := source.FlowSchemaByID(flowID); !found {
		return "", fmt.Errorf("flow %s has no admitted schema", flowID)
	}
	bundle, admitted := semanticview.Bundle(source)
	if !admitted || bundle == nil {
		return "", fmt.Errorf("flow %s requires an admitted state declaration", flowID)
	}
	declarations, _ := bundle.FlowEntityContractsByID(flowID)
	if flowID == semanticview.RootExecutionFlowID(source) {
		declarations = bundle.RootEntityContracts()
	}
	if len(declarations) != 0 {
		return "", fmt.Errorf("flow %s lost its declared entity contract", flowID)
	}
	return "", nil
}

func validateWorkflowEntityType(source semanticview.Source, flowID, actual string) error {
	expected, err := workflowEntityTypeForFlow(source, flowID)
	if err != nil {
		return err
	}
	actual = strings.TrimSpace(actual)
	if actual == "" && expected != "" {
		return fmt.Errorf("flow %s entity-bearing state requires canonical entity_type %q", strings.TrimSpace(flowID), expected)
	}
	if actual != expected {
		return fmt.Errorf("flow %s entity_type %q disagrees with canonical contract %q", strings.TrimSpace(flowID), actual, expected)
	}
	return nil
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
