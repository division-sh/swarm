package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"unicode"

	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/entityruntime"
	"swarm/internal/runtime/semanticview"
)

type roleScopedEntityToolKind string

const (
	roleScopedEntityToolReadWhole  roleScopedEntityToolKind = "read_whole"
	roleScopedEntityToolReadField  roleScopedEntityToolKind = "read_field"
	roleScopedEntityToolSaveField  roleScopedEntityToolKind = "save_field"
	roleScopedEntityToolUpdatePath roleScopedEntityToolKind = "update_path"
)

type roleScopedEntityToolSpec struct {
	Kind       roleScopedEntityToolKind
	EntityType string
	Field      string
	Subpath    string
}

var legacyEntityToolSurfaceNames = map[string]struct{}{
	"create_entity":      {},
	"get_entity":         {},
	"get_subject_status": {},
	"query_entities":     {},
	"query_metrics":      {},
	"save_entity_field":  {},
	"search_entities":    {},
}

func removeLegacyEntityToolSurface(entries map[string]RegisteredTool) {
	for name := range legacyEntityToolSurfaceNames {
		delete(entries, name)
	}
}

func roleScopedEntityToolsEnabledForActor(source semanticview.Source, actor models.AgentConfig) bool {
	if source == nil {
		return false
	}
	contractSource, ok := source.AgentContractSource(strings.TrimSpace(actor.ID))
	if !ok {
		return false
	}
	flowID := strings.TrimSpace(contractSource.FlowID)
	if flowID == "" {
		return false
	}
	schema, ok := source.FlowSchemaByID(flowID)
	if !ok {
		return false
	}
	return schema.ToolSurface.RoleScopedEntityTools
}

func IsRoleScopedEntityTool(toolName string) bool {
	return roleScopedEntityToolNameKind(toolName) != ""
}

func roleScopedEntityToolNameKind(toolName string) roleScopedEntityToolKind {
	toolName = strings.TrimSpace(toolName)
	if toolName == "read_file" {
		return ""
	}
	switch {
	case strings.HasPrefix(toolName, "read_"):
		return roleScopedEntityToolReadWhole
	case strings.HasPrefix(toolName, "save_"):
		return roleScopedEntityToolSaveField
	case strings.HasPrefix(toolName, "update_"):
		return roleScopedEntityToolUpdatePath
	default:
		return ""
	}
}

func roleScopedEntityToolSchemaEntriesForActor(source semanticview.Source, actor models.AgentConfig, contract entityruntime.Contract) map[string]ContractSchemaEntry {
	specs := roleScopedEntityToolSpecsForActor(source, actor, contract)
	out := make(map[string]ContractSchemaEntry, len(specs))
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		spec := specs[name]
		out[name] = roleScopedEntityToolSchemaEntry(contract, spec)
	}
	return out
}

func roleScopedEntityToolSchemaEntry(contract entityruntime.Contract, spec roleScopedEntityToolSpec) ContractSchemaEntry {
	entry := ContractSchemaEntry{
		Category:    "entity_persistence",
		Description: roleScopedEntityToolDescription(spec),
		InputSchema: ObjectSchema(map[string]any{}),
	}
	if spec.Kind == roleScopedEntityToolSaveField || spec.Kind == roleScopedEntityToolUpdatePath {
		typeRef := ""
		if spec.Subpath == "" {
			if decl, err := entityruntime.FieldDecl(contract, spec.Field); err == nil {
				typeRef = decl.Type
			}
		} else if typeRefForSubpath, ok := roleScopedEntitySubpathTypeRef(contract, spec.Field, spec.Subpath); ok {
			typeRef = typeRefForSubpath
		}
		entry.InputSchema = ObjectSchema(map[string]any{
			"value": entityContractJSONSchema(contract, typeRef, map[string]struct{}{}),
		}, "value")
	}
	return entry
}

func roleScopedEntityToolDescription(spec roleScopedEntityToolSpec) string {
	switch spec.Kind {
	case roleScopedEntityToolReadWhole:
		return fmt.Sprintf("Read the current turn %s entity. The target entity is resolved from the trigger event; no entity_id is accepted.", spec.EntityType)
	case roleScopedEntityToolReadField:
		return fmt.Sprintf("Read %s from the current turn %s entity. The target entity is resolved from the trigger event; no entity_id is accepted.", spec.Field, spec.EntityType)
	case roleScopedEntityToolSaveField:
		return fmt.Sprintf("Save %s on the current turn %s entity. The target entity is resolved from the trigger event; no entity_id is accepted.", spec.Field, spec.EntityType)
	case roleScopedEntityToolUpdatePath:
		return fmt.Sprintf("Update %s.%s on the current turn %s entity. The target entity is resolved from the trigger event; no entity_id is accepted.", spec.Field, spec.Subpath, spec.EntityType)
	default:
		return "Role-scoped entity tool."
	}
}

func roleScopedEntityToolSpecsForActor(source semanticview.Source, actor models.AgentConfig, contract entityruntime.Contract) map[string]roleScopedEntityToolSpec {
	entityName := roleScopedToolNamePart(contract.EntityType)
	if entityName == "" {
		return nil
	}
	out := map[string]roleScopedEntityToolSpec{
		"read_" + entityName: {
			Kind:       roleScopedEntityToolReadWhole,
			EntityType: contract.EntityType,
		},
	}
	for _, field := range entityruntime.FieldNames(contract) {
		fieldName := roleScopedToolNamePart(field)
		if fieldName == "" {
			continue
		}
		out["read_"+entityName+"_"+fieldName] = roleScopedEntityToolSpec{
			Kind:       roleScopedEntityToolReadField,
			EntityType: contract.EntityType,
			Field:      field,
		}
	}
	for _, field := range roleScopedWritableFields(source, actor, contract) {
		fieldName := roleScopedToolNamePart(field)
		if fieldName == "" {
			continue
		}
		out["save_"+entityName+"_"+fieldName] = roleScopedEntityToolSpec{
			Kind:       roleScopedEntityToolSaveField,
			EntityType: contract.EntityType,
			Field:      field,
		}
		for _, subpath := range roleScopedTopLevelSubpaths(contract, field) {
			subpathName := roleScopedToolNamePart(subpath)
			if subpathName == "" {
				continue
			}
			out["update_"+entityName+"_"+fieldName+"_"+subpathName] = roleScopedEntityToolSpec{
				Kind:       roleScopedEntityToolUpdatePath,
				EntityType: contract.EntityType,
				Field:      field,
				Subpath:    subpath,
			}
		}
	}
	return out
}

func roleScopedWritableFields(source semanticview.Source, actor models.AgentConfig, contract entityruntime.Contract) []string {
	if source == nil {
		return nil
	}
	_, entry, ok := semanticview.ResolveAgentRegistryEntry(source, actor)
	if !ok {
		return nil
	}
	decl, ok := roleScopedEntityWriteDecl(entry, contract)
	if !ok {
		return nil
	}
	seen := map[string]struct{}{}
	add := func(rule runtimecontracts.AgentEntityWriteRule) {
		if rule.All {
			for _, field := range entityruntime.FieldNames(contract) {
				seen[field] = struct{}{}
			}
			return
		}
		for _, field := range rule.Fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			if _, err := entityruntime.FieldDecl(contract, field); err == nil {
				seen[field] = struct{}{}
			}
		}
	}
	add(decl.Create)
	add(decl.Save)
	out := make([]string, 0, len(seen))
	for field := range seen {
		out = append(out, field)
	}
	sort.Strings(out)
	return out
}

func roleScopedEntityWriteDecl(entry runtimecontracts.AgentRegistryEntry, contract entityruntime.Contract) (runtimecontracts.AgentEntityWriteDecl, bool) {
	if len(entry.EntityWrites) == 0 {
		return runtimecontracts.AgentEntityWriteDecl{}, false
	}
	if contract.FlowID != "" {
		if decl, ok := entry.EntityWrites[contract.FlowID+"."+contract.EntityType]; ok {
			return decl, true
		}
	}
	decl, ok := entry.EntityWrites[contract.EntityType]
	return decl, ok
}

func roleScopedTopLevelSubpaths(contract entityruntime.Contract, field string) []string {
	decl, err := entityruntime.FieldDecl(contract, field)
	if err != nil || !deliveryNamedType(contract, decl.Type) {
		return nil
	}
	named := contract.Types.Types[deliveryTypeName(contract, decl.Type)]
	out := make([]string, 0, len(named.Fields))
	for name := range named.Fields {
		name = strings.TrimSpace(name)
		if name != "" {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func roleScopedEntitySubpathTypeRef(contract entityruntime.Contract, field, subpath string) (string, bool) {
	decl, err := entityruntime.FieldDecl(contract, field)
	if err != nil || !deliveryNamedType(contract, decl.Type) {
		return "", false
	}
	named := contract.Types.Types[deliveryTypeName(contract, decl.Type)]
	subDecl, ok := named.Fields[strings.TrimSpace(subpath)]
	if !ok {
		return "", false
	}
	return subDecl.Type, true
}

func roleScopedToolNamePart(raw string) string {
	raw = strings.TrimSpace(raw)
	var b strings.Builder
	lastUnderscore := false
	for _, r := range raw {
		switch {
		case r == '_' || r == '-' || r == '.' || r == '/':
			if !lastUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				lastUnderscore = true
			}
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(unicode.ToLower(r))
			lastUnderscore = false
		default:
			if !lastUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

func roleScopedEntityToolSpecForActor(source semanticview.Source, actor models.AgentConfig, toolName string) (roleScopedEntityToolSpec, entityruntime.Contract, bool) {
	if !roleScopedEntityToolsEnabledForActor(source, actor) {
		return roleScopedEntityToolSpec{}, entityruntime.Contract{}, false
	}
	contract, ok := entityruntime.ResolveForActor(source, actor.ID)
	if !ok {
		return roleScopedEntityToolSpec{}, entityruntime.Contract{}, false
	}
	specs := roleScopedEntityToolSpecsForActor(source, actor, contract)
	spec, ok := specs[strings.TrimSpace(toolName)]
	return spec, contract, ok
}

func (e *Executor) execRoleScopedEntityTool(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error) {
	db, source, payload, err := e.entityToolDependencies(input)
	if err != nil {
		return nil, err
	}
	forbidden := []string{"entity_id", "flow_instance", "field"}
	for _, key := range forbidden {
		if value, ok := payload[key]; ok && value != nil {
			return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.identity", false, "%s is resolved by the runtime and must not be supplied", key)
		}
	}
	spec, contract, ok := roleScopedEntityToolSpecForActor(source, actor, name)
	if !ok {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.resolve", false, "role-scoped entity tool is not available for actor %s: %s", strings.TrimSpace(actor.ID), strings.TrimSpace(name))
	}
	entityID := roleScopedCurrentEntityID(ctx)
	if entityID == "" {
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.current_entity", false, "current turn entity_id is required for %s", strings.TrimSpace(name))
	}
	row, found, err := loadEntityState(ctx, db, entityID)
	if err != nil {
		return nil, WrapRuntimeError("query_failed", "tool-executor", "role_scoped_entity_tool.lookup", true, err, "load current entity %s", entityID)
	}
	if !found {
		return nil, NewRuntimeError("not_found", "tool-executor", "role_scoped_entity_tool.lookup", false, "current entity %s not found", entityID)
	}
	if err := roleScopedEntityMatchesContract(source, row, contract); err != nil {
		return nil, err
	}
	switch spec.Kind {
	case roleScopedEntityToolReadWhole:
		return materializeEntityStateRow(source, row)
	case roleScopedEntityToolReadField:
		materialized, err := materializeEntityStateRow(source, row)
		if err != nil {
			return nil, WrapRuntimeError("query_failed", "tool-executor", "role_scoped_entity_tool.materialize", false, err, "materialize current entity %s", entityID)
		}
		fields, _ := materialized["fields"].(map[string]any)
		return fields[spec.Field], nil
	case roleScopedEntityToolSaveField:
		value, ok := payload["value"]
		if !ok {
			return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.value", false, "value is required")
		}
		return e.execSaveEntityField(ctx, actor, map[string]any{
			"entity_id": entityID,
			"field":     spec.Field,
			"value":     value,
		})
	case roleScopedEntityToolUpdatePath:
		value, ok := payload["value"]
		if !ok {
			return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.value", false, "value is required")
		}
		return e.execSaveEntityField(ctx, actor, map[string]any{
			"entity_id": entityID,
			"field":     spec.Field + "." + spec.Subpath,
			"value":     value,
		})
	default:
		return nil, NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.kind", false, "unsupported role-scoped entity tool: %s", strings.TrimSpace(name))
	}
}

func roleScopedCurrentEntityID(ctx context.Context) string {
	inbound, ok := runtimebus.InboundEventFromContext(ctx)
	if !ok {
		return ""
	}
	return strings.TrimSpace(inbound.EntityID())
}

func roleScopedEntityMatchesContract(source semanticview.Source, row map[string]any, contract entityruntime.Contract) error {
	actual, ok := entityruntime.ResolveForEntityRow(source, row)
	if !ok {
		return NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.contract", false, "current entity does not resolve to a flow-owned entity contract")
	}
	if strings.TrimSpace(actual.FlowID) != strings.TrimSpace(contract.FlowID) || strings.TrimSpace(actual.EntityType) != strings.TrimSpace(contract.EntityType) {
		return NewRuntimeError("invalid_tool_input", "tool-executor", "role_scoped_entity_tool.contract", false, "current entity does not match actor entity contract")
	}
	return nil
}
