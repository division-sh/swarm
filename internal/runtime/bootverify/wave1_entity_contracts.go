package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/platformcontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type wave1EntityContractView struct {
	FlowID     string
	EntityType string
	Contract   runtimecontracts.EntityContract
	Types      runtimecontracts.TypeCatalogDocument
	Defined    bool
}

type wave1ResolvedType struct {
	Kind       string
	Type       string
	IsOptional bool
}

var wave1PlatformEntityTypes = map[string]wave1ResolvedType{
	"id":            {Kind: "scalar", Type: "uuid"},
	"flow_instance": {Kind: "scalar", Type: "text"},
	"current_state": {Kind: "scalar", Type: "text"},
}

func wave1EntityContractForFlow(source semanticview.Source, flowID string) wave1EntityContractView {
	view := wave1EntityContractView{FlowID: strings.TrimSpace(flowID)}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return view
	}
	if view.FlowID == "." {
		entityType, contract, ok := bundle.RootPrimaryEntityContract()
		if ok {
			view.EntityType = strings.TrimSpace(entityType)
			view.Contract = contract
			view.Types = bundle.RootTypeCatalog()
			view.Defined = true
			return view
		}
		view.Types = bundle.RootTypeCatalog()
		return view
	}
	entityType, contract, ok := bundle.FlowPrimaryEntityContract(view.FlowID)
	if !ok {
		view.Types = bundle.ResolvedTypeCatalogForFlow(view.FlowID)
		return view
	}
	view.EntityType = strings.TrimSpace(entityType)
	view.Contract = contract
	view.Types = bundle.ResolvedTypeCatalogForFlow(view.FlowID)
	view.Defined = true
	return view
}

func wave1ResolveEntityPath(source semanticview.Source, flowID, ref string) (wave1ResolvedType, error) {
	resolved, _, err := wave1ResolveEntityPathWithOwner(source, flowID, ref)
	return resolved, err
}

func wave1ResolveEntityPathWithOwner(source semanticview.Source, flowID, ref string) (wave1ResolvedType, string, error) {
	ref = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(ref), "entity."))
	if ref == "" {
		return wave1ResolvedType{}, "", fmt.Errorf("entity field path is required")
	}
	segments := strings.Split(ref, ".")
	head := strings.TrimSpace(segments[0])
	if head == "fan_out_count" {
		return wave1ResolvedType{}, "", fmt.Errorf("entity.fan_out_count is platform bookkeeping; use the handler-local fan_out.count")
	}
	view := wave1EntityContractForFlow(source, flowID)
	if !view.Defined {
		return wave1ResolvedType{}, "", fmt.Errorf("flow %s has no declared entity contract for entity.%s", defaultFlowLabel(flowID), head)
	}
	field, ok := view.Contract.Fields[head]
	if !ok {
		if platformcontext.LegacyEntityMetadataField(head) {
			return wave1ResolvedType{}, "", fmt.Errorf("%s", legacyEntityMetadataDiagnostic(head))
		}
		return wave1ResolvedType{}, "", fmt.Errorf("flow %s entity_type %s does not declare field %q", defaultFlowLabel(flowID), view.EntityType, head)
	}
	current := strings.TrimSpace(field.Type)
	optional := field.IsOptional
	if current == "" {
		return wave1ResolvedType{}, "", fmt.Errorf("flow %s entity_type %s field %q has empty type", defaultFlowLabel(flowID), view.EntityType, head)
	}
	for idx := 1; idx < len(segments); idx++ {
		segment := strings.TrimSpace(segments[idx])
		if segment == "" {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q contains empty segment", ref)
		}
		resolved, err := wave1ResolveDeclaredEntityType(view.Types, current)
		if err != nil {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q: %w", ref, err)
		}
		if resolved.Kind == runtimecontracts.CatalogTypeList {
			if segment == "size" && idx == len(segments)-1 {
				return wave1ResolvedType{Kind: "scalar", Type: "integer", IsOptional: optional}, view.FlowID, nil
			}
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q traverses list type %q through unsupported segment %q", ref, current, segment)
		}
		if resolved.Kind != runtimecontracts.CatalogTypeObject {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q cannot traverse non-composite type %q through segment %q", ref, current, segment)
		}
		fieldSpec, ok := resolved.Field(segment)
		if !ok {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q references undeclared nested field %q", ref, segment)
		}
		current = strings.TrimSpace(fieldSpec.TypeRef)
		optional = optional || fieldSpec.IsOptional
		if current == "" {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q nested field %q has empty type", ref, segment)
		}
	}
	return wave1ProjectDeclaredEntityLeaf(view, current, ref, optional)
}

func wave1ProjectDeclaredEntityLeaf(view wave1EntityContractView, current, ref string, optional bool) (wave1ResolvedType, string, error) {
	resolved, err := wave1ResolveDeclaredEntityType(view.Types, current)
	if err != nil {
		return wave1ResolvedType{}, "", fmt.Errorf("entity path %q: %w", ref, err)
	}
	var kind string
	switch resolved.Kind {
	case runtimecontracts.CatalogTypeText:
		kind = "scalar"
		if resolved.Name != "" {
			kind = "enum"
		}
	case runtimecontracts.CatalogTypeInteger, runtimecontracts.CatalogTypeNumber, runtimecontracts.CatalogTypeBoolean:
		kind = "scalar"
	case runtimecontracts.CatalogTypeObject:
		kind = "named"
	case runtimecontracts.CatalogTypeList, runtimecontracts.CatalogTypeMap:
		kind = string(resolved.Kind)
	default:
		return wave1ResolvedType{}, "", fmt.Errorf("entity path %q has no declared typed leaf for %q", ref, current)
	}
	return wave1ResolvedType{Kind: kind, Type: current, IsOptional: optional}, view.FlowID, nil
}

func wave1ResolvePlatformEntityPath(ref string) (wave1ResolvedType, error) {
	ref = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(ref), platformcontext.EntityRoot+"."))
	if ref == "" {
		return wave1ResolvedType{}, fmt.Errorf("_entity field path is required")
	}
	segments := strings.Split(ref, ".")
	head := strings.TrimSpace(segments[0])
	if head == "gates" {
		if len(segments) == 1 {
			return wave1ResolvedType{Kind: "named", Type: "gates"}, nil
		}
		if len(segments) > 2 {
			return wave1ResolvedType{}, fmt.Errorf("_entity.gates supports one gate name segment; got %q", ref)
		}
		return wave1ResolvedType{Kind: "scalar", Type: "boolean"}, nil
	}
	if resolved, ok := wave1PlatformEntityTypes[head]; ok {
		if len(segments) > 1 {
			return wave1ResolvedType{}, fmt.Errorf("_entity.%s is a platform scalar and does not support nested path %q", head, ref)
		}
		return resolved, nil
	}
	if platformcontext.EntityFieldUnsupported(head) {
		return wave1ResolvedType{}, fmt.Errorf("_entity.%s is not exposed; supported _entity fields are id, current_state, flow_instance, and gates", head)
	}
	return wave1ResolvedType{}, fmt.Errorf("_entity.%s is not a supported platform entity metadata field", head)
}

func legacyEntityMetadataDiagnostic(field string) string {
	field = strings.TrimSpace(field)
	switch field {
	case "entity_id":
		return "entity.entity_id is platform entity metadata; use _entity.id"
	case "current_state", "flow_instance", "gates":
		return fmt.Sprintf("entity.%s is platform entity metadata; use _entity.%s", field, field)
	default:
		return fmt.Sprintf("entity.%s is platform entity metadata and is not supported through entity.*", field)
	}
}

func wave1ResolveDeclaredEntityType(types runtimecontracts.TypeCatalogDocument, typeRef string) (runtimecontracts.ResolvedCatalogType, error) {
	if err := runtimecontracts.ValidateWave1TypeReference(typeRef, "entity field"); err != nil {
		return runtimecontracts.ResolvedCatalogType{}, err
	}
	return (runtimecontracts.CatalogTypeReference{Type: typeRef, Catalog: types}).Resolve()
}

func defaultFlowLabel(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "." {
		return "root"
	}
	return flowID
}

func executableNodeDiagnostic(node runtimeidentity.ExecutableNode) string {
	if !node.Valid() {
		return "node <invalid>"
	}
	parts := make([]string, 0, 3)
	if node.FlowPath() != "" {
		parts = append(parts, "flow "+node.FlowPath())
	}
	parts = append(parts, "node "+node.NodeID())
	return strings.Join(parts, " ")
}
