package bootverify

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
	Kind string
	Type string
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
	if view.FlowID == "" {
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

func wave1PinFieldName(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "entity.")
	if idx := strings.IndexByte(raw, '.'); idx >= 0 {
		return strings.TrimSpace(raw[:idx])
	}
	return strings.TrimSpace(raw)
}

func wave1RootFieldContract(source semanticview.Source, field string) (wave1EntityContractView, bool) {
	field = strings.TrimSpace(field)
	if field == "" {
		return wave1EntityContractView{}, false
	}
	root := wave1EntityContractForFlow(source, "")
	if !root.Defined {
		return wave1EntityContractView{}, false
	}
	_, ok := root.Contract.Fields[field]
	return root, ok
}

func wave1FlowReadsRootField(source semanticview.Source, flowID, field string) bool {
	flowID = strings.TrimSpace(flowID)
	field = strings.TrimSpace(field)
	if flowID == "" || field == "" {
		return false
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return false
	}
	for _, pin := range bundle.FlowReadPins(flowID) {
		if wave1PinFieldName(pin) == field {
			return true
		}
	}
	return false
}

func wave1FlowWritesRootField(source semanticview.Source, flowID, field string) bool {
	flowID = strings.TrimSpace(flowID)
	field = strings.TrimSpace(field)
	if flowID == "" || field == "" {
		return false
	}
	for _, pin := range source.FlowWritePins(flowID) {
		if wave1PinFieldName(pin) == field {
			return true
		}
	}
	return false
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
	view := wave1EntityContractForFlow(source, flowID)
	if !view.Defined || view.Contract.Fields == nil {
		if flowID != "" && wave1FlowReadsRootField(source, flowID, head) {
			root, ok := wave1RootFieldContract(source, head)
			if ok {
				view = root
			}
		}
	}
	if !view.Defined {
		return wave1ResolvedType{}, "", fmt.Errorf("flow %s has no declared Wave 1 entity contract for entity.%s", defaultFlowLabel(flowID), head)
	}
	field, ok := view.Contract.Fields[head]
	if !ok && flowID != "" && view.FlowID != "" && wave1FlowReadsRootField(source, flowID, head) {
		root, rootOK := wave1RootFieldContract(source, head)
		if rootOK {
			view = root
			field = root.Contract.Fields[head]
			ok = true
		}
	}
	if !ok {
		if platformcontext.LegacyEntityMetadataField(head) {
			return wave1ResolvedType{}, "", fmt.Errorf("%s", legacyEntityMetadataDiagnostic(head))
		}
		return wave1ResolvedType{}, "", fmt.Errorf("flow %s entity_type %s does not declare field %q", defaultFlowLabel(flowID), view.EntityType, head)
	}
	current := strings.TrimSpace(field.Type)
	if current == "" {
		return wave1ResolvedType{}, "", fmt.Errorf("flow %s entity_type %s field %q has empty type", defaultFlowLabel(flowID), view.EntityType, head)
	}
	for idx := 1; idx < len(segments); idx++ {
		segment := strings.TrimSpace(segments[idx])
		if segment == "" {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q contains empty segment", ref)
		}
		if _, ok := wave1ListElementType(current); ok {
			if segment == "size" && idx == len(segments)-1 {
				return wave1ResolvedType{Kind: "scalar", Type: "integer"}, view.FlowID, nil
			}
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q traverses list type %q through unsupported segment %q", ref, current, segment)
		}
		kind, named, err := wave1ResolveNamedType(view.Types, current)
		if err != nil {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q: %w", ref, err)
		}
		if kind != "named" {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q cannot traverse non-composite type %q through segment %q", ref, current, segment)
		}
		fieldSpec, ok := named.Fields[segment]
		if !ok {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q references undeclared nested field %q", ref, segment)
		}
		current = strings.TrimSpace(fieldSpec.Type)
		if current == "" {
			return wave1ResolvedType{}, "", fmt.Errorf("entity path %q nested field %q has empty type", ref, segment)
		}
	}
	kind, _, err := wave1ResolveNamedType(view.Types, current)
	if err != nil {
		return wave1ResolvedType{}, "", fmt.Errorf("entity path %q: %w", ref, err)
	}
	return wave1ResolvedType{Kind: kind, Type: current}, view.FlowID, nil
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

func wave1ResolveNamedType(types runtimecontracts.TypeCatalogDocument, typeRef string) (string, runtimecontracts.NamedTypeDecl, error) {
	typeRef = strings.TrimSpace(typeRef)
	if typeRef == "" {
		return "", runtimecontracts.NamedTypeDecl{}, fmt.Errorf("type reference is required")
	}
	if elemType, ok := wave1ListElementType(typeRef); ok {
		if _, _, err := wave1ResolveNamedType(types, elemType); err != nil {
			return "", runtimecontracts.NamedTypeDecl{}, err
		}
		return "list", runtimecontracts.NamedTypeDecl{}, nil
	}
	if wave1BuiltinScalar(typeRef) {
		return "scalar", runtimecontracts.NamedTypeDecl{}, nil
	}
	if _, ok := types.Enums[typeRef]; ok {
		return "enum", runtimecontracts.NamedTypeDecl{}, nil
	}
	if named, ok := types.Types[typeRef]; ok {
		return "named", named, nil
	}
	if scalar, ok := types.Scalars[typeRef]; ok {
		base := strings.TrimSpace(scalar.Base)
		if base == "" {
			return "", runtimecontracts.NamedTypeDecl{}, fmt.Errorf("scalar %q has empty base", typeRef)
		}
		return wave1ResolveNamedType(types, base)
	}
	return "", runtimecontracts.NamedTypeDecl{}, fmt.Errorf("type %q is not declared in the resolved type catalog", typeRef)
}

func wave1BuiltinScalar(typeRef string) bool {
	typeRef = strings.TrimSpace(strings.ToLower(typeRef))
	switch {
	case typeRef == "text",
		typeRef == "string",
		typeRef == "integer",
		typeRef == "boolean",
		typeRef == "timestamp",
		typeRef == "uuid",
		typeRef == "numeric",
		strings.HasPrefix(typeRef, "numeric("):
		return true
	default:
		return false
	}
}

func wave1ListElementType(typeRef string) (string, bool) {
	typeRef = strings.TrimSpace(typeRef)
	if len(typeRef) >= 2 && strings.HasPrefix(typeRef, "[") && strings.HasSuffix(typeRef, "]") {
		elem := strings.TrimSpace(typeRef[1 : len(typeRef)-1])
		return elem, elem != ""
	}
	return "", false
}

func defaultFlowLabel(flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return "root"
	}
	return flowID
}

func wave1EntityEnvelopeField(field string) bool {
	return platformcontext.LegacyEntityMetadataField(field)
}
