package contracts

import (
	"fmt"
	"strings"
)

type FlowInputInstanceSourceKind string

const (
	FlowInputInstanceSourcePayload       FlowInputInstanceSourceKind = "payload"
	FlowInputInstanceSourceEventID       FlowInputInstanceSourceKind = "event_id"
	FlowInputInstanceSourceGeneratedUUID FlowInputInstanceSourceKind = "generated_uuid"
)

type FlowInputInstanceSource struct {
	Kind FlowInputInstanceSourceKind
	Path string
}

// FlowInputInstanceSourceTypeEvidence is the complete type evidence for one
// receiver-owned instance key. SourceType always comes from the event contract
// or an intrinsic platform source; CarryType is only an optional assertion.
type FlowInputInstanceSourceTypeEvidence struct {
	Field        TemplateInstanceField
	Source       FlowInputInstanceSource
	SourceType   CatalogTypeReference
	CarryType    CatalogTypeReference
	ReceiverType CatalogTypeReference
}

// FlowInputInstanceEventCatalog is the narrow event-schema capability needed
// by source type resolution. Semantic overlays use it to expose admitted
// provider schemas without moving type ownership out of this package.
type FlowInputInstanceEventCatalog interface {
	ResolveFlowEventCatalogEntry(flowID, eventType string) (EventCatalogEntry, string, bool)
}

func ResolveFlowInputInstanceSource(mode FlowInputResolutionMode, raw string) (FlowInputInstanceSource, error) {
	path := strings.TrimSpace(raw)
	switch path {
	case FlowInputCarrySourceGeneratedUUID:
		if mode != FlowInputResolutionModeCreate {
			return FlowInputInstanceSource{}, fmt.Errorf("generated.uuid is only valid for resolution mode create; selecting pins must source an existing payload field")
		}
		return FlowInputInstanceSource{Kind: FlowInputInstanceSourceGeneratedUUID, Path: path}, nil
	case FlowInputCarrySourceEventID:
		if mode != FlowInputResolutionModeCreate {
			return FlowInputInstanceSource{}, fmt.Errorf("event.id is only valid for resolution mode create; selecting pins must source an existing payload field")
		}
		return FlowInputInstanceSource{Kind: FlowInputInstanceSourceEventID, Path: path}, nil
	}
	if strings.HasPrefix(path, "generated.") {
		return FlowInputInstanceSource{}, fmt.Errorf("only generated.uuid is supported")
	}
	if !topLevelPayloadSource(path) {
		return FlowInputInstanceSource{}, fmt.Errorf("use one top-level payload.<field>, event.id, or generated.uuid source")
	}
	return FlowInputInstanceSource{Kind: FlowInputInstanceSourcePayload, Path: path}, nil
}

func (s FlowInputInstanceSource) RequiresDeliveryProjection() bool {
	return s.Kind == FlowInputInstanceSourceGeneratedUUID || s.Kind == FlowInputInstanceSourceEventID
}

// ResolveFlowInputInstanceSourceType centralizes source parsing, authoritative
// source-type resolution, optional carry assertion, and receiver compatibility.
func (b *WorkflowContractBundle) ResolveFlowInputInstanceSourceType(eventCatalog FlowInputInstanceEventCatalog, flowID string, pin FlowInputEventPin, instance TemplateInstanceContract) (FlowInputInstanceSourceTypeEvidence, error) {
	if instance.Field.Empty() {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("receiver flow %s must declare instance: <field>", strings.TrimSpace(flowID))
	}
	field := instance.Field.String()
	carry, ok := pin.Carries[field]
	if !ok {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("flow %s is one instance per %s; input pin %s must declare a carry named %s (add carries: %s: {from: payload.<field>})", strings.TrimSpace(flowID), field, pin.PinName(), field, field)
	}
	source, err := ResolveFlowInputInstanceSource(pin.Resolution.Mode, carry.From)
	if err != nil {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("carry %s source %q is invalid for resolution mode %s: %w", field, strings.TrimSpace(carry.From), pin.Resolution.Mode.String(), err)
	}

	receiverDecl, ok := instance.PrimaryEntity.Contract.Fields[field]
	if !ok || strings.TrimSpace(receiverDecl.Type) == "" {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("receiver entity.%s has no declared type", field)
	}
	evidence := FlowInputInstanceSourceTypeEvidence{
		Field:        instance.Field,
		Source:       source,
		ReceiverType: CatalogTypeReference{Type: strings.TrimSpace(receiverDecl.Type), Catalog: cloneTypeCatalogDocument(instance.PrimaryEntity.Types)},
	}

	switch source.Kind {
	case FlowInputInstanceSourcePayload:
		sourceField := strings.TrimPrefix(source.Path, "payload.")
		resolved, ok := resolveFlowInputInstanceEventFieldType(b, eventCatalog, flowID, pin.EventType(), sourceField)
		if !ok {
			return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("carry %s source %s has no declared type on input event %s", field, source.Path, pin.EventType())
		}
		evidence.SourceType = resolved
	case FlowInputInstanceSourceGeneratedUUID, FlowInputInstanceSourceEventID:
		evidence.SourceType = CatalogTypeReference{Type: "uuid"}
	default:
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("carry %s source %q has unsupported typed source kind %q", field, source.Path, source.Kind)
	}

	if declared := strings.TrimSpace(carry.Type); declared != "" {
		evidence.CarryType = CatalogTypeReference{Type: declared, Catalog: cloneTypeCatalogDocument(instance.PrimaryEntity.Types)}
		if err := requireInstanceSourceTypesCompatible(evidence.SourceType, evidence.CarryType); err != nil {
			return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("key_types_incompatible: carry %s source %s actual type %s is incompatible with declared carry type %s: %w", field, source.Path, evidence.SourceType.Type, declared, err)
		}
	}
	if err := requireInstanceSourceTypesCompatible(evidence.SourceType, evidence.ReceiverType); err != nil {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("key_types_incompatible: carry %s source %s actual type %s is incompatible with receiver entity.%s type %s: %w", field, source.Path, evidence.SourceType.Type, field, evidence.ReceiverType.Type, err)
	}
	return evidence, nil
}

func resolveFlowInputInstanceEventFieldType(bundle *WorkflowContractBundle, eventCatalog FlowInputInstanceEventCatalog, flowID, eventType, field string) (CatalogTypeReference, bool) {
	if resolved, ok := ResolveEventFieldType(bundle, flowID, eventType, field); ok {
		return resolved, true
	}
	if eventCatalog == nil {
		return CatalogTypeReference{}, false
	}
	entry, _, ok := eventCatalog.ResolveFlowEventCatalogEntry(flowID, eventType)
	if !ok {
		return CatalogTypeReference{}, false
	}
	decl, ok := entry.Payload.Properties[strings.TrimSpace(field)]
	if !ok || strings.TrimSpace(decl.Type) == "" {
		return CatalogTypeReference{}, false
	}
	var catalog TypeCatalogDocument
	if bundle != nil {
		catalog = bundle.ResolvedTypeCatalogForFlow(flowID)
	}
	return CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(catalog)}, true
}

func requireInstanceSourceTypesCompatible(source, target CatalogTypeReference) error {
	sourceType, err := source.Resolve()
	if err != nil {
		return fmt.Errorf("source type %q does not resolve: %w", source.Type, err)
	}
	targetType, err := target.Resolve()
	if err != nil {
		return fmt.Errorf("target type %q does not resolve: %w", target.Type, err)
	}
	sourceFamily := instanceSourceScalarFamily(sourceType.Kind)
	targetFamily := instanceSourceScalarFamily(targetType.Kind)
	if sourceFamily == "" || targetFamily == "" || sourceFamily != targetFamily {
		return fmt.Errorf("resolved scalar families are %s and %s", sourceType.Kind, targetType.Kind)
	}
	return nil
}

func instanceSourceScalarFamily(kind CatalogTypeKind) CatalogTypeKind {
	switch kind {
	case CatalogTypeText:
		return CatalogTypeText
	case CatalogTypeInteger, CatalogTypeNumber:
		return CatalogTypeNumber
	case CatalogTypeBoolean:
		return CatalogTypeBoolean
	default:
		return ""
	}
}

func topLevelPayloadSource(path string) bool {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "payload.") {
		return false
	}
	field := strings.TrimSpace(strings.TrimPrefix(path, "payload."))
	return field != "" && !strings.Contains(field, ".") && path == "payload."+field
}
