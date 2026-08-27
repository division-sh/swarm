package contracts

import (
	"fmt"
	"slices"
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
// or an intrinsic platform source.
type FlowInputInstanceSourceTypeEvidence struct {
	Field        TemplateInstanceField
	Source       FlowInputInstanceSource
	SourceType   CatalogTypeReference
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
	case FlowInputInstanceSourceGeneratedUUIDPath:
		if mode != FlowInputResolutionModeCreate {
			return FlowInputInstanceSource{}, fmt.Errorf("generated.uuid is only valid for resolution mode create; selecting pins must source an existing payload field")
		}
		return FlowInputInstanceSource{Kind: FlowInputInstanceSourceGeneratedUUID, Path: path}, nil
	case FlowInputInstanceSourceEventIDPath:
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
// source-type resolution, and receiver compatibility.
func (b *WorkflowContractBundle) ResolveFlowInputInstanceSourceType(eventCatalog FlowInputInstanceEventCatalog, flowID string, pin CompiledFlowInputPin, instance TemplateInstanceContract) (FlowInputInstanceSourceTypeEvidence, error) {
	return b.resolveFlowInputInstanceSourceType(eventCatalog, flowID, pin, instance, true)
}

func (b *WorkflowContractBundle) resolveFlowInputInstanceSourceType(eventCatalog FlowInputInstanceEventCatalog, flowID string, pin CompiledFlowInputPin, instance TemplateInstanceContract, requireCompatibility bool) (FlowInputInstanceSourceTypeEvidence, error) {
	if instance.Field.Empty() {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("receiver flow %s must declare instance: <field>", strings.TrimSpace(flowID))
	}
	field := instance.Field.Path()
	resolution := pin.Resolution()
	rawSource := strings.TrimSpace(resolution.From)
	defaultSource := "payload." + field
	if rawSource == "" {
		rawSource = defaultSource
	} else if rawSource == defaultSource {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("resolution.from %q is redundant; omit it to derive the receiver instance source", rawSource)
	}
	source, err := ResolveFlowInputInstanceSource(resolution.Mode, rawSource)
	if err != nil {
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("resolution source %q is invalid for mode %s: %w", rawSource, FlowInputResolutionModeCode(resolution.Mode), err)
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
		resolved, required, ok := resolveFlowInputInstanceEventFieldType(b, eventCatalog, flowID, pin.EventType(), sourceField)
		if !ok {
			return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("resolution source %s has no declared type on input event %s", source.Path, pin.EventType())
		}
		if !required {
			return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("resolution source %s must be a required producer event field", source.Path)
		}
		evidence.SourceType = resolved
	case FlowInputInstanceSourceGeneratedUUID, FlowInputInstanceSourceEventID:
		evidence.SourceType = CatalogTypeReference{Type: "uuid"}
	default:
		return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("resolution source %q has unsupported typed source kind %q", source.Path, source.Kind)
	}

	if requireCompatibility {
		if err := requireInstanceSourceTypesCompatible(evidence.SourceType, evidence.ReceiverType); err != nil {
			return FlowInputInstanceSourceTypeEvidence{}, fmt.Errorf("key_types_incompatible: resolution source %s type %s is incompatible with receiver entity.%s type %s: %w", source.Path, evidence.SourceType.Type, field, evidence.ReceiverType.Type, err)
		}
	}
	return evidence, nil
}

func resolveFlowInputInstanceEventFieldType(bundle *WorkflowContractBundle, eventCatalog FlowInputInstanceEventCatalog, flowID, eventType, field string) (CatalogTypeReference, bool, bool) {
	var entry EventCatalogEntry
	var ok bool
	if eventCatalog != nil {
		entry, _, ok = eventCatalog.ResolveFlowEventCatalogEntry(flowID, eventType)
	} else if bundle != nil {
		entry, _, ok = bundle.ResolveFlowEventCatalogEntry(flowID, eventType)
	}
	if !ok {
		return CatalogTypeReference{}, false, false
	}
	decl, ok := entry.Payload.Properties[strings.TrimSpace(field)]
	if !ok || strings.TrimSpace(decl.Type) == "" {
		return CatalogTypeReference{}, false, false
	}
	var catalog TypeCatalogDocument
	if bundle != nil {
		catalog = bundle.ResolvedTypeCatalogForFlow(flowID)
	}
	return CatalogTypeReference{Type: strings.TrimSpace(decl.Type), Catalog: cloneTypeCatalogDocument(catalog)}, slices.Contains(entry.Payload.Required, strings.TrimSpace(field)), true
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
	if !instanceSourceScalarAssignable(sourceType.Kind, targetType.Kind) {
		return fmt.Errorf("resolved scalar types are %s and %s", sourceType.Kind, targetType.Kind)
	}
	return nil
}

func instanceSourceScalarAssignable(source, target CatalogTypeKind) bool {
	switch source {
	case CatalogTypeText, CatalogTypeBoolean, CatalogTypeNumber:
		return source == target
	case CatalogTypeInteger:
		return target == CatalogTypeInteger || target == CatalogTypeNumber
	default:
		return false
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
