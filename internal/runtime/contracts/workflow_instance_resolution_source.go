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

func topLevelPayloadSource(path string) bool {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "payload.") {
		return false
	}
	field := strings.TrimSpace(strings.TrimPrefix(path, "payload."))
	return field != "" && !strings.Contains(field, ".") && path == "payload."+field
}
