package store

import "errors"

var ErrUnknownSchemaType = errors.New("store: unknown schema type")

var ErrRunNotFound = errors.New("store: run not found")

var ErrInvalidRunListCursor = errors.New("store: invalid run list cursor")

var ErrEventNotFound = errors.New("store: event not found")

var ErrInvalidObservabilityCursor = errors.New("store: invalid observability cursor")

var ErrEntityNotFound = errors.New("store: entity not found")

var ErrAgentNotFound = errors.New("store: agent not found")

var ErrSessionNotFound = errors.New("store: session not found")

var ErrTurnNotFound = errors.New("store: turn not found")

var ErrAmbiguousEntityRunID = errors.New("store: ambiguous entity run_id")

var ErrInvalidEntityCursor = errors.New("store: invalid entity cursor")

var ErrInvalidConversationCursor = errors.New("store: invalid conversation cursor")

var ErrInvalidPendingAgentDeliveryCursor = errors.New("store: invalid pending agent delivery cursor")

var ErrOperatorConversationRunIDCapability = errors.New("store: operator conversation read surface run_id capability unavailable")

var ErrInvalidEntityReadParam = errors.New("store: invalid entity read parameter")

type EntityReadParamError struct {
	Field  string
	Reason string
}

func (e *EntityReadParamError) Error() string {
	if e == nil {
		return ErrInvalidEntityReadParam.Error()
	}
	return ErrInvalidEntityReadParam.Error() + ": " + e.Field + ": " + e.Reason
}

func (e *EntityReadParamError) Is(target error) bool {
	return target == ErrInvalidEntityReadParam
}
