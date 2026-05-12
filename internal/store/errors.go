package store

import "errors"

var ErrUnknownSchemaType = errors.New("store: unknown schema type")

var ErrRunNotFound = errors.New("store: run not found")

var ErrInvalidRunListCursor = errors.New("store: invalid run list cursor")

var ErrEventNotFound = errors.New("store: event not found")

var ErrInvalidObservabilityCursor = errors.New("store: invalid observability cursor")

var ErrEntityNotFound = errors.New("store: entity not found")

var ErrAmbiguousEntityRunID = errors.New("store: ambiguous entity run_id")

var ErrInvalidEntityCursor = errors.New("store: invalid entity cursor")

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
