package engine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

var (
	ErrChainDepthExceeded           = errors.New("engine: chain depth exceeded")
	ErrMissingSemanticSource        = errors.New("engine: semantic source is required")
	ErrMissingStateRepo             = errors.New("engine: state repository is required")
	ErrMissingMutationOwner         = errors.New("engine: mutation owner is required")
	ErrMissingEntityLocker          = errors.New("engine: entity locker is required")
	ErrMissingDispatcher            = errors.New("engine: post-commit dispatcher is required")
	ErrMissingNodeID                = errors.New("engine: node id is required")
	ErrMissingNodeHandler           = errors.New("engine: node handler is required")
	ErrInvalidTransition            = errors.New("engine: invalid transition")
	ErrEmitPersistencePrerequisite  = errors.New("engine: emit persistence prerequisite missing")
	ErrEmitPayloadContractViolation = errors.New("engine: emit payload contract violation")
	ErrInvalidConfig                = errors.New("engine: invalid config")
	ErrNotImplemented               = errors.New("engine: not implemented")
	ErrFanOutBoundExceeded          = errors.New("engine: fan_out bound exceeded")
)

type EmitPayloadContractKind string

const (
	EmitPayloadSchemaUnresolved EmitPayloadContractKind = "schema_unresolved"
	EmitPayloadSchemaMismatch   EmitPayloadContractKind = "schema_mismatch"
	EmitPayloadEnvelopeField    EmitPayloadContractKind = "authored_envelope_field"
)

type EmitPayloadContractError struct {
	Event      string
	Kind       EmitPayloadContractKind
	Path       string
	Constraint string
	Expected   string
	Actual     string
	Detail     string
	Cause      error
}

func (e *EmitPayloadContractError) Error() string {
	if e == nil {
		return ErrEmitPayloadContractViolation.Error()
	}
	return fmt.Sprintf("%s: event %s %s", ErrEmitPayloadContractViolation, strings.TrimSpace(e.Event), strings.TrimSpace(e.Detail))
}

func (e *EmitPayloadContractError) Is(target error) bool {
	return target == ErrEmitPayloadContractViolation
}

func (e *EmitPayloadContractError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *EmitPayloadContractError) Attributes() map[string]any {
	if e == nil {
		return nil
	}
	return map[string]any{
		"event":      strings.TrimSpace(e.Event),
		"kind":       string(e.Kind),
		"path":       strings.TrimSpace(e.Path),
		"constraint": strings.TrimSpace(e.Constraint),
		"expected":   strings.TrimSpace(e.Expected),
		"actual":     strings.TrimSpace(e.Actual),
		"detail":     strings.TrimSpace(e.Detail),
	}
}

// IsEmitPayloadContractFailure accepts only the live typed error or its exact
// normalized envelope. Fan-out uses this owner to distinguish authored item
// rejection from infrastructure and planner failures that must not advance.
func IsEmitPayloadContractFailure(err error) bool {
	var typed *EmitPayloadContractError
	if errors.As(err, &typed) && typed != nil {
		return true
	}
	normalized, ok := failures.As(err)
	if !ok || normalized == nil || normalized.Failure.Class != failures.ClassSchemaInvalid ||
		normalized.Failure.Detail.Code != "emit_payload_contract_violation" ||
		!normalized.Failure.Deterministic || normalized.Failure.Retryable {
		return false
	}
	attributes := normalized.Failure.Detail.Attributes
	for _, name := range []string{"event", "kind", "path", "constraint", "expected", "actual", "detail"} {
		value, present := attributes[name].(string)
		if !present || strings.TrimSpace(value) == "" {
			return false
		}
	}
	switch EmitPayloadContractKind(attributes["kind"].(string)) {
	case EmitPayloadSchemaUnresolved, EmitPayloadSchemaMismatch, EmitPayloadEnvelopeField:
		return true
	default:
		return false
	}
}
