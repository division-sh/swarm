package operatorread

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrRunNotFound                           = errors.New("operator read: run not found")
	ErrInvalidRunListCursor                  = errors.New("operator read: invalid run list cursor")
	ErrEventNotFound                         = errors.New("operator read: event not found")
	ErrInvalidObservabilityCursor            = errors.New("operator read: invalid observability cursor")
	ErrEntityNotFound                        = errors.New("operator read: entity not found")
	ErrAgentNotFound                         = errors.New("operator read: agent not found")
	ErrAgentTargetAmbiguous                  = errors.New("operator read: agent target is ambiguous")
	ErrSessionNotFound                       = errors.New("operator read: session not found")
	ErrTurnNotFound                          = errors.New("operator read: turn not found")
	ErrAmbiguousEntityRunID                  = errors.New("operator read: ambiguous entity run_id")
	ErrInvalidEntityCursor                   = errors.New("operator read: invalid entity cursor")
	ErrInvalidConversationCursor             = errors.New("operator read: invalid conversation cursor")
	ErrInvalidPendingAgentDeliveryCursor     = errors.New("operator read: invalid pending agent delivery cursor")
	ErrInvalidEntityReadParam                = errors.New("operator read: invalid entity read parameter")
	ErrInvalidAgentDeliveryDiagnosticsCursor = errors.New("operator read: invalid agent delivery diagnostics cursor")
	ErrInvalidAgentDeliveryLifecycleCursor   = errors.New("operator read: invalid agent delivery lifecycle cursor")
	ErrInvalidAgentDeliveryLifecycleStatus   = errors.New("operator read: invalid agent delivery lifecycle status")
)

func IsAgentTargetAmbiguous(err error) bool { return errors.Is(err, ErrAgentTargetAmbiguous) }

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

func (e *EntityReadParamError) Is(target error) bool { return target == ErrInvalidEntityReadParam }

func (e AgentDeliveryDiagnosticsCursorError) Error() string {
	field := strings.TrimSpace(e.Field)
	if field == "" {
		field = "cursor"
	}
	return fmt.Sprintf("invalid agent delivery diagnostics %s", field)
}

func (e AgentDeliveryDiagnosticsCursorError) Unwrap() error {
	return ErrInvalidAgentDeliveryDiagnosticsCursor
}

func (AgentDeliveryLifecycleCursorError) Error() string {
	return "invalid agent delivery lifecycle cursor"
}

func (AgentDeliveryLifecycleCursorError) Unwrap() error {
	return ErrInvalidAgentDeliveryLifecycleCursor
}

func (e AgentDeliveryLifecycleStatusError) Error() string {
	status := strings.TrimSpace(e.Status)
	if status == "" {
		return "invalid agent delivery lifecycle status"
	}
	return fmt.Sprintf("invalid agent delivery lifecycle status %q", status)
}

func (e AgentDeliveryLifecycleStatusError) Unwrap() error {
	return ErrInvalidAgentDeliveryLifecycleStatus
}
