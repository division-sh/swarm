package agentcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

const DirectiveOperationMethod = "agent.send_directive"

type DirectiveOperationState string

const (
	DirectiveOperationPrepared      DirectiveOperationState = "prepared"
	DirectiveOperationExecuting     DirectiveOperationState = "executing"
	DirectiveOperationExecuted      DirectiveOperationState = "executed"
	DirectiveOperationSucceeded     DirectiveOperationState = "succeeded"
	DirectiveOperationFailed        DirectiveOperationState = "failed"
	DirectiveOperationIndeterminate DirectiveOperationState = "indeterminate"
)

var (
	ErrDirectiveInProgress           = errors.New("agent directive in progress")
	ErrDirectiveCompletionPending    = errors.New("agent directive completion pending")
	ErrDirectiveExecutionFailed      = errors.New("agent directive execution failed")
	ErrDirectiveOutcomeIndeterminate = errors.New("agent directive outcome indeterminate")
	ErrDirectiveTransitionConflict   = errors.New("agent directive transition conflict")
	ErrDirectiveIdempotencyConflict  = errors.New("agent directive idempotency conflict")
	ErrDirectiveProviderDrained      = errors.New("agent directive provider attempt drained")
)

type DirectiveOperation struct {
	OperationID             string
	Method                  string
	ActorTokenID            string
	IdempotencyKey          string
	RequestHash             string
	AgentIdentity           runtimeagentidentity.Identity
	Directive               string
	RequestedRunID          string
	ResolvedRunID           string
	RunIDResolution         string
	Source                  string
	OperatorID              string
	DirectiveEventID        string
	State                   DirectiveOperationState
	ExecutionOwnerID        string
	ExecutionLeaseExpiresAt time.Time
	Response                json.RawMessage
	Failure                 *runtimefailures.Envelope
	ExecutionAdmittedAt     time.Time
	ExecutedAt              time.Time
	CompletedAt             time.Time
	CreatedAt               time.Time
	UpdatedAt               time.Time
	ExpiresAt               time.Time
}

func (o DirectiveOperation) Normalized() DirectiveOperation {
	o.OperationID = strings.TrimSpace(o.OperationID)
	o.Method = strings.TrimSpace(o.Method)
	o.ActorTokenID = strings.TrimSpace(o.ActorTokenID)
	o.IdempotencyKey = strings.TrimSpace(o.IdempotencyKey)
	o.RequestHash = strings.TrimSpace(o.RequestHash)
	o.AgentIdentity = o.AgentIdentity.Normalize()
	o.Directive = strings.TrimSpace(o.Directive)
	o.RequestedRunID = strings.TrimSpace(o.RequestedRunID)
	o.ResolvedRunID = strings.TrimSpace(o.ResolvedRunID)
	o.RunIDResolution = strings.TrimSpace(o.RunIDResolution)
	o.Source = strings.TrimSpace(o.Source)
	o.OperatorID = strings.TrimSpace(o.OperatorID)
	o.DirectiveEventID = strings.TrimSpace(o.DirectiveEventID)
	o.ExecutionOwnerID = strings.TrimSpace(o.ExecutionOwnerID)
	o.Failure = runtimefailures.CloneEnvelope(o.Failure)
	return o
}

func (o DirectiveOperation) AgentID() string {
	return o.AgentIdentity.AgentID()
}

func (o DirectiveOperation) FlowInstance() string {
	return o.AgentIdentity.FlowInstance()
}

func ValidateDirectiveOperationEvidence(op DirectiveOperation) error {
	op = op.Normalized()
	if err := op.AgentIdentity.Validate(); err != nil {
		return fmt.Errorf("directive operation agent identity is invalid: %w", err)
	}
	if op.RequestedRunID != "" && op.RequestedRunID != op.AgentIdentity.RunID {
		return fmt.Errorf("directive operation requested run does not match concrete agent identity")
	}
	if op.ResolvedRunID != "" && op.ResolvedRunID != op.AgentIdentity.RunID {
		return fmt.Errorf("directive operation resolved run does not match concrete agent identity")
	}
	hasResponse := len(bytes.TrimSpace(op.Response)) > 0
	hasFailure := op.Failure != nil
	wantsResponse := op.State == DirectiveOperationExecuted || op.State == DirectiveOperationSucceeded
	wantsFailure := op.State == DirectiveOperationFailed || op.State == DirectiveOperationIndeterminate
	if hasResponse != wantsResponse {
		return fmt.Errorf("directive operation state %s response presence = %t, want %t", op.State, hasResponse, wantsResponse)
	}
	if hasFailure != wantsFailure {
		return fmt.Errorf("directive operation state %s failure presence = %t, want %t", op.State, hasFailure, wantsFailure)
	}
	if hasFailure {
		if err := runtimefailures.ValidateEnvelope(*op.Failure); err != nil {
			return fmt.Errorf("directive operation state %s failure is invalid: %w", op.State, err)
		}
	}
	return nil
}

type ReserveDirectiveOperationRequest struct {
	Operation DirectiveOperation
	Event     events.AdmittedEvent
	Now       time.Time
}

type DirectiveOperationReservation struct {
	Operation DirectiveOperation
	Created   bool
}

type DirectiveOperationReconcileResult struct {
	Finalized     int
	Repaired      int
	Failed        int
	Indeterminate int
	Deleted       int
}

type DirectiveExecutionAdmissionRequest struct {
	OperationID      string
	OwnerID          string
	Now              time.Time
	Lease            time.Duration
	ExecutionPosture executionposture.Posture
}

type DirectiveExecutionAdmission struct {
	Operation DirectiveOperation
	Event     events.AdmittedEvent
}

// DirectiveExecutionOrigin is the exact durable authority for provider work
// launched while executing one admitted board directive.
type DirectiveExecutionOrigin struct {
	OperationID      string
	ExecutionOwnerID string
}

func NewDirectiveExecutionOrigin(op DirectiveOperation) (DirectiveExecutionOrigin, error) {
	op = op.Normalized()
	origin := DirectiveExecutionOrigin{
		OperationID:      op.OperationID,
		ExecutionOwnerID: op.ExecutionOwnerID,
	}
	if err := origin.Validate(); err != nil {
		return DirectiveExecutionOrigin{}, err
	}
	if op.State != DirectiveOperationExecuting {
		return DirectiveExecutionOrigin{}, fmt.Errorf("directive provider origin requires an executing operation")
	}
	return origin, nil
}

func (o DirectiveExecutionOrigin) Normalize() DirectiveExecutionOrigin {
	o.OperationID = strings.TrimSpace(o.OperationID)
	o.ExecutionOwnerID = strings.TrimSpace(o.ExecutionOwnerID)
	return o
}

func (o DirectiveExecutionOrigin) Validate() error {
	o = o.Normalize()
	for name, value := range map[string]string{
		"operation_id": o.OperationID, "execution_owner_id": o.ExecutionOwnerID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("directive provider origin %s must be a UUID: %w", name, err)
		}
	}
	return nil
}

func (o DirectiveExecutionOrigin) Same(other DirectiveExecutionOrigin) bool {
	o, other = o.Normalize(), other.Normalize()
	return o.OperationID == other.OperationID && o.ExecutionOwnerID == other.ExecutionOwnerID
}

type DirectiveOperationStore interface {
	ReserveDirectiveOperation(context.Context, ReserveDirectiveOperationRequest) (DirectiveOperationReservation, error)
	AdmitDirectiveExecution(context.Context, DirectiveExecutionAdmissionRequest) (DirectiveExecutionAdmission, error)
	RenewDirectiveExecutionLease(context.Context, string, string, time.Time, time.Duration) error
	RecordDirectiveExecuted(context.Context, string, string, json.RawMessage, time.Time) (DirectiveOperation, error)
	FinalizeDirectiveSuccess(context.Context, string, time.Time, time.Duration) (DirectiveOperation, error)
	FinalizeDirectiveFailure(context.Context, string, string, runtimefailures.Envelope, time.Time, time.Duration) (DirectiveOperation, error)
	LoadDirectiveOperation(context.Context, string) (DirectiveOperation, bool, error)
	LoadDirectiveOperationByKey(context.Context, string, string, string) (DirectiveOperation, bool, error)
	ReconcileDirectiveOperation(context.Context, string, time.Time, time.Duration) (DirectiveOperation, bool, error)
	ReconcileDirectiveOperations(context.Context, time.Time, time.Duration) (DirectiveOperationReconcileResult, error)
}

type DirectiveOperationError struct {
	Err       error
	Operation DirectiveOperation
}

type DirectiveIdempotencyConflictError struct {
	OriginalRequestHash    string
	ConflictingRequestHash string
	OperationID            string
}

func (e *DirectiveIdempotencyConflictError) Error() string {
	return ErrDirectiveIdempotencyConflict.Error()
}

func (e *DirectiveIdempotencyConflictError) Is(target error) bool {
	return target == ErrDirectiveIdempotencyConflict
}

func (e *DirectiveOperationError) Error() string {
	if e == nil {
		return ""
	}
	op := e.Operation.Normalized()
	base := "agent directive operation failed"
	if e.Err != nil {
		base = e.Err.Error()
	}
	if op.OperationID == "" {
		return base
	}
	return fmt.Sprintf("%s: operation_id=%s state=%s", base, op.OperationID, op.State)
}

func (e *DirectiveOperationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func ErrorForDirectiveOperation(op DirectiveOperation) error {
	op = op.Normalized()
	var err error
	switch op.State {
	case DirectiveOperationExecuting:
		err = ErrDirectiveInProgress
	case DirectiveOperationExecuted:
		err = ErrDirectiveCompletionPending
	case DirectiveOperationFailed:
		err = ErrDirectiveExecutionFailed
	case DirectiveOperationIndeterminate:
		err = ErrDirectiveOutcomeIndeterminate
	default:
		err = ErrDirectiveTransitionConflict
	}
	return &DirectiveOperationError{Err: err, Operation: op}
}
