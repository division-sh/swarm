package channelonboarding

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Disposable #2353 diagnostic, never intended for a product or landing branch.
const signingDiagnosticRecordLimit = 256
const signingDiagnosticCauseLimit = 64
const signingDiagnosticDepthLimit = 32

type SigningDiagnosticCause struct {
	Parent   int    `json:"parent"`
	Type     string `json:"type"`
	Sentinel string `json:"sentinel,omitempty"`
	SQLState string `json:"sqlstate,omitempty"`
}

type SigningDiagnosticRecord struct {
	Call        uint64                   `json:"call"`
	Method      string                   `json:"method"`
	OperationID string                   `json:"operation_id,omitempty"`
	Revision    int64                    `json:"revision,omitempty"`
	Phase       string                   `json:"phase"`
	Provenance  string                   `json:"provenance"`
	ElapsedNS   int64                    `json:"elapsed_ns"`
	Failed      bool                     `json:"failed"`
	Incomplete  bool                     `json:"incomplete"`
	Causes      []SigningDiagnosticCause `json:"causes,omitempty"`
}

type SigningDiagnostic struct {
	mu         sync.Mutex
	emit       func(SigningDiagnosticRecord)
	records    []SigningDiagnosticRecord
	next       uint64
	active     int
	closed     bool
	incomplete bool
}

func NewSigningDiagnostic(emit func(SigningDiagnosticRecord)) *SigningDiagnostic {
	return &SigningDiagnostic{emit: emit}
}

func (d *SigningDiagnostic) Dispose() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.emit = nil
	d.incomplete = d.incomplete || d.active != 0
	return !d.incomplete
}

func (d *SigningDiagnostic) Snapshot() ([]SigningDiagnosticRecord, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]SigningDiagnosticRecord(nil), d.records...), !d.incomplete
}

type signingDiagnosticContextKey struct{}

type signingDiagnosticCall struct {
	mu        sync.Mutex
	owner     *SigningDiagnostic
	id        uint64
	method    string
	operation string
	revision  int64
	phase     string
	started   time.Time
}

func (s *Service) beginSigningDiagnostic(ctx context.Context, method string) (context.Context, *signingDiagnosticCall) {
	if s == nil || s.testSigningDiagnostic == nil {
		return ctx, nil
	}
	d := s.testSigningDiagnostic
	d.mu.Lock()
	if d.closed {
		d.incomplete = true
		d.mu.Unlock()
		return ctx, nil
	}
	d.next++
	d.active++
	call := &signingDiagnosticCall{owner: d, id: d.next, method: method, phase: "entry", started: time.Now()}
	d.mu.Unlock()
	return context.WithValue(ctx, signingDiagnosticContextKey{}, call), call
}

func (c *signingDiagnosticCall) finish(err error) {
	if c == nil {
		return
	}
	c.record("return", err)
	c.owner.mu.Lock()
	c.owner.active--
	c.owner.mu.Unlock()
}

func SigningDiagnosticPhase(ctx context.Context, phase string) {
	if ctx == nil {
		return
	}
	if c, _ := ctx.Value(signingDiagnosticContextKey{}).(*signingDiagnosticCall); c != nil {
		if !signingDiagnosticKnownPhase(phase) {
			phase = "unknown_phase"
			c.owner.mu.Lock()
			c.owner.incomplete = true
			c.owner.mu.Unlock()
		}
		c.mu.Lock()
		c.phase = phase
		c.mu.Unlock()
	}
}

func signingDiagnosticKnownPhase(phase string) bool {
	switch phase {
	case "entry", "start.validate_input", "start.principal", "start.catalog", "start.resolve_candidate", "start.provider_admission",
		"start.request_lookup", "start.rebind_existing", "start.slot_state", "start.verb_admission", "start.reserve",
		"retry.load_operation", "retry.locale_admission", "retry.rebind_candidate", "retry.drive",
		"advance.provider_admission", "advance.load_operation", "advance.preparing", "advance.credentials_admitted",
		"advance.activating_provider", "advance.awaiting_external_identity", "advance.awaiting_operator_confirmation",
		"advance.publishing_activation", "advance.publishing_process_activation", "advance.promoting_registration",
		"advance.retiring_predecessor", "advance.delivering_confirmation", "advance.succeeded", "advance.failed", "advance.retired",
		"advance.target_admission", "advance.activation_preflight", "advance.identity",
		"result.identity_read", "result.binding_read", "result.readiness", "readiness.require_owner", "readiness.activation_read",
		"readiness.publication_acquire", "readiness.binding_proof_read", "readiness.credential_projection",
		"readiness.confirmation_outcome_read", "readiness.registration_read", "readiness.native_qualification_read":
		return true
	default:
		return false
	}
}

func signingDiagnosticOperation(ctx context.Context, op Operation) {
	if c, _ := ctx.Value(signingDiagnosticContextKey{}).(*signingDiagnosticCall); c != nil {
		c.mu.Lock()
		if uuid.Validate(op.OperationID) == nil {
			c.operation, c.revision = op.OperationID, op.Revision
		}
		c.mu.Unlock()
	}
}

func signingDiagnosticError(ctx context.Context, provenance string, err error) {
	if c, _ := ctx.Value(signingDiagnosticContextKey{}).(*signingDiagnosticCall); c != nil && err != nil {
		c.record(provenance, err)
	}
}

func (c *signingDiagnosticCall) record(provenance string, err error) {
	c.mu.Lock()
	r := SigningDiagnosticRecord{Call: c.id, Method: c.method, OperationID: c.operation, Revision: c.revision,
		Phase: c.phase, Provenance: provenance, ElapsedNS: time.Since(c.started).Nanoseconds(), Failed: err != nil}
	c.mu.Unlock()
	r.Causes, r.Incomplete = signingDiagnosticCauses(err)
	d := c.owner
	d.mu.Lock()
	if d.closed || len(d.records) == signingDiagnosticRecordLimit {
		d.incomplete = true
		d.mu.Unlock()
		return
	}
	d.incomplete = d.incomplete || r.Incomplete
	d.records = append(d.records, r)
	emit := d.emit
	d.mu.Unlock()
	if emit != nil {
		emit(r)
	}
}

func signingDiagnosticCauses(err error) ([]SigningDiagnosticCause, bool) {
	type pending struct {
		err           error
		parent, depth int
	}
	stack := []pending{{err: err, parent: -1}}
	var result []SigningDiagnosticCause
	for len(stack) != 0 {
		next := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if next.err == nil {
			continue
		}
		if len(result) == signingDiagnosticCauseLimit || next.depth == signingDiagnosticDepthLimit {
			return result, true
		}
		cause := SigningDiagnosticCause{Parent: next.parent, Type: signingDiagnosticType(next.err)}
		cause.Sentinel = signingDiagnosticSentinel(next.err)
		if native, ok := next.err.(*pq.Error); ok && native != nil {
			code := string(native.Code)
			if len(code) != 5 {
				return result, true
			}
			for _, b := range []byte(code) {
				if !(b >= 'A' && b <= 'Z' || b >= '0' && b <= '9') {
					return result, true
				}
			}
			cause.SQLState = code
		}
		index := len(result)
		result = append(result, cause)
		switch wrapped := next.err.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			if len(children) > signingDiagnosticCauseLimit-len(result) {
				return result, true
			}
			for i := len(children) - 1; i >= 0; i-- {
				stack = append(stack, pending{children[i], index, next.depth + 1})
			}
		case interface{ Unwrap() error }:
			stack = append(stack, pending{wrapped.Unwrap(), index, next.depth + 1})
		}
	}
	return result, false
}

func signingDiagnosticType(err error) string {
	name := fmt.Sprintf("%T", err)
	switch name {
	case "*pq.Error", "*fmt.wrapError", "*fmt.wrapErrors", "*errors.joinError", "*errors.errorString",
		"context.deadlineExceededError", "*fs.PathError", "syscall.Errno", "*url.Error", "*net.OpError",
		"*failures.Error", "*channelonboarding.CredentialRequiredError", "*channelonboarding.TerminalActivationError":
		return name
	default:
		return "opaque_error_type"
	}
}

func signingDiagnosticSentinel(err error) string {
	if !reflect.TypeOf(err).Comparable() {
		return ""
	}
	for _, known := range []struct {
		err  error
		name string
	}{
		{context.Canceled, "context_canceled"}, {context.DeadlineExceeded, "context_deadline"},
		{sql.ErrNoRows, "sql_no_rows"}, {sql.ErrConnDone, "sql_connection_done"}, {sql.ErrTxDone, "sql_transaction_done"},
		{driver.ErrBadConn, "driver_bad_connection"}, {ErrInvalidRequest, "onboarding_invalid"},
		{ErrConflict, "onboarding_conflict"}, {ErrRevisionConflict, "onboarding_revision"}, {ErrNotFound, "onboarding_not_found"},
		{operatorchannel.ErrNotFound, "identity_not_found"}, {operatorchannel.ErrConflict, "identity_conflict"},
		{operatorchannel.ErrRevisionConflict, "identity_revision"}, {operatorchannel.ErrCredentialStale, "identity_credential_stale"},
		{operatorchannel.ErrProofUnavailable, "identity_proof_unavailable"}, {credentials.ErrCredentialValueUnusable, "credential_unusable"},
	} {
		if err == known.err {
			return known.name
		}
	}
	return ""
}
