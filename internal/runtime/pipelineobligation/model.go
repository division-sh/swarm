package pipelineobligation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

var (
	ErrBusy         = errors.New("pipeline processing obligation is already claimed")
	ErrIneligible   = errors.New("pipeline processing obligation is not eligible")
	ErrMissingScope = errors.New("committed pipeline processing scope is missing")
	ErrInvalidScope = errors.New("committed pipeline processing scope is invalid")
	ErrStaleClaim   = errors.New("pipeline processing claim is stale or released")
	ErrWrongClaim   = errors.New("pipeline processing claim does not own this obligation")
)

const DecisionRouteRetryDelay = 30 * time.Second

// CommittedScope is the exact dispatch scope persisted with an admitted event.
// It is not an executable delivery and is never inferred from current routes.
type CommittedScope string

const (
	ScopeDirect     CommittedScope = "direct"
	ScopeSubscribed CommittedScope = "subscribed"
)

func ParseCommittedScope(raw string) (CommittedScope, error) {
	scope := CommittedScope(strings.TrimSpace(raw))
	switch scope {
	case ScopeDirect, ScopeSubscribed:
		return scope, nil
	default:
		return "", fmt.Errorf("committed pipeline scope %q is invalid", raw)
	}
}

type Purpose string

const (
	PurposePublication   Purpose = "publication"
	PurposeRecovery      Purpose = "recovery"
	PurposeDecisionRoute Purpose = "decision_route"
)

func (p Purpose) valid() bool {
	return p == PurposePublication || p == PurposeRecovery || p == PurposeDecisionRoute
}

// DispositionKind is the complete durable processing outcome vocabulary.
// Deferred leaves the platform acknowledgement absent and retains/requeues a
// named decision-route obligation. Every other kind is terminal.
type DispositionKind string

const (
	DispositionAcknowledged DispositionKind = "acknowledged"
	DispositionDeferred     DispositionKind = "deferred"
	DispositionTerminal     DispositionKind = "terminal_error"
	DispositionDeadLetter   DispositionKind = "dead_letter"
	DispositionQuarantined  DispositionKind = "quarantined"
)

type Disposition struct {
	kind       DispositionKind
	reasonCode string
	failure    *runtimefailures.Envelope
	retryAt    time.Time
}

// RetryRelease is a non-durable execution result. The current claim owner
// releases the claim without writing a receipt or mutating a decision route.
type RetryRelease struct {
	reasonCode string
	failure    *runtimefailures.Envelope
}

func (r RetryRelease) ReasonCode() string { return r.reasonCode }
func (r RetryRelease) Failure() *runtimefailures.Envelope {
	return runtimefailures.CloneEnvelope(r.failure)
}

// ExecutionOutcome is the domain handler's typed processing result. Continue
// leaves final acknowledgement to the enclosing dispatch. Durable dispositions
// are settled by the durable owner; retry release leaves the obligation
// unchanged and replayable.
type ExecutionOutcome struct {
	disposition  *Disposition
	retryRelease *RetryRelease
}

func Continue() ExecutionOutcome {
	return ExecutionOutcome{}
}

func DeferExecution(reasonCode string, retryAt time.Time, failure *runtimefailures.Envelope) ExecutionOutcome {
	disposition := Deferred(reasonCode, retryAt, failure)
	return ExecutionOutcome{disposition: &disposition}
}

func DeadLetterExecution(reasonCode string, failure *runtimefailures.Envelope) ExecutionOutcome {
	disposition := DeadLetter(reasonCode, failure)
	return ExecutionOutcome{disposition: &disposition}
}

func ReleaseForRetry(reasonCode string, failure *runtimefailures.Envelope) ExecutionOutcome {
	retryRelease := RetryRelease{
		reasonCode: strings.TrimSpace(reasonCode),
		failure:    runtimefailures.CloneEnvelope(failure),
	}
	return ExecutionOutcome{retryRelease: &retryRelease}
}

func (o ExecutionOutcome) Disposition() (Disposition, bool) {
	if o.disposition == nil {
		return Disposition{}, false
	}
	return *o.disposition, true
}

func (o ExecutionOutcome) RetryRelease() (RetryRelease, bool) {
	if o.retryRelease == nil {
		return RetryRelease{}, false
	}
	return *o.retryRelease, true
}

func (o ExecutionOutcome) ContinueDispatch() bool {
	return o.disposition == nil && o.retryRelease == nil
}

func Acknowledged(reasonCode string) Disposition {
	reasonCode = strings.TrimSpace(reasonCode)
	if reasonCode == "" {
		reasonCode = "pipeline_persisted"
	}
	return Disposition{kind: DispositionAcknowledged, reasonCode: reasonCode}
}

func Deferred(reasonCode string, retryAt time.Time, failure *runtimefailures.Envelope) Disposition {
	return Disposition{
		kind:       DispositionDeferred,
		reasonCode: strings.TrimSpace(reasonCode),
		retryAt:    retryAt.UTC(),
		failure:    runtimefailures.CloneEnvelope(failure),
	}
}

func Terminal(reasonCode string, failure *runtimefailures.Envelope) Disposition {
	return failedDisposition(DispositionTerminal, reasonCode, failure)
}

func DeadLetter(reasonCode string, failure *runtimefailures.Envelope) Disposition {
	return failedDisposition(DispositionDeadLetter, reasonCode, failure)
}

func Quarantined(reasonCode string, failure *runtimefailures.Envelope) Disposition {
	return failedDisposition(DispositionQuarantined, reasonCode, failure)
}

func failedDisposition(kind DispositionKind, reasonCode string, failure *runtimefailures.Envelope) Disposition {
	reasonCode = strings.TrimSpace(reasonCode)
	if reasonCode == "" && failure != nil {
		reasonCode = strings.TrimSpace(failure.Detail.Code)
	}
	return Disposition{kind: kind, reasonCode: reasonCode, failure: runtimefailures.CloneEnvelope(failure)}
}

func (d Disposition) Kind() DispositionKind { return d.kind }
func (d Disposition) ReasonCode() string    { return d.reasonCode }
func (d Disposition) Failure() *runtimefailures.Envelope {
	return runtimefailures.CloneEnvelope(d.failure)
}
func (d Disposition) RetryAt() time.Time { return d.retryAt }
func (d Disposition) Successful() bool   { return d.kind == DispositionAcknowledged }
func (d Disposition) Terminal() bool     { return d.kind != "" && d.kind != DispositionDeferred }
func (d Disposition) ValidateFor(purpose Purpose) error {
	if !purpose.valid() {
		return fmt.Errorf("pipeline processing purpose %q is invalid", purpose)
	}
	switch d.kind {
	case DispositionAcknowledged:
		if d.failure != nil || !d.retryAt.IsZero() {
			return errors.New("acknowledged pipeline disposition cannot carry failure or retry time")
		}
	case DispositionDeferred:
		if purpose != PurposeDecisionRoute && purpose != PurposePublication {
			return errors.New("deferred pipeline disposition requires decision-route or publication ownership")
		}
		if d.retryAt.IsZero() {
			return errors.New("deferred pipeline disposition requires retry time")
		}
	case DispositionTerminal, DispositionDeadLetter, DispositionQuarantined:
		if d.failure == nil && strings.TrimSpace(d.reasonCode) == "" {
			return errors.New("failed pipeline disposition requires a reason or failure")
		}
		if !d.retryAt.IsZero() {
			return errors.New("terminal pipeline disposition cannot carry retry time")
		}
	default:
		return fmt.Errorf("pipeline disposition %q is invalid", d.kind)
	}
	return nil
}

// Claim is an opaque capability issued by one selected-store owner. Runtime
// callers can identify its event and purpose but cannot mint or inspect its
// fencing token.
type Claim struct {
	eventID string
	purpose Purpose
	token   string
	issuer  string
}

func (c Claim) EventID() string  { return c.eventID }
func (c Claim) Purpose() Purpose { return c.purpose }

type ClaimIssuer struct {
	id string
}

func NewClaimIssuer() *ClaimIssuer {
	return &ClaimIssuer{id: uuid.NewString()}
}

func (i *ClaimIssuer) Issue(eventID string, purpose Purpose) (Claim, error) {
	eventID = strings.TrimSpace(eventID)
	if i == nil || strings.TrimSpace(i.id) == "" {
		return Claim{}, errors.New("pipeline claim issuer is required")
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return Claim{}, fmt.Errorf("pipeline claim event id: %w", err)
	}
	if !purpose.valid() {
		return Claim{}, fmt.Errorf("pipeline claim purpose %q is invalid", purpose)
	}
	return Claim{eventID: eventID, purpose: purpose, token: uuid.NewString(), issuer: i.id}, nil
}

func (i *ClaimIssuer) Verify(claim Claim, eventID string, purpose Purpose) error {
	eventID = strings.TrimSpace(eventID)
	if i == nil || claim.issuer != i.id || claim.token == "" {
		return ErrStaleClaim
	}
	if claim.eventID != eventID || claim.purpose != purpose {
		return ErrWrongClaim
	}
	return nil
}

func (i *ClaimIssuer) Token(claim Claim) (string, error) {
	if err := i.Verify(claim, claim.eventID, claim.purpose); err != nil {
		return "", err
	}
	return claim.token, nil
}

type ClaimQuery struct {
	RunID   string
	Purpose Purpose
}

func (q ClaimQuery) Validate() error {
	if q.Purpose != PurposeRecovery && q.Purpose != PurposeDecisionRoute {
		return fmt.Errorf("pipeline claim query purpose %q is invalid", q.Purpose)
	}
	runID := strings.TrimSpace(q.RunID)
	if q.Purpose == PurposeDecisionRoute && runID != "" {
		return errors.New("decision-route pipeline claim query cannot select a run")
	}
	if runID != "" {
		if _, err := uuid.Parse(runID); err != nil {
			return fmt.Errorf("pipeline claim query run id: %w", err)
		}
	}
	return nil
}

func GlobalRecoveryQuery() ClaimQuery {
	return ClaimQuery{Purpose: PurposeRecovery}
}

func RunRecoveryQuery(runID string) ClaimQuery {
	return ClaimQuery{RunID: strings.TrimSpace(runID), Purpose: PurposeRecovery}
}

func DecisionRouteQuery() ClaimQuery {
	return ClaimQuery{Purpose: PurposeDecisionRoute}
}

type ClaimedWork struct {
	Event        events.Event
	Scope        CommittedScope
	Claim        Claim
	Acknowledged bool

	preDispatchDisposition *Disposition
}

// PreclassifiedWork retains exact event evidence for an obligation that was
// claimed successfully but cannot safely enter dispatch. The current claim
// remains the only authority allowed to commit its terminal disposition.
func PreclassifiedWork(work ClaimedWork, disposition Disposition) (ClaimedWork, error) {
	if strings.TrimSpace(work.Event.ID()) == "" || work.Event.ID() != work.Claim.EventID() {
		return ClaimedWork{}, ErrWrongClaim
	}
	if err := disposition.ValidateFor(work.Claim.Purpose()); err != nil {
		return ClaimedWork{}, err
	}
	if !disposition.Terminal() || disposition.Successful() {
		return ClaimedWork{}, errors.New("preclassified pipeline work requires a terminal non-success disposition")
	}
	copy := disposition
	work.preDispatchDisposition = &copy
	return work, nil
}

func (w ClaimedWork) PreDispatchDisposition() (Disposition, bool) {
	if w.preDispatchDisposition == nil {
		return Disposition{}, false
	}
	return *w.preDispatchDisposition, true
}

type GlobalWorkPresence struct {
	ProcessingEligible  bool
	DecisionRouteDue    bool
	OldestEligibleEvent time.Time
}

func (p GlobalWorkPresence) Any() bool {
	return p.ProcessingEligible || p.DecisionRouteDue
}

type startupRecoveryDiagnosticsKey struct{}

// WithStartupRecoveryDiagnostics marks the canonical recovery pass whose
// aftermath is part of startup observability. Periodic and run-queue sweeps do
// not carry this marker.
func WithStartupRecoveryDiagnostics(ctx context.Context) context.Context {
	return context.WithValue(ctx, startupRecoveryDiagnosticsKey{}, true)
}

func StartupRecoveryDiagnosticsEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(startupRecoveryDiagnosticsKey{}).(bool)
	return enabled
}

type RunSummary struct {
	RunID              string
	Replayable         int
	Acknowledged       int
	TerminalNonSuccess int
	Deferred           int
	ProcessedDeferred  int
	DiagnosticExcluded int
	RunInactive        bool
	RunForked          bool
}

func (s RunSummary) HasOpenWork() bool {
	return s.Replayable > 0 || s.Deferred > 0
}

func (s RunSummary) BlocksCompletion() bool {
	unprocessedDeferred := s.Deferred - s.ProcessedDeferred
	return s.Replayable > 0 || s.TerminalNonSuccess > 0 || unprocessedDeferred > 0
}

// Store is the only runtime-visible owner of durable platform-pipeline work.
// Backend connections, transactions, locks, and lease values never cross it.
type Store interface {
	ClaimPublication(context.Context, string) (Claim, error)
	ClaimEvent(context.Context, string, Purpose) (ClaimedWork, error)
	ClaimNext(context.Context, ClaimQuery) (ClaimedWork, bool, error)
	MarkDecisionProcessed(context.Context, Claim) error
	Settle(context.Context, Claim, Disposition) error
	Release(context.Context, Claim) error
	GlobalWorkPresence(context.Context) (GlobalWorkPresence, error)
	SummarizeRun(context.Context, string) (RunSummary, error)
	TerminalizeRun(context.Context, string, Disposition, time.Time) (int, error)
}
