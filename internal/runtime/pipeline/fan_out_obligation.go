package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type FanOutClaimRequest struct {
	Owner      string
	BundleHash string
	Candidate  *fanoutobligation.IntentKey
	Now        time.Time
	Lease      time.Duration
}

func (r FanOutClaimRequest) Validate() error {
	if r.Owner == "" || r.Now.IsZero() || r.Lease <= 0 {
		return fmt.Errorf("fan-out claim requires owner, observation time, and positive lease")
	}
	if err := runtimecontracts.ValidateBundleHash(r.BundleHash); err != nil {
		return fmt.Errorf("fan-out claim requires exact admitted bundle: %w", err)
	}
	if r.Candidate != nil {
		return r.Candidate.Validate()
	}
	return nil
}

// FanOutTurnResult describes the disposition of one exact candidate. Queue
// exhaustion belongs to the shared selector, never to an individual executor.
type FanOutTurnResult struct {
	Refill bool
}

type FanOutChunkOutcome struct {
	Ordinal     int
	Publication runtimeengine.DurablePublicationPlan
	Failure     json.RawMessage
}

// FanOutSafeAggregateError explicitly authorizes deterministic lower-half-first
// isolation. Unknown aggregate failures must block instead of being bisected.
type FanOutSafeAggregateError struct {
	Failure runtimefailures.Envelope
	cause   error
}

func NewFanOutSafeAggregateError(failure runtimefailures.Envelope, cause error) error {
	if runtimefailures.ValidateEnvelope(failure) != nil || failure.Retryable || !failure.Deterministic || failure.Class == runtimefailures.ClassInternalFailure || failure.Class == runtimefailures.ClassOutcomeUncertain {
		return fmt.Errorf("fan-out aggregate semantic failure evidence is invalid")
	}
	return &FanOutSafeAggregateError{Failure: failure, cause: cause}
}

func (e *FanOutSafeAggregateError) Error() string {
	if e == nil {
		return ""
	}
	return "fan-out aggregate: " + e.Failure.Detail.Code
}

func (e *FanOutSafeAggregateError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func FanOutSafeAggregateFailure(err error) (*FanOutSafeAggregateError, bool) {
	var failure *FanOutSafeAggregateError
	matched := errors.As(err, &failure)
	return failure, matched
}

func (o FanOutChunkOutcome) Validate() error {
	if o.Ordinal < 0 {
		return fmt.Errorf("fan-out chunk ordinal cannot be negative")
	}
	if (o.Publication == nil) == (len(o.Failure) == 0) {
		return fmt.Errorf("fan-out chunk outcome requires exactly one publication or semantic failure")
	}
	if o.Publication != nil {
		return o.Publication.ValidateDurablePublicationPlan()
	}
	if err := fanoutobligation.ValidateSemanticRejection(o.Failure); err != nil {
		return fmt.Errorf("fan-out semantic failure: %w", err)
	}
	return nil
}

type FanOutChunkCommand struct {
	Claim    fanoutobligation.Claim
	Outcomes []FanOutChunkOutcome
	Now      time.Time
}

func (c FanOutChunkCommand) Validate() error {
	if err := c.Claim.Validate(); err != nil {
		return err
	}
	if len(c.Outcomes) == 0 || c.Now.IsZero() {
		return fmt.Errorf("fan-out chunk requires outcomes and observation time")
	}
	for index, outcome := range c.Outcomes {
		if err := outcome.Validate(); err != nil {
			return fmt.Errorf("fan-out chunk outcome %d: %w", index, err)
		}
	}
	return nil
}

type CommittedFanOutChunk struct {
	Intent            fanoutobligation.Intent
	Publications      []runtimeengine.CommittedDurablePublication
	PostCommitFailure error
}

type FanOutRetryableRelease struct {
	Claim            fanoutobligation.Claim
	Now              time.Time
	ObservedDuration time.Duration
	Failure          runtimefailures.Envelope
}

type FanOutBlockRequest struct {
	Claim   fanoutobligation.Claim
	Now     time.Time
	Failure runtimefailures.Envelope
}

func (r FanOutBlockRequest) Validate() error {
	if err := r.Claim.Validate(); err != nil {
		return err
	}
	if r.Now.IsZero() {
		return fmt.Errorf("fan-out block requires observation time")
	}
	if err := runtimefailures.ValidateEnvelope(r.Failure); err != nil {
		return fmt.Errorf("fan-out block requires typed failure: %w", err)
	}
	return nil
}

func (r FanOutRetryableRelease) Validate() error {
	if err := r.Claim.Validate(); err != nil {
		return err
	}
	if r.Now.IsZero() || r.ObservedDuration < 0 {
		return fmt.Errorf("fan-out retryable release requires observation time and nonnegative duration")
	}
	return (fanoutobligation.RetryWait{ReadyAt: r.Now, Failure: r.Failure}).Validate()
}

type FanOutEvaluationInput struct {
	StartOrdinal int
	Items        []any
	Trigger      events.Event
}

func (i FanOutEvaluationInput) Validate(intent fanoutobligation.Intent) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	end := intent.ChunkEndOrdinal()
	if i.StartOrdinal != intent.Cursor || len(i.Items) != end-intent.Cursor || i.Trigger.ID() != intent.Request.Capsule.Lineage.ParentEventID || i.Trigger.RunID() != intent.Request.Capsule.Lineage.RunID {
		return fmt.Errorf("fan-out evaluation input disagrees with immutable intent")
	}
	return nil
}

type FanOutObligationOwner interface {
	FanOutSummaryOwner
	BeginFanOutPublicationGroup(context.Context, fanoutobligation.Claim) (runtimepipelineobligation.PublicationGroup, error)
	ClaimFanOutIntent(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error)
	LoadFanOutEvaluation(context.Context, fanoutobligation.Claim) (FanOutEvaluationInput, error)
	CommitFanOutChunk(context.Context, FanOutChunkCommand) (CommittedFanOutChunk, error)
	ReleaseFanOutClaim(context.Context, fanoutobligation.Claim) (FanOutClaimSettlement, error)
	ReleaseFanOutRetryable(context.Context, FanOutRetryableRelease) (FanOutClaimSettlement, error)
	BlockFanOutClaim(context.Context, FanOutBlockRequest) (FanOutClaimSettlement, error)
	CancelRunFanOut(context.Context, string, string, time.Time) error
}

// FanOutClaimSettlement carries selected-store commit truth independently of
// post-commit cleanup errors.
type FanOutClaimSettlement struct {
	Acknowledged bool
}

// FanOutPublicationPlanner carries the explicit bounded publication lifetime;
// ordinary engine publication remains an independently owned operation.
type FanOutPublicationPlanner interface {
	EnginePublicationPlanner
	PrepareFanOutPublications(context.Context, runtimepipelineobligation.PublicationGroup, []FanOutPublicationRequest) ([]FanOutPublicationPreparation, error)
	SealFanOutPublications(context.Context, runtimepipelineobligation.PublicationGroup, int, []runtimeengine.DurablePublicationPlan) error
	FinalizeFanOutPublications(context.Context, runtimepipelineobligation.PublicationGroup, []runtimeengine.CommittedDurablePublication) error
	DispatchFanOutPublications(context.Context, runtimepipelineobligation.PublicationGroup, []runtimeengine.CommittedDurablePublication) error
}

type FanOutPublicationRequest struct {
	Ordinal int
	Intent  runtimeengine.EmitIntent
}

// Per-ordinal rejection remains separate from whole acquisition failure. The
// pipeline's existing failure algebra alone decides whether a row is rejected.
type FanOutPublicationPreparation struct {
	Ordinal     int
	Publication runtimeengine.DurablePublicationPlan
	Err         error
}

type FanOutSummaryOwner interface {
	FanOutRunSummary(context.Context, string, time.Time) (fanoutobligation.RunSummary, error)
}
