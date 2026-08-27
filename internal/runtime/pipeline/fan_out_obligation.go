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
)

type FanOutClaimRequest struct {
	Owner      string
	BundleHash string
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
	return nil
}

type FanOutChunkOutcome struct {
	Ordinal     int
	Publication runtimeengine.DurablePublicationPlan
	Failure     json.RawMessage
}

// FanOutItemSemanticError is the only store-call failure that may become a
// terminal ordinal outcome. The ordinal is part of the typed evidence rather
// than inferred from a database or error string.
type FanOutItemSemanticError struct {
	Ordinal int
	Failure runtimefailures.Envelope
	cause   error
}

func NewFanOutItemSemanticError(ordinal int, failure runtimefailures.Envelope, cause error) error {
	if ordinal < 0 || runtimefailures.ValidateEnvelope(failure) != nil || failure.Retryable || !failure.Deterministic || failure.Class == runtimefailures.ClassInternalFailure || failure.Class == runtimefailures.ClassOutcomeUncertain {
		return fmt.Errorf("fan-out item semantic failure evidence is invalid")
	}
	return &FanOutItemSemanticError{Ordinal: ordinal, Failure: failure, cause: cause}
}

func (e *FanOutItemSemanticError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("fan-out ordinal %d: %s", e.Ordinal, e.Failure.Detail.Code)
}

func (e *FanOutItemSemanticError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
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

func FanOutItemSemanticFailure(err error) (*FanOutItemSemanticError, bool) {
	var failure *FanOutItemSemanticError
	return failure, errors.As(err, &failure)
}

func FanOutSafeAggregateFailure(err error) (*FanOutSafeAggregateError, bool) {
	var failure *FanOutSafeAggregateError
	return failure, errors.As(err, &failure)
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
	return nil
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
	end := intent.Cursor + intent.NextChunkSize
	if end > intent.Request.Cardinality {
		end = intent.Request.Cardinality
	}
	if i.StartOrdinal != intent.Cursor || len(i.Items) != end-intent.Cursor || i.Trigger.ID() != intent.Request.Capsule.Lineage.ParentEventID || i.Trigger.RunID() != intent.Request.Capsule.Lineage.RunID {
		return fmt.Errorf("fan-out evaluation input disagrees with immutable intent")
	}
	return nil
}

type FanOutObligationOwner interface {
	ClaimFanOutIntent(context.Context, FanOutClaimRequest) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error)
	LoadFanOutEvaluation(context.Context, fanoutobligation.Claim) (FanOutEvaluationInput, error)
	CommitFanOutChunk(context.Context, FanOutChunkCommand) (CommittedFanOutChunk, error)
	ReleaseFanOutClaim(context.Context, fanoutobligation.Claim) error
	ReleaseFanOutRetryable(context.Context, FanOutRetryableRelease) error
	BlockFanOutClaim(context.Context, FanOutBlockRequest) error
	CancelRunFanOut(context.Context, string, string, time.Time) error
	FanOutRunSummary(context.Context, string, time.Time) (fanoutobligation.RunSummary, error)
}
