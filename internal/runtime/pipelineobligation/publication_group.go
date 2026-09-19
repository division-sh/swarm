package pipelineobligation

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

// PublicationGroup owns the exact publications of one admitted fan-out turn.
// Seal fixes preparation membership and an exclusive ordinal end. A smaller
// attempt is permitted only after the selected owner proves safe rollback.
// Neither collecting a disposition nor observing it consumes mutation authority.
type PublicationGroup interface {
	// Claim is one-member acquisition, not lookup of a preissued batch member.
	Claim(context.Context, int, events.Event) (Claim, error)
	// ClaimBatch returns exact claims in request order or no capabilities on
	// failure. All distinct parent fences share one bounded admission lifetime.
	ClaimBatch(context.Context, []PublicationClaimRequest) ([]Claim, error)
	RecordPrepared(context.Context, Claim, PublicationPreparation) error
	Seal(context.Context, int, []Claim) error
	// ValidateCommittedMembership proves only exact local cleanup membership.
	// It performs no SQL, survives Close/retirement, and never authorizes execution.
	ValidateCommittedMembership([]Claim) error
	ValidateCommitted(context.Context, []Claim) error
	Settle(context.Context, []PublicationSettlementMember) (PublicationGroupOutcome, error)
	ReadPublicationSettlement(context.Context, []PublicationSettlementMember) (PublicationSettlementSnapshot, error)
	Close(context.Context) error
}

type PublicationClaimRequest struct {
	Ordinal int
	Event   events.Event
}

// PublicationPreparation is the canonical planner's proof of the lawful
// pre-visibility route projection, not permission to change business facts.
type PublicationPreparation interface {
	ValidatePreparedFanOutEvent(original events.Event) error
}

type PublicationSettlementMember struct {
	Claim       Claim
	Disposition Disposition
}

type PublicationSettlementResult struct {
	Claim   Claim
	Outcome SettlementOutcome
}

// Results are populated only for acknowledged commits, including commits whose
// subsequent exact cleanup failed. An error does not erase those results.
type PublicationGroupOutcome struct {
	Results []PublicationSettlementResult
}

type PublicationSettlementState uint8

const (
	PublicationSettlementSatisfied PublicationSettlementState = iota + 1
	PublicationSettlementPending
	PublicationSettlementConflict
)

type PublicationSettlementObservation struct {
	Claim  Claim
	State  PublicationSettlementState
	Reason string
}

// Observed satisfaction is not an acknowledgement or an executable handoff.
type PublicationSettlementSnapshot struct {
	ObservedAt time.Time
	Rows       []PublicationSettlementObservation
}
