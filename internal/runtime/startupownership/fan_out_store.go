package startupownership

import (
	"context"
	"errors"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// FanOutCandidate is an observation of durable fairness, not an execution grant.
// The selected owner must revalidate its exact grant and candidate at claim time.
type FanOutCandidate struct {
	GrantID    string
	Key        fanoutobligation.IntentKey
	PositionAt time.Time
	CreatedAt  time.Time
}

func (c FanOutCandidate) Validate() error {
	if c.GrantID == "" || c.PositionAt.IsZero() || c.CreatedAt.IsZero() {
		return errors.New("fan-out candidate requires exact grant and durable fairness observation")
	}
	return c.Key.Validate()
}

// FanOutRegistrationObservation preserves the complete acknowledged grant
// census. Closing is negative serving eligibility, never mutation authority.
type FanOutRegistrationObservation struct {
	Grant   GrantEvidence
	Closing bool
}

// FanOutServingStore is supplied by the retained selected-store session. Every
// bound mutation consumes current generation, source-set and run authority in
// its transaction; registration is not a substitute for mutation admission.
type FanOutServingStore interface {
	NextFanOutCandidate(context.Context, []FanOutRegistrationObservation, []fanoutobligation.IntentKey) (FanOutCandidate, bool, error)
	BindFanOutGrant(GrantEvidence) (pipeline.FanOutObligationOwner, error)
}

type fanOutServingStoreProvider interface {
	FanOutServingStore() (FanOutServingStore, error)
}

type FanOutExecutor interface {
	ServeFanOutCandidate(context.Context, pipeline.FanOutObligationOwner, fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error)
	ReportFanOutServingError(context.Context, error)
}
