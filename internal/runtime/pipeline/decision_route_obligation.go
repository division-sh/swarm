package pipeline

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

const DecisionRouteRetryDelay = 30 * time.Second

// DecisionRouteObligationStore owns durable retries for committed gate
// decisions whose frozen route has not completed.
type DecisionRouteObligationStore interface {
	ListDueDecisionRouteObligations(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error)
	DeferDecisionRouteObligation(context.Context, string, time.Time, *runtimefailures.Envelope) error
	QuarantineDecisionRouteObligation(context.Context, string, time.Time, *runtimefailures.Envelope) error
	CompleteDecisionRouteObligation(context.Context, string, time.Time) error
}
