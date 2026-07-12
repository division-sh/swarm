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

// DecisionCardLifecycleOutboxStore owns lifecycle events created by run-level
// mutations that may execute before the bundle EventBus exists.
type DecisionCardLifecycleOutboxStore interface {
	ListPendingDecisionCardLifecycleEvents(context.Context, string, int) ([]events.Event, error)
	CompleteDecisionCardLifecycleEvent(context.Context, string, time.Time) error
}
