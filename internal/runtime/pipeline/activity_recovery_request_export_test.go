package pipeline

import (
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/engine"
)

func ActivityRecoveryRequestForTest(intent engine.ActivityIntent) (events.Event, error) {
	request, err := activityRequestEmitIntentFromAdmittedSource(intent)
	return request.Event, err
}
