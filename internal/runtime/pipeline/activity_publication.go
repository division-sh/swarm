package pipeline

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type activityPublication struct {
	eventType events.EventType
	eventID   string
}

// Project the frozen declaration against the admitted execution source before
// creating a result ID. Journal replay copies the recorded result instead.
func admitActivityPublication(intent runtimeengine.ActivityIntent, declaration string) (activityPublication, error) {
	eventType, err := runtimepinrouting.AdmitPublicationIdentity(intent.ExecutionFlowID.String(), declaration, intent.RoutingSource)
	if err != nil {
		return activityPublication{}, fmt.Errorf("activity result publication: %w", err)
	}
	return activityPublication{eventType: eventType, eventID: activityResultEventID(intent, string(eventType))}, nil
}
