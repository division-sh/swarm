package runforkpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// A source occurrence is read from its fixed cut, never from current timers or
// a new schedule admission. Existing pending-work projection retains its exact
// historical delivery rows independently of this immutable publication proof.
func runForkPublishedArrivalEvidence(snapshot *runForkRevisionSnapshot, activation genericschedule.Activation) (events.AdmittedEvent, error) {
	if snapshot == nil || snapshot.RunID != activation.Command.RunID || snapshot.Revision <= 0 || activation.CurrentEventID == "" {
		return events.AdmittedEvent{}, fmt.Errorf("published arrival evidence requires its exact source cut and occurrence")
	}
	var found *runForkRevisionEvent
	for i := range snapshot.Events {
		row := &snapshot.Events[i]
		if row.EventID != activation.CurrentEventID {
			continue
		}
		if found != nil || row.RunID != snapshot.RunID || row.FirstRevision <= 0 || row.Revision < row.FirstRevision || row.Revision > snapshot.Revision {
			return events.AdmittedEvent{}, fmt.Errorf("published arrival event contradicts its exact historical cut")
		}
		found = row
	}
	if found == nil {
		return events.AdmittedEvent{}, fmt.Errorf("published arrival lacks its exact historical event")
	}
	event, err := decodeRunForkRevisionEvent(*found)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	if _, err := activation.ValidatePublishedOccurrence(event.Event()); err != nil {
		return events.AdmittedEvent{}, err
	}
	return event, nil
}
