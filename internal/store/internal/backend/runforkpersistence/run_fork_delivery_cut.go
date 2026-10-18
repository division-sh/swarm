package runforkpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
)

// This invocation-private index retains admitted historical facts, not current
// source rows or execution permission. Recipient planning still proves the
// publication and constructed receiver before any child mutation.
type runForkReplayCut struct {
	runID    string
	revision int64
	events   map[string]runForkRevisionEvent
}

func newRunForkReplayCut(snapshot *runForkRevisionSnapshot) (runForkReplayCut, error) {
	if snapshot == nil || snapshot.RunID == "" || snapshot.Revision <= 0 {
		return runForkReplayCut{}, fmt.Errorf("fork replay requires its exact fixed cut")
	}
	cut := runForkReplayCut{runID: snapshot.RunID, revision: snapshot.Revision,
		events: make(map[string]runForkRevisionEvent, len(snapshot.Events))}
	for _, row := range snapshot.Events {
		if err := cut.requireFact(row.RunID, row.EventID, row.runForkRevisionedFact); err != nil {
			return runForkReplayCut{}, err
		}
		if _, duplicate := cut.events[row.EventID]; duplicate {
			return runForkReplayCut{}, fmt.Errorf("fork replay publication %s is duplicated at the cut", row.EventID)
		}
		cut.events[row.EventID] = row
	}
	deliveries := make(map[string]bool, len(snapshot.Deliveries))
	for _, row := range snapshot.Deliveries {
		if err := cut.requireFact(row.Snapshot.RunID, row.Snapshot.DeliveryID, row.runForkRevisionedFact); err != nil {
			return runForkReplayCut{}, err
		}
		if _, found := cut.events[row.Snapshot.EventID]; !found {
			return runForkReplayCut{}, fmt.Errorf("fork replay delivery %s has no publication at the cut", row.Snapshot.DeliveryID)
		}
		if deliveries[row.Snapshot.DeliveryID] {
			return runForkReplayCut{}, fmt.Errorf("fork replay delivery %s is duplicated at the cut", row.Snapshot.DeliveryID)
		}
		deliveries[row.Snapshot.DeliveryID] = true
	}
	return cut, nil
}

func (c runForkReplayCut) requireFact(runID, id string, fact runForkRevisionedFact) error {
	if id == "" || runID != c.runID || fact.FirstRevision <= 0 || fact.Revision < fact.FirstRevision || fact.Revision > c.revision {
		return fmt.Errorf("fork replay fact %s contradicts its exact fixed cut", id)
	}
	return nil
}

func (c runForkReplayCut) event(eventID string) (events.Event, error) {
	var empty events.Event
	row, found := c.events[eventID]
	if c.runID == "" || !found {
		return empty, fmt.Errorf("fork replay publication %s is absent at the cut", eventID)
	}
	admitted, err := decodeRunForkRevisionEvent(row)
	if err != nil {
		return empty, fmt.Errorf("decode fork replay publication %s at the cut: %w", eventID, err)
	}
	return admitted.Event().Clone(), nil
}
