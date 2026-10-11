package runforkpersistence

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// This invocation-private index retains admitted historical facts, not current
// source rows or execution permission. Recipient planning still proves the
// publication and constructed receiver before any child mutation.
type runForkReplayCut struct {
	runID      string
	revision   int64
	events     map[string]runForkRevisionEvent
	deliveries map[string]runForkRevisionDelivery
}

func newRunForkReplayCut(snapshot *runForkRevisionSnapshot) (runForkReplayCut, error) {
	if snapshot == nil || snapshot.RunID == "" || snapshot.Revision <= 0 {
		return runForkReplayCut{}, fmt.Errorf("fork replay requires its exact fixed cut")
	}
	cut := runForkReplayCut{runID: snapshot.RunID, revision: snapshot.Revision,
		events:     make(map[string]runForkRevisionEvent, len(snapshot.Events)),
		deliveries: make(map[string]runForkRevisionDelivery, len(snapshot.Deliveries))}
	for _, row := range snapshot.Events {
		if err := cut.requireFact(row.RunID, row.EventID, row.runForkRevisionedFact); err != nil {
			return runForkReplayCut{}, err
		}
		if _, duplicate := cut.events[row.EventID]; duplicate {
			return runForkReplayCut{}, fmt.Errorf("fork replay publication %s is duplicated at the cut", row.EventID)
		}
		cut.events[row.EventID] = row
	}
	for _, row := range snapshot.Deliveries {
		if err := cut.requireFact(row.Snapshot.RunID, row.Snapshot.DeliveryID, row.runForkRevisionedFact); err != nil {
			return runForkReplayCut{}, err
		}
		if _, found := cut.events[row.Snapshot.EventID]; !found {
			return runForkReplayCut{}, fmt.Errorf("fork replay delivery %s has no publication at the cut", row.Snapshot.DeliveryID)
		}
		if _, duplicate := cut.deliveries[row.Snapshot.DeliveryID]; duplicate {
			return runForkReplayCut{}, fmt.Errorf("fork replay delivery %s is duplicated at the cut", row.Snapshot.DeliveryID)
		}
		cut.deliveries[row.Snapshot.DeliveryID] = row
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
	row, found := c.events[eventID]
	if c.runID == "" || !found {
		return events.Event{}, fmt.Errorf("fork replay publication %s is absent at the cut", eventID)
	}
	admitted, err := decodeRunForkRevisionEvent(row)
	if err != nil {
		return events.Event{}, fmt.Errorf("decode fork replay publication %s at the cut: %w", eventID, err)
	}
	return admitted.Event().Clone(), nil
}

func (c runForkReplayCut) delivery(eventID, deliveryID string) (deliverylifecycle.Snapshot, error) {
	row, found := c.deliveries[deliveryID]
	if c.runID == "" || !found {
		return deliverylifecycle.Snapshot{}, fmt.Errorf("fork replay delivery %s is absent at the cut", deliveryID)
	}
	if _, found := c.events[eventID]; !found || row.Snapshot.EventID != eventID {
		return deliverylifecycle.Snapshot{}, fmt.Errorf("fork replay delivery %s contradicts its source publication", deliveryID)
	}
	return row.Snapshot, nil
}
