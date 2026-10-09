package eventfixture

import (
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

// StagedChild prepares validated child facts without acquiring SQL authority.
// The private fixture writer preserves the caller's later explicit history cut.
func StagedChild(
	eventID, runID, parentEventID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) (events.Event, eventrecord.Record, error) {
	var empty events.Event
	var emptyRecord eventrecord.Record
	facts, err := eventFacts(eventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		return empty, emptyRecord, err
	}
	event, err := events.NewChildEvent(events.ChildEventInput{
		Facts:   facts,
		Lineage: events.EventLineage{RunID: runID, ParentEventID: parentEventID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		return empty, emptyRecord, err
	}
	bound, err := BindPayload(event)
	if err != nil {
		return empty, emptyRecord, err
	}
	admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return empty, emptyRecord, err
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return empty, emptyRecord, fmt.Errorf("selected-fork replay fixture requires exact lineage persistence")
	}
	settlement, err := fixtureSettlement(admitted.Event())
	if err != nil {
		return empty, emptyRecord, err
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return empty, emptyRecord, err
	}
	return event, record, nil
}
