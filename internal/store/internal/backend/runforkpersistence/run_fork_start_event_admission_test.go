package runforkpersistence

import (
	"bytes"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestOriginalFirstTurnSourceAdmissionIsExclusiveAndExact(t *testing.T) {
	sourceRun, childRun := uuid.NewString(), uuid.NewString()
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.first", "operator", "", []byte(`{"value":7.0}`), 0,
		sourceRun, "", events.EventEnvelope{}, time.Unix(100, 0).UTC())
	event, err := eventtest.AdmitPayload(event, ".", "work.first")
	if err != nil {
		t.Fatal(err)
	}
	input, err := runfork.InputPublicationFromEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	admission := runForkSourceStateAdmission{forkRunID: childRun,
		snapshot: &runForkRevisionSnapshot{RunID: sourceRun, Revision: 1}, firstTurn: &input}
	source := runfork.RunForkSelectedContractSourceEvent{
		SourceEventID: event.ID(), EventName: string(event.Type()), Payload: event.Payload(),
		RoutingSource: event.RoutingSource(), ExecutionMode: event.ExecutionMode(), InputPublication: input,
	}
	projected, state, err := admission.project(source)
	if err != nil || state != nil || projected.SourceEventID != event.ID() || !bytes.Equal(projected.Payload, source.Payload) ||
		len(admission.snapshot.Events) != 0 || len(admission.snapshot.Deliveries) != 0 {
		t.Fatalf("first-turn preparation copied inclusive source work: %+v %v", projected, err)
	}
	for _, variant := range []string{"later_event", "changed_payload", "changed_name", "no_intention"} {
		t.Run(variant, func(t *testing.T) {
			changed, owner := source, admission
			switch variant {
			case "later_event":
				changed.SourceEventID = uuid.NewString()
			case "changed_payload":
				changed.Payload = []byte(`{"value":7}`)
			case "changed_name":
				changed.EventName = "work.later"
			case "no_intention":
				owner.firstTurn = nil
			}
			if _, _, err := owner.project(changed); err == nil {
				t.Fatal("unrelated input borrowed exclusive original-start authority")
			}
		})
	}
}
