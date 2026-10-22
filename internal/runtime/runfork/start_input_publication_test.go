package runfork

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

func originalStartInputFixture(t *testing.T) RunForkPlan {
	t.Helper()
	runID := uuid.NewString()
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.start", "operator", "", []byte(`{"integer":7,"double":7.0}`), 0,
		runID, "", events.EventEnvelope{}, time.Unix(100, 0).UTC())
	event, err := eventtest.AdmitPayload(event, ".", "work.start")
	if err != nil {
		t.Fatal(err)
	}
	input, err := InputPublicationFromEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	return (RunForkPlan{SourceRunID: runID, ForkPoint: RunForkPoint{Kind: RunForkPointRunStart, Revision: 1}, StartFirstTurn: &input}).
		WithHistoricalEvents(1, nil)
}

func TestOriginalStartFirstTurnDoesNotInventHistoricalMembership(t *testing.T) {
	plan := originalStartInputFixture(t)
	input, present, err := plan.OriginalStartFirstTurn()
	if err != nil || !present {
		t.Fatalf("real original intention refused: present=%v err=%v", present, err)
	}
	event, _ := input.Event()
	if _, historical := plan.HistoricalInputPublication(event.ID()); historical {
		t.Fatal("first turn manufactured historical input")
	}
	ids, known := plan.HistoricalEventIDs(1)
	if !known || len(ids) != 0 || len(plan.PendingWork) != 0 {
		t.Fatal("first turn manufactured historical events or deliveries")
	}
	selected, admitted := plan.ExecutionInputPublication(event.ID())
	if !admitted || !reflect.DeepEqual(selected.Coordinates(), input.Coordinates()) {
		t.Fatal("selected root admission lost exact original input coordinates")
	}
	if _, admitted := plan.ExecutionInputPublication(uuid.NewString()); admitted {
		t.Fatal("unrelated later input borrowed original intention")
	}
	plan.StartFirstTurn = nil
	if _, present, err := plan.OriginalStartFirstTurn(); err != nil || present {
		t.Fatal("eventless start invented a first turn")
	}
}

func TestOriginalStartFirstTurnRejectsContradictoryCutEvidence(t *testing.T) {
	for _, variant := range []string{"inclusive_event", "deployment", "foreign_run", "historical_member", "event_on_start", "empty_input"} {
		t.Run(variant, func(t *testing.T) {
			plan := originalStartInputFixture(t)
			event, _ := plan.StartFirstTurn.Event()
			switch variant {
			case "inclusive_event":
				plan.ForkPoint = RunForkPoint{Kind: RunForkPointEvent, Revision: 1, EventID: event.ID()}
			case "deployment":
				plan.ForkPoint.Kind = RunForkPointDeploymentRevision
			case "foreign_run":
				plan.SourceRunID = uuid.NewString()
			case "historical_member":
				plan = plan.WithHistoricalEvents(1, []string{event.ID()})
			case "event_on_start":
				plan.ForkPoint.EventID = event.ID()
			case "empty_input":
				plan.StartFirstTurn = &InputPublication{}
			}
			if _, present, err := plan.OriginalStartFirstTurn(); err == nil || present {
				t.Fatalf("contradictory %s accepted", variant)
			}
			if _, admitted := plan.ExecutionInputPublication(event.ID()); admitted {
				t.Fatal("invalid intention minted root-input authority")
			}
		})
	}
}
