package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func pendingJoinOrderFixture(t *testing.T) (runfork.RunForkSelectedContractSourceEvent, *runForkRevisionSnapshot) {
	t.Helper()
	snapshot, barrier := transferredBarrierHistoryFixture(t, true, executionmode.Mock)
	source, err := genericschedule.NewTransferredFanOutBarrierOccurrence(barrier)
	if err != nil {
		t.Fatal(err)
	}
	command, err := projectRunForkBarrierContinuationCommand(barrier, uuid.NewString(), barrier.Registration.PlanRef, nil, source.Command.Due.Absolute, barrier.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := genericschedule.ProjectPendingJoinContinuation(source, command)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Events, snapshot.Deliveries = nil, nil
	return runfork.RunForkSelectedContractSourceEvent{SourceEventID: source.Publication.EventID, JoinContinuation: continuation}, snapshot
}

func TestPendingJoinInputsPreserveMixedPublicationChronology(t *testing.T) {
	for _, offset := range []time.Duration{-time.Second, 0, time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			pending, snapshot := pendingJoinOrderFixture(t)
			physical := runfork.RunForkSelectedContractSourceEvent{SourceEventID: uuid.NewString()}
			at := pending.JoinContinuation.ChildCommand().Due.Absolute.Add(offset)
			snapshot.Events = []runForkRevisionEvent{{RunID: snapshot.RunID, EventID: physical.SourceEventID, CreatedAt: at}}
			want := []string{pending.SourceEventID, physical.SourceEventID}
			if offset < 0 || offset == 0 && physical.SourceEventID < pending.SourceEventID {
				want[0], want[1] = want[1], want[0]
			}
			for _, inputs := range [][]runfork.RunForkSelectedContractSourceEvent{{physical, pending}, {pending, physical}} {
				if err := orderRunForkSourceJoinInputs(inputs, snapshot); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual([]string{inputs[0].SourceEventID, inputs[1].SourceEventID}, want) {
					t.Fatal("input encounter order replaced retained due/publication chronology")
				}
				original, _ := pending.JoinContinuation.PendingSource()
				for _, input := range inputs {
					if retained, found := input.JoinContinuation.PendingSource(); found && !reflect.DeepEqual(original, retained) {
						t.Fatal("ordering altered the immutable continuation")
					}
				}
			}
		})
	}
}

func TestPendingJoinInputOrderingRefusesContradictoryMembership(t *testing.T) {
	for _, fault := range []string{"no_cut", "absent_physical", "foreign_cut_event", "duplicate_cut_event", "duplicate_input", "claimed_pending_event", "wrong_pending_coordinate", "missing_time"} {
		t.Run(fault, func(t *testing.T) {
			pending, snapshot := pendingJoinOrderFixture(t)
			physical := runfork.RunForkSelectedContractSourceEvent{SourceEventID: uuid.NewString()}
			snapshot.Events = []runForkRevisionEvent{{RunID: snapshot.RunID, EventID: physical.SourceEventID, CreatedAt: pending.JoinContinuation.ChildCommand().Due.Absolute}}
			inputs := []runfork.RunForkSelectedContractSourceEvent{physical, pending}
			switch fault {
			case "no_cut":
				snapshot = nil
			case "absent_physical":
				snapshot.Events = nil
			case "foreign_cut_event":
				snapshot.Events[0].RunID = uuid.NewString()
			case "duplicate_cut_event":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
			case "duplicate_input":
				inputs = append(inputs, pending)
			case "claimed_pending_event":
				snapshot.Events = append(snapshot.Events, runForkRevisionEvent{RunID: snapshot.RunID, EventID: pending.SourceEventID, CreatedAt: snapshot.Events[0].CreatedAt})
			case "wrong_pending_coordinate":
				inputs[1].SourceEventID = uuid.NewString()
			case "missing_time":
				snapshot.Events[0].CreatedAt = time.Time{}
			}
			before := append([]runfork.RunForkSelectedContractSourceEvent(nil), inputs...)
			if err := orderRunForkSourceJoinInputs(inputs, snapshot); err == nil {
				t.Fatal("contradictory chronology was admitted")
			}
			if !reflect.DeepEqual(before, inputs) {
				t.Fatal("refusal changed retained input order or evidence")
			}
		})
	}
}
