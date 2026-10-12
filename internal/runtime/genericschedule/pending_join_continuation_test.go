package genericschedule

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestPendingJoinContinuationPreservesImmediateObligationWithoutEvent(t *testing.T) {
	for _, flow := range []string{"", "orders"} {
		for _, kind := range []timeridentity.TimerHandleKind{timeridentity.TimerHandleJoinComplete, timeridentity.TimerHandleJoinTimeout} {
			for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
				t.Run(flow+"/"+string(kind)+"/"+string(mode), func(t *testing.T) {
					_, source, _ := transferredJoinOccurrenceFixture(t, flow, kind, mode)
					child := publishedJoinContinuationChild(t, source.Command)
					continued, err := ProjectPendingJoinContinuation(source, child)
					if err != nil {
						t.Fatal(err)
					}
					if !continued.Present() || continued.Published() || continued.SourceEvent().ID() != "" ||
						continued.SourceRunID() != source.Command.RunID || continued.SourceEventID() != source.Publication.EventID {
						t.Fatal("pending continuation fabricated publication or skipped its immediate parent")
					}
					pending, ok := continued.PendingSource()
					before, err := source.EvidenceDigest()
					if err != nil {
						t.Fatal(err)
					}
					got, err := pending.EvidenceDigest()
					if !ok || err != nil || before != got {
						t.Fatal("continuation lost its exact pending-source evidence")
					}
					projected, err := source.Project(child, "pending-selected-owner")
					if err != nil {
						t.Fatal(err)
					}
					publication, err := continued.RetainedPublication("pending-selected-owner")
					if err != nil || publication != projected.Publication {
						t.Fatal("continuation differs from canonical pending correspondence")
					}
					event, err := continued.Event(activityidentity.ForkLineageEventID(child.RunID, source.Publication.EventID), "pending-selected-owner")
					if err != nil || !event.CreatedAt().Equal(source.Command.Due.Absolute) {
						t.Fatalf("pending continuation changed retained due: %v", err)
					}
					if err := continued.ValidateEvent(event, "pending-selected-owner"); err != nil {
						t.Fatal(err)
					}
					pending.Publication.CommittedCause.EventID = uuid.NewString()
					source.Publication.CommittedCause.EventID = uuid.NewString()
					retained, _ := continued.PendingSource()
					after, err := retained.EvidenceDigest()
					if err != nil || before != after {
						t.Fatal("pending carrier aliases input or returned cause evidence")
					}
				})
			}
		}
	}
}

func TestPendingJoinContinuationRefusesChangedMeaningAndAbsentEvidence(t *testing.T) {
	_, source, _ := transferredJoinOccurrenceFixture(t, "orders", timeridentity.TimerHandleJoinComplete, executionmode.Mock)
	for _, fault := range []string{"empty", "same_run", "due", "mode", "cause"} {
		t.Run(fault, func(t *testing.T) {
			current := source
			child := publishedJoinContinuationChild(t, source.Command)
			switch fault {
			case "empty":
				current = TransferredJoinOccurrence{}
			case "same_run":
				child.RunID = source.Command.RunID
			case "due":
				child.Due.Absolute = child.Due.Absolute.Add(time.Millisecond)
			case "mode":
				child.ExecutionMode = executionmode.Live
			case "cause":
				current.Publication.CommittedCause.EventID = ""
			}
			if continued, err := ProjectPendingJoinContinuation(current, child); err == nil || continued.Present() {
				t.Fatal("invalid pending source or changed child meaning was admitted")
			}
		})
	}
}
