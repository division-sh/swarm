package runforkpersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

func TestPublishedBarrierContinuationPreservesOriginalOccurrenceWithoutRearming(t *testing.T) {
	for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
		t.Run(string(mode), func(t *testing.T) {
			snapshot, obligations := publishedBarrierEvidenceFixture(t, false, mode, deliverylifecycle.StatusPending,
				fanoutbarrier.Summary{Total: 2, Succeeded: 1, DeadLettered: 1}, attemptgeneration.Generation{})
			source, err := projectRunForkGenericActivation(snapshot.Timers[0])
			if err != nil {
				t.Fatal(err)
			}
			original, _, err := runForkPublishedFanOutBarrierEvidence(snapshot, *obligations[0].Barrier, source)
			if err != nil {
				t.Fatal(err)
			}
			// This model supplies explicit child coordinates, not admission of an
			// executable schedule or a guessed receiver. Entity/path stay exact.
			child := source.Command
			child.RunID = uuid.NewString()
			if err := child.Validate(); err != nil {
				t.Fatal(err)
			}
			continued, err := genericschedule.ProjectPublishedJoinContinuation(source, original.Event(), child)
			if err != nil {
				t.Fatalf("exact published barrier could not continue: %v", err)
			}
			event, err := continued.Event(activityidentity.ForkLineageEventID(child.RunID, original.Event().ID()), "selected-barrier-continuation")
			if err != nil {
				t.Fatal(err)
			}
			if !continued.ChildCommand().Payload.Equal(source.Command.Payload) || event.ExecutionMode() != mode || !event.CreatedAt().Equal(original.Event().CreatedAt()) {
				t.Fatal("barrier transfer changed frozen summary, mode or original due")
			}
			lineage, present := event.SelectedForkLineage()
			if !present || lineage.SourceRunID() != snapshot.RunID || lineage.SourceEventID() != original.Event().ID() || event.RunID() != child.RunID {
				t.Fatal("barrier transfer lost exact source/child occurrence lineage")
			}
			if source.ValidateForkJoinRestorationSource() == nil {
				t.Fatal("transferred publication authorized rearming its fired source")
			}
		})
	}
}

func TestPublishedBarrierContinuationRejectsChangedFrozenEvidence(t *testing.T) {
	for _, change := range []string{"due", "mode", "summary", "extra_payload", "event", "authority"} {
		t.Run(change, func(t *testing.T) {
			snapshot, obligations := publishedBarrierEvidenceFixture(t, false, executionmode.Live, deliverylifecycle.StatusPending,
				fanoutbarrier.Summary{Total: 2, Succeeded: 1, DeadLettered: 1}, attemptgeneration.Generation{})
			source, err := projectRunForkGenericActivation(snapshot.Timers[0])
			if err != nil {
				t.Fatal(err)
			}
			original, _, err := runForkPublishedFanOutBarrierEvidence(snapshot, *obligations[0].Barrier, source)
			if err != nil {
				t.Fatal(err)
			}
			child := source.Command
			child.RunID = uuid.NewString()
			switch change {
			case "due":
				child.Due = genericschedule.AbsoluteDue(child.Due.Absolute.Add(time.Second))
			case "mode":
				child.ExecutionMode = executionmode.Mock
			case "summary", "extra_payload":
				payload := child.Payload.Interface().(map[string]any)
				if change == "summary" {
					payload["join"] = (fanoutbarrier.Summary{Total: 2, Succeeded: 2}).Context()
				} else {
					payload["invented_authority"] = true
				}
				child.Payload, err = canonicaljson.FromGo(payload)
				if err != nil {
					t.Fatal(err)
				}
			}
			continuation, err := genericschedule.ProjectPublishedJoinContinuation(source, original.Event(), child)
			if change != "event" && change != "authority" {
				if err == nil || continuation.Present() {
					t.Fatal("changed barrier meaning accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			publication, err := continuation.RetainedPublication("exact-selected-owner")
			if err != nil {
				t.Fatal(err)
			}
			if change == "event" {
				publication.EventID = uuid.NewString()
			} else {
				publication.AuthorityStamp = ""
			}
			if err := (genericschedule.TransferredJoinOccurrence{Command: child, Publication: publication}).Validate(); err == nil {
				t.Fatal("invalid retained publication accepted")
			}
		})
	}
}
