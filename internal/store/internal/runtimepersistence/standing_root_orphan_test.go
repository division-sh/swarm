package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type rootedStandingCleanupStore interface {
	rootedStandingStore
	genericschedule.Store
}

func TestS1DormantDifferentContentRemovalUsesPredecessorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedStandingCleanupStore)
			sourceA, _ := admittedRootForStorage(t, "orphan-a")
			sourceB, _ := sourceartifactfixture.Rooted(t, "orphan-b", sourceartifactfixture.New("nodes.yaml", []byte("worker: {type: procedure}\n# replacement declaration\n")))
			if sourceA.Artifact().BundleHash() == sourceB.Artifact().BundleHash() {
				t.Fatal("fixture requires distinct content as well as distinct roots")
			}
			ctxA := runtimecorrelation.WithRootAdmission(testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash()), sourceA.Admission())
			ctxB := runtimecorrelation.WithRootAdmission(testAuthorActivityContextForBundle(sourceB.Artifact().BundleHash()), sourceB.Admission())
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctxA, source); err != nil {
					t.Fatal(err)
				}
			}
			candidate := rootedStandingCandidate(sourceA)
			created, err := selected.ReconcileStandingService(ctxA, candidate)
			if err != nil {
				t.Fatal(err)
			}
			candidate.Source, candidate.RootAdmission = sourceartifactfixture.FactFor(sourceB.Artifact()), sourceB.Admission()
			candidate.BindingEnabled, candidate.BindingBlockReason = false, runlifecycle.StandingBindingCredentialsAbsent
			dormant, err := selected.ReconcileStandingService(ctxB, candidate)
			if err != nil || dormant.RunID != created.RunID || dormant.RootReference != sourceB.Admission().Reference() || dormant.EffectiveState != "dormant" {
				t.Fatalf("retain dormant declaration B independently of run A: %+v, %v", dormant, err)
			}
			requireRunRoot(t, ctxA, selected, created.RunID, sourceA)
			// A paused run can still own retained work. Exercise cleanup through
			// the real timer owner rather than just accepting an empty quiescence.
			activation := admitGenericScheduleFixture(t, ctxA, selected, testRootGenericScheduleCommand(t,
				created.RunID, uuid.NewString(), "orphan-old-source", genericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)),
			))
			beforeCleanup, err := ReadWorkflowJournalStorageForTest(ctxA, selected, created.RunID)
			if err != nil {
				t.Fatal(err)
			}
			results, err := selected.ReconcileStandingServiceSet(ctxB, nil)
			if err != nil || len(results) != 1 || results[0].RunID != created.RunID || results[0].EffectiveState != "orphaned" || results[0].RootReference != sourceB.Admission().Reference() {
				t.Fatalf("removing declaration B could not clean up retained run A: %+v, %v", results, err)
			}
			if len(results[0].TimerCancellations) != 1 || results[0].TimerCancellations[0].ActivationID != activation.ID || !results[0].DeliveryContinuationRequired {
				t.Fatalf("orphan did not acknowledge exact cleanup/continuation: %+v", results[0])
			}
			cancelled, found, err := selected.LoadGenericScheduleActivation(ctxA, activation.ID)
			if err != nil || !found || cancelled.Status != genericschedule.StatusCancelled || cancelled.CancelCause != "standing_declaration_removed" || cancelled.CancelledAt.IsZero() {
				t.Fatalf("retained run's timer was not terminally cleaned up: %+v, %t, %v", cancelled, found, err)
			}
			cleanup, err := ReadWorkflowJournalStorageForTest(ctxA, selected, created.RunID)
			wantCleanup := beforeCleanup
			wantCleanup.RunForkRevisions++
			if err != nil || cleanup != wantCleanup {
				t.Fatalf("cleanup did not retain exactly one predecessor timer revision without another story: got=%+v want=%+v err=%v", cleanup, wantCleanup, err)
			}
			statuses, err := selected.ListStandingServiceStatuses(ctxB)
			if err != nil || len(statuses) != 1 || statuses[0].DeclarationPresent || statuses[0].RootReference != sourceB.Admission().Reference() || statuses[0].Transition != "orphaned" || statuses[0].Reason != "standing_declaration_removed" {
				t.Fatalf("orphan declaration/journal lost its own evidence: %+v, %v", statuses, err)
			}
			requireRunRoot(t, ctxA, selected, created.RunID, sourceA)
			before, err := ReadWorkflowJournalStorageForTest(ctxA, selected, created.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if repeated, err := selected.ReconcileStandingServiceSet(ctxB, nil); err != nil || len(repeated) != 0 {
				t.Fatalf("repeated removal redid orphan cleanup: %+v, %v", repeated, err)
			}
			after, err := ReadWorkflowJournalStorageForTest(ctxA, selected, created.RunID)
			if err != nil || after != before {
				t.Fatalf("repeated removal appended story or revisions: before=%+v after=%+v err=%v", before, after, err)
			}
			repeatedStatuses, err := selected.ListStandingServiceStatuses(ctxB)
			if err != nil || !reflect.DeepEqual(repeatedStatuses, statuses) {
				t.Fatalf("repeated removal changed the durable declaration/journal: %+v, %v", repeatedStatuses, err)
			}
		})
	}
}
