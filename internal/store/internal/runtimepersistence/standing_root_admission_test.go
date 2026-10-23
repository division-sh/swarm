package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type rootedStandingStore interface {
	rootedRunStore
	runtimepipeline.StandingServicePersistence
	Close() error
}

func TestS1StandingRootedContextAndPublicationFencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedStandingStore)
			sourceA, _ := admittedRootForStorage(t, "shop-a")
			sourceB, _ := admittedRootForStorage(t, "shop-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			ctxA := runtimecorrelation.WithRootAdmission(ctx, sourceA.Admission())
			ctxB := runtimecorrelation.WithRootAdmission(ctx, sourceB.Admission())
			if _, err := selected.EnsureRootedSource(ctx, sourceA); err != nil {
				t.Fatal(err)
			}
			candidate := rootedStandingCandidate(sourceA)
			created, err := selected.ReconcileStandingService(ctxA, candidate)
			if err != nil {
				t.Fatal(err)
			}
			candidate.RootAdmission = sourceB.Admission()
			if _, err := selected.ReconcileStandingService(ctxB, candidate); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("unretained root acquired a standing declaration: %v", err)
			}
			requireRunRoot(t, ctxA, selected, created.RunID, sourceA)
			if _, err := selected.EnsureRootedSource(ctx, sourceB); err != nil {
				t.Fatal(err)
			}
			revised, err := selected.ReconcileStandingService(ctxB, candidate)
			if err != nil || revised.RootReference != sourceB.Admission().Reference() {
				t.Fatalf("admitted rooted declaration could not revise its predecessor: %+v, %v", revised, err)
			}
			if _, err := selected.PublishStandingService(ctxA, created.ServiceID, created.RunID, created.Generation); err == nil {
				t.Fatal("stale rooted runtime published another root's standing generation")
			}
			if _, err := selected.SuspendStandingService(ctxB, runtimepipeline.StandingServiceOperation{
				ServiceID: created.ServiceID, Expected: &created, ExecutionPosture: executionposture.Live,
			}); err == nil || !strings.Contains(err.Error(), "authority changed before mutation") {
				t.Fatalf("stale standing observation did not fail at its authority fence: %v", err)
			}
			invalid := runtimecorrelation.WithRootAdmission(ctxB, sourceartifact.RootAdmission{})
			if _, err := selected.ReconcileStandingService(invalid, candidate); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("cleared root context was repaired from persisted state: %v", err)
			}
			if _, err := selected.PublishStandingService(invalid, created.ServiceID, created.RunID, created.Generation); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("cleared root context published a standing generation: %v", err)
			}
			canceled, cancel := context.WithCancel(ctxB)
			cancel()
			if _, err := selected.ReconcileStandingService(canceled, candidate); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled standing admission changed its failure classification: %v", err)
			}
			loaded, found, err := selected.LoadReconciledStandingService(ctxB, candidate)
			if err != nil || !found || loaded.PublicationSequence != 0 || loaded.EffectiveState != "active" {
				t.Fatalf("refused controls changed standing authority: %+v, %t, %v", loaded, found, err)
			}
			sequence, err := selected.PublishStandingService(ctxB, created.ServiceID, created.RunID, created.Generation)
			if err != nil || sequence != 1 {
				t.Fatalf("exact rooted runtime could not publish: sequence=%d, %v", sequence, err)
			}
			requireRunRoot(t, ctxB, selected, created.RunID, sourceB)
		})
	}
}

func TestS1StandingDormantDeclarationKeepsIndependentRunRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedStandingStore)
			sourceA, _ := admittedRootForStorage(t, "shop-a")
			sourceB, _ := admittedRootForStorage(t, "shop-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			candidate := rootedStandingCandidate(sourceA)
			created, err := selected.ReconcileStandingService(runtimecorrelation.WithRootAdmission(ctx, sourceA.Admission()), candidate)
			if err != nil {
				t.Fatal(err)
			}
			candidate.RootAdmission = sourceB.Admission()
			candidate.BindingEnabled = false
			candidate.BindingBlockReason = runlifecycle.StandingBindingCredentialsAbsent
			ctxB := runtimecorrelation.WithRootAdmission(ctx, sourceB.Admission())
			for range 2 {
				dormant, err := selected.ReconcileStandingService(ctxB, candidate)
				if err != nil || dormant.RootReference != sourceB.Admission().Reference() || dormant.EffectiveState != "dormant" || dormant.RunID != created.RunID {
					t.Fatalf("dormant declaration lost its independent root: %+v, %v", dormant, err)
				}
				requireRunRoot(t, ctx, selected, created.RunID, sourceA)
			}
			if _, err := selected.ResumeStandingService(ctxB, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, ExecutionPosture: executionposture.Live}); err == nil || !strings.Contains(err.Error(), "cannot resume") {
				t.Fatalf("credential-absent resume did not fail at its lifecycle gate: %v", err)
			}
			candidate.BindingEnabled, candidate.BindingBlockReason = true, ""
			resumed, err := selected.ReconcileStandingService(ctxB, candidate)
			if err != nil || resumed.RunID != created.RunID || resumed.Generation != created.Generation || resumed.RootReference != sourceB.Admission().Reference() {
				t.Fatalf("fresh enabled declaration did not revise the exact retained run: %+v, %v", resumed, err)
			}
			requireRunRoot(t, ctxB, selected, created.RunID, sourceB)
		})
	}
}

func TestS1StandingResetAfterReopenUsesDeclarationRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, reopen := openRootedStandingRestartFixture(t, backend)
			sourceA, pathA := admittedRootForStorage(t, "shop-a")
			sourceB, pathB := admittedRootForStorage(t, "shop-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			candidate := rootedStandingCandidate(sourceA)
			created, err := selected.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := selected.MarkTerminalRun(ctx, runlifecycle.TerminalRequest{RunID: created.RunID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			candidate.RootAdmission = sourceB.Admission()
			stopped, err := selected.ReconcileStandingService(runtimecorrelation.WithRootAdmission(ctx, sourceB.Admission()), candidate)
			if err != nil || stopped.RootReference != sourceB.Admission().Reference() || stopped.RestartDisposition.Executable() {
				t.Fatalf("terminal generation was rebound or reopened: %+v, %v", stopped, err)
			}
			requireRunRoot(t, ctx, selected, created.RunID, sourceA)
			for _, path := range []string{pathA, pathB} {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			selected = reopen()
			statuses, err := selected.ListStandingServiceStatuses(ctx)
			if err != nil || len(statuses) != 1 || statuses[0].RootReference != sourceB.Admission().Reference() {
				t.Fatalf("restart lost declaration evidence: %+v, %v", statuses, err)
			}
			restored, err := selected.GetRootedSource(ctx, statuses[0].RootReference)
			if err != nil {
				t.Fatal(err)
			}
			ctx = runtimecorrelation.WithRootAdmission(ctx, restored.Admission())
			reset, err := selected.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Expected: &statuses[0].StandingServiceReconciliation, ExecutionPosture: executionposture.Live})
			if err != nil || reset.Generation != created.Generation+1 || reset.RootReference != restored.Admission().Reference() {
				t.Fatalf("reset reconstructed from a different source root: %+v, %v", reset, err)
			}
			requireRunRoot(t, ctx, selected, reset.RunID, sourceB)
			requireRunRoot(t, ctx, selected, created.RunID, sourceA)
			predecessor, err := ReadRuntimeLogRunSnapshotForTest(ctx, selected, created.RunID)
			if err != nil || predecessor.State != runlifecycle.StateCancelled {
				t.Fatalf("reset revived the terminal predecessor: %+v, %v", predecessor, err)
			}
		})
	}
}

func rootedStandingCandidate(source sourceartifact.RootedSource) runtimepipeline.StandingServiceCandidate {
	return runtimepipeline.StandingServiceCandidate{
		BindingEnabled: true, ServiceID: runtimeflowidentity.StandingServiceID("service"), FlowPath: "service",
		Source: sourceartifactfixture.FactFor(source.Artifact()), RootAdmission: source.Admission(),
	}
}

func TestS1StandingRootRevisionCommitUncertaintyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, committed := range []bool{false, true} {
			name := backend + "/rollback_before_lost_ack"
			if committed {
				name = backend + "/commit_before_lost_ack"
			}
			t.Run(name, func(t *testing.T) {
				owner, _, connector := newStopCommitStore(t, backend)
				selected := owner.(rootedStandingStore)
				sourceA, _ := admittedRootForStorage(t, "shop-a")
				sourceB, _ := admittedRootForStorage(t, "shop-b")
				ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
				for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
					if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
						t.Fatal(err)
					}
				}
				candidate := rootedStandingCandidate(sourceA)
				created, err := selected.ReconcileStandingService(ctx, candidate)
				if err != nil {
					t.Fatal(err)
				}
				cause := errors.New("standing root revision COMMIT acknowledgement unavailable")
				var calls atomic.Int32
				connector.arm(func(tx driver.Tx) error {
					calls.Add(1)
					if committed {
						return errors.Join(cause, tx.Commit())
					}
					return errors.Join(cause, tx.Rollback())
				})
				candidate.RootAdmission = sourceB.Admission()
				if _, err := selected.ReconcileStandingService(ctx, candidate); !errors.Is(err, cause) || calls.Load() != 1 {
					t.Fatalf("standing revision lost uncertainty or replayed: calls=%d err=%v", calls.Load(), err)
				}
				want := sourceA
				if committed {
					want = sourceB
				}
				statuses, err := selected.ListStandingServiceStatuses(ctx)
				if err != nil || len(statuses) != 1 || statuses[0].RootReference != want.Admission().Reference() || statuses[0].RunID != created.RunID || statuses[0].Generation != created.Generation {
					t.Fatalf("standing declaration escaped its atomic outcome: %+v, %v", statuses, err)
				}
				requireRunRoot(t, ctx, selected, created.RunID, want)
				repeated, err := selected.ReconcileStandingService(ctx, candidate)
				if err != nil || repeated.RootReference != sourceB.Admission().Reference() || repeated.RunID != created.RunID || repeated.Generation != created.Generation {
					t.Fatalf("explicit rooted reconciliation created another generation or lost target: %+v, %v", repeated, err)
				}
				requireRunRoot(t, ctx, selected, created.RunID, sourceB)
			})
		}
	}
}

func openRootedStandingRestartFixture(t *testing.T, backend string) (rootedStandingStore, func() rootedStandingStore) {
	t.Helper()
	var open func() rootedStandingStore
	if backend == "sqlite" {
		path := filepath.Join(t.TempDir(), "standing.db")
		open = func() rootedStandingStore { return newBootstrappedSQLiteRuntimeStoreForPath(t, path) }
	} else {
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		open = func() rootedStandingStore {
			selected, err := NewPostgresStore(dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := selected.Close(); err != nil {
					t.Error(err)
				}
			})
			bootstrapTestPostgresStore(t, selected)
			return selected
		}
	}
	current := open()
	return current, func() rootedStandingStore {
		if err := current.Close(); err != nil {
			t.Fatal(err)
		}
		current = open()
		return current
	}
}

func TestS1StandingSameContentRootRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedStandingStore)
			sourceA, _ := admittedRootForStorage(t, "shop-a")
			sourceB, _ := admittedRootForStorage(t, "shop-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			candidate := runtimepipeline.StandingServiceCandidate{
				BindingEnabled: true, ServiceID: runtimeflowidentity.StandingServiceID("service"), FlowPath: "service",
				Source: sourceartifactfixture.FactFor(sourceA.Artifact()), RootAdmission: sourceA.Admission(),
			}
			created, err := selected.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			candidate.RootAdmission = sourceB.Admission()
			revised, err := selected.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if revised.Transition != "revised" || revised.RunID != created.RunID || revised.Generation != created.Generation {
				t.Errorf("same-content root revision was ignored or created another generation: before=%+v after=%+v", created, revised)
			}
			requireRunRoot(t, ctx, selected, created.RunID, sourceB)
		})
	}
}
