package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type rootedRunStore interface {
	rootedSourceStore
	runlifecycle.OperationOwner
	LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
}

func TestS1RunRootCreationRequiresRetainedPairBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunStore)
			sourceA, _ := admittedRootForStorage(t, "shop-a")
			sourceB, _ := admittedRootForStorage(t, "shop-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			if _, err := selected.EnsureRootedSource(ctx, sourceB); err != nil {
				t.Fatal(err)
			}
			request := rootedRunCreationRequest(sourceA)
			if _, err := selected.CreateRun(ctx, request); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("another retained root of the same content authorized creation: %v", err)
			}
			if _, err := selected.LoadRunRootedSource(ctx, request.RunID); !errors.Is(err, runlifecycle.ErrRunNotFound) {
				t.Fatalf("refused creation left run evidence: %v", err)
			}
			if _, err := selected.EnsureRootedSource(ctx, sourceA); err != nil {
				t.Fatal(err)
			}
			if disposition, err := selected.CreateRun(ctx, request); err != nil || disposition != runlifecycle.MutationApplied {
				t.Fatalf("create exact rooted run: %s, %v", disposition, err)
			}
			if disposition, err := selected.CreateRun(ctx, request); err != nil || disposition != runlifecycle.MutationExactNoop {
				t.Fatalf("replay exact rooted run: %s, %v", disposition, err)
			}
			request.RootAdmission = sourceB.Admission()
			if _, err := selected.CreateRun(ctx, request); err == nil {
				t.Fatal("same-content create replay retargeted a bound run to another root")
			}
			requireRunRoot(t, ctx, selected, request.RunID, sourceA)
		})
	}
}

func TestS1RunRootRevisionAndHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunStore)
			sourceA, pathA := admittedRootForStorage(t, "shop-a")
			sourceB, pathB := admittedRootForStorage(t, "shop-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			if _, err := selected.EnsureRootedSource(ctx, sourceA); err != nil {
				t.Fatal(err)
			}
			request := rootedRunCreationRequest(sourceA)
			if _, err := selected.CreateRun(ctx, request); err != nil {
				t.Fatal(err)
			}
			revision := runlifecycle.SourceRevisionRequest{
				RunID: request.RunID, Source: request.Source, RootAdmission: sourceB.Admission(),
			}
			if _, err := selected.ReviseRunSource(ctx, revision); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("revision adopted an unretained same-content root: %v", err)
			}
			requireRunRoot(t, ctx, selected, request.RunID, sourceA)
			if _, err := selected.EnsureRootedSource(ctx, sourceB); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{pathA, pathB} {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			// A later root admission and removal of both authored trees cannot
			// retarget the already-bound responsibility.
			requireRunRoot(t, ctx, selected, request.RunID, sourceA)
			if disposition, err := selected.ReviseRunSource(ctx, revision); err != nil || disposition != runlifecycle.MutationApplied {
				t.Fatalf("explicit same-content root revision: %s, %v", disposition, err)
			}
			if disposition, err := selected.ReviseRunSource(ctx, revision); err != nil || disposition != runlifecycle.MutationExactNoop {
				t.Fatalf("exact root revision replay: %s, %v", disposition, err)
			}
			requireRunRoot(t, ctx, selected, request.RunID, sourceB)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if loaded, err := selected.LoadRunRootedSource(canceled, request.RunID); !errors.Is(err, context.Canceled) || loaded.Validate() == nil {
				t.Fatalf("canceled run read disclosed executable root: %+v, %v", loaded, err)
			}
			revision.RootAdmission = sourceA.Admission()
			if _, err := selected.ReviseRunSource(canceled, revision); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled revision changed source: %v", err)
			}
			requireRunRoot(t, ctx, selected, request.RunID, sourceB)
			before, err := ReadRuntimeLogRunSnapshotForTest(ctx, selected, request.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if removed, err := RemoveRuntimeLogFixtureSourceArtifactForTest(ctx, selected, request.Source.BundleHash()); err != nil || removed != 1 {
				t.Fatalf("remove source content: %d, %v", removed, err)
			}
			if loaded, err := selected.LoadRunRootedSource(ctx, request.RunID); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) || loaded.Validate() == nil {
				t.Fatalf("run history reconstructed deleted source: %+v, %v", loaded, err)
			}
			after, err := ReadRuntimeLogRunSnapshotForTest(ctx, selected, request.RunID)
			if err != nil || after.RootReference != before.RootReference || after.BundleHash != before.BundleHash || !after.Origin.Equal(before.Origin) {
				t.Fatalf("content deletion lost historical root identity: %+v, %v", after, err)
			}
		})
	}
}

func TestS1RunRootCommitUncertaintyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, revision := range []bool{false, true} {
			for _, committed := range []bool{false, true} {
				name := backend + "/create"
				if revision {
					name = backend + "/revise"
				}
				if committed {
					name += "/commit_before_lost_ack"
				} else {
					name += "/rollback_before_lost_ack"
				}
				t.Run(name, func(t *testing.T) {
					owner, _, connector := newStopCommitStore(t, backend)
					selected := owner.(rootedRunStore)
					sourceA, _ := admittedRootForStorage(t, "shop-a")
					sourceB, _ := admittedRootForStorage(t, "shop-b")
					ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
					for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
						if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
							t.Fatal(err)
						}
					}
					request := rootedRunCreationRequest(sourceA)
					mutate := func() (runlifecycle.MutationDisposition, error) { return selected.CreateRun(ctx, request) }
					want := sourceA
					if revision {
						if _, err := mutate(); err != nil {
							t.Fatal(err)
						}
						mutate = func() (runlifecycle.MutationDisposition, error) {
							return selected.ReviseRunSource(ctx, runlifecycle.SourceRevisionRequest{
								RunID: request.RunID, Source: request.Source, RootAdmission: sourceB.Admission(),
							})
						}
						if committed {
							want = sourceB
						}
					}
					cause := errors.New("injected run root COMMIT acknowledgement unavailable")
					var calls atomic.Int32
					connector.arm(func(tx driver.Tx) error {
						calls.Add(1)
						if committed {
							return errors.Join(cause, tx.Commit())
						}
						return errors.Join(cause, tx.Rollback())
					})
					if _, err := mutate(); !errors.Is(err, cause) || calls.Load() != 1 {
						t.Fatalf("root mutation lost uncertainty or replayed: calls=%d, err=%v", calls.Load(), err)
					}
					if revision || committed {
						requireRunRoot(t, ctx, selected, request.RunID, want)
					} else if _, err := selected.LoadRunRootedSource(ctx, request.RunID); !errors.Is(err, runlifecycle.ErrRunNotFound) {
						t.Fatalf("rolled-back creation retained run source: %v", err)
					}
					disposition, err := mutate()
					if err != nil || (disposition == runlifecycle.MutationExactNoop) != committed {
						t.Fatalf("explicit root mutation reconciliation: %s, %v", disposition, err)
					}
					if revision {
						want = sourceB
					}
					requireRunRoot(t, ctx, selected, request.RunID, want)
				})
			}
		}
	}
}

func TestS1RunRootReadRefusesUnacknowledgedSnapshotBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, connector := newStopCommitStore(t, backend)
			selected := owner.(rootedRunStore)
			source, _ := admittedRootForStorage(t, "shop-a")
			ctx := testAuthorActivityContextForBundle(source.Artifact().BundleHash())
			if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
				t.Fatal(err)
			}
			request := rootedRunCreationRequest(source)
			if _, err := selected.CreateRun(ctx, request); err != nil {
				t.Fatal(err)
			}
			cause := errors.New("injected run-root snapshot closure failure")
			var calls atomic.Int32
			connector.arm(func(tx driver.Tx) error {
				calls.Add(1)
				return errors.Join(cause, tx.Rollback())
			})
			got, err := selected.LoadRunRootedSource(ctx, request.RunID)
			if !errors.Is(err, cause) || calls.Load() != 1 || got.Validate() == nil {
				t.Fatalf("failed native snapshot disclosed rooted authority: valid=%t, calls=%d, err=%v", got.Validate() == nil, calls.Load(), err)
			}
			requireRunRoot(t, ctx, selected, request.RunID, source)
		})
	}
}

func requireRunRoot(t *testing.T, ctx context.Context, selected rootedRunStore, runID string, want sourceartifact.RootedSource) {
	t.Helper()
	got, err := selected.LoadRunRootedSource(ctx, runID)
	if err != nil || !got.Admission().Equal(want.Admission()) || got.Artifact().BundleHash() != want.Artifact().BundleHash() {
		t.Fatalf("run lost its frozen exact root: %+v, %v", got, err)
	}
	snapshot, err := ReadRuntimeLogRunSnapshotForTest(ctx, selected, runID)
	if err != nil || snapshot.RootReference != want.Admission().Reference() {
		t.Fatalf("run history disagrees with exact retained source: %+v, %v", snapshot, err)
	}
}

func rootedRunCreationRequest(source sourceartifact.RootedSource) runlifecycle.CreateRequest {
	return runlifecycle.CreateRequest{
		RunID: uuid.NewString(), Origin: runlifecycle.DeploymentRunOrigin(),
		Source: sourceartifactfixture.FactFor(source.Artifact()), RootAdmission: source.Admission(),
		StartedAt: time.Now().UTC(),
	}
}
