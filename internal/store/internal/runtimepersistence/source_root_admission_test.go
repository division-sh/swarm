package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
)

type rootedSourceStore interface {
	selectedSourceArtifactStore
	EnsureRootedSource(context.Context, sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error)
	GetRootedSource(context.Context, sourceartifact.RootAdmissionReference) (sourceartifact.RootedSource, error)
	EnsureRootedSourceWithData(context.Context, sourceartifact.RootedSource, durabledata.Catalog) (sourceartifact.RootedEnsureResult, error)
}

func TestS1RootAdmissionCommitUncertaintyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, withData := range []bool{false, true} {
			for _, committed := range []bool{false, true} {
				name := backend + "/source"
				if withData {
					name += "_with_data"
				}
				if committed {
					name += "/commit_before_lost_ack"
				} else {
					name += "/rollback_before_lost_ack"
				}
				t.Run(name, func(t *testing.T) {
					owner, _, connector := newStopCommitStore(t, backend)
					selected := owner.(rootedSourceStore)
					source, _ := admittedRootForStorage(t, "shop-a")
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					ensure := func() (sourceartifact.RootedEnsureResult, error) {
						if withData {
							return selected.EnsureRootedSourceWithData(ctx, source, durabledata.Catalog{BundleHash: source.Admission().BundleHash()})
						}
						return selected.EnsureRootedSource(ctx, source)
					}
					cause := errors.New("injected rooted source COMMIT acknowledgment unavailable")
					var calls atomic.Int32
					connector.arm(func(tx driver.Tx) error {
						calls.Add(1)
						if committed {
							return errors.Join(cause, tx.Commit())
						}
						return errors.Join(cause, tx.Rollback())
					})
					if _, err := ensure(); !errors.Is(err, cause) || calls.Load() != 1 {
						t.Fatalf("lost rooted uncertainty or retried automatically: calls=%d err=%v", calls.Load(), err)
					}
					loaded, err := selected.GetRootedSource(ctx, source.Admission().Reference())
					if committed {
						if err != nil || !loaded.Admission().Equal(source.Admission()) || !bytes.Equal(loaded.Artifact().LogicalBlob(), source.Artifact().LogicalBlob()) {
							t.Fatalf("acknowledged-loss readback lost committed root: %+v, %v", loaded, err)
						}
					} else {
						if !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
							t.Fatalf("rolled-back root became available: %v", err)
						}
						if _, err := selected.GetSourceArtifact(ctx, source.Admission().BundleHash()); !errors.Is(err, sourceartifact.ErrNotFound) {
							t.Fatalf("rolled-back rooted transaction left content: %v", err)
						}
					}
					repeated, err := ensure()
					if err != nil || repeated.Created == committed || repeated.Artifact.Created == committed {
						t.Fatalf("explicit exact reconciliation failed: %+v, %v", repeated, err)
					}
				})
			}
		}
	}
}

func selectedRootAdmissionStore(t *testing.T, backend string) rootedSourceStore {
	t.Helper()
	if backend == "sqlite" {
		return newBootstrappedSQLiteRuntimeStoreForTest(t)
	}
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
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

func admittedRootForStorage(t *testing.T, directory string) (sourceartifact.RootedSource, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), directory)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nodes.yaml"), []byte("worker: {type: procedure}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	rooted, err := artifact.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	return rooted, root
}

func TestS1RootAdmissionRetainedOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend)
			ctx := context.Background()
			sourceA, rootA := admittedRootForStorage(t, "shop-a")
			sourceB, rootB := admittedRootForStorage(t, "shop-b")
			if sourceA.Artifact().BundleHash() != sourceB.Artifact().BundleHash() || sourceA.Admission().Equal(sourceB.Admission()) {
				t.Fatal("fixture failed to distinguish rooted identity from content")
			}
			if _, err := selected.EnsureSourceArtifact(ctx, sourceA.Artifact()); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.GetRootedSource(ctx, sourceA.Admission().Reference()); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("content-only ensure minted root evidence: %v", err)
			}
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				result, err := selected.EnsureRootedSource(ctx, source)
				if err != nil || !result.Created || result.Artifact.Created || !result.Source.Admission().Equal(source.Admission()) {
					t.Fatalf("ensure exact root: %+v, %v", result, err)
				}
				repeat, err := selected.EnsureRootedSource(ctx, source)
				if err != nil || repeat.Created || repeat.Artifact.Created {
					t.Fatalf("exact root ensure changed on repetition: %+v, %v", repeat, err)
				}
			}
			for _, root := range []string{rootA, rootB} {
				if err := os.RemoveAll(root); err != nil {
					t.Fatal(err)
				}
			}
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				loaded, err := selected.GetRootedSource(ctx, source.Admission().Reference())
				if err != nil || loaded.Validate() != nil || !loaded.Admission().Equal(source.Admission()) ||
					!bytes.Equal(loaded.Artifact().LogicalBlob(), source.Artifact().LogicalBlob()) {
					t.Fatalf("retained root lost after tree removal: %+v, %v", loaded, err)
				}
				if _, present := loaded.Artifact().RootAdmission(); present {
					t.Fatal("retained root mutated shared content-only artifact")
				}
			}
			missing := sourceA.Admission().Reference()
			missing.RootDirectory = "unadmitted"
			if _, err := selected.GetRootedSource(ctx, missing); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("caller-selected root string minted authority: %v", err)
			}
		})
	}
}

func TestS1RootAdmissionCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend)
			source, _ := admittedRootForStorage(t, "shop-a")
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := selected.EnsureRootedSource(ctx, source); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled root was admitted: %v", err)
			}
			if _, err := selected.GetSourceArtifact(context.Background(), source.Admission().BundleHash()); !errors.Is(err, sourceartifact.ErrNotFound) {
				t.Fatalf("canceled source content persisted: %v", err)
			}
			if _, err := selected.EnsureRootedSource(context.Background(), source); err != nil {
				t.Fatal(err)
			}
			if _, err := selected.GetRootedSource(ctx, source.Admission().Reference()); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled read disclosed root evidence: %v", err)
			}
			if _, err := selected.EnsureRootedSource(context.Background(), sourceartifact.RootedSource{}); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("zero root evidence persisted: %v", err)
			}
		})
	}
}

func TestS1RootAdmissionCatalogFailureRollsBackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend)
			source, _ := admittedRootForStorage(t, "shop-a")
			ctx := context.Background()
			invalid := durabledata.Catalog{
				BundleHash: source.Admission().BundleHash(), Declarations: []durabledata.Declaration{{}},
			}
			if _, err := selected.EnsureRootedSourceWithData(ctx, source, invalid); err == nil {
				t.Fatal("invalid catalog admitted rooted source")
			}
			if _, err := selected.GetSourceArtifact(ctx, source.Admission().BundleHash()); !errors.Is(err, sourceartifact.ErrNotFound) {
				t.Fatalf("catalog failure retained partial content: %v", err)
			}
			if _, err := selected.GetRootedSource(ctx, source.Admission().Reference()); !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) {
				t.Fatalf("catalog failure retained partial root evidence: %v", err)
			}
			valid := durabledata.Catalog{BundleHash: source.Admission().BundleHash()}
			if _, err := selected.EnsureRootedSourceWithData(ctx, source, valid); err != nil {
				t.Fatalf("new valid admission could not recover after catalog rollback: %v", err)
			}
		})
	}
}
