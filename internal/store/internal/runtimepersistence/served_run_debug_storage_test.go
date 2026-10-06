package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestServedRunDebugStoragePreservesFourNativeFieldsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Now().UTC())
			for _, cut := range []string{"null-candidate", "due-candidate", "paused", "completed", "missing"} {
				t.Run(cut, func(t *testing.T) {
					lookup := runID
					switch cut {
					case "null-candidate":
						if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
							if fixture.postgres {
								return runlifecyclefixture.ForcePostgresCompletionCandidateRevision(ctx, tx, runID)
							}
							return runlifecyclefixture.ForceSQLiteCompletionCandidateRevision(ctx, tx, runID)
						}); err != nil {
							t.Fatal(err)
						}
					case "due-candidate":
						forceRunLifecycleCandidateParity(t, fixture, ctx, runID, time.Date(2030, 1, 2, 3, 4, 5, 123456000, time.UTC))
					case "paused":
						if disposition, err := transitionRunLifecycleParity(fixture, ctx, runID, runtimerunlifecycle.StatePaused); err != nil || disposition != runtimerunlifecycle.MutationApplied {
							t.Fatalf("pause fixture: %s %v", disposition, err)
						}
					case "completed":
						if disposition, err := transitionRunLifecycleParity(fixture, ctx, runID, runtimerunlifecycle.StateRunning); err != nil || disposition != runtimerunlifecycle.MutationApplied {
							t.Fatalf("resume fixture: %s %v", disposition, err)
						}
						if snapshot, disposition, err := completeRunLifecycleCandidateParity(fixture, ctx, runID, time.Now().UTC()); err != nil || snapshot.State != runtimerunlifecycle.StateCompleted || disposition != runtimerunlifecycle.MutationApplied {
							t.Fatalf("complete fixture: %+v %s %v", snapshot, disposition, err)
						}
					case "missing":
						lookup = uuid.NewString()
					}
					want := servedRunDebugNativeOracle(t, ctx, fixture, lookup)
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
					if err != nil {
						t.Fatal(err)
					}
					probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					got := readServedRunDebugSection(ctx, fixture.store, backend, "runs", lookup)
					counts := probe.Snapshot()
					restore()
					if got != want || counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
						t.Fatalf("debug read left native snapshot: got=%q want=%q counts=%+v", got, want, counts)
					}
					after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("debug read mutated storage: %v", err)
					}
				})
			}
		})
	}
}

// This is the frozen native four-column oracle, not the owner implementation.
func servedRunDebugNativeOracle(t *testing.T, ctx context.Context, fixture runLifecycleCandidateParityFixture, runID string) string {
	t.Helper()
	query := `SELECT status, completion_revision, COALESCE(completion_due_at, ''), bundle_hash FROM runs WHERE run_id = ?`
	if fixture.postgres {
		query = `SELECT status, completion_revision, COALESCE(completion_due_at::text, ''), bundle_hash FROM runs WHERE run_id = $1::uuid`
	}
	var status, due, bundle string
	var revision int64
	err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&status, &revision, &due, &bundle)
	if err == sql.ErrNoRows {
		return "runs: []"
	}
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("runs: [%s %d %s %s]", status, revision, due, bundle)
}

func TestServedRunDebugStorageRefusesUnavailableSnapshotsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Now().UTC())
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got := readServedRunDebugSection(cancelled, fixture.store, backend, "runs", runID); !strings.HasPrefix(got, "runs: ") || !strings.Contains(got, context.Canceled.Error()) || strings.Contains(got, "[running") {
				t.Fatalf("cancelled snapshot returned evidence: %q", got)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `ALTER TABLE runs RENAME TO unavailable_debug_runs`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `ALTER TABLE unavailable_debug_runs RENAME TO runs`)
					return err
				}); err != nil {
					t.Error(err)
				}
			}()
			if got := readServedRunDebugSection(ctx, fixture.store, backend, "runs", runID); !strings.HasPrefix(got, "runs: ") || !strings.Contains(got, "runs") || got == "runs: []" || strings.Contains(got, "[running") {
				t.Fatalf("unavailable snapshot returned partial evidence: %q", got)
			}
		})
	}
}
