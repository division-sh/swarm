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

// These are native read products, not served-startup or directory eligibility
// receipts. Header and authored-data membership must remain independent.
func TestQ10ServedDiagnosticsPreserveMembershipAndAbsentLifecycleBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run, sibling := uuid.NewString(), uuid.NewString()
			const (
				fielded   = "96000000-0000-4000-8000-000000000001"
				imported  = "96000000-0000-4000-8000-000000000002"
				fieldless = "96000000-0000-4000-8000-000000000003"
				noStage   = "96000000-0000-4000-8000-000000000004"
			)
			seedEntityToolStorageWitness(t, fixture, run, fielded, true, true)
			seedEntityToolStorageWitness(t, fixture, run, imported, false, true)
			seedEntityToolStorageWitness(t, fixture, run, fieldless, true, false)
			seedEntityToolStorageWitness(t, fixture, run, noStage, true, true)
			seedEntityToolStorageWitness(t, fixture, sibling, imported, true, true)
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE flow_instances SET stage_defined=FALSE, current_state='pending' WHERE run_id=$1 AND entity_id=$2`, run, noStage)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			data := readServedRunDebugSection(ctx, fixture.store, backend.name, "entity_state", run)
			want := fmt.Sprintf("entity_state: [%s storage-witness/%s queued]; [%s storage-witness/%s <nil>]; [%s storage-witness/%s <nil>]", fielded, fielded, imported, imported, noStage, noStage)
			if data != want || strings.Contains(data, fieldless) {
				t.Fatalf("data membership, exact run or lifecycle absence changed: got=%q want=%q", data, want)
			}
			headers := readServedRunDebugSection(ctx, fixture.store, backend.name, "flow_instances", run)
			if !strings.Contains(headers, fieldless) || !strings.Contains(headers, noStage) || strings.Contains(headers, imported) {
				t.Fatalf("header census was filtered through data membership: %q", headers)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("diagnostics escaped original read owner: %+v", counts)
			}
			physical, err := ReadWorkflowStateObservationRowsForTest(ctx, fixture.store)
			if err != nil || len(physical) != 4 {
				t.Fatalf("global physical field-row count changed: %+v/%v", physical, err)
			}
			seen := map[string]int{}
			importsWithoutHeader, foreignHeaders := 0, 0
			for _, row := range physical {
				seen[row.EntityID]++
				if row.FlowInstance != "storage-witness/"+row.EntityID {
					t.Fatalf("physical data path changed: %+v", row)
				}
				switch row.EntityID {
				case imported:
					if row.CurrentState == nil {
						importsWithoutHeader++
					} else if *row.CurrentState == "queued" {
						foreignHeaders++
					} else {
						t.Fatalf("same-identity foreign header changed: %+v", row)
					}
				case noStage:
					if row.CurrentState == nil || *row.CurrentState != "pending" {
						t.Fatalf("internal no-stage posture was omitted: %+v", row)
					}
				case fielded:
					if row.CurrentState == nil || *row.CurrentState != "queued" {
						t.Fatalf("constructed field-row stage changed: %+v", row)
					}
				default:
					t.Fatalf("global field inventory included a fieldless or invented row: %+v", row)
				}
			}
			if seen[fielded] != 1 || seen[imported] != 2 || seen[noStage] != 1 || importsWithoutHeader != 1 || foreignHeaders != 1 {
				t.Fatalf("global membership or exact run pairing changed: seen=%v imports=%d foreign=%d", seen, importsWithoutHeader, foreignHeaders)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 3 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("global observation escaped original read owner: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got := readServedRunDebugSection(cancelled, fixture.store, backend.name, "entity_state", run); !strings.Contains(got, context.Canceled.Error()) || strings.Contains(got, fielded) {
				t.Fatalf("cancelled data diagnostic returned evidence: %q", got)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE flow_instances SET entity_type='foreign' WHERE run_id=$1 AND entity_id=$2`, run, fielded)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got := readServedRunDebugSection(ctx, fixture.store, backend.name, "entity_state", run); got == want || strings.Contains(got, "queued]") || strings.Contains(got, "<nil>]") {
				t.Fatalf("malformed pair returned partial/clean diagnostic evidence: %q", got)
			}
		})
	}
}
