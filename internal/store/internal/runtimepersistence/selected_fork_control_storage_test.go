package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

func TestSelectedForkControlStorageExactPredicateAndIsolationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			f := newSelectedCompletionFixture(t, selected, db, sqlite)
			parity := runLifecycleCandidateParityFixture{store: selected.(runLifecycleCandidateParityStore), db: db, postgres: !sqlite}
			var hash, binding string
			if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, f.forkRun).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT binding_id FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, f.forkRun).Scan(&binding); err != nil {
				t.Fatal(err)
			}
			seedTestAgentRow(t, ctx, db, !sqlite, mustTestAgentIdentityForRun(f.sourceRun, "selected-agent", "selected-test"), "active")
			for _, cut := range []struct {
				name                    string
				state                   runtimerunlifecycle.State
				loaded, candidate, want bool
			}{
				{"paused-unloaded", runtimerunlifecycle.StatePaused, false, false, false},
				{"paused-no-candidate", runtimerunlifecycle.StatePaused, true, false, false},
				{"running-unloaded", runtimerunlifecycle.StateRunning, false, true, true},
				{"running-awaiting-mutation", runtimerunlifecycle.StateRunning, true, false, true},
				{"running-candidate", runtimerunlifecycle.StateRunning, true, true, false},
				{"terminal-unloaded", runtimerunlifecycle.StateCompleted, false, false, false},
			} {
				t.Run(cut.name, func(t *testing.T) {
					if cut.state == runtimerunlifecycle.StateCompleted {
						snapshot, disposition, err := completeRunLifecycleCandidateParity(parity, ctx, f.forkRun, time.Now().UTC())
						if err != nil || snapshot.State != cut.state || disposition != runtimerunlifecycle.MutationApplied {
							t.Fatalf("canonical completed fixture: %+v %s %v", snapshot, disposition, err)
						}
					} else {
						disposition, err := transitionRunLifecycleParity(parity, ctx, f.forkRun, cut.state)
						if err != nil || (disposition != runtimerunlifecycle.MutationApplied && disposition != runtimerunlifecycle.MutationExactNoop) {
							t.Fatalf("canonical active fixture: %s %v", disposition, err)
						}
					}
					if cut.candidate {
						forceRunLifecycleCandidateParity(t, parity, ctx, f.forkRun, time.Now().UTC())
					} else if err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
						if sqlite {
							return runlifecyclefixture.ForceSQLiteCompletionCandidateRevision(ctx, tx, f.forkRun)
						}
						return runlifecyclefixture.ForcePostgresCompletionCandidateRevision(ctx, tx, f.forkRun)
					}); err != nil {
						t.Fatal(err)
					}
					loaded := "unloaded"
					if cut.loaded {
						loaded = hash
					}
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
					if err != nil {
						t.Fatal(err)
					}
					probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
					if err != nil {
						t.Fatal(err)
					}
					got, err := ReadSelectedForkControlStorageForTest(ctx, selected, f.forkRun, loaded, "selected-agent")
					if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
						t.Fatalf("selected control escaped its original read snapshot: %+v", counts)
					}
					restore()
					want := SelectedForkControlStorage{AwaitingMutation: cut.want, BindingID: binding, AgentCount: 1}
					if err != nil || got != want {
						t.Fatalf("exact selected control predicate: got=%+v want=%+v err=%v", got, want, err)
					}
					foreign, err := ReadSelectedForkControlStorageForTest(ctx, selected, f.forkRun, loaded, "foreign-agent")
					if err != nil || foreign.AgentCount != 0 || foreign.BindingID != binding || foreign.AwaitingMutation != cut.want {
						t.Fatalf("foreign agent contaminated selected control evidence: %+v %v", foreign, err)
					}
					after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("control observation mutated its original owner: %v", err)
					}
				})
			}
		})
	}
}

func TestSelectedForkControlStorageDiscardsPartialEvidenceBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		got, err := ReadSelectedForkControlStorageForTest(ctx, owner, uuid.NewString(), "loaded", "agent")
		if err == nil || got != (SelectedForkControlStorage{}) {
			t.Fatalf("foreign owner returned evidence: %+v %v", got, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"invalid-run", "missing-run", "missing-binding", "empty-bundle", "empty-agent", "cancelled", "closed", "late-read-error"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
				if cut == "closed" {
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
					got, err := ReadSelectedForkControlStorageForTest(ctx, selected, uuid.NewString(), "loaded", "selected-agent")
					if err == nil || got != (SelectedForkControlStorage{}) {
						t.Fatalf("closed owner returned evidence: %+v %v", got, err)
					}
					return
				}
				f := newSelectedCompletionFixture(t, selected, db, sqlite)
				runID, hash, agent, readCtx := f.forkRun, "loaded", "selected-agent", ctx
				switch cut {
				case "invalid-run":
					runID = "not-canonical"
				case "missing-run":
					runID = uuid.NewString()
				case "missing-binding":
					runID = f.sourceRun
				case "empty-bundle":
					hash = " "
				case "empty-agent":
					agent = " "
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "late-read-error":
					if _, err := db.ExecContext(ctx, `ALTER TABLE agents RENAME TO unavailable_control_agents`); err != nil {
						t.Fatal(err)
					}
				}
				got, err := ReadSelectedForkControlStorageForTest(readCtx, selected, runID, hash, agent)
				if err == nil || got != (SelectedForkControlStorage{}) || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed observation retained partial evidence: %+v %v", got, err)
				}
			})
		}
	}
}
