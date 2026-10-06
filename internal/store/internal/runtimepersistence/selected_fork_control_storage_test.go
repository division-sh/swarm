package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSelectedForkControlStorageExactPredicateAndIsolationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			f := newSelectedCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContext()
			var hash, binding string
			if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, f.forkRun).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, `SELECT binding_id FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1`, f.forkRun).Scan(&binding); err != nil {
				t.Fatal(err)
			}
			seedTestAgentRow(t, ctx, db, !sqlite, mustTestAgentIdentityForRun(f.sourceRun, "selected-agent", "selected-test"), "active")
			for _, cut := range []struct {
				name, status, loaded string
				due                  any
				want                 bool
			}{
				{"paused-unloaded", "paused", "unloaded", nil, false},
				{"paused-no-candidate", "paused", hash, nil, false},
				{"running-unloaded", "running", "unloaded", time.Now().UTC(), true},
				{"running-awaiting-mutation", "running", hash, nil, true},
				{"running-candidate", "running", hash, time.Now().UTC(), false},
				{"terminal-unloaded", "completed", "unloaded", nil, false},
			} {
				t.Run(cut.name, func(t *testing.T) {
					if _, err := db.ExecContext(ctx, `UPDATE runs SET status=$1,completion_due_at=$2,completion_revision=1 WHERE run_id=$3`, cut.status, cut.due, f.forkRun); err != nil {
						t.Fatal(err)
					}
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
					if err != nil {
						t.Fatal(err)
					}
					got, err := ReadSelectedForkControlStorageForTest(ctx, selected, f.forkRun, cut.loaded, "selected-agent")
					want := SelectedForkControlStorage{AwaitingMutation: cut.want, BindingID: binding, AgentCount: 1}
					if err != nil || got != want {
						t.Fatalf("exact selected control predicate: got=%+v want=%+v err=%v", got, want, err)
					}
					foreign, err := ReadSelectedForkControlStorageForTest(ctx, selected, f.forkRun, cut.loaded, "foreign-agent")
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
