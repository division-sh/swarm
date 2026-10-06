package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestWorkspaceEffectFailureStorageRetainsAllFailureTextBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			var expected []WorkspaceEffectFailureStorage
			for _, key := range []string{"first-run", "other-run"} {
				f := newCompletionSettlementFixture(t, selected.(completionSettlementTestStore), db, sqlite)
				authority := f.authority
				authority.BudgetScopes = nil
				ctx := runtimeeffects.WithLogicalOperationIdentity(f.contextFor(authority), "diagnostic:"+key)
				ctx = withManagedCompletionTestSurface(t, ctx, authority, "claude_cli")
				frame := managedCompletionTestFrameWithEvent(t, authority, "claude_cli", managedCompletionTestEvent(authority))
				handle, err := runtimeeffects.BeginManagedCompletion(ctx, "claude_cli", []byte(key), frame, nil)
				if err != nil {
					t.Fatal(err)
				}
				// The authorized NULL-failure attempt must not appear in diagnostics.
				before, err := ReadWorkspaceEffectFailuresForTest(ctx, selected)
				if err != nil || !reflect.DeepEqual(before, expected) {
					t.Fatalf("NULL failure acquired diagnostic evidence: %+v %v", before, err)
				}
				failureErr := runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "claude_cli_process_start_failed", "effect-proof", "launch", map[string]any{"reason": key})
				failure, _ := runtimefailures.EnvelopeFromError(failureErr)
				if err := handle.Settle(ctx, runtimeeffects.StateTerminalFailure, &failure, map[string]any{"launch_rejected": true}); err != nil {
					t.Fatal(err)
				}
				var stored WorkspaceEffectFailureStorage
				if err := db.QueryRowContext(ctx, `SELECT state,CAST(failure AS TEXT) FROM runtime_external_effect_attempts WHERE attempt_id=$1`, handle.Attempt().AttemptID).Scan(&stored.State, &stored.Failure); err != nil {
					t.Fatal(err)
				}
				expected = append(expected, stored)
			}
			ctx := context.Background()
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadWorkspaceEffectFailuresForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			sort.Slice(got, func(i, j int) bool { return got[i].Failure < got[j].Failure })
			sort.Slice(expected, func(i, j int) bool { return expected[i].Failure < expected[j].Failure })
			if !reflect.DeepEqual(got, expected) || len(got) != 2 {
				t.Fatalf("diagnostics lost stored state/text or another run: %+v want=%+v", got, expected)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("failure diagnostic observation mutated evidence: %v", err)
			}
		})
	}
}

func TestWorkspaceEffectFailureStorageRefusesInvalidOwnersBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if got, err := ReadWorkspaceEffectFailuresForTest(ctx, owner); err == nil || got != nil {
			t.Fatalf("unowned diagnostic evidence: %+v %v", got, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"cancelled", "closed", "missing-table"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, _ := decisionCardTestStore(t, backend)
				readCtx := ctx
				switch cut {
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "closed":
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "missing-table":
					db, _ := decisionCardStoreDB(t, selected)
					if _, err := db.ExecContext(ctx, `ALTER TABLE runtime_external_effect_attempts RENAME TO unavailable_diagnostic_attempts`); err != nil {
						t.Fatal(err)
					}
				}
				got, err := ReadWorkspaceEffectFailuresForTest(readCtx, selected)
				if err == nil || got != nil || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed diagnostic read returned partial evidence: %+v %v", got, err)
				}
			})
		}
	}
}
