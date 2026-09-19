package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// These are preservation probes of the existing enclosing named mutations.
// They neither create a fan-out group nor replace directive/provider finalizers.
func TestP16DirectiveFinalizationRetainsPipelineClaimFence(t *testing.T) {
	for _, success := range []bool{false, true} {
		name := "failure"
		if success {
			name = "success"
		}
		t.Run(name, func(t *testing.T) {
			forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
				store := requireProviderDirectiveStore(t, fixture)
				origin, operation, event := admitProviderDirectiveOrigin(t, fixture, store, "p16-"+name)
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 5*time.Second)
				defer cancel()
				failure := agentcontrol.DirectiveExecutionLeaseExpiredFailure()
				if !success {
					before := readP16PreservationSnapshot(t, fixture.db)
					_, err := store.FinalizeDirectiveFailure(ctx, operation.OperationID, uuid.NewString(), failure, time.Now().UTC(), time.Hour)
					if !errors.Is(err, agentcontrol.ErrDirectiveInProgress) {
						t.Fatalf("foreign execution owner finalized current directive: %v", err)
					}
					requireP16PreservationSnapshot(t, fixture.db, before)
				}
				if success {
					if _, err := store.RecordDirectiveExecuted(ctx, operation.OperationID, origin.ExecutionOwnerID, directiveOperationResponseForTest(operation), time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				}
				settledAt := time.Now().UTC()
				finalize := func() error {
					if success {
						_, err := store.FinalizeDirectiveSuccess(ctx, operation.OperationID, settledAt, time.Hour)
						return err
					}
					_, err := store.FinalizeDirectiveFailure(ctx, operation.OperationID, origin.ExecutionOwnerID, failure, settledAt, time.Hour)
					return err
				}
				release := holdP16DirectivePipelineClaim(t, ctx, fixture, event)
				before := readP16PreservationSnapshot(t, fixture.db)
				if err := finalize(); !errors.Is(err, pipelineobligation.ErrBusy) {
					t.Fatalf("%s finalization bypassed current pipeline claim: %v", name, err)
				}
				requireP16PreservationSnapshot(t, fixture.db, before)
				release()
				if err := finalize(); err != nil {
					t.Fatalf("finalization after actual claim release: %v", err)
				}
				if success {
					requireDirectiveOperationState(t, store, operation.OperationID, agentcontrol.DirectiveOperationSucceeded, "")
					assertDirectiveSuccessSettlement(t, fixture.db, mustLoadP16Directive(t, store, operation.OperationID))
				} else {
					requireDirectiveOperationState(t, store, operation.OperationID, agentcontrol.DirectiveOperationFailed, failure.Detail.Code)
					assertDirectiveReceipt(t, fixture.db, event.ID(), "error", &failure)
				}
				settled := readP16PreservationSnapshot(t, fixture.db)
				if err := finalize(); err != nil {
					t.Fatalf("matching finalization replay: %v", err)
				}
				requireP16PreservationSnapshot(t, fixture.db, settled)
			})
		})
	}
}

func TestP16ProviderDirectiveParentMutationPreservesAtomicity(t *testing.T) {
	for _, launched := range []bool{false, true} {
		for _, fault := range []string{"pipeline_claim", "directive_update"} {
			name := "prelaunch/" + fault
			if launched {
				name = "drained/" + fault
			}
			t.Run(name, func(t *testing.T) {
				forEachProviderDrainStore(t, func(t *testing.T, fixture completionSettlementFixture) {
					store := requireProviderDirectiveStore(t, fixture)
					origin, operation, event := admitProviderDirectiveOrigin(t, fixture, store, "p16-provider")
					ctx := providerDirectiveContext(t, fixture, origin, event, "p16-provider")
					var mutate func() error
					wantState := agentcontrol.DirectiveOperationFailed
					wantCode := "provider_attempt_superseded_before_launch"
					var attemptID string
					if launched {
						handle := beginObservedCompletionForSettlementTest(t, ctx, "anthropic_api", "p16-provider")
						attemptID = handle.Attempt().AttemptID
						transition := supersedeProviderDrainFixture(t, fixture, manager.AgentLifecycleTerminated)
						if transition.ProviderDrainCount != 1 {
							t.Fatalf("expected exact captured directive drain: %+v", transition)
						}
						settlement := completionDirectiveSettlementForTest(t, handle.Attempt().Authority.Target, fixture, event, "anthropic_api", "provider-head-current", "forbidden-head")
						mutate = func() error {
							result, err := handle.SettleCompletion(ctx, settlement)
							if err == nil && (!result.Committed || !result.OriginSettled || result.Disposition != effects.CompletionSettlementDrained || !result.Origin.Directive.Same(origin)) {
								t.Errorf("drained settlement lost exact origin: %+v", result)
							}
							return err
						}
						wantState, wantCode = agentcontrol.DirectiveOperationIndeterminate, "provider_attempt_drained_before_directive_completion"
					} else {
						handle, err := beginManagedCompletionForTest(t, ctx, "anthropic_api", []byte("p16-provider"))
						if err != nil {
							t.Fatal(err)
						}
						attemptID = handle.Attempt().AttemptID
						mutate = func() error {
							_, err := commitProviderDrainTransition(t, fixture, "teardown", manager.AgentLifecycleTerminated)
							return err
						}
					}
					var remove func()
					if fault == "pipeline_claim" {
						remove = holdP16DirectivePipelineClaim(t, ctx, fixture, event)
					} else {
						backend := "postgres"
						if fixture.sqlite {
							backend = "sqlite"
						}
						drop := installDirectiveRejectStateTrigger(t, directiveAmbiguityBackend{name: backend, db: fixture.db}, wantState)
						var once sync.Once
						remove = func() { once.Do(drop) }
						t.Cleanup(remove)
					}
					before := readP16PreservationSnapshot(t, fixture.db)
					err := mutate()
					if fault == "pipeline_claim" {
						if !errors.Is(err, pipelineobligation.ErrBusy) {
							t.Fatalf("provider parent mutation bypassed current pipeline claim: %v", err)
						}
					} else {
						requireP16DirectiveUpdateFault(t, err, fixture.sqlite)
					}
					requireP16PreservationSnapshot(t, fixture.db, before)
					remove()
					if err := mutate(); err != nil {
						t.Fatalf("provider parent mutation after proven rollback: %v", err)
					}
					requireDirectiveOperationState(t, store, operation.OperationID, wantState, wantCode)
					op := mustLoadP16Directive(t, store, operation.OperationID)
					assertDirectiveReceipt(t, fixture.db, event.ID(), "error", op.Failure)
					if launched {
						requireProviderDrainState(t, fixture, attemptID, "settled")
					} else {
						requireExternalAttemptState(t, fixture.db, fixture.sqlite, attemptID, effects.StateTerminalFailure)
					}
				})
			})
		}
	}
}

func holdP16DirectivePipelineClaim(t *testing.T, ctx context.Context, fixture completionSettlementFixture, event events.Event) func() {
	t.Helper()
	owner := fixture.store.(interface {
		PipelineObligations() pipelineobligation.Store
	}).PipelineObligations()
	claim, err := owner.ClaimPublication(ctx, event.ID())
	if err != nil {
		t.Fatalf("hold exact directive event pipeline claim: %v", err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			if err := owner.Release(context.WithoutCancel(ctx), claim); err != nil {
				t.Errorf("release exact directive event pipeline claim: %v", err)
			}
		})
	}
	t.Cleanup(release)
	return release
}

func mustLoadP16Directive(t *testing.T, store providerDirectiveTestStore, operationID string) agentcontrol.DirectiveOperation {
	t.Helper()
	op, found, err := store.LoadDirectiveOperation(testAuthorActivityContext(), operationID)
	if err != nil || !found {
		t.Fatalf("load directive operation: found=%v err=%v", found, err)
	}
	return op
}

func requireP16DirectiveUpdateFault(t *testing.T, err error, sqlite bool) {
	t.Helper()
	if sqlite {
		var native interface{ Code() int }
		if !errors.As(err, &native) || native.Code() != 1811 || !strings.Contains(native.(error).Error(), "injected terminal transition failure") {
			t.Fatalf("expected exact SQLite directive-update trigger fault: %v", err)
		}
		return
	}
	var native *pq.Error
	if !errors.As(err, &native) || native.Code != "P0001" || native.Message != "injected terminal transition failure" {
		t.Fatalf("expected exact PostgreSQL directive-update trigger fault: %v", err)
	}
}

func readP16PreservationSnapshot(t *testing.T, db *sql.DB, extraTables ...string) map[string][]string {
	t.Helper()
	out := make(map[string][]string)
	// Each fixture owns its database and has no autonomous worker. Compare full
	// rows, not only counts, so rollback cannot hide rewritten failure/history.
	tables := []string{"agent_directive_operations", "event_receipts", "api_idempotency", "author_activity_occurrences", "author_activity_order", "run_fork_revisions", "run_fork_revision_heads", "run_fork_fact_revisions", "runtime_external_effect_operations", "runtime_external_effect_attempts", "runtime_provider_attempt_drains", "runtime_effect_budget_reservations", "agent_turns", "spend_ledger", "agents"}
	for _, table := range append(tables, extraTables...) {
		rows, err := db.Query(`SELECT * FROM ` + table)
		if err != nil {
			t.Fatalf("read preservation table %s: %v", table, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			targets := make([]any, len(columns))
			for i := range values {
				targets[i] = &values[i]
			}
			if err := rows.Scan(targets...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			raw, err := json.Marshal(values)
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			out[table] = append(out[table], string(raw))
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil {
			t.Fatal(err)
		}
		sort.Strings(out[table])
	}
	return out
}

func requireP16PreservationSnapshot(t *testing.T, db *sql.DB, before map[string][]string) {
	t.Helper()
	after := readP16PreservationSnapshot(t, db)
	if !reflect.DeepEqual(before, after) {
		for table, rows := range after {
			if !reflect.DeepEqual(rows, before[table]) {
				t.Errorf("preservation changed %s: before=%v after=%v", table, before[table], rows)
			}
		}
		t.Fatal("directive/provider mutation changed durable evidence despite refusal or exact replay")
	}
}
