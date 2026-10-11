package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type selectedAuthorityLeaseClockRollback struct{}

func (selectedAuthorityLeaseClockRollback) Error() string {
	return "selected authority lease clock proof rolled back"
}

// This effect-leaf proof reuses the legacy selected binding fixture. Its child
// remains paused; neither a running execution row nor this fence admits serving.
func TestSelectedAuthorityLeaseClockExpiresInsideNativeTransactionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, db, sqlite := selectedForkDiscardTestStore(t, backend)
			fixture := newSelectedCompletionFixture(t, selected, db, sqlite)
			ctx := testAuthorActivityContextForBundle(fixture.request.DeclarationPlan.BundleHash)
			source, err := correlation.NewSourceArtifactFact(fixture.request.DeclarationPlan.BundleHash)
			if err != nil {
				t.Fatal(err)
			}
			ctx = correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, fixture.forkRun), source)
			issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			// Claim accepts any positive duration; no lease or clock is rewritten.
			authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "lease-clock-proof", time.Second)
			if err != nil || !authority.Valid() {
				t.Fatalf("claim actual short selected lease: authority=%+v err=%v", authority, err)
			}
			ctx = runtimeeffects.WithAuthority(ctx, authority)
			originalAuthority := authority
			before, err := ReadSelectedExecutionStorageForTest(ctx, selected, fixture.forkRun)
			if err != nil || before.State != "running" || before.RunStatus != "paused" || before.Occurrences != 1 {
				t.Fatalf("lease proof lacks its exact paused child and claimed execution: before=%+v err=%v", before, err)
			}

			rollback := selectedAuthorityLeaseClockRollback{}
			var transactionTime time.Time
			var setupErr, observeErr, requireErr error
			var observedBefore, observedAfter, checkedAfter bool
			outcome := runSelectedFixtureMutation(ctx, selected, "selected lease clock window", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					transactionTime, setupErr = storegenericschedule.SelectedStoreTimeTx(txctx, tx, !sqlite, time.Now)
					if setupErr != nil {
						return errors.Join(rollback, setupErr)
					}
					remaining := authority.LeaseExpiresAt.Sub(transactionTime)
					wait := remaining + 5*time.Millisecond
					if remaining <= 0 || wait > 1100*time.Millisecond {
						setupErr = errors.New("native transaction did not begin inside the bounded actual lease window")
						return errors.Join(rollback, setupErr)
					}
					// The canonical observer verifies the recorded owner, fence,
					// generation and fingerprints before the actual lease expires.
					switch owner := selected.(type) {
					case *PostgresStore:
						observedBefore, setupErr = owner.effectPostgresOwner.ObserveCurrentExternalEffectAuthority(txctx, tx, authority)
					case *SQLiteRuntimeStore:
						observedBefore, setupErr = owner.effectSQLiteOwner.ObserveCurrentExternalEffectAuthority(txctx, tx, authority)
					default:
						setupErr = errors.New("selected lease proof lacks a native effect owner")
					}
					if setupErr != nil || !observedBefore {
						if setupErr == nil {
							setupErr = errors.New("actual selected lease was not current before waiting")
						}
						return errors.Join(rollback, setupErr)
					}
					// A real current grant is still not child activation. Source
					// admission and a running execution row cannot start a paused run.
					var runEligible bool
					var scopeErr, executionErr error
					switch owner := selected.(type) {
					case *PostgresStore:
						runEligible, scopeErr = owner.runLifecyclePostgresOwner.ObserveRunExecution(txctx, tx, fixture.forkRun)
						executionErr = owner.runLifecyclePostgresOwner.RequireRunExecutionTx(txctx, tx, fixture.forkRun)
					case *SQLiteRuntimeStore:
						runEligible, scopeErr = owner.runLifecycleSQLiteOwner.ObserveRunExecution(txctx, tx, fixture.forkRun)
						executionErr = owner.runLifecycleSQLiteOwner.RequireRunExecutionTx(txctx, tx, fixture.forkRun)
					}
					if scopeErr != nil || runEligible || !errors.Is(executionErr, runtimerunlifecycle.ErrRunExecutionAuthority) {
						setupErr = fmt.Errorf("paused child accepted claimed execution: eligible=%t observe=%v mutation=%v", runEligible, scopeErr, executionErr)
						return errors.Join(rollback, setupErr)
					}
					timer := time.NewTimer(wait)
					defer timer.Stop()
					select {
					case <-txctx.Done():
						setupErr = txctx.Err()
						return errors.Join(rollback, setupErr)
					case <-timer.C:
					}
					if !time.Now().UTC().After(authority.LeaseExpiresAt) {
						setupErr = errors.New("bounded wait did not cross the actual claimed lease expiry")
						return errors.Join(rollback, setupErr)
					}
					checkedAfter = true
					switch owner := selected.(type) {
					case *PostgresStore:
						observedAfter, observeErr = owner.effectPostgresOwner.ObserveCurrentExternalEffectAuthority(txctx, tx, authority)
						requireErr = owner.effectPostgresOwner.RequireCurrentExternalEffectAuthorityTx(txctx, tx, authority)
					case *SQLiteRuntimeStore:
						observedAfter, observeErr = owner.effectSQLiteOwner.ObserveCurrentExternalEffectAuthority(txctx, tx, authority)
						requireErr = owner.effectSQLiteOwner.RequireCurrentExternalEffectAuthorityTx(txctx, tx, authority)
					}
					// Roll back even on the hostile baseline's unexpected success:
					// no story, completion, or revision publication is authorized.
					return errors.Join(rollback, observeErr, requireErr)
				})
			})
			if !errors.Is(outcome, rollback) || setupErr != nil || !observedBefore || !checkedAfter {
				t.Fatalf("native lease proof did not reach both effect owners inside its exact transaction: tx_at=%s expires=%s before=%t checked_after=%t setup=%v outcome=%v", transactionTime, authority.LeaseExpiresAt, observedBefore, checkedAfter, setupErr, outcome)
			}
			after, err := ReadSelectedExecutionStorageForTest(ctx, selected, fixture.forkRun)
			if err != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(originalAuthority, authority) {
				t.Fatalf("rolled-back lease proof changed execution state or bound authority: before=%+v after=%+v err=%v", before, after, err)
			}
			if observeErr != nil {
				t.Errorf("expired native authority observation failed instead of declining: %v", observeErr)
			}
			if observedAfter {
				t.Error("native observer accepted selected authority after its lease expired within the same transaction")
			}
			if envelope, typed := runtimefailures.EnvelopeFromError(requireErr); !typed || envelope.Class != runtimefailures.ClassSupersededGeneration {
				t.Errorf("native mutable fence did not return typed stale authority after lease expiry: %v", requireErr)
			}
			t.Logf("selected effect-leaf lease window backend=%s tx_at=%s expires=%s observer=%t fence=%v rollback=true child_paused=true", backend, transactionTime.Format(time.RFC3339Nano), authority.LeaseExpiresAt.Format(time.RFC3339Nano), observedAfter, requireErr)
		})
	}
}
