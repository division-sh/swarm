package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestWorkspaceInvocationPhasesRetainNativeStateWithoutMutationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			active := seedDeliveryRecoveryClaim(t, fixture, ctx)
			closed := seedDeliveryRecoveryClaim(t, fixture, ctx)
			if _, err := fixture.store.SettleSuccess(ctx, closed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			completion := newCompletionSettlementFixture(t, fixture.store.(completionSettlementTestStore), fixture.db, backend.name == "sqlite")
			authority := completion.authority
			authority.BudgetScopes = nil
			effectCtx := runtimeeffects.WithLogicalOperationIdentity(completion.contextFor(authority), "optional-diagnostic")
			effectCtx = withManagedCompletionTestSurface(t, effectCtx, authority, "claude_cli")
			frame := managedCompletionTestFrameWithEvent(t, authority, "claude_cli", managedCompletionTestEvent(authority))
			handle, err := runtimeeffects.BeginManagedCompletion(effectCtx, "claude_cli", []byte("private provider input must not enter diagnostics"), frame, nil)
			if err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadWorkspaceInvocationPhasesForTest(ctx, fixture.store)
			if err != nil || len(got.Deliveries) != 3 || len(got.Effects) != 1 {
				t.Fatalf("native phases: %+v %v", got, err)
			}
			for _, row := range got.Deliveries {
				if row.DeliveryID == active.Claim.DeliveryID() {
					if row.Status != "in_progress" || row.ClaimVersion != active.Claim.Version() || row.AttemptClosureKind == nil || *row.AttemptClosureKind != "open" || row.AttemptOpenMarker == nil || !*row.AttemptOpenMarker || row.AttemptStartedAt == nil || row.AttemptExpiresAt == nil || row.AttemptOutcome != nil || row.AttemptCompletedAt != nil {
						t.Fatalf("active attempt lost exact phase: %+v", row)
					}
				} else if row.DeliveryID == closed.Claim.DeliveryID() {
					if row.Status != "delivered" || row.AttemptClosureKind == nil || *row.AttemptClosureKind != "settled" || row.AttemptOutcome == nil || *row.AttemptOutcome != "delivered" || row.SettledAt == nil || row.AttemptCompletedAt == nil {
						t.Fatalf("settled attempt lost exact phase: %+v", row)
					}
				} else if row.DeliveryID != completion.origin.DeliveryID() {
					t.Fatalf("foreign fabricated delivery: %+v", row)
				}
			}
			if effect := got.Effects[0]; effect.AttemptID != handle.Attempt().AttemptID || effect.OperationID != handle.Attempt().OperationID || effect.State != "authorized" || effect.AuthorizedAt == nil || effect.LaunchedAt != nil || effect.ResponseObservedAt != nil || effect.CompletedAt != nil {
				t.Fatalf("effect phase lost NULL or identity: %+v", effect)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("optional read changed native state: %v", err)
			}
		})
	}
}

func TestWorkspaceInvocationPhasesRejectUnownedAndLateFailureBothStores(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		got, err := ReadWorkspaceInvocationPhasesForTest(ctx, owner)
		if err == nil || got.Deliveries != nil || got.Effects != nil {
			t.Fatalf("unowned phases: %+v %v", got, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		for _, cut := range []string{"cancelled", "closed", "missing_attempts", "late_missing_effects"} {
			t.Run(backend.name+"/"+cut, func(t *testing.T) {
				fixture := backend.open(t)
				seedDeliveryRecoveryClaim(t, fixture, testAuthorActivityContext())
				readCtx := ctx
				switch cut {
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "closed":
					if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "missing_attempts", "late_missing_effects":
					query := `ALTER TABLE event_delivery_attempts RENAME TO unavailable_optional_attempts`
					if cut == "late_missing_effects" {
						query = `ALTER TABLE runtime_external_effect_attempts RENAME TO unavailable_optional_effects`
					}
					if _, err := fixture.db.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
				}
				got, err := ReadWorkspaceInvocationPhasesForTest(readCtx, fixture.store)
				if err == nil || got.Deliveries != nil || got.Effects != nil || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("failed %s leaked partial phase data: %+v %v", cut, got, err)
				}
			})
		}
	}
}
