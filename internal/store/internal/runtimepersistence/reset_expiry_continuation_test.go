package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestResetContinuationReevaluatesDirectiveExpiryBothStores(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same-key"
		if restart {
			name = "startup-recovery"
		}
		t.Run(name, func(t *testing.T) {
			forEachDirectiveAmbiguityBackend(t, func(t *testing.T, backend directiveAmbiguityBackend) {
				ctx := testAuthorActivityContext()
				seedDirectiveOperationRun(t, backend.db, backend.name == "postgres")
				created := time.Now().UTC()
				reserved, err := backend.store.ReserveDirectiveOperation(ctx, directiveOperationReservationForTest(t, uuid.NewString(), uuid.NewString(), "reset-expiry", "reset-expiry", created))
				if err != nil {
					t.Fatal(err)
				}
				const owner = "reset-expiry-owner"
				if _, err := admitDirectiveExecutionForTest(ctx, backend.store, reserved.Operation.OperationID, owner, created, time.Minute); err != nil {
					t.Fatal(err)
				}
				if _, err := backend.store.RecordDirectiveExecuted(ctx, reserved.Operation.OperationID, owner, directiveOperationResponseForTest(reserved.Operation), created); err != nil {
					t.Fatal(err)
				}
				if _, err := backend.store.FinalizeDirectiveSuccess(ctx, reserved.Operation.OperationID, created, 2*time.Second); err != nil {
					t.Fatal(err)
				}
				selected := backend.store.(interface {
					startupownership.Store
					destructivereset.InventoryReader
					destructivereset.LockManager
					destructivereset.QuiescenceStore
				})
				capability, err := selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-expiry"))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := capability.Release(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				coordinator := &destructivereset.Coordinator{
					Operations: capability, RuntimeContexts: &resetReceiptLifecycle{completed: map[string]bool{}}, Locks: selected,
					Planner: destructivereset.InventoryPlanner{Reader: selected}, Quiescer: destructivereset.Quiescer{Store: selected},
					Cleaner: destructivereset.Cleaner{Store: resetRetainedCleanup{capability}}, Containers: resetNoContainerTargets{},
				}
				req := destructivereset.Request{OperationID: uuid.NewString(), ActorTokenID: "operator", IdempotencyKey: "expiry", RequestHash: "expiry", IncludeSourceArtifactsSet: true}
				if _, err := coordinator.Execute(ctx, req); err == nil || !strings.Contains(err.Error(), "directive") {
					t.Fatalf("unexpired terminal directive must block cleanup: %v", err)
				}
				before, err := capability.ReadResetOperation(ctx, req.OperationID)
				if err != nil || before.Phase != destructivereset.PhaseQuiesced {
					t.Fatalf("pending phase: %+v, %v", before, err)
				}
				// This is the actual authority TTL, not an increased assertion timeout.
				time.Sleep(time.Until(created.Add(2*time.Second)) + time.Millisecond)
				if restart {
					if err := capability.Release(context.Background()); err != nil {
						t.Fatal(err)
					}
					capability, err = selected.AcquireProcessCapability(ctx, testStartupAcquireRequest("reset-expiry-recovered"))
					if err != nil {
						t.Fatal(err)
					}
					coordinator.Operations = capability
					coordinator.Cleaner = destructivereset.Cleaner{Store: resetRetainedCleanup{capability}}
					recovery, err := coordinator.RecoverPending(ctx)
					if err != nil || recovery == nil {
						t.Fatalf("expired directive startup recovery: %v", err)
					}
					defer recovery.Close(context.Background())
					if err := recovery.Complete(ctx); err != nil {
						t.Fatal(err)
					}
				} else if _, err := coordinator.Execute(ctx, req); err != nil {
					t.Fatal(err)
				}
				after, err := capability.ReadResetOperation(ctx, req.OperationID)
				if err != nil || after.Phase != destructivereset.PhaseCompleted || !after.Request.RequestedAt.Equal(before.Request.RequestedAt) || !after.Cleanup.AppliedAt.After(created.Add(2*time.Second)) {
					t.Fatalf("expiry continuation changed admission or failed to advance attempt clock: %+v, %v", after, err)
				}
			})
		})
	}
}
