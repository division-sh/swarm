package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

var family145AdministrativeLayeringCuts = map[string][]string{
	"testRuntimeNukeLayeredPostgresCapacity": {
		"storetest.StartPostgresRuntimeStore(t)", "storetest.LimitPostgresAdministrativeFixturePool(context.Background(), selected)",
		"context.WithTimeout(context.Background(), 10*time.Second)", "acquireAdministrativeResetCapability(t, selected)",
		"external.waitUntilBlocked(t, ctx)", "assertAdministrativeUnrelatedQueryProgresses(t, ctx, selected)", "external.release()", "assertAdministrativeCapacityReleased(t, selected)",
		"cancel()", "if !joined", "<-result", "joined = true", "capability.Release(ctx)",
	},
	"TestAdministrativeOperationCancellationReleasesLeaseAndCapacity": {
		"storetest.StartPostgresRuntimeStore(t)", "storetest.LimitPostgresAdministrativeFixturePool(context.Background(), selected)", "acquireAdministrativeResetCapability(t, selected)",
		"context.WithCancel(context.Background())", "context.WithTimeout(context.Background(), 10*time.Second)", "context.WithTimeout(context.Background(), 2*time.Second)",
		"external.waitUntilBlocked(t, blockedCtx)", "cancel()", "external.release()", "if !joined", "joined = true", "<-result",
		"assertAdministrativeCapacityReleased(t, selected)", "selected.AcquireDestructiveReset(leaseCtx)", "lease.Release(leaseCtx)", "capability.Release(context.Background())",
	},
	"TestAdministrativeLeaseDoesNotBorrowSurroundingTransaction": {
		"storetest.StartPostgresRuntimeStore(t)", "storetest.LimitPostgresAdministrativeFixturePool(context.Background(), selected)",
		"context.WithTimeout(context.Background(), 5*time.Second)", "storetest.HoldPostgresAdministrativeSurroundingRead(ctx, selected)", "surrounding.Close()",
		"selected.AcquireDestructiveReset(ctx)", "sync.OnceValue(func() error {\n\t\treturn lease.Release(ctx)\n\t})", "release()", "surrounding.ReadAfterLeaseRelease(ctx)",
		"storetest.ObserveSelectedPool(t, selected).MaxOpenConnections",
	},
	"assertAdministrativeUnrelatedQueryProgresses": {
		"context.WithTimeout(ctx, 2*time.Second)", "storetest.ReadPostgresAdministrativeUnrelatedProgress(queryCtx, selected)", "if value != 1",
	},
	"assertAdministrativeCapacityReleased": {
		"storetest.ObserveSelectedPool(t, selected).MaxOpenConnections", "if got :=", "got != 1",
	},
}

func family145AdministrativeLayeringProofError(function, before, after string) error {
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, cut := range family145AdministrativeLayeringCuts[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("administrative layering lost original owner/capacity/lifetime cut: %s", cut)
		}
	}
	if function == "TestAdministrativeLeaseDoesNotBorrowSurroundingTransaction" {
		if strings.Index(after, "HoldPostgresAdministrativeSurroundingRead(") > strings.Index(after, "AcquireDestructiveReset(") ||
			strings.LastIndex(after, "if err := release();") > strings.Index(after, "ReadAfterLeaseRelease(") {
			return fmt.Errorf("surrounding work moved outside the real lease acquisition/release window")
		}
	}
	return nil
}

func assertFamily145AdministrativePoolAndSurroundingOwnership(t *testing.T) {
	t.Helper()
	limit := family145ConversationForkOwnerSource(t, "internal/backend/postgres/administrative_fixture.go", "LimitAdministrativeFixturePool")
	for _, cut := range []string{"b.db.SetMaxOpenConns(1)", "b.capacityMu.Lock()", "b.capacityReservations != 0", "b.db.Stats().InUse != 0"} {
		if !strings.Contains(limit, cut) {
			t.Fatalf("original native pool limit/active-work refusal changed: %s", cut)
		}
	}
	if strings.Index(limit, "b.capacityReservations != 0") > strings.Index(limit, "b.Ping(ctx)") || strings.Index(limit, "b.db.Stats().InUse != 0") > strings.Index(limit, "b.Ping(ctx)") {
		t.Fatal("retained/active native pool guard moved after potentially blocked ping")
	}
	if strings.Contains(limit, "SetMaxIdleConns") || strings.Contains(limit, "baseOpenConnections =") {
		t.Fatal("one-slot fixture changed another original pool policy")
	}
	hold := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_administrative_layering_fixture.go", "HoldPostgresAdministrativeSurroundingReadForTest")
	for _, cut := range []string{"owner.backend.RunTransaction(ctx,", "postgresbackend.ReadAdministrativeSurroundingTransactionTx(ctx, tx)", "return rollback", "case <-read.release:", "case <-ctx.Done():", "close(ready)", "close(read.checked)", "close(read.done)", "administrativeSurroundingCloseError(err, rollback, ctx.Err())"} {
		if !strings.Contains(hold, cut) {
			t.Fatalf("fixed surrounding transaction lost actual original coordinator/join/cancellation: %s", cut)
		}
	}
}
