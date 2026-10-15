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

func TestFamily145AdministrativeLayeringEntireChainKeepsCapacityAndLifetimeAssertions(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	original := map[string]string{}
	for _, row := range rows {
		cuts, ok := family145AdministrativeLayeringCuts[row.Function]
		if !ok {
			continue
		}
		original[row.Function] = row.Before
		if err := family145AdministrativeLayeringProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatalf("%s: %v", row.Function, err)
		}
		for _, cut := range cuts {
			if family145AdministrativeLayeringProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost administrative ownership/capacity/lifetime cut accepted: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			hostile, err := family145BusAssertionCounterexample(row.After, strings.SplitN(assertion, " => ", 2)[0])
			if err != nil {
				t.Fatal(err)
			}
			if family145AdministrativeLayeringProofError(row.Function, row.Before, hostile) == nil {
				t.Fatalf("lost original administrative assertion accepted: %s", assertion)
			}
		}
	}
	if len(original) != len(family145AdministrativeLayeringCuts) {
		t.Fatalf("administrative ownership/caller inventory incomplete: %d", len(original))
	}
	data, err := os.ReadFile("../../../internal/apiv1/operator_administrative_layering_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := family145DescriptorReadinessEscape(data); err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct{ before, owner string }{
		{"assertAdministrativeUnrelatedQueryProgresses", "ReadAdministrativeUnrelatedProgress"},
		{"TestAdministrativeLeaseDoesNotBorrowSurroundingTransaction", "ReadAdministrativeSurroundingTransactionTx"},
	} {
		owned := family145ConversationForkOwnerSource(t, "internal/backend/postgres/administrative_fixture.go", pair.owner)
		old, current := family145ConnectorSQL(t, "package proof\n"+original[pair.before]), family145ConnectorSQL(t, "package proof\n"+owned)
		if len(old) != 1 || len(current) != 1 || old[0] != current[0] {
			t.Fatalf("administrative actual independent SQL cut changed: %v/%v", old, current)
		}
	}
	assertFamily145AdministrativePoolAndSurroundingOwnership(t)
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
