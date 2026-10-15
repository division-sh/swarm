package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestDeliveryRecoveryConstraintRequiresNativeRejectionAndPreservesRowsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claim := seedDeliveryRecoveryClaim(t, fixture, ctx)
			input := DeliveryConstraintInput{Case: DeliveryConstraintEventRunMismatch, DeliveryID: claim.Claim.DeliveryID(), OtherRunID: uuid.NewString()}
			before := forkReceiverFaultSnapshot(t, ctx, fixture.store)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			rejected, err := ProbeDeliveryConstraintForTest(ctx, fixture.store, input)
			if err != nil || !rejected {
				t.Fatalf("native foreign-run constraint was not proved: %t/%v", rejected, err)
			}
			counts := probe.Snapshot()
			if counts.Total.Begun != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("constraint proof bypassed original rollback: %+v", counts)
			}
			restore()
			if !reflect.DeepEqual(before, forkReceiverFaultSnapshot(t, ctx, fixture.store)) {
				t.Fatal("rejected constraint changed complete storage or clocks")
			}
			assertDeliveryConstraintNonConstraintRefusals(t, ctx, fixture, input)
		})
	}
}

func assertDeliveryConstraintNonConstraintRefusals(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, input DeliveryConstraintInput) {
	t.Helper()
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if rejected, err := ProbeDeliveryConstraintForTest(ctx, owner, input); rejected || err == nil {
			t.Fatalf("unavailable owner proved a constraint: %t/%v", rejected, err)
		}
	}
	for _, bad := range []DeliveryConstraintInput{{Case: "foreign", DeliveryID: input.DeliveryID}, {Case: input.Case, DeliveryID: "invalid", OtherRunID: input.OtherRunID}, {Case: input.Case, DeliveryID: input.DeliveryID}} {
		if rejected, err := ProbeDeliveryConstraintForTest(ctx, fixture.store, bad); rejected || err == nil {
			t.Fatalf("invalid probe masqueraded as schema rejection: %t/%v", rejected, err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if rejected, err := ProbeDeliveryConstraintForTest(cancelled, fixture.store, input); rejected || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation proved a constraint: %t/%v", rejected, err)
	}
	for _, statement := range []string{"SELEC invalid_delivery_recovery_syntax", "SELECT missing_delivery_recovery_column"} {
		err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error { _, err := tx.ExecContext(ctx, statement); return err })
		if err == nil || delivery.IsDeliveryConstraintRejection(err) {
			t.Fatalf("native parse/name error counted as a constraint: %v", err)
		}
	}
	if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if rejected, err := ProbeDeliveryConstraintForTest(ctx, fixture.store, input); rejected || err == nil {
		t.Fatalf("closed owner proved a constraint: %t/%v", rejected, err)
	}
}

func TestDeliveryRecoveryRetryCutPreservesExactHourAndOtherStorageBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claim := seedDeliveryRecoveryClaim(t, fixture, ctx)
			seedDeliveryRecoveryClaim(t, fixture, ctx)
			failure := runtimefailures.FromError(errors.New("retry recovery proof"), "delivery-recovery-test", "retry")
			owner := fixture.store.(deliveryFixtureStore)
			if _, err := owner.SettleFailure(ctx, claim.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: &failure.Failure, RetryBase: time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()}); err != nil {
				t.Fatal(err)
			}
			before := forkReceiverFaultSnapshot(t, ctx, fixture.store)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			lower := time.Now().Add(-time.Hour).UTC()
			if err := MakeDeliveryRecoveryRetryEligibleForTest(ctx, fixture.store, claim.Claim.DeliveryID()); err != nil {
				t.Fatal(err)
			}
			upper := time.Now().Add(-time.Hour).UTC()
			assertDeliveryRecoveryRetryTransaction(t, probe)
			restore()
			after := forkReceiverFaultSnapshot(t, ctx, fixture.store)
			assertForkReceiverFaultOnlyColumns(t, before, after, []forkReceiverFaultEdit{{table: "event_deliveries", key: "delivery_id", value: claim.Claim.DeliveryID(), columns: []string{"next_eligible_at"}}})
			stored, err := owner.Snapshot(ctx, claim.Claim.DeliveryID())
			if err != nil {
				t.Fatal(err)
			}
			if stored.NextEligibleAt.Before(lower.Truncate(time.Microsecond)) || stored.NextEligibleAt.After(upper.Add(time.Microsecond)) {
				t.Fatalf("retry cut changed one-hour clock: %s not in [%s,%s]", stored.NextEligibleAt, lower, upper)
			}
			for _, id := range []string{uuid.NewString(), "invalid"} {
				if err := MakeDeliveryRecoveryRetryEligibleForTest(ctx, fixture.store, id); err == nil {
					t.Fatal("retry cut accepted missing or invalid delivery")
				}
			}
			if !reflect.DeepEqual(after, forkReceiverFaultSnapshot(t, ctx, fixture.store)) {
				t.Fatal("refused retry cut changed storage")
			}
		})
	}
}

func assertDeliveryRecoveryRetryTransaction(t *testing.T, probe *transactiontest.Collector) {
	t.Helper()
	counts := probe.Snapshot()
	if counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Active != 0 || counts.Total.Revision.Finalizations != 0 {
		t.Fatalf("retry cut bypassed original unrevisioned transaction: %+v", counts)
	}
}

func TestDeliveryRecoveryFaultCleanupAfterDurableInstallAndCancellationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, kind := range []string{"diagnostic", "poison"} {
			phases := []string{"lost_ack", "cancelled"}
			if backend.name == "postgres" {
				phases = append(phases, "partial_function")
			}
			for _, phase := range phases {
				t.Run(backend.name+"/"+kind+"/"+phase, func(t *testing.T) {
					runDeliveryRecoveryFaultCleanupCase(t, backend.open(t), backend.name == "postgres", kind, phase)
				})
			}
		}
	}
}

func runDeliveryRecoveryFaultCleanupCase(t *testing.T, fixture authorActivityReceiptFixture, postgres bool, kind, phase string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	_, entityID := seedWorkflowProjectionFaultCut(t, fixture, ctx)
	cleanup := func() {
		if err := removeDeliveryRecoveryFault(context.WithoutCancel(ctx), fixture.store, kind); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(cleanup)
	installErr := installDeliveryRecoveryFault(ctx, fixture.store, kind, entityID, phase == "partial_function")
	if installErr != nil {
		t.Fatal(installErr)
	}
	// The real native write committed before the transport-facing error.
	lostAck := errors.New("injected lost acknowledgement after native fault install")
	if phase == "lost_ack" {
		installErr = lostAck
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if phase == "lost_ack" && !errors.Is(installErr, lostAck) {
		t.Fatal("durable/error return cut disappeared")
	}
	objects := deliveryRecoveryFaultObjectCount(t, ctx, fixture.store, kind)
	want := 1
	if postgres && phase != "partial_function" {
		want = 2
	}
	if objects != want {
		t.Fatalf("native fault did not persist its exact objects: %d want%d", objects, want)
	}
	if err := removeDeliveryRecoveryFault(context.WithoutCancel(cancelled), fixture.store, kind); err != nil {
		t.Fatal(err)
	}
	cleanup()
	if got := deliveryRecoveryFaultObjectCount(t, ctx, fixture.store, kind); got != 0 {
		t.Fatalf("idempotent joined cleanup left %d native objects", got)
	}
}

func installDeliveryRecoveryFault(ctx context.Context, selected any, kind, entityID string, partial bool) error {
	if !partial {
		if kind == "diagnostic" {
			return InstallDeliveryDiagnosticFaultForTest(ctx, selected)
		}
		return InstallNodeRecoveryPoisonFaultForTest(ctx, selected, entityID)
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		if kind == "diagnostic" {
			return delivery.InstallConformanceDiagnosticFunctionFaultTx(ctx, tx)
		}
		return entitystore.InstallNodeRecoveryPoisonFunctionFaultTx(ctx, tx, entityID)
	})
}

func removeDeliveryRecoveryFault(ctx context.Context, selected any, kind string) error {
	if kind == "diagnostic" {
		return RemoveDeliveryDiagnosticFaultForTest(ctx, selected)
	}
	return RemoveNodeRecoveryPoisonFaultForTest(ctx, selected)
}

func deliveryRecoveryFaultObjectCount(t *testing.T, ctx context.Context, selected any, kind string) int {
	t.Helper()
	_, postgres := selected.(*PostgresStore)
	count, err := readFanOutSemanticStorage(ctx, selected, []string{}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		if kind == "diagnostic" {
			out, err := delivery.ReadConformanceDiagnosticFaultObjectsTx(ctx, tx, postgres)
			return out.Functions + out.Triggers, err
		}
		return entitystore.CountNodeRecoveryPoisonFaultObjectsTx(ctx, tx, postgres)
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}
