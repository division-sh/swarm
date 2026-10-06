package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestNotifyExecutionStorageKeepsExactScopeAndOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			first := seedDeliveryRecoveryClaim(t, fixture, ctx)
			later := eventtest.ExistingRunRootIngress(uuid.NewString(), "notify.later", "gateway", "", nil, 0, first.Snapshot.RunID, events.EventEnvelope{}, time.Now().UTC().Add(time.Second))
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, later, []events.DeliveryRoute{first.Snapshot.Route}); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.store.SettleSuccess(ctx, first.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			got, err := ReadNotifyAgentDeliveryStatusForTest(ctx, fixture.store, first.Snapshot.RunID, "agent-a", "recovery/instance-a")
			if err != nil || got != "pending" {
				t.Fatalf("latest exact delivery status=%q err=%v", got, err)
			}
			for _, foreign := range []struct{ run, agent, instance string }{
				{uuid.NewString(), "agent-a", "recovery/instance-a"},
				{first.Snapshot.RunID, "other-agent", "recovery/instance-a"},
				{first.Snapshot.RunID, "agent-a", "other/instance"},
			} {
				if got, err := ReadNotifyAgentDeliveryStatusForTest(ctx, fixture.store, foreign.run, foreign.agent, foreign.instance); !errors.Is(err, sql.ErrNoRows) || got != "" {
					t.Fatalf("foreign coordinates produced evidence: %q %v", got, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 4 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("readback escaped original coordinator: %+v", counts)
			}
		})
	}
}

func TestNotifyExecutionStorageRefusesRawCancelledClosedAndUnavailableOwnersBothStores(t *testing.T) {
	run := uuid.NewString()
	refuse := func(t *testing.T, ctx context.Context, owner any) {
		t.Helper()
		if got, err := ReadNotifyAgentDeliveryStatusForTest(ctx, owner, run, "agent-a", "recovery/instance-a"); err == nil || errors.Is(err, sql.ErrNoRows) || got != "" {
			t.Fatalf("invalid owner returned evidence: %q %v", got, err)
		}
	}
	for _, owner := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(t, context.Background(), owner)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			refuse(t, cancelled, fixture.store)
			if err := runUnrevisionedEventFixtureTransactionForTest(context.Background(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "ALTER TABLE event_deliveries RENAME TO unavailable_notify_observation")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), fixture.store)
			if err := runUnrevisionedEventFixtureTransactionForTest(context.Background(), fixture.store, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, "ALTER TABLE unavailable_notify_observation RENAME TO event_deliveries")
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), fixture.store)
		})
	}
}
