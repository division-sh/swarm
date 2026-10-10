package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestExactAgentDeliveryStatusPreservesPhysicalStateAndOriginalReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			claimed := seedDeliveryRecoveryClaim(t, fixture, ctx)
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if status, err := ReadExactAgentDeliveryStatusForTest(ctx, fixture.store, claimed.Snapshot.EventID, "agent-a"); err != nil || status != "in_progress" {
				t.Fatalf("physical active status=%q,%v", status, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("status escaped original reader: %+v", counts)
			}
			if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			if status, err := ReadExactAgentDeliveryStatusForTest(ctx, fixture.store, claimed.Snapshot.EventID, "agent-a"); err != nil || status != "delivered" {
				t.Fatalf("physical terminal status=%q,%v", status, err)
			}
			for _, key := range [][2]string{{uuid.NewString(), "agent-a"}, {claimed.Snapshot.EventID, "foreign-agent"}} {
				if status, err := ReadExactAgentDeliveryStatusForTest(ctx, fixture.store, key[0], key[1]); !errors.Is(err, sql.ErrNoRows) || status != "" {
					t.Fatalf("foreign status fabricated evidence: %q,%v", status, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if status, err := ReadExactAgentDeliveryStatusForTest(cancelled, fixture.store, claimed.Snapshot.EventID, "agent-a"); !errors.Is(err, context.Canceled) || status != "" {
				t.Fatalf("cancelled status returned evidence: %q,%v", status, err)
			}
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if status, err := ReadExactAgentDeliveryStatusForTest(ctx, fixture.store, claimed.Snapshot.EventID, "agent-a"); err == nil || status != "" {
				t.Fatalf("closed status returned evidence: %q,%v", status, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if status, err := ReadExactAgentDeliveryStatusForTest(context.Background(), invalid, uuid.NewString(), "agent-a"); err == nil || status != "" {
			t.Fatalf("missing status returned evidence: %q,%v", status, err)
		}
	}
}
