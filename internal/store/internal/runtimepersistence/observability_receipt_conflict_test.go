package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestObservabilityReceiptFaultsUseOriginalWriterAndExactFixedRowsBothStores(t *testing.T) {
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if err := SetObservabilityReceiptConflictForTest(context.Background(), invalid, uuid.NewString(), PendingDeliveryWithDeadLetterReceipt, time.Now().UTC()); err == nil {
			t.Fatalf("foreign owner %T applied a receipt fault", invalid)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			at := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
			for _, cell := range []struct {
				conflict                     ObservabilityReceiptConflict
				subscriber, outcome, effects string
				failure                      bool
			}{
				{PendingDeliveryWithDeadLetterReceipt, "agent-pending", "dead_letter", `{"retry_count":9}`, true},
				{FailedDeliveryWithSuccessReceipt, "agent-failed", "success", `{"retry_count":0}`, false},
				{FailedDeliveryWithDeadLetterReceipt, "agent-failed", "dead_letter", `{"retry_count":7,"error":"receipt-loses"}`, false},
			} {
				event := eventtest.RunCreatingRootIngress(uuid.NewString(), "receipt.conflict.control", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, at)
				if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, event, []string{cell.subscriber}); err != nil {
					t.Fatal(err)
				}
				probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if err := SetObservabilityReceiptConflictForTest(ctx, fixture.store, event.ID(), cell.conflict, at); err != nil {
					t.Fatal(err)
				}
				if counts := probe.Snapshot(); counts.Total.WriteCommits != 1 || counts.Active != 0 {
					t.Fatalf("receipt fault escaped original writer: %+v", counts)
				}
				restore()
				var subscriber, outcome, effects, processed string
				var failure sql.NullString
				if err := readServedDeliveryObservation(ctx, fixture.store, func(ctx context.Context, tx *sql.Tx) error {
					return tx.QueryRowContext(ctx, `SELECT subscriber_id,outcome,CAST(side_effects AS TEXT),CAST(failure AS TEXT),CAST(processed_at AS TEXT)
						FROM event_receipts WHERE CAST(event_id AS TEXT)=$1 AND subscriber_type='platform' AND processed_at=$2`, event.ID(), at).Scan(&subscriber, &outcome, &effects, &failure, &processed)
				}); err != nil {
					t.Fatal(err)
				}
				var gotEffects, wantEffects map[string]any
				if err := json.Unmarshal([]byte(effects), &gotEffects); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(cell.effects), &wantEffects); err != nil {
					t.Fatal(err)
				}
				if subscriber != cell.subscriber || outcome != cell.outcome || !reflect.DeepEqual(gotEffects, wantEffects) || failure.Valid != cell.failure || processed == "" {
					t.Fatalf("fixed receipt fault row drift: %s/%s/%s/%v/%s", subscriber, outcome, effects, failure, processed)
				}
				if cell.failure {
					envelope, err := runtimefailures.UnmarshalEnvelope([]byte(failure.String))
					if err != nil || envelope.Class != runtimefailures.ClassConnectorFailure || envelope.Detail.Code != "receipt_should_not_win" || envelope.Component != "api-test" || envelope.Operation != "read" {
						t.Fatalf("receipt failure drift: %+v/%v", envelope, err)
					}
				}
			}
			for _, cell := range []struct {
				id       string
				conflict ObservabilityReceiptConflict
				at       time.Time
			}{
				{"", PendingDeliveryWithDeadLetterReceipt, at}, {"bad", PendingDeliveryWithDeadLetterReceipt, at},
				{uuid.Nil.String(), PendingDeliveryWithDeadLetterReceipt, at}, {uuid.NewString(), 0, at}, {uuid.NewString(), 255, at}, {uuid.NewString(), PendingDeliveryWithDeadLetterReceipt, time.Time{}},
			} {
				if err := SetObservabilityReceiptConflictForTest(ctx, fixture.store, cell.id, cell.conflict, cell.at); err == nil {
					t.Fatal("invalid receipt fault admitted")
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := SetObservabilityReceiptConflictForTest(cancelled, fixture.store, uuid.NewString(), PendingDeliveryWithDeadLetterReceipt, at); err == nil {
				t.Fatal("cancelled receipt fault admitted")
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := SetObservabilityReceiptConflictForTest(ctx, fixture.store, uuid.NewString(), PendingDeliveryWithDeadLetterReceipt, at); err == nil {
				t.Fatal("closed receipt fault admitted")
			}
		})
	}
}
