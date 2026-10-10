package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestNodeDeliveryDiagnosticsPreserveRowsReceiptsAndDeadLettersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			selected := fixture.store.(interface {
				semanticEventFixtureStore
				exactDeadLetterStore
				PipelineObligations() runtimepipelineobligation.Store
			})
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "node.diagnostic.fixture", "runtime", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			target := events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: uuid.NewString()}.Normalized())
			routes := []events.DeliveryRoute{
				{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("node_1")), Target: target},
				{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("node_2")), Target: target},
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
				t.Fatal(err)
			}
			settlePipelineParityEvent(t, ctx, selected.PipelineObligations(), event.ID(), runtimepipelineobligation.Acknowledged("diagnostic_fixture"))
			failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, "connector_failed", nil)
			if err := selected.RecordDeadLetter(ctx, runtimedeadletters.Record{
				OriginalEventID: event.ID(), OriginalEvent: string(event.Type()), OriginalPayload: event.Payload(),
				FlowInstance: "runtime", Failure: failure, HandlerNode: "fixture_handler", Timestamp: event.CreatedAt().Add(time.Second).Format(time.RFC3339Nano),
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			out, err := ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, event.ID())
			if err != nil || len(out.Rows) != 2 || len(out.DeadLetters) != 1 || len(out.Receipts) != 1 {
				t.Fatalf("complete exact diagnostic=%+v,err=%v", out, err)
			}
			for i, route := range routes {
				if !strings.HasPrefix(out.Rows[i], route.Recipient.ID()+" status=pending reason= target=") {
					t.Fatalf("delivery order/fields=%q", out.Rows[i])
				}
			}
			if !strings.Contains(out.DeadLetters[0], "fixture_handler failure="+string(runtimefailures.ClassConnectorFailure)+" detail=") ||
				!strings.HasPrefix(out.Receipts[0], "platform/pipeline outcome=success reason=diagnostic_fixture failure=") {
				t.Fatalf("dead-letter/receipt fields lost: %+v", out)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("diagnostic escaped original read snapshot: %+v", counts)
			}
			foreign, err := ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, uuid.NewString())
			if err != nil || !reflect.DeepEqual(foreign, NodeDeliveryDiagnosticStorage{}) {
				t.Fatalf("foreign key borrowed diagnostic: %+v,%v", foreign, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := ReadNodeDeliveryDiagnosticStorageForTest(cancelled, selected, event.ID()); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, NodeDeliveryDiagnosticStorage{}) {
				t.Fatalf("cancelled diagnostic retained partial facts: %+v,%v", got, err)
			}
			if got, err := ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, "invalid"); err == nil || !reflect.DeepEqual(got, NodeDeliveryDiagnosticStorage{}) {
				t.Fatalf("invalid event returned diagnostic: %+v,%v", got, err)
			}
			if err := runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `DROP TABLE dead_letters`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, event.ID()); err == nil || !strings.Contains(err.Error(), "dead letter dump") || !reflect.DeepEqual(got, NodeDeliveryDiagnosticStorage{}) {
				t.Fatalf("failed auxiliary read returned partial delivery evidence: %+v,%v", got, err)
			}
			if probe.Snapshot().Active != 0 {
				t.Fatal("failed diagnostic retained an active read")
			}
			switch owner := selected.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if got, err := ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, event.ID()); err == nil || !reflect.DeepEqual(got, NodeDeliveryDiagnosticStorage{}) {
				t.Fatalf("closed diagnostic retained facts: %+v,%v", got, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if got, err := ReadNodeDeliveryDiagnosticStorageForTest(context.Background(), invalid, uuid.NewString()); err == nil || !reflect.DeepEqual(got, NodeDeliveryDiagnosticStorage{}) {
			t.Fatalf("missing owner returned diagnostic: %+v,%v", got, err)
		}
	}
}
