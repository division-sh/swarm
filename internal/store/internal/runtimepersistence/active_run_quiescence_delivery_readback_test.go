package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type activeRunDeliveryQuiescenceReadStore interface {
	authorActivityReceiptStore
	ApplyActiveRunQuiescence(context.Context, runtimerunquiescence.Request) (runtimerunquiescence.Result, error)
	ActiveRunDeliveryQuiesced(context.Context, string, events.DeliveryRoute) (string, bool, error)
	LoadRunDebugTracePage(context.Context, string, operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, string, error)
}

var _ activeRunDeliveryQuiescenceReadStore = (*PostgresStore)(nil)
var _ activeRunDeliveryQuiescenceReadStore = (*SQLiteRuntimeStore)(nil)

func TestActiveRunDeliveryQuiescenceReadbackParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(activeRunDeliveryQuiescenceReadStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 22, 18, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			eventID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.ExistingRunRootIngress(
				eventID, events.EventType("quiescence.requested"), "gateway", "", nil, 0,
				runID, events.EventEnvelope{}, now,
			)
			identity := testAgentIdentity(t, "agent-a", "quiescence/instance-a")
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: identity}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit active-run delivery: %v", err)
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil {
				t.Fatalf("claim active-run delivery: %v", err)
			}
			if claimed.Snapshot.Status != runtimedelivery.StatusInProgress {
				t.Fatalf("claimed status = %q, want in_progress", claimed.Snapshot.Status)
			}
			dryRun, err := selected.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
				OperationName: "test_active_run_delivery_quiescence_dry_run",
				DryRun:        true,
				RequestedAt:   now.Add(30 * time.Second),
				RunIDs:        []string{runID},
				ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
				ControlledBy:  "test",
				DeliveryNote:  "test active-run delivery quiescence dry-run",
			})
			if err != nil || !dryRun.DryRun || len(dryRun.Deliveries) != 1 {
				t.Fatalf("dry-run quiescence = %#v, err=%v", dryRun, err)
			}
			assertQuiescenceHandlerSelectionCount(t, fixture, ctx, claimed.Snapshot.DeliveryID, 0)
			unchanged, err := selected.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil || unchanged.Status != runtimedelivery.StatusInProgress {
				t.Fatalf("delivery after dry-run = %#v, err=%v", unchanged, err)
			}

			result, err := selected.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
				OperationName: "test_active_run_delivery_quiescence_readback",
				RequestedAt:   now.Add(time.Minute),
				RunIDs:        []string{runID},
				ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
				ControlledBy:  "test",
				DeliveryNote:  "test active-run delivery quiescence",
			})
			if err != nil {
				t.Fatalf("ApplyActiveRunQuiescence: %v", err)
			}
			if len(result.Deliveries) != 1 || !result.Deliveries[0].Changed {
				t.Fatalf("quiesced deliveries = %#v, want one changed delivery", result.Deliveries)
			}

			reason, quiesced, err := selected.ActiveRunDeliveryQuiesced(ctx, eventID, route)
			if err != nil || !quiesced || reason != runtimerunquiescence.ServeAbandonReasonCode {
				t.Fatalf("active-run delivery quiescence = reason:%q quiesced:%v err:%v", reason, quiesced, err)
			}
			unrelated := route
			unrelated.AgentIdentity = testAgentIdentity(t, "agent-a", "quiescence/instance-b")
			if reason, quiesced, err := selected.ActiveRunDeliveryQuiesced(ctx, eventID, unrelated); err != nil || quiesced || reason != "" {
				t.Fatalf("unrelated delivery quiescence = reason:%q quiesced:%v err:%v", reason, quiesced, err)
			}
			assertQuiescenceHandlerSelectionCount(t, fixture, ctx, claimed.Snapshot.DeliveryID, 1)
			rows, _, err := selected.LoadRunDebugTracePage(ctx, runID, operatorread.RunDebugTraceQueryOptions{Limit: 10})
			if err != nil {
				t.Fatalf("LoadRunDebugTracePage: %v", err)
			}
			var found bool
			for _, row := range rows {
				if row.DeliveryID != claimed.Snapshot.DeliveryID {
					continue
				}
				found = true
				if row.HandlerRuleSelection == nil || row.HandlerRuleSelection.Context != handlerselection.ContextNone || row.HandlerRuleSelection.Disposition != handlerselection.DispositionNotApplicable {
					t.Fatalf("terminalized trace selection = %#v", row.HandlerRuleSelection)
				}
			}
			if !found {
				t.Fatalf("terminalized delivery %s missing from trace: %#v", claimed.Snapshot.DeliveryID, rows)
			}
			requireCompleteRunForkRevision(t, ctx, fixture, runID)
		})
	}
}

func assertQuiescenceHandlerSelectionCount(t testing.TB, fixture authorActivityReceiptFixture, ctx context.Context, deliveryID string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id = ?`
	if fixture.dialect == authoractivityfixture.DialectPostgres {
		query = `SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id = $1::uuid`
	}
	var got int
	if err := fixture.db.QueryRowContext(ctx, query, deliveryID).Scan(&got); err != nil {
		t.Fatalf("count handler selection: %v", err)
	}
	if got != want {
		t.Fatalf("handler selection count = %d, want %d", got, want)
	}
}
