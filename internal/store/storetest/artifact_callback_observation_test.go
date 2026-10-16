package storetest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestArtifactCallbackObservationsKeepPhysicalScopeAndRefuseForeignOwners(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	foreignSQLite := StartSQLiteRuntimeStore(t)
	eventID := uuid.NewString()
	reads := []func(context.Context, any, string) (bool, error){
		func(ctx context.Context, owner any, id string) (bool, error) {
			row, err := ReadPostgresArtifactCallbackEventContext(ctx, owner, id)
			return row == (ArtifactCallbackEventContextStorage{}), err
		},
		func(ctx context.Context, owner any, id string) (bool, error) {
			row, err := ReadPostgresArtifactCallbackSourceRoute(ctx, owner, id)
			return row == "", err
		},
		func(ctx context.Context, owner any, id string) (bool, error) {
			count, err := CountPostgresArtifactCallbackSettledTarget(ctx, owner, id, `{}`, "foreign-node")
			return count == 0, err
		},
	}
	for _, read := range []func(context.Context, any, string) (string, error){
		ReadPostgresArtifactCallbackTargetDiagnostics,
		ReadPostgresArtifactCallbackDeliveries,
		ReadPostgresArtifactCallbackReceipts,
		ReadPostgresArtifactCallbackSettlementDiagnostics,
	} {
		reads = append(reads, func(ctx context.Context, owner any, id string) (bool, error) {
			out, err := read(ctx, owner, id)
			return out == "", err
		})
		out, err := read(context.Background(), selected, eventID)
		if err != nil {
			t.Fatal(err)
		}
		if readZeroArtifactCallbackAggregate(out) != nil {
			t.Fatalf("empty native aggregate changed: %s", out)
		}
	}
	for _, read := range reads {
		for _, owner := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}, foreignSQLite} {
			if zero, err := read(context.Background(), owner, eventID); !zero || err == nil {
				t.Fatalf("foreign %T supplied evidence: zero=%t err=%v", owner, zero, err)
			}
		}
		if zero, err := read(context.Background(), selected, "not-a-uuid"); !zero || err == nil {
			t.Fatalf("invalid identity supplied evidence: zero=%t err=%v", zero, err)
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if zero, err := read(cancelled, selected, eventID); !zero || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled observation supplied evidence: zero=%t err=%v", zero, err)
		}
	}
	if row, err := ReadPostgresArtifactCallbackEventContext(context.Background(), selected, eventID); row != (ArtifactCallbackEventContextStorage{}) || !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing event was synthesized: %+v/%v", row, err)
	}
	assertArtifactCallbackNativePhysicalRows(t, selected)
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	for _, read := range reads {
		if zero, err := read(context.Background(), selected, eventID); !zero || err == nil {
			t.Fatalf("closed observation supplied evidence: zero=%t err=%v", zero, err)
		}
	}
}

func readZeroArtifactCallbackAggregate(raw string) error {
	if raw == "[]" {
		return nil
	}
	var aggregate map[string][]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &aggregate); err != nil {
		return err
	}
	deliveries, hasDeliveries := aggregate["deliveries"]
	receipts, hasReceipts := aggregate["receipts"]
	if len(aggregate) != 2 || !hasDeliveries || !hasReceipts || len(deliveries) != 0 || len(receipts) != 0 {
		return errors.New("empty combined diagnostic changed")
	}
	return nil
}

func assertArtifactCallbackNativePhysicalRows(t *testing.T, selected RunFixtureStore) {
	t.Helper()
	ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
	at := time.Unix(1700000800, 0).UTC()
	run := uuid.NewString()
	RequireRun(t, ctx, selected, RunFixture{RunID: run, Origin: ScenarioSetupOrigin(), StartedAt: at})
	node, err := runtimeidentity.ParseExecutableNode("flow_a", "fixture-node")
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fixture.callback", "fixture", "", []byte(`{"value":1}`), 0, run,
		events.EventEnvelope{}, eventtest.RootRoutingSource(run), at)
	CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a"}),
	}}, pipelineobligation.ScopeSubscribed)
	row, err := ReadPostgresArtifactCallbackEventContext(ctx, selected, event.ID())
	if err != nil || row.EntityID != "" || row.FlowInstance != "" || row.Payload != `{"value": 1}` {
		t.Fatalf("physical NULL defaults/payload bytes changed: %+v/%v", row, err)
	}
	source, err := ReadPostgresArtifactCallbackSourceRoute(ctx, selected, event.ID())
	if err != nil || source != row.SourceRoute || !json.Valid([]byte(source)) {
		t.Fatalf("independent source-route read changed physical rendering: %q/%v", source, err)
	}
	if count, err := CountPostgresTemplateNodeTarget(ctx, selected, event.ID(), `{}`, node.Key()); err != nil || count != 1 {
		t.Fatalf("native node target lost actual pending delivery: %d/%v", count, err)
	}
	if count, err := CountPostgresArtifactCallbackSettledTarget(ctx, selected, event.ID(), `{}`, node.Key()); err != nil || count != 0 {
		t.Fatalf("pending delivery was treated as settled: %d/%v", count, err)
	}
	terminalizer := selected.(interface {
		TerminalizeRun(context.Context, string, string) ([]runtimedelivery.Terminalization, error)
	})
	if _, err := terminalizer.TerminalizeRun(ctx, run, "fixture_stop"); err != nil {
		t.Fatal(err)
	}
	for _, read := range []func(context.Context, any, string) (string, error){ReadPostgresArtifactCallbackTargetDiagnostics, ReadPostgresArtifactCallbackDeliveries} {
		raw, err := read(ctx, selected, event.ID())
		var rows []struct {
			EventID      string `json:"event_id"`
			SubscriberID string `json:"subscriber_id"`
			Status       string `json:"status"`
		}
		if err != nil || json.Unmarshal([]byte(raw), &rows) != nil || len(rows) != 1 || rows[0].SubscriberID != node.Key() || rows[0].Status != "dead_letter" {
			t.Fatalf("real delivery diagnostic changed: %s/%v", raw, err)
		}
		if rows[0].EventID != "" && rows[0].EventID != event.ID() {
			t.Fatalf("full-row diagnostic borrowed another event: %s", raw)
		}
	}
	raw, err := ReadPostgresArtifactCallbackSettlementDiagnostics(ctx, selected, event.ID())
	var combined struct {
		Deliveries []struct{ Status string }
		Receipts   []json.RawMessage
	}
	if err != nil || json.Unmarshal([]byte(raw), &combined) != nil || len(combined.Deliveries) != 1 || combined.Deliveries[0].Status != "dead_letter" || len(combined.Receipts) != 0 {
		t.Fatalf("combined diagnostic lost real delivered/receipt scope: %s/%v", raw, err)
	}
	if count, err := CountPostgresArtifactCallbackSettledTarget(ctx, selected, event.ID(), `{}`, node.Key()); err != nil || count != 0 {
		t.Fatalf("dead-letter delivery was treated as delivered: %d/%v", count, err)
	}
}
