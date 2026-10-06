package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestIssue2564ClosedEvidenceBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, db, ctx := backend.open(t)
			runID := correlation.RunIDFromContext(ctx)
			ctx = testAuthorActivityContext()
			at := time.Now().UTC().Truncate(time.Microsecond)
			var eventIDs, deliveryIDs []string
			for _, node := range []string{"evidence-primary", "evidence-sibling"} {
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "evidence.probe", "fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
				route := testEntitylessNodeDeliveryRoute(node)
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				id, err := deliverylifecycle.DeliveryID(event.ID(), canonicalDeliveryFixtureRouteValue(runID, route))
				if err != nil {
					t.Fatal(err)
				}
				eventIDs = append(eventIDs, event.ID())
				deliveryIDs = append(deliveryIDs, id)
			}
			for _, entry := range []struct {
				version          int
				closure, outcome string
				duration         any
			}{{1, "lease_expired", "lease_expired", nil}, {2, "settled", "delivered", 0}} {
				if _, err := db.ExecContext(ctx, `INSERT INTO event_delivery_attempts(delivery_id,claim_version,claim_token,started_at,lease_expires_at,open_marker,closure_kind,outcome,completed_at,duration_ms) VALUES($1,$2,$3,$4,$5,FALSE,$6,$7,$4,$8)`, deliveryIDs[0], entry.version, uuid.NewString(), at, at.Add(time.Minute), entry.closure, entry.outcome, entry.duration); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO event_delivery_handler_rule_selections(delivery_id,selection_context,disposition,flow_path,declaration_family,semantic_path,display_label) VALUES($1,'handler_rules','evaluation_failed','.','handler_rule','nodes["worker"].handlers["probe"].rules[0]','failed')`, deliveryIDs[0]); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE event_deliveries SET continuation_handoff_at=$1 WHERE delivery_id=$2`, at, deliveryIDs[0]); err != nil {
				t.Fatal(err)
			}
			before := db.Stats().InUse
			for _, invalid := range []any{nil, db, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}} {
				if partial, err := ObserveDeliveryEventEvidenceForTest(ctx, invalid, eventIDs[0]); err == nil || !reflect.DeepEqual(partial, DeliveryEventEvidence{}) {
					t.Fatalf("raw/nil evidence admitted: %T %+v %v", invalid, partial, err)
				}
				if partial, err := ObserveEntityMutationHistoryForTest(ctx, invalid, runID); err == nil || partial != nil {
					t.Fatalf("raw/nil mutation evidence admitted: %T %v", invalid, err)
				}
			}
			got, err := ObserveDeliveryEventEvidenceForTest(ctx, selected, eventIDs[0])
			if err != nil || len(got.Deliveries) != 1 {
				t.Fatalf("exact event evidence: %+v err=%v", got, err)
			}
			row := got.Deliveries[0]
			if row.DeliveryID != deliveryIDs[0] || !row.HandoffPresent || !row.HandoffAt.Equal(at) || row.HandlerSelections != 1 || len(row.Attempts) != 2 || row.Attempts[0].ClosureKind != "lease_expired" || row.Attempts[1].ClosureKind != "settled" {
				t.Fatalf("timestamp/unsuccessful attempt/failed selection lost: %+v", row)
			}
			sibling, err := ObserveDeliveryEventEvidenceForTest(ctx, selected, eventIDs[1])
			if err != nil || len(sibling.Deliveries) != 1 || len(sibling.Deliveries[0].Attempts) != 0 || sibling.Deliveries[0].HandlerSelections != 0 || sibling.Deliveries[0].HandoffPresent || !sibling.Deliveries[0].HandoffAt.IsZero() {
				t.Fatalf("sibling facts leaked: %+v %v", sibling, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if partial, err := ObserveDeliveryEventEvidenceForTest(canceled, selected, eventIDs[0]); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(partial, DeliveryEventEvidence{}) {
				t.Fatalf("partial canceled evidence: %+v err=%v", partial, err)
			}
			got.Deliveries[0].Attempts[0].Outcome = "changed"
			fresh, err := ObserveDeliveryEventEvidenceForTest(ctx, selected, eventIDs[0])
			if err != nil || fresh.Deliveries[0].Attempts[0].Outcome != "lease_expired" || db.Stats().InUse != before {
				t.Fatalf("readback retained/mutated authority: %+v %v", fresh, err)
			}
			if partial, err := ObserveDeliveryEventEvidenceForTest(ctx, selected, "invalid"); err == nil || !reflect.DeepEqual(partial, DeliveryEventEvidence{}) {
				t.Fatalf("invalid selector accepted: %+v %v", partial, err)
			}
		})
	}
}
