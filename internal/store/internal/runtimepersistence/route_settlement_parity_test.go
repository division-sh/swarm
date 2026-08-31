package runtimepersistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type routeSettlementOperatorStore interface {
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
	ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
}

func TestDirectiveEventPersistsTypedNoSubscriberByDesign(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			payload := json.RawMessage("{\n  \"directive\": \"continue\", \"attempt\": 1.0\n}")
			event := eventtest.DiagnosticDirect(
				uuid.NewString(), events.EventTypePlatformAgentDirective, "runtime", "", payload,
				0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			commit := func(txctx context.Context, tx *sql.Tx, store any) error {
				story, err := eventFixtureStory(txctx)
				if err != nil {
					return err
				}
				switch selected := store.(type) {
				case *PostgresStore:
					_, err = selected.eventPostgresOwner.CommitDirectiveEventTx(txctx, tx, story, privaterunforkrevision.NewEffects(), admitted)
					if err == nil {
						var restored events.AdmittedEvent
						var found bool
						restored, found, err = selected.eventPostgresOwner.LoadDirectiveEventTx(txctx, tx, event.ID())
						if err == nil && (!found || !bytes.Equal(restored.Event().Payload(), payload)) {
							return fmt.Errorf("postgres directive payload = %q found=%v, want %q", restored.Event().Payload(), found, payload)
						}
					}
				case *SQLiteRuntimeStore:
					_, err = selected.eventSQLiteOwner.CommitDirectiveEventTx(txctx, tx, story, privaterunforkrevision.NewEffects(), admitted)
					if err == nil {
						var restored events.AdmittedEvent
						var found bool
						restored, found, err = selected.eventSQLiteOwner.LoadDirectiveEventTx(txctx, tx, event.ID())
						if err == nil && (!found || !bytes.Equal(restored.Event().Payload(), payload)) {
							return fmt.Errorf("sqlite directive payload = %q found=%v, want %q", restored.Event().Payload(), found, payload)
						}
					}
				}
				return err
			}
			switch selected := fixture.store.(type) {
			case *PostgresStore:
				err = selected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error { return commit(txctx, tx, selected) })
			case *SQLiteRuntimeStore:
				err = selected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error { return commit(txctx, tx, selected) })
			}
			if err != nil {
				t.Fatalf("commit directive: %v", err)
			}
			assertPersistedNoDeliverySettlement(t, ctx, fixture, event.ID(), events.EventWriteDirectiveDirect)
		})
	}
}

func TestRuntimeLogEventPersistsTypedNoSubscriberByDesign(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			for _, run := range []string{"", runID} {
				event := eventtest.DiagnosticDirect(
					uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"settled"}`),
					0, run, "", events.EventEnvelope{}, time.Now().UTC(),
				)
				if err := commitDiagnosticRuntimeLogFixture(ctx, fixture.store.(diagnosticRuntimeLogFixtureStore), event); err != nil {
					t.Fatalf("commit runtime log: %v", err)
				}
				assertPersistedNoDeliverySettlement(t, ctx, fixture, event.ID(), events.EventWriteRuntimeLogDirect)
			}
		})
	}
}

func TestOperatorEventReadbackProjectsTypedDeliveryTargetOwnership(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			entityID := eventtest.UUID("review-instance-1")
			tests := []struct {
				name  string
				owner events.DeliveryTargetOwnership
			}{
				{name: "existing_entity", owner: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/instance-1", EntityID: entityID})},
				{name: "materializing_entity", owner: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/instance-2", EntityID: eventtest.UUID("review-instance-2")})},
				{name: "entityless_receiver", owner: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/instance-3"})},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					event := eventtest.ExistingRunRootIngress(
						uuid.NewString(), "assessment.reported", "reviewer", "", json.RawMessage(`{}`), 0,
						runID, events.EventEnvelope{}, time.Now().UTC(),
					)
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("review-finalize")), Target: test.owner}
					if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, []events.DeliveryRoute{route}); err != nil {
						t.Fatalf("commit delivered event: %v", err)
					}
					full, err := fixture.store.(routeSettlementOperatorStore).LoadOperatorEvent(ctx, event.ID())
					if err != nil {
						t.Fatalf("LoadOperatorEvent: %v", err)
					}
					if full.NoDelivery != nil || len(full.Deliveries) != 1 {
						t.Fatalf("operator settlement = deliveries:%#v no_delivery:%#v", full.Deliveries, full.NoDelivery)
					}
					got := full.Deliveries[0].Target
					want := test.owner.Route()
					if got.Kind != test.owner.Code() || got.FlowID != want.FlowID || got.FlowInstance != want.FlowInstance || got.EntityID != want.EntityID {
						t.Fatalf("operator target = %#v, want %s %#v", got, test.owner.Code(), want)
					}
				})
			}
		})
	}
}

func TestOperatorEventReadbackSeparatesConnectTargetsFromDeliveryOwnership(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review.unmatched", "gateway", "", json.RawMessage(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
				t.Fatal(err)
			}

			receiver := events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte("review-receiver")))
			candidate, err := events.NewConnectCandidateEvidence(receiver, events.MustNodeDeliveryRecipient(mustPersistenceRootNode("reviewer")), "review", agentidentity.Plan{}, events.ConnectCandidateAccepted)
			if err != nil {
				t.Fatal(err)
			}
			target := events.RouteIdentity{FlowID: "review", FlowInstance: "review/one", EntityID: eventtest.UUID("review-one-owner")}
			plan, err := events.NewConnectPlanEvaluation(events.AdmitConnectPlanIdentity(sha256.Sum256([]byte("review-plan"))), events.ConnectPlanResolved, []events.RouteIdentity{target}, []events.ConnectCandidateEvidence{candidate})
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := events.NewConnectEvaluationLedger([]events.ConnectPlanEvaluation{plan})
			if err != nil {
				t.Fatal(err)
			}
			settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryMatchedNoRecipient, ledger)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(settlement)
			if err != nil {
				t.Fatal(err)
			}
			if backend.name == "postgres" {
				_, err = fixture.db.ExecContext(ctx, `UPDATE events SET route_settlement = $1::jsonb WHERE event_id = $2::uuid`, raw, event.ID())
			} else {
				_, err = fixture.db.ExecContext(ctx, `UPDATE events SET route_settlement = ? WHERE event_id = ?`, raw, event.ID())
			}
			if err != nil {
				t.Fatalf("store connect settlement: %v", err)
			}

			full, err := fixture.store.(routeSettlementOperatorStore).LoadOperatorEvent(ctx, event.ID())
			if err != nil {
				t.Fatalf("LoadOperatorEvent: %v", err)
			}
			if full.NoDelivery == nil || len(full.NoDelivery.Plans) != 1 || len(full.NoDelivery.Plans[0].Targets) != 1 {
				t.Fatalf("operator no-delivery projection = %#v", full.NoDelivery)
			}
			got := full.NoDelivery.Plans[0].Targets[0]
			if got.FlowID != target.FlowID || got.FlowInstance != target.FlowInstance || got.EntityID != target.EntityID {
				t.Fatalf("connect target = %#v, want %#v", got, target)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "kind") {
				t.Fatalf("connect target leaked delivery ownership discriminator: %s", encoded)
			}
		})
	}
}

func TestOperatorEventListRouteSettlementTotalityParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			base := time.Now().UTC().Truncate(time.Microsecond)
			delivered := eventtest.ExistingRunRootIngress(uuid.NewString(), "review.delivered", "gateway", "", json.RawMessage(`{}`), 0, runID, events.EventEnvelope{}, base)
			matchedEmpty := eventtest.ExistingRunRootIngress(uuid.NewString(), "review.empty", "gateway", "", json.RawMessage(`{}`), 0, runID, events.EventEnvelope{}, base.Add(time.Second))
			deliberateEmpty := eventtest.DiagnosticDirect(uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{"message":"proof"}`), 0, runID, "", events.EventEnvelope{}, base.Add(2*time.Second))
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("review-finalize")), Target: events.MustExistingEntityTarget(events.RouteIdentity{
				FlowID: "review", FlowInstance: "review/instance-1", EntityID: eventtest.UUID("review-instance-1"),
			})}
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, delivered, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			if err := commitSemanticEventFixture(ctx, fixture.store, matchedEmpty); err != nil {
				t.Fatal(err)
			}
			if err := commitDiagnosticRuntimeLogFixture(ctx, fixture.store.(diagnosticRuntimeLogFixtureStore), deliberateEmpty); err != nil {
				t.Fatal(err)
			}

			selected := fixture.store.(routeSettlementOperatorStore)
			seen := map[string]operatorread.OperatorEventFull{}
			cursor := ""
			for {
				page, err := selected.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID}, Limit: 1, Cursor: cursor, Order: "asc"})
				if err != nil {
					t.Fatalf("ListOperatorEvents: %v", err)
				}
				for _, event := range page.Events {
					seen[event.EventID] = event
				}
				if page.NextCursor == "" {
					break
				}
				if page.NextCursor == cursor {
					t.Fatal("event pagination cursor did not advance")
				}
				cursor = page.NextCursor
			}
			if len(seen) != 3 {
				t.Fatalf("paginated events = %#v, want exactly three", seen)
			}
			if got := seen[delivered.ID()]; len(got.Deliveries) != 1 || got.NoDelivery != nil {
				t.Fatalf("delivered union = %#v", got)
			}
			if got := seen[matchedEmpty.ID()]; len(got.Deliveries) != 0 || got.NoDelivery == nil || got.NoDelivery.Reason != events.NoDeliveryDeclaredConsumerNoPlan.Code() {
				t.Fatalf("matched-empty union = %#v", got)
			}
			if got := seen[deliberateEmpty.ID()]; len(got.Deliveries) != 0 || got.NoDelivery == nil || got.NoDelivery.Reason != events.NoDeliveryNoSubscriberByDesign.Code() {
				t.Fatalf("deliberate-empty union = %#v", got)
			}
		})
	}
}

func TestEventObservationRejectsMalformedRoutingSettlement(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "review.malformed", "gateway", "", json.RawMessage(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
				t.Fatal(err)
			}
			raw := `{"write_class":"normal_publication","arm":"no_delivery","reason":"unknown_reason","evaluation":{"plans":[]}}`
			var err error
			if backend.name == "postgres" {
				_, err = fixture.db.ExecContext(ctx, `UPDATE events SET route_settlement = $1::jsonb WHERE event_id = $2::uuid`, raw, event.ID())
			} else {
				_, err = fixture.db.ExecContext(ctx, `UPDATE events SET route_settlement = ? WHERE event_id = ?`, raw, event.ID())
			}
			if err != nil {
				t.Fatalf("corrupt route settlement: %v", err)
			}
			if _, err := fixture.store.(routeSettlementOperatorStore).LoadOperatorEvent(ctx, event.ID()); err == nil || !strings.Contains(err.Error(), "route settlement") {
				t.Fatalf("malformed settlement read error = %v", err)
			}
		})
	}
}

func assertPersistedNoDeliverySettlement(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, eventID string, wantClass events.EventWriteClass) {
	t.Helper()
	record, found, err := loadEventProducerIdentityRecord(ctx, fixture, eventID)
	if err != nil || !found {
		t.Fatalf("load event settlement: found=%v err=%v", found, err)
	}
	settlement, err := record.DecodeSettlement()
	if err != nil {
		t.Fatalf("decode event settlement: %v", err)
	}
	if settlement.WriteClass() != wantClass || !settlement.NoDelivery() || settlement.Reason() != events.NoDeliveryNoSubscriberByDesign || settlement.Ledger().Present() {
		t.Fatalf("settlement = %#v, want deliberate no-delivery class %s", settlement, wantClass.Code())
	}
	if err := settlement.Validate(nil); err != nil {
		t.Fatalf("validate persisted settlement: %v", err)
	}
}

var _ routeSettlementOperatorStore = (*PostgresStore)(nil)
var _ routeSettlementOperatorStore = (*SQLiteRuntimeStore)(nil)
