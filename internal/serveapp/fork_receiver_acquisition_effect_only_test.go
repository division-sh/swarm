package serveapp

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func requireSupplementalForkAcquisitionSettlement(t *testing.T, rt servedControlProofRuntime, runID, eventID string, policy canonicalrouting.ForkReceiverPolicy) events.DeliveryRoute {
	t.Helper()
	// The fixture authors a direct static consumer, not a runtime-classified owner.
	entityID := flowidentity.EntityID("consumer")
	wantTarget := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: entityID}
	var matched []servedForkDeliveryEvidence
	var deliveryID string
	for id, delivery := range readServedForkDeliveryEvidence(t, rt, runID) {
		if node, ok := delivery.route.Recipient.Node(); delivery.eventID == eventID && ok && node.FlowPath() == "consumer" && node.NodeID() == "collector" {
			matched = append(matched, delivery)
			deliveryID = id
		}
	}
	if len(matched) != 1 {
		t.Fatalf("acquiring receiver deliveries=%d, want one", len(matched))
	}
	route := matched[0].route
	handler, local, ok := route.ConnectClaim.NodeHandlerOwner()
	if !ok || handler.FlowPath() != "consumer" || handler.NodeID() != "collector" || local != "work.ready" || route.Target.Code() != "materializing_entity" || route.Target.Route() != wantTarget {
		t.Fatalf("acquisition lost exact authored handler/future target: %+v", route)
	}
	var status, outcome, failure string
	var claim, outcomeClaim, outcomes int
	if err := rt.DB.QueryRow(`SELECT d.status,d.claim_version,o.outcome,o.claim_version,COALESCE(CAST(o.failure AS TEXT),'') FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id WHERE d.run_id=$1 AND d.delivery_id=$2`, runID, deliveryID).Scan(&status, &claim, &outcome, &outcomeClaim, &failure); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_delivery_outcomes WHERE delivery_id=$1`, deliveryID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || outcome != "delivered" || claim != 1 || outcomeClaim != 1 || outcomes != 1 {
		t.Fatalf("acquisition settlement=%s/%s claims=%d/%d outcomes=%d failure=%s", status, outcome, claim, outcomeClaim, outcomes, failure)
	}
	var publicEvent operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &publicEvent)
	if publicEvent.RunID != runID || len(publicEvent.Deliveries) != 1 || publicEvent.Deliveries[0].DeliveryID != deliveryID || publicEvent.Deliveries[0].Status != "delivered" || publicEvent.Deliveries[0].Target != (operatorread.OperatorDeliveryTarget{Kind: "materializing_entity", FlowID: "consumer", FlowInstance: "consumer", EntityID: entityID}) {
		t.Fatalf("public acquisition target/settlement: %+v", publicEvent)
	}
	row := readForkReceiverRows(t, rt, runID)["consumer"]
	if row.ID != entityID || row.Flow != "consumer" || row.Type != "receipt" || row.State != "active" || !reflect.DeepEqual(row.Fields, map[string]any{"marker": "consumer-created"}) {
		t.Fatalf("canonical receiver creation/marker: %+v", row)
	}
	var publicEntity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &publicEntity)
	if publicEntity.Entity.RunID != runID || publicEntity.Entity.EntityID != entityID || publicEntity.Entity.FlowInstance != "consumer" || publicEntity.Entity.EntityType != "receipt" || publicEntity.Entity.CurrentState != "active" || !reflect.DeepEqual(publicEntity.Fields, row.Fields) {
		t.Fatalf("public acquired receiver: %+v", publicEntity)
	}
	type markerMutation struct {
		Old, New                   any
		WriterType, WriterID, Step string
	}
	wantMutations := []markerMutation{{nil, "consumer-created", "platform", "workflow_engine", "create"}}
	if policy == canonicalrouting.ForkReceiverExplicitCreate {
		wantMutations = []markerMutation{
			{nil, "", "platform", "entity_initial_value", "create_entity"},
			{"", "consumer-created", "platform", "workflow_engine", "create"},
		}
	} else if policy != canonicalrouting.ForkReceiverAutoMaterializing {
		t.Fatalf("unsupported supplemental test policy %d", policy)
	}
	rows, err := rt.DB.Query(`SELECT COALESCE(CAST(old_value AS TEXT),'null'),CAST(new_value AS TEXT),writer_type,writer_id,handler_step FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='marker'`, runID, entityID, eventID)
	if err != nil {
		t.Fatal(err)
	}
	var gotMutations []markerMutation
	for rows.Next() {
		var mutation markerMutation
		var oldJSON, newJSON string
		if err := rows.Scan(&oldJSON, &newJSON, &mutation.WriterType, &mutation.WriterID, &mutation.Step); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(oldJSON), &mutation.Old); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(newJSON), &mutation.New); err != nil {
			t.Fatal(err)
		}
		gotMutations = append(gotMutations, mutation)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	sort.Slice(gotMutations, func(i, j int) bool { return gotMutations[i].WriterID < gotMutations[j].WriterID })
	if !reflect.DeepEqual(gotMutations, wantMutations) {
		t.Fatalf("fresh acquisition marker history: got=%+v want=%+v", gotMutations, wantMutations)
	}
	var mutations, emitted, downstream int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2 AND domain='authored_field' AND path='marker'`, runID, eventID).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	// Only exact platform runtime-log diagnostics are excluded from domain effects.
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`, runID, eventID, string(events.EventAdmissionDiagnosticDirect), string(events.EventTypePlatformRuntimeLog)).Scan(&emitted); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.source_event_id=$2`, runID, eventID).Scan(&downstream); err != nil {
		t.Fatal(err)
	}
	if mutations != len(wantMutations) || emitted != 0 || downstream != 0 {
		t.Fatalf("effect-only acquisition census: marker writes=%d emitted=%d downstream deliveries=%d", mutations, emitted, downstream)
	}
	return route
}

// Supplemental coverage deliberately omits the post-revision finished event.
// The original emission-bearing F11 success cases remain separate and unchanged.
func TestSelectedForkSupplementalReceiverAcquisitionWithoutPostRevisionEmissionBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, tc := range []struct {
			name   string
			policy canonicalrouting.ForkReceiverPolicy
		}{{"auto_materializing", canonicalrouting.ForkReceiverAutoMaterializing}, {"explicit_create", canonicalrouting.ForkReceiverExplicitCreate}} {
			t.Run(string(backend)+"/"+tc.name, func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkReceiverAcquisitionWithoutFinishedEmission(t, tc.policy))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "acquisition-seed"})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				waitForkReceiverSourceCompletion(t, rt, seed.RunID)
				seedRows := readForkReceiverRows(t, rt, seed.RunID)
				if _, exists := seedRows["consumer"]; exists || len(seedRows) != 1 {
					t.Fatalf("seed invented an unexecuted acquiring receiver: %+v", seedRows)
				}
				started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "acquisition-request"})
				if started.RunID != seed.RunID {
					t.Fatal("source request escaped seeded run")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				waitForkReceiverSourceCompletion(t, rt, seed.RunID)
				var frontier string
				if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, seed.RunID).Scan(&frontier); err != nil {
					t.Fatal(err)
				}
				sourceRoute := requireSupplementalForkAcquisitionSettlement(t, rt, seed.RunID, frontier, tc.policy)
				sourceRows := readForkReceiverRows(t, rt, seed.RunID)
				producer := sourceRows["producer"]
				if len(sourceRows) != 3 || producer.ID != flowidentity.EntityID("producer") || producer.Type != "work" || producer.Fields["marker"] != "producer-owned" || producer.ID == flowidentity.EntityID("consumer") {
					t.Fatalf("independent source producer/receiver ownership: %+v", sourceRows)
				}
				sourceEvidence := readForkReceiverProducerEvidence(t, rt, seed.RunID, frontier)
				if sourceEvidence.Source.Kind() != events.RoutingSourceStaticFlow || sourceEvidence.Source.Route() != (events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producer.ID}) {
					t.Fatalf("immutable source producer: %+v", sourceEvidence)
				}
				before, companions := readServedForkRecipientSourceDomain(t, rt, seed.RunID), readForkReceiverCompanions(t, rt, seed.RunID)
				defer func() {
					if !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(companions, readForkReceiverCompanions(t, rt, seed.RunID)) {
						t.Error("supplemental acquisition changed settled source domain or companions")
					}
				}()
				params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier, "confirm_source_freeze": true, "idempotency_key": "acquisition-fork"}
				var fork, replay apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
				if fork.SourceRunID != seed.RunID || fork.ForkEventID != frontier || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 {
					t.Fatalf("supplemental acquisition fork identity: %+v", fork)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
				childEvent := activityidentity.ForkLineageEventID(fork.ForkRunID, frontier)
				childRoute := requireSupplementalForkAcquisitionSettlement(t, rt, fork.ForkRunID, childEvent, tc.policy)
				if !reflect.DeepEqual(sourceRoute.ConnectClaim, childRoute.ConnectClaim) || readForkReceiverProducerEvidence(t, rt, fork.ForkRunID, childEvent) != sourceEvidence {
					t.Fatal("acquisition fork changed compiled handler or immutable producer")
				}
				childRows := readForkReceiverRows(t, rt, fork.ForkRunID)
				if len(childRows) != 3 || !reflect.DeepEqual(childRows["consumer"], sourceRows["consumer"]) || !reflect.DeepEqual(childRows["producer"], producer) {
					t.Fatalf("fork acquisition rehomed or duplicated a physical owner: source=%+v child=%+v", sourceRows, childRows)
				}
				root := childRows[fork.ForkRunID]
				if root.ID != fork.ForkRunID || root.Type != "root" || root.State != "active" || root.Fields["marker"] != "root-owned" {
					t.Fatalf("fork canonical root remapping: %+v", childRows)
				}
				childBefore, childCompanions := readServedForkRecipientSourceDomain(t, rt, fork.ForkRunID), readForkReceiverCompanions(t, rt, fork.ForkRunID)
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
				if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(childBefore, readServedForkRecipientSourceDomain(t, rt, fork.ForkRunID)) || !reflect.DeepEqual(childCompanions, readForkReceiverCompanions(t, rt, fork.ForkRunID)) {
					t.Fatal("same-key fork retry repeated acquisition/effects or changed response")
				}
			})
		}
	}
}
