package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func requireSupplementalForkAcquisitionSettlement(t *testing.T, selectedProofEndpoint string, selectedProofReceiverStateReader receiverProofStateReader, selectedProofRuntime *runtimepkg.Runtime, runID, eventID string, policy canonicalrouting.ForkReceiverPolicy) events.DeliveryRoute {
	t.Helper()
	entityID := flowidentity.EntityID("consumer")
	wantTarget := events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: entityID}
	var matched []servedForkDeliveryEvidence
	var deliveryID string
	for id, delivery := range readServedForkDeliveryEvidence(t, selectedProofReceiverStateReader, runID) {
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
	if !ok || handler.FlowPath() != "consumer" || handler.NodeID() != "collector" || local != "work.ready" || route.Target.Code() != "existing_entity" || route.Target.Route() != wantTarget {
		t.Fatalf("mutation lost exact authored handler/constructed target: %+v", route)
	}
	settlement, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (storetest.ForkReceiverSettlementStorage, error) {
		return storetest.ReadForkReceiverSettlementStorage(ctx, selectedProofReceiverStateReader, runID, deliveryID)
	})
	if err != nil {
		t.Fatal(err)
	}
	status, outcome, failure := settlement.Status, settlement.Outcome, settlement.Failure
	claim, outcomeClaim := settlement.Claim, settlement.OutcomeClaim
	outcomes, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
		return storetest.CountForkReceiverSettledAttemptsStorage(ctx, selectedProofReceiverStateReader, deliveryID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || outcome != "delivered" || claim != 1 || outcomeClaim != 1 || outcomes != 1 {
		t.Fatalf("acquisition settlement=%s/%s claims=%d/%d outcomes=%d failure=%s", status, outcome, claim, outcomeClaim, outcomes, failure)
	}
	var publicEvent operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, selectedProofEndpoint, "event.get", map[string]any{"event_id": eventID}, &publicEvent)
	if publicEvent.RunID != runID || len(publicEvent.Deliveries) != 1 || publicEvent.Deliveries[0].DeliveryID != deliveryID || publicEvent.Deliveries[0].Status != "delivered" || publicEvent.Deliveries[0].Target != (operatorread.OperatorDeliveryTarget{Kind: "existing_entity", FlowID: "consumer", FlowInstance: "consumer", EntityID: entityID}) {
		t.Fatalf("public acquisition target/settlement: %+v", publicEvent)
	}
	row := readWorkspaceProofReceiverRows(t, selectedProofRuntime.Bus.Store(), runID)["consumer"]
	if row.ID != entityID || row.Flow != "consumer" || row.Type != "receipt" || row.State != "active" || !reflect.DeepEqual(row.Fields, map[string]any{"marker": "consumer-created"}) {
		t.Fatalf("canonical receiver creation/marker: %+v", row)
	}
	var publicEntity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, selectedProofEndpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &publicEntity)
	if publicEntity.Entity.RunID != runID || publicEntity.Entity.EntityID != entityID || publicEntity.Entity.FlowInstance != "consumer" || publicEntity.Entity.EntityType != "receipt" || publicEntity.Entity.CurrentState != "active" || !reflect.DeepEqual(publicEntity.Fields, row.Fields) {
		t.Fatalf("public acquired receiver: %+v", publicEntity)
	}
	type markerMutation struct {
		Old, New                   any
		WriterType, WriterID, Step string
	}
	wantMutations := []markerMutation{{nil, "consumer-created", "platform", "workflow_engine", "mutate"}}
	if policy != canonicalrouting.ForkReceiverConstructorOwned {
		t.Fatalf("unsupported supplemental test policy %d", policy)
	}
	rows, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) ([]storetest.ReceiverFieldMutationStorage, error) {
		return storetest.ReadReceiverMarkerMutationStorage(ctx, selectedProofReceiverStateReader, runID, entityID, eventID)
	})
	if err != nil {
		t.Fatal(err)
	}
	var gotMutations []markerMutation
	for _, row := range rows {
		mutation := markerMutation{WriterType: row.WriterType, WriterID: row.WriterID, Step: row.Step}
		oldJSON, newJSON := row.Old, row.New
		if err := json.Unmarshal([]byte(oldJSON), &mutation.Old); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(newJSON), &mutation.New); err != nil {
			t.Fatal(err)
		}
		gotMutations = append(gotMutations, mutation)
	}
	sort.Slice(gotMutations, func(i, j int) bool {
		return gotMutations[i].WriterID < gotMutations[j].WriterID
	})
	if !reflect.DeepEqual(gotMutations, wantMutations) {
		t.Fatalf("fresh acquisition marker history: got=%+v want=%+v", gotMutations, wantMutations)
	}
	mutations, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
		return storetest.CountReceiverMarkerMutationsStorage(ctx, selectedProofReceiverStateReader, runID, eventID)
	})
	if err != nil {
		t.Fatal(err)
	}
	emitted, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
		return storetest.CountSourceNonDiagnosticEventsStorage(ctx, selectedProofReceiverStateReader, runID, eventID, string(events.EventAdmissionDiagnosticDirect), string(events.EventTypePlatformRuntimeLog))
	})
	if err != nil {
		t.Fatal(err)
	}
	downstream, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
		return storetest.CountSourceRunDeliveryStorage(ctx, selectedProofReceiverStateReader, runID, eventID)
	})
	if err != nil {
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
		}{{"constructor_owned", canonicalrouting.ForkReceiverConstructorOwned}} {
			t.Run(string(backend)+"/"+tc.name, func(t *testing.T) {
				publication := newExactOriginPublicationProbe(t)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkReceiverAcquisitionWithoutFinishedEmission(t, tc.policy))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "acquisition-seed"})
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, seed.RunID)
				seedRows := readWorkspaceProofReceiverRows(t, rt.Runtime.Bus.Store(), seed.RunID)
				wantSeedRows := map[string]forkReceiverRow{seed.RunID: {ID: seed.RunID, Flow: seed.RunID, Type: "root", State: "active", Fields: map[string]any{"marker": "root-owned"}}, "producer": {ID: flowidentity.EntityID("producer"), Flow: "producer", Type: "work", State: "waiting", Fields: map[string]any{}}, "consumer": {ID: flowidentity.EntityID("consumer"), Flow: "consumer", Type: "receipt", State: "waiting", Fields: map[string]any{}}}
				if !reflect.DeepEqual(seedRows, wantSeedRows) {
					t.Fatalf("seed did not construct the exact inert child tree: got=%+v want=%+v", seedRows, wantSeedRows)
				}
				started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"token": "receiver-proof"}, "idempotency_key": "acquisition-request"})
				if started.RunID != seed.RunID {
					t.Fatal("source request escaped seeded run")
				}
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, seed.RunID)
				var frontier string
				if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='producer/work.ready'`, seed.RunID).Scan(&frontier); err != nil {
					t.Fatal(err)
				}
				sourceRoute := requireSupplementalForkAcquisitionSettlement(t, rt.Endpoint, rt.ReceiverStateReader, rt.Runtime, seed.RunID, frontier, tc.policy)
				sourceRows := readWorkspaceProofReceiverRows(t, rt.Runtime.Bus.Store(), seed.RunID)
				producer := sourceRows["producer"]
				if len(sourceRows) != 3 || producer.ID != flowidentity.EntityID("producer") || producer.Type != "work" || producer.Fields["marker"] != "producer-owned" || producer.ID == flowidentity.EntityID("consumer") {
					t.Fatalf("independent source producer/receiver ownership: %+v", sourceRows)
				}
				sourceEvidence := readForkReceiverProducerEvidence(t, rt.Runtime.Bus.Store(), seed.RunID, frontier)
				if sourceEvidence.Source.Kind() != events.RoutingSourceStaticFlow || sourceEvidence.Source.Route() != (events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: producer.ID}) {
					t.Fatalf("immutable source producer: %+v", sourceEvidence)
				}
				if err := waitExactOriginPublications(t.Context(), publication, seed.EventID, started.EventID); err != nil {
					t.Fatal(err)
				}
				before, companions := readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), seed.RunID), readForkReceiverCompanions(t, rt.ReceiverStateReader, seed.RunID)
				defer func() {
					if !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), seed.RunID)) || !reflect.DeepEqual(companions, readForkReceiverCompanions(t, rt.ReceiverStateReader, seed.RunID)) {
						t.Error("supplemental acquisition changed settled source domain or companions")
					}
				}()
				params := map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier, "allow_source_freeze": true, "idempotency_key": "acquisition-fork"}
				var fork, replay apiv1.RunForkExecutionResult
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
				if fork.SourceRunID != seed.RunID || fork.ForkEventID != frontier || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ExecutedEventCount != 1 {
					t.Fatalf("supplemental acquisition fork identity: %+v", fork)
				}
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, fork.ForkRunID)
				childEvent := activityidentity.ForkLineageEventID(fork.ForkRunID, frontier)
				childRoute := requireSupplementalForkAcquisitionSettlement(t, rt.Endpoint, rt.ReceiverStateReader, rt.Runtime, fork.ForkRunID, childEvent, tc.policy)
				if !reflect.DeepEqual(sourceRoute.ConnectClaim, childRoute.ConnectClaim) || readForkReceiverProducerEvidence(t, rt.Runtime.Bus.Store(), fork.ForkRunID, childEvent) != sourceEvidence {
					t.Fatal("acquisition fork changed compiled handler or immutable producer")
				}
				childRows := readWorkspaceProofReceiverRows(t, rt.Runtime.Bus.Store(), fork.ForkRunID)
				if len(childRows) != 3 || !reflect.DeepEqual(childRows["consumer"], sourceRows["consumer"]) || !reflect.DeepEqual(childRows["producer"], producer) {
					t.Fatalf("fork acquisition rehomed or duplicated a physical owner: source=%+v child=%+v", sourceRows, childRows)
				}
				root := childRows[fork.ForkRunID]
				if root.ID != fork.ForkRunID || root.Type != "root" || root.State != "active" || root.Fields["marker"] != "root-owned" {
					t.Fatalf("fork canonical root remapping: %+v", childRows)
				}
				childBefore, childCompanions := readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), fork.ForkRunID), readForkReceiverCompanions(t, rt.ReceiverStateReader, fork.ForkRunID)
				requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
				if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(childBefore, readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), fork.ForkRunID)) || !reflect.DeepEqual(childCompanions, readForkReceiverCompanions(t, rt.ReceiverStateReader, fork.ForkRunID)) {
					t.Fatal("same-key fork retry repeated acquisition/effects or changed response")
				}
			})
		}
	}
}
