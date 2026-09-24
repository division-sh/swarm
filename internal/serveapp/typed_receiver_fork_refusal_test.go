package serveapp

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

// H: real HTTP/native stores through the retained in-process mock lifecycle.
// Source node consumption is supported; this does not claim dynamic fork success.
func TestTypedReceiverConfigSourceAndForkRefusalBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyForkReceiverRecordedConfig(t))
			params := receiverInitializationPublishParams(rt, "recorded-config-source", "business-key", `{"count":7,"ratio":7.0,"active":false,"label":"recorded business config","attributes":{"nested":[7,7.0]}}`)
			requestWire, err := json.Marshal(params)
			if err != nil || !bytes.Contains(requestWire, []byte(`"ratio":7.0`)) || !bytes.Contains(requestWire, []byte(`[7,7.0]`)) {
				t.Fatalf("test transport lost explicit decimal tokens: %s err=%v", requestWire, err)
			}
			t.Logf("public input wire with explicit integral decimals: %s", requestWire)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			waitForkReceiverSourceCompletion(t, rt, seed.RunID)
			const path = "account/ti-138096d2b56ac1568ac40ea7"
			entityID := flowidentity.EntityID(path)
			var creating, consumed string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.ready' AND source_event_id=$2`, seed.RunID, seed.EventID).Scan(&creating); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2 AND source_event_id=$3`, seed.RunID, path+"/account.initialized", creating).Scan(&consumed); err != nil {
				t.Fatal(err)
			}
			// Semantic public ingress maps 7, 7.0 and 7e0 to CEL int. Declared
			// typed defaults are not ingress and must retain their double kind.
			want := map[string]any{"account_id": "business-key", "count": int64(7), "ratio": int64(7), "active": false, "label": "recorded business config", "attributes": map[string]any{"nested": []any{int64(7), int64(7)}},
				"status": false, "flow_path": "business/path", "instance_id": "business-instance", "workflow_version": float64(7)}
			var raw string
			if err := rt.DB.QueryRow(`SELECT CAST(config AS TEXT) FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND flow_template='account'`, seed.RunID, path).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var envelope map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &envelope); err != nil {
				t.Fatal(err)
			}
			config, err := canonicaljson.CloneRuntimeValue(envelope["config"])
			if err != nil || !reflect.DeepEqual(config, want) {
				gotWire, _ := canonicaljson.MarshalPreservingNumberKinds(config)
				wantWire, _ := canonicaljson.MarshalPreservingNumberKinds(want)
				t.Fatalf("recorded config lost kinds/business identity: got=%s want=%s raw=%s err=%v", gotWire, wantWire, raw, err)
			}
			if envelope["instance_id"] != "ti-138096d2b56ac1568ac40ea7" || envelope["flow_path"] != path || envelope["storage_ref"] != path {
				t.Fatalf("business names overwrote physical control identity: %#v", envelope)
			}
			// Public JSON decoding uses a generic numeric carrier. The native
			// config check above separately distinguishes integers from doubles.
			wire, err := canonicaljson.MarshalPreservingNumberKinds(want)
			if err != nil {
				t.Fatal(err)
			}
			var publicWant map[string]any
			if err := json.Unmarshal(wire, &publicWant); err != nil {
				t.Fatal(err)
			}
			var event operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": consumed}, &event)
			if event.EventID != consumed || event.RunID != seed.RunID || event.SourceEventID != creating || !reflect.DeepEqual(event.Payload, publicWant) || len(event.Deliveries) != 1 {
				t.Fatalf("exact config-derived event/public recipient: %+v", event)
			}
			node, err := identity.ParseExecutableNode("account", "collector")
			if err != nil {
				t.Fatal(err)
			}
			if d := event.Deliveries[0]; d.Status != "delivered" || d.Target.EntityID != entityID || d.Target.FlowInstance != path || d.SubscriberType != "node" || d.SubscriberID != node.Key() {
				t.Fatalf("exact config consumer route: %+v", d)
			}
			var public operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &public)
			publicWant["processed_count"] = float64(1)
			if public.Entity.RunID != seed.RunID || public.Entity.EntityID != entityID || public.Entity.FlowInstance != path || !reflect.DeepEqual(public.Fields, publicWant) {
				t.Fatalf("final node/public readback lost initialized fields: %+v", public)
			}
			var claims, writes int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o ON o.delivery_id=d.delivery_id WHERE d.event_id=$1 AND d.claim_version=1 AND o.claim_version=1 AND o.outcome='delivered'`, consumed).Scan(&claims); err != nil || claims != 1 {
				t.Fatalf("config consumer claims=%d err=%v", claims, err)
			}
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND writer_type='platform' AND writer_id='workflow_engine' AND handler_step='mutate' AND domain='authored_field' AND path IN ('count','label','ratio','active','attributes','status','flow_path','instance_id','workflow_version')`, seed.RunID, entityID, consumed).Scan(&writes); err != nil || writes != 9 {
				t.Fatalf("final node config-derived writes=%d err=%v", writes, err)
			}
			before := snapshotForkReceiverApplication(t, rt)
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if duplicate.RunID != seed.RunID || duplicate.EventID != seed.EventID || !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
				t.Fatal("source duplicate changed initialization or execution")
			}
			for _, frontier := range []struct{ name, eventID string }{{"creating_delivery", creating}, {"settled_consumer", consumed}} {
				t.Run(frontier.name, func(t *testing.T) {
					for attempt := 0; attempt < 2; attempt++ {
						err := requireServedJSONRPCError(t, rt.Endpoint, "run.fork", map[string]any{"source_run_id": seed.RunID, "fork_event_id": frontier.eventID, "allow_source_freeze": true, "idempotency_key": frontier.name})
						details, ok := err.Data["details"].(map[string]any)
						if err.Code != -32603 || !ok {
							t.Fatalf("unexpected fork refusal: %+v", err)
						}
						failure := decodeServedFailureEnvelope(t, details["failure"])
						capabilities, marshalErr := json.Marshal(failure.Detail.Attributes["capabilities"])
						if string(failure.Class) != "platform.dependency_unavailable" || failure.Detail.Code != "selected_contract_deferred_work_owner_unavailable" || failure.Component != "selected-contract-run-fork" || failure.Operation != "admit-deferred-work-ownership" || !failure.Retryable || failure.Deterministic || marshalErr != nil || string(capabilities) != `["dynamic_flow_instance_creation"]` {
							t.Fatalf("wrong exact #642 refusal: %+v capabilities=%s err=%v", failure, capabilities, marshalErr)
						}
						if !reflect.DeepEqual(before, snapshotForkReceiverApplication(t, rt)) {
							t.Fatal("refused dynamic fork mutated application/story/revision facts")
						}
					}
				})
			}
			t.Logf("supported source config/node consumption; fork explicitly refused: %s", wire)
		})
	}
}
