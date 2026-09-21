package serveapp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedTypedReceiverInitializationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyServedReceiverInitialization(t))
			_, rt := start()
			requireTypedReceiverInitializationCases(t, rt)
		})
	}
}

// Shared assertions run unchanged through both in-process HTTP and release-process HTTP.
func requireTypedReceiverInitializationCases(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	for _, tc := range []struct {
		name, values string
		config       map[string]any
	}{
		{"zero_false", `{"count":0,"ratio":2.5,"active":false,"label":"kept","attributes":{"nested":[1,2.5]}}`, map[string]any{
			"count": int64(0), "ratio": 2.5, "active": false, "label": "kept", "attributes": map[string]any{"nested": []any{int64(1), 2.5}},
		}},
		{"defaults", `{"active":true,"label":"kept","attributes":[]}`, map[string]any{
			"count": int64(3), "ratio": float64(2), "active": true, "label": "kept", "attributes": []any{},
		}},
		// Public event-schema admission normalizes optional null to absence
		// before initialize runs; this is not initializer-null admission.
		{"optional_null_normalized", `{"count":null,"active":true,"label":"kept","attributes":{}}`, map[string]any{
			"count": int64(3), "ratio": float64(2), "active": true, "label": "kept", "attributes": map[string]any{},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := "account-" + tc.name
			params := receiverInitializationPublishParams(rt, tc.name, account, tc.values)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			waitPublicationSiteCompletion(t, rt, seed.RunID)
			if tc.name == "optional_null_normalized" {
				var public operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": seed.EventID}, &public)
				values, ok := public.Payload["values"].(map[string]any)
				if _, present := values["count"]; !ok || present {
					t.Fatalf("optional-null normalization not visible in admitted payload: %+v", public.Payload)
				}
			}
			tc.config["account_id"] = account
			path, entityID := requireServedReceiverInitialization(t, rt, seed, tc.config, 1)
			before := readServedForkRecipientSourceDomain(t, rt, seed.RunID)
			companions := readForkReceiverCompanions(t, rt, seed.RunID)
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			if duplicate.EventID != seed.EventID || duplicate.RunID != seed.RunID || !reflect.DeepEqual(before, readServedForkRecipientSourceDomain(t, rt, seed.RunID)) || !reflect.DeepEqual(companions, readForkReceiverCompanions(t, rt, seed.RunID)) {
				t.Fatal("duplicate creation changed occurrence or execution history")
			}
			// Initialization is creation-only: missing and conflicting values
			// must neither reject reuse nor overwrite the persisted config.
			for index, values := range []string{`{}`, `{"count":99,"ratio":7.5,"active":true,"label":"replacement","attributes":{}}`} {
				reuseParams := receiverInitializationPublishParams(rt, fmt.Sprintf("%s-reuse-%d", tc.name, index), account, values)
				reuseParams["run_id"] = seed.RunID
				reuse := requireServedEventPublishRPCResult(t, rt.Endpoint, reuseParams)
				waitPublicationSiteCompletion(t, rt, seed.RunID)
				gotPath, gotEntity := requireServedReceiverInitialization(t, rt, reuse, tc.config, index+2)
				if reuse.RunID != seed.RunID || gotPath != path || gotEntity != entityID {
					t.Fatal("reuse changed receiver or run identity")
				}
			}
		})
	}
	for _, tc := range []struct{ name, values string }{
		{"missing_required", `{"active":true,"attributes":{}}`},
		{"missing_active", `{"label":"kept","attributes":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, receiverInitializationPublishParams(rt, tc.name, tc.name, tc.values))
			producer, err := identity.ParseExecutableNode(".", "producer")
			if err != nil {
				t.Fatal(err)
			}
			waitServedEventPublishDeliveryStatusCount(t, rt.DB, rt.Backend, seed.EventID, "node", producer.Key(), "dead_letter", 1)
			var settled int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id WHERE d.event_id=$1 AND d.claim_version=1 AND o.claim_version=1 AND o.outcome='dead_letter'`, seed.EventID).Scan(&settled); err != nil {
				t.Fatal(err)
			}
			if settled != 1 {
				t.Fatalf("initialization refusal first-claim settlement=%d, want 1", settled)
			}
			for label, query := range map[string]string{
				"receiver":    `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='account'`,
				"readiness":   `SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path LIKE 'account/%'`,
				"entity":      `SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND flow_instance LIKE 'account/%'`,
				"publication": `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='work.ready'`,
			} {
				var count int
				if err := rt.DB.QueryRow(query, seed.RunID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Errorf("failed initialization left %s rows=%d", label, count)
				}
			}
		})
	}
	t.Run("wrong_integer_at_ingress", func(t *testing.T) {
		params := receiverInitializationPublishParams(rt, "wrong-integer", "wrong-integer", `{"count":"3","active":true,"label":"kept","attributes":{}}`)
		err := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", params)
		if err.Data["code"] != apiv1.PayloadValidationFailedCode {
			t.Fatalf("wrong integer admission error=%+v", err)
		}
		key := params["idempotency_key"].(string)
		if count := servedEventPublishEventCountByIdempotencyKey(t, rt.DB, rt.Backend, key); count != 0 {
			t.Fatalf("invalid ingress persisted %d events", count)
		}
		if count := servedEventPublishAPIIdempotencyCount(t, rt.DB, rt.Backend, "event.publish", key); count != 0 {
			t.Fatalf("invalid ingress persisted %d command receipts", count)
		}
	})
}

func receiverInitializationPublishParams(rt servedControlProofRuntime, key, account, values string) map[string]any {
	return map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "idempotency_key": "typed-init-" + key,
		"payload": map[string]any{"account_id": account, "values": json.RawMessage(values)}}
}

func requireServedReceiverInitialization(t *testing.T, rt servedControlProofRuntime, seed servedEventPublishRPCResult, want map[string]any, executions int) (string, string) {
	t.Helper()
	var path, entityID, raw string
	if err := rt.DB.QueryRow(`SELECT f.instance_path,e.entity_id,CAST(f.config AS TEXT) FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.flow_instance=f.instance_path WHERE f.run_id=$1 AND f.flow_template='account'`, seed.RunID).Scan(&path, &entityID, &raw); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	if config["flow_path"] != path || config["storage_ref"] != path || config["instance_kind"] != "template" {
		t.Fatalf("receiver config has wrong durable coordinates: %s", raw)
	}
	// The durable companion wraps business config with runtime coordinates.
	got, err := canonicaljson.CloneRuntimeValue(config["config"])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted initialization=%#v, want %#v (raw %s)", got, want, raw)
	}
	var companions int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='account'`, seed.RunID).Scan(&companions); err != nil {
		t.Fatal(err)
	}
	if companions != 1 {
		t.Fatalf("receiver companions=%d, want 1", companions)
	}
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
	if entity.Entity.EntityID != entityID || entity.Entity.RunID != seed.RunID || !reflect.DeepEqual(entity.Fields, map[string]any{"account_id": want["account_id"], "processed_count": float64(executions)}) {
		t.Fatalf("receiver public business state=%+v; initialization must not leak into entity fields", entity)
	}
	var eventID string
	if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.ready' AND source_event_id=$2`, seed.RunID, seed.EventID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	var event operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
	node, err := identity.ParseExecutableNode("account", "collector")
	if err != nil {
		t.Fatal(err)
	}
	if event.EventID != eventID || event.EventName != "work.ready" || event.RunID != seed.RunID || event.SourceEventID != seed.EventID || event.Payload["account_id"] != want["account_id"] || len(event.Deliveries) != 1 {
		t.Fatalf("initialization publication identity/recipients=%+v", event)
	}
	delivery := event.Deliveries[0]
	if delivery.SubscriberType != "node" || delivery.SubscriberID != node.Key() || delivery.Status != "delivered" || delivery.Target.FlowInstance != path || delivery.Target.EntityID != entityID {
		t.Fatalf("initialization exact delivery owner=%+v", delivery)
	}
	// Settlement and its required handoff must be durable before inspecting the
	// exact claim census. No elapsed-time or assertion extension is introduced.
	var settled int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id WHERE d.event_id=$1 AND d.claim_version=1 AND o.claim_version=1 AND o.outcome='delivered'`, eventID).Scan(&settled); err != nil {
		t.Fatal(err)
	}
	if settled != 1 {
		t.Fatalf("initialization first-claim settlement=%d, want 1", settled)
	}
	return path, entityID
}
