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
	for _, missing := range []string{"account_id", "values"} {
		t.Run("missing_required_"+missing, func(t *testing.T) {
			params := receiverInitializationPublishParams(rt, "missing-"+missing, "missing-"+missing, `{}`)
			delete(params["payload"].(map[string]any), missing)
			before := receiverIngressApplicationSnapshot(t, rt)
			refusal := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", params)
			if refusal.Data["code"] != apiv1.PayloadValidationFailedCode {
				t.Fatalf("required message refusal=%+v", refusal)
			}
			after := receiverIngressApplicationSnapshot(t, rt)
			requireReceiverIngressRejectionDiagnostic(t, before, after, "work.requested")
			for table, rows := range after {
				if !reflect.DeepEqual(before[table], rows) {
					t.Errorf("rejected input changed %s", table)
				}
			}
			key := params["idempotency_key"].(string)
			if count := servedEventPublishEventCountByIdempotencyKey(t, rt.DB, rt.Backend, key); count != 0 {
				t.Fatalf("rejected message persisted %d events", count)
			}
		})
	}
	for _, tc := range []struct {
		name, values string
		config       map[string]any
	}{
		{"zero_false", `{"count":0,"ratio":2.5,"active":false,"label":"kept","attributes":{"nested":[1,2.5]}}`, map[string]any{
			"count": int64(0), "ratio": 2.5, "active": false, "label": "kept", "attributes": map[string]any{"nested": []any{int64(1), 2.5}},
		}},
		{"optional_fields_omitted", `{"active":true,"label":"kept","attributes":[]}`, map[string]any{
			"active": true, "label": "kept", "attributes": []any{},
		}},
		// Public event-schema admission normalizes optional null to absence
		// before same-name constructor projection; no default is reapplied.
		{"optional_null_normalized", `{"count":null,"active":true,"label":"kept","attributes":{}}`, map[string]any{
			"active": true, "label": "kept", "attributes": map[string]any{},
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
			want := map[string]any{"account_id": account, "values": tc.config}
			path, entityID := requireServedReceiverInitialization(t, rt, seed, want, 1)
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
				gotPath, gotEntity := requireServedReceiverInitialization(t, rt, reuse, want, index+2)
				if reuse.RunID != seed.RunID || gotPath != path || gotEntity != entityID {
					t.Fatal("reuse changed receiver or run identity")
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
	var path, entityID, raw, fieldsRaw string
	if err := rt.DB.QueryRow(`SELECT f.instance_path,e.entity_id,CAST(f.config AS TEXT),CAST(e.fields AS TEXT) FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.flow_instance=f.instance_path WHERE f.run_id=$1 AND f.flow_template='account'`, seed.RunID).Scan(&path, &entityID, &raw, &fieldsRaw); err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &config); err != nil {
		t.Fatal(err)
	}
	if config["flow_path"] != path || config["storage_ref"] != path || config["instance_kind"] != "template" {
		t.Fatalf("receiver config has wrong durable coordinates: %s", raw)
	}
	// Business state has one field owner; the companion contains runtime controls only.
	var persisted map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(fieldsRaw), &persisted); err != nil {
		t.Fatal(err)
	}
	got, err := canonicaljson.CloneRuntimeValue(persisted)
	if err != nil {
		t.Fatal(err)
	}
	fields := got.(map[string]any)
	delete(fields, "processed_count")
	if _, exists := config["config"]; exists {
		t.Fatalf("business copy in header: %s", raw)
	}
	if !reflect.DeepEqual(fields, want) {
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
	publicRaw, err := json.Marshal(want["values"])
	if err != nil {
		t.Fatal(err)
	}
	var publicValues any
	if err := json.Unmarshal(publicRaw, &publicValues); err != nil {
		t.Fatal(err)
	}
	if entity.Entity.EntityID != entityID || entity.Entity.RunID != seed.RunID || !reflect.DeepEqual(entity.Fields, map[string]any{"account_id": want["account_id"], "processed_count": float64(executions), "values": publicValues}) {
		t.Fatalf("receiver public business state=%+v; supplied values must have the declared state owner", entity)
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
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o ON o.delivery_id=d.delivery_id WHERE d.event_id=$1 AND d.claim_version=1 AND o.claim_version=1 AND o.outcome='delivered'`, eventID).Scan(&settled); err != nil {
		t.Fatal(err)
	}
	if settled != 1 {
		t.Fatalf("initialization first-claim settlement=%d, want 1", settled)
	}
	return path, entityID
}
