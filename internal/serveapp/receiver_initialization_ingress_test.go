package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// R18: public admission of a provider-imported creating input, with no authored
// producer/connect hop. This is not a Telegram transport or private-harness proof.
func TestReceiverInitializationPublicProviderSchemaIngressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyProviderReceiverInitialization(t))
			var source semanticview.Source
			opts.TestRuntimeReadyHook = func(runtime *runtimepkg.Runtime) { source = runtime.Options.WorkflowModule.SemanticSource() }
			_, rt := start()
			const input = "inbound.telegram.text_message"
			requireProviderTriggerEventSource(t, source, input)
			if _, authored := source.AuthoredEventEntries()[input]; authored {
				t.Fatal("provider schema was replaced by an authored event declaration")
			}
			if len(source.ExecutableNodeRecords()) != 1 {
				t.Fatal("direct input proof unexpectedly has an intermediate producer")
			}
			for path, schema := range source.FlowSchemaEntries() {
				if len(schema.Connect) != 0 {
					t.Fatalf("direct input proof contains authored connect at %s", path)
				}
			}
			requireReceiverInitializationPublicProviderIngressCases(t, rt)
		})
	}
}

// Keep the public admission and durable readback obligations identical for the
// in-process server and the independently built release executable.
func requireReceiverInitializationPublicProviderIngressCases(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	const input = "inbound.telegram.text_message"
	payload := map[string]any{"conversation_reference": "2307", "conversation_scope": "direct", "external_account_reference": "2307", "provider_message_reference": 7, "text": "direct initialization"}
	for _, tc := range []struct {
		name, event, code string
		payload           map[string]any
	}{
		{"private_child", "telegram-chat/chat.initialized", apiv1.EventNotDeclaredCode, map[string]any{"conversation_reference": "2307", "initial_text": "not a declared input", "message_number": 7, "enabled": false}},
		{"wrong_provider_integer", input, apiv1.PayloadValidationFailedCode, map[string]any{"conversation_reference": "2307", "conversation_scope": "direct", "external_account_reference": "2307", "provider_message_reference": "7", "text": "invalid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := receiverIngressApplicationSnapshot(t, rt)
			err := requireServedJSONRPCError(t, rt.Endpoint, "event.publish", map[string]any{"bundle_hash": rt.BundleHash, "event_name": tc.event, "payload": tc.payload, "idempotency_key": tc.name})
			if err.Data["code"] != tc.code {
				t.Fatalf("scope/type refusal=%+v, want %s", err, tc.code)
			}
			after := receiverIngressApplicationSnapshot(t, rt)
			if tc.code == apiv1.PayloadValidationFailedCode {
				requireReceiverIngressRejectionDiagnostic(t, before, after, input)
			}
			for table, rows := range after {
				if !reflect.DeepEqual(before[table], rows) {
					t.Errorf("rejected public input changed %s: before=%v after=%v", table, before[table], rows)
				}
			}
		})
	}
	// No run, concrete target, source event, entity, or flow-instance hints.
	params := map[string]any{"bundle_hash": rt.BundleHash, "event_name": input, "payload": payload, "idempotency_key": "direct-provider-input"}
	seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
	if !seed.NewRunCreated || seed.RunID == "" || seed.EventID == "" {
		t.Fatalf("target-free creation acknowledgment=%+v", seed)
	}
	waitPublicationSiteCompletion(t, rt, seed.RunID)
	const path = "telegram-chat/ti-1531b1e4416a29704d690346"
	entityID := flowidentity.EntityID(path)
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(config AS TEXT) FROM flow_instances WHERE run_id=$1 AND instance_path=$2 AND flow_template='telegram-chat'`, seed.RunID, path).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var descriptor map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &descriptor); err != nil {
		t.Fatal(err)
	}
	config, err := canonicaljson.CloneRuntimeValue(descriptor["config"])
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"conversation_reference": "2307", "initial_text": "direct initialization", "message_number": int64(7), "enabled": false}
	if !reflect.DeepEqual(config, want) {
		t.Fatalf("provider-initialized typed config=%#v, want %#v", config, want)
	}
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
	wantFields := map[string]any{"conversation_reference": "2307", "initial_text": "direct initialization", "message_number": float64(7), "enabled": false}
	if entity.Entity.RunID != seed.RunID || entity.Entity.EntityID != entityID || entity.Entity.FlowInstance != path || !reflect.DeepEqual(entity.Fields, wantFields) {
		t.Fatalf("real child did not consume typed initialization: %+v", entity)
	}
	var autoEvent string
	if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2 AND source_event_id=$3`, seed.RunID, path+"/chat.initialized", seed.EventID).Scan(&autoEvent); err != nil {
		t.Fatal(err)
	}
	node, err := identity.ParseExecutableNode("telegram-chat", "receiver")
	if err != nil {
		t.Fatal(err)
	}
	for _, eventID := range []string{seed.EventID, autoEvent} {
		var public operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &public)
		if public.EventID != eventID || public.RunID != seed.RunID || len(public.Deliveries) != 1 {
			t.Fatalf("direct input publication/recipients=%+v", public)
		}
		delivery := public.Deliveries[0]
		if delivery.SubscriberType != "node" || delivery.SubscriberID != node.Key() || delivery.Status != "delivered" || delivery.Target.FlowInstance != path || delivery.Target.EntityID != entityID {
			t.Fatalf("direct input exact owner=%+v", delivery)
		}
		if eventID == autoEvent && (public.SourceEventID != seed.EventID || !reflect.DeepEqual(public.Payload, wantFields)) {
			t.Fatalf("initialization publication lost exact typed config/source: %+v", public)
		}
		var settled int
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id WHERE d.event_id=$1 AND d.claim_version=1 AND o.claim_version=1 AND o.outcome='delivered'`, eventID).Scan(&settled); err != nil {
			t.Fatal(err)
		}
		if settled != 1 {
			t.Fatalf("direct input first-claim outcomes=%d, want 1", settled)
		}
	}
	var count int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name NOT LIKE 'platform.%'`, seed.RunID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("direct input introduced an intermediate publication: count=%d err=%v", count, err)
	}
	before := receiverIngressApplicationSnapshot(t, rt)
	duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
	if duplicate.RunID != seed.RunID || duplicate.EventID != seed.EventID || !reflect.DeepEqual(before, receiverIngressApplicationSnapshot(t, rt)) {
		t.Fatal("duplicate direct input recreated receiver or repeated execution")
	}
}

func receiverIngressApplicationSnapshot(t *testing.T, rt servedControlProofRuntime) map[string][]string {
	t.Helper()
	all, out := snapshotForkReceiverApplication(t, rt), map[string][]string{}
	for _, table := range []string{
		"runs", "events", "flow_instances", "flow_instance_runtime_readiness", "entity_state", "entity_mutations", "routing_rules",
		"event_deliveries", "event_delivery_attempts", "event_delivery_outcomes", "event_receipts", "committed_replay_scopes", "api_idempotency",
		"author_activity_order", "author_activity_occurrences", "run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions",
	} {
		if _, exists := all[table]; !exists {
			t.Fatalf("missing native application table %s", table)
		}
		out[table] = all[table]
	}
	out["events/columns"] = all["events/columns"]
	return out
}

// Payload rejection appends one runtime diagnostic, not an admitted publication.
// Account for that exact append; retain every preexisting row and all other facts.
func requireReceiverIngressRejectionDiagnostic(t *testing.T, before, after map[string][]string, event string) {
	t.Helper()
	var columns []string
	if err := json.Unmarshal([]byte(before["events/columns"][0]), &columns); err != nil {
		t.Fatal(err)
	}
	previous := make(map[string]bool, len(before["events"]))
	for _, row := range before["events"] {
		previous[row] = true
	}
	retained := []string{}
	added := 0
	for _, row := range after["events"] {
		if previous[row] {
			retained = append(retained, row)
			continue
		}
		added++
		var values []any
		if err := json.Unmarshal([]byte(row), &values); err != nil || len(values) != len(columns) {
			t.Fatalf("invalid diagnostic row: %s err=%v", row, err)
		}
		fields := make(map[string]any, len(columns))
		for i, column := range columns {
			fields[column] = values[i]
		}
		if fields["event_class"] != "diagnostic_direct" || fields["event_name"] != "platform.runtime_log" || fields["run_id"] != nil || fields["entity_id"] != nil || fields["flow_instance"] != nil || fields["source_event_id"] != nil {
			t.Fatalf("rejection created a non-diagnostic event: %s", row)
		}
		var payload struct {
			Level   string `json:"log_level"`
			Details struct {
				Action, Component string
				Event             string `json:"event_type"`
			}
		}
		raw, ok := fields["payload"].(string)
		if !ok {
			t.Fatalf("diagnostic payload is not JSON text: %s", row)
		}
		if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload.Level != "warn" || payload.Details.Action != "payload_validation_rejected" || payload.Details.Component != "event-bus" || payload.Details.Event != event {
			t.Fatalf("unexpected rejection diagnostic: %s err=%v", raw, err)
		}
	}
	if added != 1 {
		t.Fatalf("rejection diagnostics=%d, want exactly one", added)
	}
	after["events"] = retained
}
