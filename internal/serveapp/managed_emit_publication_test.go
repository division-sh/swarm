package serveapp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestManagedEmitPublicationExactScopeBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, mode := range []string{"root", "imported", "nested", "template"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				source := canonicalrouting.CopyManagedEmitPublication(t, mode)
				rt, _, restart := newRetainedMailboxCompletionRuntime(t, backend, source)
				scope, instance, request := ".", "", "work.requested"
				if mode == "imported" {
					scope, instance, request = "source", "source", "source/work.requested"
				}
				if mode == "nested" {
					scope, instance, request = "outer/source", "outer/source", "outer/source/work.requested"
				}
				if mode == "template" {
					scope = "source"
				}
				params := map[string]any{"event_name": request, "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "alpha"}, "idempotency_key": "managed-emit"}
				accepted := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, accepted.RunID)
				if mode == "root" {
					instance = accepted.RunID
				}
				if mode == "template" {
					if err := rt.DB.QueryRow("SELECT instance_path FROM flow_instances WHERE run_id=$1 AND flow_template='source'", accepted.RunID).Scan(&instance); err != nil {
						t.Fatal(err)
					}
				}
				assertResult := func(runID, flow, path, role string, expected any) {
					t.Helper()
					name := "work.result"
					if flow != "." {
						name = path + "/work.result"
					}
					var rawSource, payload []byte
					var id, producer, producerType, parent, kind, authority string
					if err := rt.DB.QueryRow("SELECT event_id,source_route,payload,produced_by,produced_by_type,source_event_id,routing_source_kind,COALESCE(routing_source_authority,'') FROM events WHERE run_id=$1 AND event_name=$2", runID, name).Scan(&id, &rawSource, &payload, &producer, &producerType, &parent, &kind, &authority); err != nil {
						t.Fatalf("managed publication: %v\n%s", err, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
					}
					var routing events.RouteIdentity
					if err := json.Unmarshal(rawSource, &routing); err != nil {
						t.Fatal(err)
					}
					if routing.FlowID != flow || routing.FlowInstance != path {
						t.Fatalf("wrong managed source: %s", rawSource)
					}
					var value map[string]any
					if err := json.Unmarshal(payload, &value); err != nil || value["value"] != expected {
						t.Fatalf("sibling schema substituted: %s %v", payload, err)
					}
					var count, delivered int
					wantKind := events.RoutingSourceStaticFlow
					if mode == "template" && flow == "source" {
						wantKind = events.RoutingSourceConcreteTemplateInstance
					}
					if _, err := events.RestoreRoutingSource(kind, routing, authority); err != nil {
						t.Fatal(err)
					}
					if producerType != "agent" || kind != wantKind.StorageCode() || authority != "" {
						t.Fatalf("wrong admitted agent source: producer=%s type=%s kind=%s authority=%s", producer, producerType, kind, authority)
					}
					agentPath := path
					if flow == "." {
						agentPath = ""
					}
					if err := rt.DB.QueryRow("SELECT COUNT(*) FROM event_deliveries d JOIN agents a ON a.run_id=d.run_id AND a.agent_id=d.subscriber_id WHERE d.event_id=$1 AND d.subscriber_type='agent' AND d.subscriber_id=$2 AND d.status='delivered' AND a.role=$3 AND a.flow_instance=$4", parent, producer, role, agentPath).Scan(&count); err != nil || count != 1 {
						t.Fatalf("publication did not originate from the exact managed actor and inbound delivery: %d %v", count, err)
					}
					if err := rt.DB.QueryRow("SELECT COUNT(*),SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) FROM event_deliveries WHERE event_id=$1", id).Scan(&count, &delivered); err != nil || count != 1 || delivered != 1 {
						t.Fatalf("final consumer count=%d delivered=%d err=%v", count, delivered, err)
					}
					var recipient string
					if err := rt.DB.QueryRow("SELECT subscriber_id FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'", id).Scan(&recipient); err != nil {
						t.Fatal(err)
					}
					node, err := identity.ParseExecutableNodeKey(recipient)
					if err != nil || node.FlowPath() != flow || node.NodeID() != "final" {
						t.Fatalf("wrong final receiver: %s %v", recipient, err)
					}
					ack := strings.TrimSuffix(name, "result") + "ack"
					if err := rt.DB.QueryRow("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2 AND source_event_id=$3", runID, ack, id).Scan(&count); err != nil || count != 1 {
						t.Fatalf("final consumer did not emit exactly one acknowledgement: %d %v", count, err)
					}
					if err := rt.DB.QueryRow("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name LIKE '%foreign.only'", runID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("sibling-only tool gained publication authority: %d %v", count, err)
					}
				}
				assertResult(accepted.RunID, scope, instance, "source-writer", "exact")
				siblingParams := map[string]any{"event_name": "sibling/sibling.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "sibling"}, "idempotency_key": "managed-sibling"}
				sibling := requireServedEventPublishRPCResult(t, rt.Endpoint, siblingParams)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, sibling.RunID)
				assertResult(sibling.RunID, "sibling", "sibling", "sibling-writer", true)
				rt, _ = restart()
				duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != accepted.EventID || duplicate.RunID != accepted.RunID {
					t.Fatal("restart duplicate changed ingress identity")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, accepted.RunID)
				assertResult(accepted.RunID, scope, instance, "source-writer", "exact")
				siblingDuplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, siblingParams)
				if siblingDuplicate.EventID != sibling.EventID || siblingDuplicate.RunID != sibling.RunID {
					t.Fatal("sibling restart replay changed identity")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, sibling.RunID)
				assertResult(sibling.RunID, "sibling", "sibling", "sibling-writer", true)

				hostileParams := map[string]any{"event_name": request, "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "hostile"}, "idempotency_key": "managed-hostile"}
				hostile := requireServedEventPublishRPCResult(t, rt.Endpoint, hostileParams)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, hostile.RunID)
				var count int
				var failure []byte
				if err := rt.DB.QueryRow("SELECT failure FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='dead_letter'", hostile.RunID).Scan(&failure); err != nil || len(failure) == 0 {
					t.Fatalf("missing exact sibling-tool refusal: %s %v\n%s", failure, err, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, hostile.RunID))
				}
				// The mock provider adapter rejects an invisible tool before gateway
				// dispatch. Its durable raw turn proves the attempted call, unlike a
				// generic dead-letter assertion alone.
				var rawRequest, rawResponse []byte
				var parsed bool
				if err := rt.DB.QueryRow("SELECT request_payload,response_payload,parse_ok FROM agent_turns WHERE run_id=$1", hostile.RunID).Scan(&rawRequest, &rawResponse, &parsed); err != nil {
					t.Fatal(err)
				}
				var turn struct {
					Calls []struct {
						Name string `json:"name"`
					} `json:"calls"`
				}
				var providerRequest struct {
					Tools []struct {
						Name string `json:"name"`
					} `json:"tools"`
				}
				if err := json.Unmarshal(rawResponse, &turn); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(rawRequest, &providerRequest); err != nil {
					t.Fatal(err)
				}
				if parsed || len(turn.Calls) != 1 || turn.Calls[0].Name != "emit_foreign_only" {
					t.Fatalf("wrong refusal witness: parsed=%v response=%s", parsed, rawResponse)
				}
				visibleResult := false
				for _, tool := range providerRequest.Tools {
					if tool.Name == "emit_foreign_only" {
						t.Fatal("sibling-only tool was exposed to the source actor")
					}
					if tool.Name == "emit_work_result" {
						visibleResult = true
					}
				}
				if !visibleResult {
					t.Fatal("legitimate exact emit tool missing from managed surface")
				}
				if err := rt.DB.QueryRow("SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name LIKE '%foreign.only' OR event_name LIKE '%work.result' OR event_name LIKE '%work.ack')", hostile.RunID).Scan(&count); err != nil || count != 0 {
					t.Fatalf("denied tool mutated publication/final consumers: %d %v", count, err)
				}
			})
		}
	}
}
