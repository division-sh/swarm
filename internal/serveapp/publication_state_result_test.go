package serveapp

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestServedStateResultPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"root", "static"} {
			for _, outcome := range []string{"accepted", "rejected"} {
				t.Run(backend+"/"+mode+"/"+outcome, func(t *testing.T) {
					opts, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyPublicationStateResult(t, mode))
					first, rt := start()
					name, local, document := "document.requested", "result."+outcome, "name: exact-content\n"
					flow := "."
					if mode == "static" {
						name, flow = "source/"+name, "source"
					}
					requestID := uuid.NewString()
					params := map[string]any{"event_name": name, "bundle_hash": rt.BundleHash, "idempotency_key": "state-result-seed",
						"payload": map[string]any{"request_id": requestID, "content": document, "result_kind": outcome}}
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
					eventName, otherEventName := local, "result.accepted"
					if outcome == "accepted" {
						otherEventName = "result.rejected"
					}
					if flow != "." {
						eventName = flow + "/" + local
						otherEventName = flow + "/" + otherEventName
					}
					var eventID string
					for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
						err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, seed.RunID, eventName).Scan(&eventID)
						if err == nil {
							break
						}
						if err != sql.ErrNoRows {
							t.Fatal(err)
						}
						time.Sleep(20 * time.Millisecond)
					}
					if eventID == "" {
						t.Fatalf("missing state-result %s\n%s\n%s", outcome, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID), first.outputString())
					}
					readback := func() string {
						waitPublicationSiteCompletion(t, rt, seed.RunID)
						var results, settled int
						if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND event_name IN ($3,$4)`, seed.RunID, seed.EventID, eventName, otherEventName).Scan(&results); err != nil {
							t.Fatal(err)
						}
						if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN event_delivery_outcomes o ON o.delivery_id=d.delivery_id WHERE d.event_id=$1 AND d.status='delivered' AND d.claim_version=1 AND o.claim_version=1 AND o.outcome='delivered'`, eventID).Scan(&settled); err != nil {
							t.Fatal(err)
						}
						if results != 1 || settled != 2 {
							t.Fatalf("result publication/settlement census: events=%d delivered first claims=%d", results, settled)
						}
						var public operatorread.OperatorEventFull
						requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &public)
						if public.EventName != eventName || public.RunID != seed.RunID || public.Payload["result_kind"] != outcome || public.Payload["request_id"] != requestID || public.Payload["content"] != document {
							t.Fatalf("state-result readback lost occurrence identity: %+v", public)
						}
						var recipients []string
						for _, delivery := range public.Deliveries {
							if delivery.Status != "delivered" {
								t.Fatalf("state-result receiver did not execute: %+v", delivery)
							}
							recipients = append(recipients, delivery.SubscriberID)
						}
						var want []string
						for _, receiver := range []string{flow, "sink"} {
							node, err := identity.ParseExecutableNode(receiver, "local")
							if err != nil {
								t.Fatal(err)
							}
							want = append(want, node.Key())
						}
						sort.Strings(want)
						sort.Strings(recipients)
						if !reflect.DeepEqual(recipients, want) {
							t.Fatalf("state-result recipients=%v want=%v", recipients, want)
						}
						var rawSource, sourceKind, sourceEvent string
						if err := rt.DB.QueryRow(`SELECT CAST(source_route AS TEXT), routing_source_kind, source_event_id FROM events WHERE event_id=$1`, eventID).Scan(&rawSource, &sourceKind, &sourceEvent); err != nil {
							t.Fatal(err)
						}
						var route events.RouteIdentity
						if err := json.Unmarshal([]byte(rawSource), &route); err != nil {
							t.Fatal(err)
						}
						validSource := sourceKind == "static_flow" && route.FlowID == flow && route.FlowInstance == flow
						if flow == "." {
							validSource = sourceKind == "root" && route.FlowID == "" && route.FlowInstance == ""
						}
						if !validSource || sourceEvent != seed.EventID {
							t.Fatalf("state-result source=%s %+v", sourceKind, route)
						}
						if route.EntityID == "" {
							t.Fatal("state writer lost its authored receiving entity")
						}
						var rawFields string
						if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, seed.RunID, route.EntityID).Scan(&rawFields); err != nil {
							t.Fatal(err)
						}
						var fields map[string]any
						if err := json.Unmarshal([]byte(rawFields), &fields); err != nil {
							t.Fatal(err)
						}
						if fields["request_id"] != requestID || fields["content"] != document || fields["result_kind"] != outcome {
							t.Fatalf("state-result state lost request/content/outcome: %s", rawFields)
						}
						var entity operatorread.OperatorEntityFull
						requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": route.EntityID}, &entity)
						if entity.Entity.RunID != seed.RunID || entity.Entity.EntityID != route.EntityID || !reflect.DeepEqual(entity.Fields, fields) {
							t.Fatalf("state-result public entity lost exact mutation owner: %+v", entity)
						}
						payload, err := json.Marshal(public.Payload)
						if err != nil {
							t.Fatal(err)
						}
						return string(payload)
					}
					before := readback()
					if code := first.stop(); code != 0 {
						t.Fatalf("state-result completed stop=%d", code)
					}
					setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
					_, rt = start()
					duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
					if duplicate.EventID != seed.EventID || duplicate.RunID != seed.RunID || readback() != before {
						t.Fatal("restart/duplicate changed state-result occurrence")
					}
				})
			}
		}
	}
}
