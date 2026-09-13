package serveapp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedGeneratedActivityPublicationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"root", "static", "template", "nested_template"} {
			for _, approval := range []bool{false, true} {
				group, outcomes := "read", []string{"success", "exhausted"}
				if approval {
					group, outcomes = "approval", []string{"approve", "revise", "reject"}
				}
				t.Run(backend+"/"+mode+"/"+group, func(t *testing.T) {
					var calls atomic.Int32
					var exhausted atomic.Bool
					provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						var input struct {
							Message string `json:"message"`
						}
						if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Message != "exact-provider-input" {
							t.Errorf("provider input: %+v, %v", input, err)
						}
						w.Header().Set("Content-Type", "application/json")
						if exhausted.Load() {
							w.WriteHeader(http.StatusServiceUnavailable)
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"delivered": true})
					}))
					defer provider.Close()
					opts, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyPublicationActivity(t, mode, provider.URL, approval))
					first, rt := start()
					inputName := "activity.requested"
					if mode == "static" {
						inputName = "source/" + inputName
					}
					type completedCase struct {
						outcome, runID, seedID, eventID, local, readback string
						params, decisionParams                           map[string]any
					}
					var completed []completedCase
					// Each outcome retains a fresh run. Only source admission and process
					// startup are shared; every result is checked again after restart.
					for _, outcome := range outcomes {
						t.Run(outcome, func(t *testing.T) {
							beforeCalls := calls.Load()
							exhausted.Store(outcome == "exhausted")
							params := map[string]any{"event_name": inputName, "bundle_hash": rt.BundleHash,
								"payload": map[string]any{"case_id": "alpha", "message": "exact-provider-input"}, "idempotency_key": "activity-seed-" + outcome}
							seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
							defer func() {
								if !t.Failed() {
									return
								}
								rows, err := rt.DB.Query(`SELECT failure FROM dead_letters WHERE original_event_id IN (SELECT event_id FROM events WHERE run_id=$1)`, seed.RunID)
								if err != nil {
									t.Logf("failure readback: %v", err)
									return
								}
								defer rows.Close()
								for rows.Next() {
									var failure string
									if err := rows.Scan(&failure); err != nil {
										t.Log(err)
										return
									}
									t.Logf("activity failure: %s", failure)
								}
								if err := rows.Err(); err != nil {
									t.Logf("failure readback: %v", err)
								}
							}()
							var decisionParams map[string]any
							if approval {
								decisionParams = lifecycleGateDecisionParams(t, rt, seed.RunID, outcome)
								decisionParams["idempotency_key"] = "activity-decision-" + outcome
								if calls.Load() != beforeCalls {
									t.Fatal("provider executed before human approval")
								}
								if outcome == "revise" {
									decisionParams["fields"] = map[string]any{"feedback": "retain exact feedback"}
								} else if outcome == "reject" {
									decisionParams["fields"] = map[string]any{"reason": "retain exact reason"}
								}
								var decision map[string]any
								requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", decisionParams, &decision)
							}
							local, wantCalls := "send.succeeded", int32(1)
							switch outcome {
							case "exhausted":
								local, wantCalls = "send.failed", 3
							case "revise":
								local, wantCalls = "send.revision_requested", 0
							case "reject":
								local, wantCalls = "send.rejected", 0
							}
							var eventID string
							for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
								err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND (event_name=$2 OR event_name LIKE $3)`, seed.RunID, local, "%/"+local).Scan(&eventID)
								if err == nil {
									break
								}
								if err != sql.ErrNoRows {
									t.Fatal(err)
								}
								time.Sleep(20 * time.Millisecond)
							}
							if eventID == "" {
								t.Fatalf("missing generated outcome; calls=%d\n%s", calls.Load(), servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
							}
							waitPublicationSiteCompletion(t, rt, seed.RunID)
							before := requireActivityPublicationReadback(t, rt, seed.RunID, eventID, mode, local)
							if calls.Load()-beforeCalls != wantCalls {
								t.Fatalf("provider calls=%d want=%d", calls.Load()-beforeCalls, wantCalls)
							}
							completed = append(completed, completedCase{outcome: outcome, runID: seed.RunID, seedID: seed.EventID, eventID: eventID, local: local, readback: before, params: params, decisionParams: decisionParams})
						})
					}
					if t.Failed() {
						return
					}
					wantTotalCalls := calls.Load()
					if code := first.stop(); code != 0 {
						t.Fatalf("activity completed stop=%d", code)
					}
					setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
					_, rt = start()
					for _, row := range completed {
						t.Run("restart/"+row.outcome, func(t *testing.T) {
							duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, row.params)
							if duplicate.EventID != row.seedID || duplicate.RunID != row.runID {
								t.Fatal("restart reminted activity ingress")
							}
							if approval {
								var decision map[string]any
								requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", row.decisionParams, &decision)
							}
							waitPublicationSiteCompletion(t, rt, row.runID)
							if after := requireActivityPublicationReadback(t, rt, row.runID, row.eventID, mode, row.local); after != row.readback || calls.Load() != wantTotalCalls {
								t.Fatalf("restart/duplicate changed generated outcome or re-executed provider: calls=%d", calls.Load())
							}
						})
					}
				})
			}
		}
	}
}

func requireActivityPublicationReadback(t *testing.T, rt servedControlProofRuntime, runID, eventID, mode, local string) string {
	t.Helper()
	var name, kind, rawSource, rawPayload string
	if err := rt.DB.QueryRow(`SELECT event_name,routing_source_kind,CAST(source_route AS TEXT),CAST(payload AS TEXT) FROM events WHERE event_id=$1 AND run_id=$2`, eventID, runID).Scan(&name, &kind, &rawSource, &rawPayload); err != nil {
		t.Fatal(err)
	}
	var source events.RouteIdentity
	if err := json.Unmarshal([]byte(rawSource), &source); err != nil {
		t.Fatal(err)
	}
	flow, path, wantKind := ".", runID, "static_flow"
	if mode != "root" {
		flow, path = "source", "source"
	}
	if mode == "nested_template" {
		flow = "outer/source"
	}
	if strings.Contains(mode, "template") {
		wantKind = "concrete_template_instance"
		if err := rt.DB.QueryRow(`SELECT instance_path FROM flow_instances WHERE run_id=$1 AND flow_template=$2`, runID, flow).Scan(&path); err != nil {
			t.Fatal(err)
		}
	}
	wantSource := events.RouteIdentity{FlowID: flow, FlowInstance: path}
	var entityID string
	err := rt.DB.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, path).Scan(&entityID)
	if err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if err == nil {
		wantSource.EntityID = entityID
		if mode == "root" {
			// The root's actual materialized entity uses the existing root source
			// variant; entityless root execution retains its exact run coordinate.
			wantKind = "root"
			wantSource = events.RouteIdentity{EntityID: entityID}
		}
	}
	wantName := local
	if flow != "." {
		wantName = path + "/" + local
	}
	if name != wantName || source != wantSource || kind != wantKind {
		t.Fatalf("generated outcome borrowed declaration/instance/entity: %s %s %+v want=%s %s %+v", name, kind, source, wantName, wantKind, wantSource)
	}
	var public operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &public)
	if public.EventName != name || public.EventID != eventID || public.RunID != runID || public.Payload["activity_id"] != "send" {
		t.Fatalf("generated public outcome: %+v", public)
	}
	if local == "send.revision_requested" && public.Payload["feedback"] != "retain exact feedback" || local == "send.rejected" && public.Payload["reason"] != "retain exact reason" {
		t.Fatalf("decision outcome lost fields: %+v", public.Payload)
	}
	var got, want []string
	for _, delivered := range public.Deliveries {
		if delivered.Status != "delivered" {
			t.Fatalf("generated outcome consumer did not execute: %+v", delivered)
		}
		got = append(got, delivered.SubscriberID)
	}
	for _, receiver := range [][2]string{{flow, "local"}, {"sink", "local"}} {
		node, err := identity.ParseExecutableNode(receiver[0], receiver[1])
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, node.Key())
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("generated outcome recipients=%v want=%v", got, want)
	}
	var resultCount int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name LIKE 'send.%' OR event_name LIKE '%/send.%')`, runID).Scan(&resultCount); err != nil {
		t.Fatal(err)
	}
	if resultCount != 1 {
		t.Fatalf("generated result arms=%d want one", resultCount)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s/%v", eventID, name, kind, rawSource, rawPayload, got)
}
