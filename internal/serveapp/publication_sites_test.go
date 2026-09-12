package serveapp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedPublicationSitesBothStores(t *testing.T) {
	proveServedPublicationSites(t, false)
}

func TestServedPublicationTextSitesBothStores(t *testing.T) {
	proveServedPublicationSites(t, true)
}

func proveServedPublicationSites(t *testing.T, textValues bool) {
	t.Helper()
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, mode := range []string{"root", "static", "template"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				copySource := canonicalrouting.CopyPublicationSites
				if textValues {
					copySource = canonicalrouting.CopyPublicationTextSites
				}
				rt := startLifecycleTemplateRuntime(t, backend, copySource(t, mode))
				runID := ""
				want := map[string][]int{}
				for _, caseID := range []string{"alpha", "beta"} {
					for _, family := range []string{"direct", "rules", "specialized", "completion", "success", "fanout", "rulefanout", "completefanout"} {
						choices := []int{1}
						if family == "rules" || family == "specialized" || family == "completion" {
							choices = []int{1, 0}
						}
						if strings.Contains(family, "fanout") {
							choices = []int{0, 1, 2}
						}
						for _, choice := range choices {
							name := family + ".requested"
							if mode == "static" {
								name = "source/" + name
							}
							items := []int{}
							values := []int{choice}
							if strings.Contains(family, "fanout") {
								for i := 0; i < choice; i++ {
									items = append(items, i+1)
								}
								values = items
							} else if family == "rules" || family == "specialized" || family == "completion" {
								values = []int{20}
								if choice > 0 {
									values = []int{10}
								}
							}
							params := map[string]any{"event_name": name, "payload": map[string]any{"case_id": caseID, "choice": choice, "items": items}, "idempotency_key": fmt.Sprintf("%s-%s-%d", caseID, family, choice)}
							if textValues {
								textItems := []string{}
								for _, item := range items {
									textItems = append(textItems, strconv.Itoa(item))
								}
								params["payload"].(map[string]any)["items"] = textItems
							}
							if runID == "" {
								params["bundle_hash"] = rt.BundleHash
							} else {
								params["run_id"] = runID
							}
							published := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
							runID = published.RunID
							waitForkReceiverSourceCompletion(t, rt, runID)
							waitPublicationSiteFanOut(t, rt, runID)
							key := caseID + "/result." + family
							want[key] = append(want[key], values...)
							requirePublicationSiteReadback(t, rt, runID, mode, want, textValues)
							duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
							if duplicate.EventID != published.EventID || duplicate.RunID != runID {
								t.Fatalf("duplicate changed ingress: %+v -> %+v", published, duplicate)
							}
							waitForkReceiverSourceCompletion(t, rt, runID)
							requirePublicationSiteReadback(t, rt, runID, mode, want, textValues)
						}
					}
				}
			})
		}
	}
}

func waitPublicationSiteFanOut(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		var pending, refused int
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND status<>'closed'`, runID).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind<>'committed'`, runID).Scan(&refused); err != nil {
			t.Fatal(err)
		}
		if refused != 0 {
			var failure string
			if err := rt.DB.QueryRow(`SELECT CAST(failure AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind<>'committed' LIMIT 1`, runID).Scan(&failure); err != nil {
				t.Fatal(err)
			}
			t.Fatalf("fan-out semantic rejection: %s", failure)
		}
		if pending == 0 {
			waitForkReceiverSourceCompletion(t, rt, runID)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fan-out did not finish: %s", servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
}

func requirePublicationSiteReadback(t *testing.T, rt servedControlProofRuntime, runID, mode string, want map[string][]int, textValues bool) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT event_id,event_name,CAST(payload AS TEXT),routing_source_kind,CAST(source_route AS TEXT),COALESCE(CAST(source_event_id AS TEXT),'') FROM events WHERE run_id=$1 AND (event_name LIKE 'result.%' OR event_name LIKE '%/result.%')`, runID)
	if err != nil {
		t.Fatal(err)
	}
	type result struct{ id, name, raw, kind, route, cause string }
	var results []result
	for rows.Next() {
		var r result
		if err := rows.Scan(&r.id, &r.name, &r.raw, &r.kind, &r.route, &r.cause); err != nil {
			t.Fatal(err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	got := map[string][]int{}
	for _, r := range results {
		var payload struct {
			CaseID string          `json:"case_id"`
			Value  json.RawMessage `json:"value"`
		}
		var route events.RouteIdentity
		if err := json.Unmarshal([]byte(r.raw), &payload); err != nil {
			t.Fatal(err)
		}
		var value int
		if textValues {
			var text string
			if err := json.Unmarshal(payload.Value, &text); err != nil {
				t.Fatal(err)
			}
			value, err = strconv.Atoi(text)
			if err != nil {
				t.Fatal(err)
			}
		} else if err := json.Unmarshal(payload.Value, &value); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(r.route), &route); err != nil {
			t.Fatal(err)
		}
		flow, path, kind := ".", runID, "static_flow"
		if mode != "root" {
			flow, path, kind = "source", "source", "static_flow"
		}
		if mode == "template" {
			kind = "concrete_template_instance"
			var rawFields, declaration string
			if err := rt.DB.QueryRow(`SELECT e.flow_instance,CAST(e.fields AS TEXT),f.flow_template FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.instance_path=e.flow_instance WHERE e.run_id=$1 AND e.entity_id=$2`, runID, route.EntityID).Scan(&path, &rawFields, &declaration); err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal([]byte(rawFields), &fields); err != nil {
				t.Fatal(err)
			}
			if fields["case_id"] != payload.CaseID || declaration != flow {
				t.Fatalf("publication borrowed instance entity: %s %s %s", r.id, declaration, rawFields)
			}
		}
		local := r.name
		if mode != "root" {
			local = strings.TrimPrefix(r.name, path+"/")
		}
		if !strings.HasPrefix(local, "result.") || strings.Contains(local, "/") || r.kind != kind || route.FlowID != flow || route.FlowInstance != path {
			t.Fatalf("publication not owned by exact source: %+v, want %s %s/%s", r, kind, flow, path)
		}
		got[payload.CaseID+"/"+local] = append(got[payload.CaseID+"/"+local], value)
		family := strings.TrimPrefix(local, "result.")
		if family != "direct" && family != "fanout" {
			producer, err := identity.ParseExecutableNode(flow, family)
			if err != nil {
				t.Fatal(err)
			}
			var selectionContext, disposition, selectedFlow, selectedFamily, semanticPath, label string
			if err := rt.DB.QueryRow(`SELECT s.selection_context,s.disposition,s.flow_path,s.declaration_family,s.semantic_path,s.display_label FROM event_delivery_handler_rule_selections s JOIN event_deliveries d ON d.delivery_id=s.delivery_id WHERE d.run_id=$1 AND d.event_id=$2 AND d.subscriber_id=$3`, runID, r.cause, producer.Key()).Scan(&selectionContext, &disposition, &selectedFlow, &selectedFamily, &semanticPath, &label); err != nil {
				t.Fatalf("publication lacks exact selected-rule cause: %v", err)
			}
			placement, context, index, wantLabel := "rules", "handler_rules", 0, "positive"
			if family == "completion" || family == "completefanout" {
				placement, context = "on_complete", "handler_on_complete"
			}
			if family == "success" {
				wantLabel = "selected"
			} else if strings.Contains(family, "fanout") {
				wantLabel = "dispatch"
			} else if value == 20 {
				index, wantLabel = 1, "otherwise"
			}
			wantPath := fmt.Sprintf("nodes[%q].handlers[%q].%s[%d]", family, family+".requested", placement, index)
			if selectionContext != context || disposition != "selected" || selectedFlow != flow || selectedFamily != "handler_rule" || semanticPath != wantPath || label != wantLabel {
				t.Fatalf("selected publication cause: %s %s %s %s %s %s; want %s %s %s", selectionContext, disposition, selectedFlow, selectedFamily, semanticPath, label, context, wantPath, wantLabel)
			}
		}
		var public operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": r.id}, &public)
		if public.EventID != r.id || public.RunID != runID || public.EventName != r.name || len(public.Deliveries) != 3 || public.Payload["case_id"] != payload.CaseID {
			t.Fatalf("public publication/delivery cardinality: %+v", public)
		}
		actual := []string{}
		for _, delivery := range public.Deliveries {
			if delivery.Status != "delivered" {
				t.Fatalf("consumer did not execute: %+v", delivery)
			}
			actual = append(actual, delivery.SubscriberID)
		}
		expected := []string{}
		for _, receiver := range [][2]string{{flow, "local"}, {flow, "wildcard"}, {"sink", "local"}} {
			node, err := identity.ParseExecutableNode(receiver[0], receiver[1])
			if err != nil {
				t.Fatal(err)
			}
			expected = append(expected, node.Key())
		}
		sort.Strings(actual)
		sort.Strings(expected)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("recipients=%v want=%v", actual, expected)
		}
	}
	for key, values := range want {
		sort.Ints(values)
		sort.Ints(got[key])
		if !reflect.DeepEqual(values, got[key]) {
			t.Fatalf("%s values=%v want=%v", key, got[key], values)
		}
		delete(got, key)
	}
	if len(got) != 0 {
		t.Fatalf("unexpected producer or sibling publications: %v", got)
	}
}
