package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
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
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestServedPublicationSitesBothStores(t *testing.T) {
	proveServedPublicationSites(t, false)
}

func TestServedPublicationTextSitesBothStores(t *testing.T) {
	proveServedPublicationSites(t, true)
}

func TestServedPublicationDirectTextSiteBothStores(t *testing.T) {
	proveServedPublicationSites(t, true, "direct")
}

func TestServedPublicationDirectRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"root", "static", "template"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				opts, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyPublicationDirectTextSite(t, mode))
				first, rt := start()
				name := "direct.requested"
				params := map[string]any{"event_name": name, "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "alpha", "choice": 1, "items": []string{}}, "idempotency_key": "publication-restart-seed"}
				published := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitPublicationSiteCompletion(t, rt.ReceiverStateReader, published.RunID)
				want := map[string][]int{"alpha/result.direct": {1}}
				requirePublicationSiteReadback(t, rt.Endpoint, rt.ReceiverStateReader, published.RunID, mode, want, true)
				if code := first.stop(); code != 0 {
					t.Fatalf("first process stop=%d", code)
				}
				setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
				second, rt := start()
				requirePublicationSiteReadback(t, rt.Endpoint, rt.ReceiverStateReader, published.RunID, mode, want, true)
				duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != published.EventID || duplicate.RunID != published.RunID {
					t.Fatalf("restart changed idempotent ingress: %+v -> %+v", published, duplicate)
				}
				waitPublicationSiteCompletion(t, rt.ReceiverStateReader, published.RunID)
				requirePublicationSiteReadback(t, rt.Endpoint, rt.ReceiverStateReader, published.RunID, mode, want, true)
				for _, caseID := range []string{"alpha", "beta"} {
					next := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": name, "run_id": published.RunID, "idempotency_key": "publication-after-restart-" + caseID, "payload": map[string]any{"case_id": caseID, "choice": 2, "items": []string{}}})
					if next.RunID != published.RunID || next.EventID == published.EventID {
						t.Fatalf("fresh publication changed run or reused original event: %+v", next)
					}
					waitPublicationSiteCompletion(t, rt.ReceiverStateReader, published.RunID)
					want[caseID+"/result.direct"] = append(want[caseID+"/result.direct"], 2)
					requirePublicationSiteReadback(t, rt.Endpoint, rt.ReceiverStateReader, published.RunID, mode, want, true)
				}
				if code := second.stop(); code != 0 {
					t.Fatalf("second process stop=%d", code)
				}
				_, rt = start()
				requirePublicationSiteReadback(t, rt.Endpoint, rt.ReceiverStateReader, published.RunID, mode, want, true)
			})
		}
	}
}

func proveServedPublicationSites(t *testing.T, textValues bool, selectedFamilies ...string) {
	t.Helper()
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, mode := range []string{"root", "static", "template"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				copySource := canonicalrouting.CopyPublicationSites
				if textValues {
					copySource = canonicalrouting.CopyPublicationTextSites
				}
				if len(selectedFamilies) != 0 {
					copySource = canonicalrouting.CopyPublicationDirectTextSite
				}
				rt := startLifecycleTemplateRuntime(t, backend, copySource(t, mode))
				runID := ""
				want := map[string][]int{}
				families := []string{"direct", "rules", "specialized", "completion", "success", "fanout", "rulefanout", "completefanout"}
				if len(selectedFamilies) != 0 {
					families = selectedFamilies
				}
				for _, caseID := range []string{"alpha", "beta"} {
					for _, family := range families {
						choices := []int{1}
						if family == "rules" || family == "specialized" || family == "completion" {
							choices = []int{1, 0}
						}
						if strings.Contains(family, "fanout") {
							choices = []int{0, 1, 2}
						}
						for _, choice := range choices {
							name := family + ".requested"
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
							waitPublicationSiteCompletion(t, rt.ReceiverStateReader, runID)
							waitPublicationSiteFanOut(t, rt.ReceiverStateReader, runID)
							key := caseID + "/result." + family
							want[key] = append(want[key], values...)
							requirePublicationSiteReadback(t, rt.Endpoint, rt.ReceiverStateReader, runID, mode, want, textValues)
							duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
							if duplicate.EventID != published.EventID || duplicate.RunID != runID {
								t.Fatalf("duplicate changed ingress: %+v -> %+v", published, duplicate)
							}
							waitPublicationSiteCompletion(t, rt.ReceiverStateReader, runID)
							requirePublicationSiteReadback(t, rt.Endpoint, rt.ReceiverStateReader, runID, mode, want, textValues)
						}
					}
				}
			})
		}
	}
}

func waitPublicationSiteCompletion(t *testing.T, reader receiverProofStateReader, runID string) {
	t.Helper()
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		var failed, incomplete int
		if err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			var err error
			failed, err = storetest.CountRunDeadLetterDeliveries(ctx, reader, runID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if failed != 0 {
			t.Fatalf("publication delivery failed: %s", servedInspectionRunDebugSummary(reader, runID))
		}
		if err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			var err error
			incomplete, err = storetest.ReadServedIncompletePipelineHandoffCount(ctx, reader, runID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if incomplete == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	stack := make([]byte, 512*1024)
	n := runtime.Stack(stack, true)
	t.Fatalf("publication pipeline did not finish: %s\n%s", servedInspectionRunDebugSummary(reader, runID), stack[:n])
}

func servedInspectionRunDebugSummary(reader receiverProofStateReader, runID string) string {
	summary, err := storetest.ReadServedRunDebugSummary(context.Background(), reader, runID)
	if err != nil {
		return fmt.Sprint(err)
	}
	return summary
}

func servedInspectionRuntimeLogDebugSummary(reader receiverProofStateReader, runID string) string {
	summary, err := storetest.ReadServedRuntimeLogDebugSummary(context.Background(), reader, runID)
	if err != nil {
		return fmt.Sprint(err)
	}
	return summary
}

func waitPublicationSiteFanOut(t *testing.T, selectedProofReceiverStateReader receiverProofStateReader, runID string) {
	t.Helper()
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		pending, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
			return storetest.CountOpenPublicationFanOutIntentsStorage(ctx, selectedProofReceiverStateReader, runID)
		})
		if err != nil {
			t.Fatal(err)
		}
		refused, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
			return storetest.CountNoncommittedPublicationFanOutOutcomesStorage(ctx, selectedProofReceiverStateReader, runID)
		})
		if err != nil {
			t.Fatal(err)
		}
		if refused != 0 {
			failure, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (string, error) {
				return storetest.ReadPublicationFanOutFailureStorage(ctx, selectedProofReceiverStateReader, runID)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Fatalf("fan-out semantic rejection: %s", failure)
		}
		if pending == 0 {
			waitPublicationSiteCompletion(t, selectedProofReceiverStateReader, runID)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fan-out did not finish: %s", servedInspectionRunDebugSummary(selectedProofReceiverStateReader, runID))
}

func requirePublicationSiteReadback(t *testing.T, selectedProofEndpoint string, selectedProofReceiverStateReader receiverProofStateReader, runID, mode string, want map[string][]int, textValues bool) {
	t.Helper()
	rows, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) ([]storetest.PublicationResultSourceStorage, error) {
		return storetest.ReadPublicationResultSourcesStorage(ctx, selectedProofReceiverStateReader, runID)
	})
	if err != nil {
		t.Fatal(err)
	}
	type result struct{ id, name, raw, kind, route, cause string }
	var results []result
	for _, row := range rows {
		results = append(results, result{row.ID, row.Name, row.Payload, row.Kind, row.Route, row.Cause})
	}
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
			identity, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (storetest.PublicationTemplateIdentityStorage, error) {
				return storetest.ReadPublicationTemplateIdentityStorage(ctx, selectedProofReceiverStateReader, runID, route.EntityID)
			})
			if err != nil {
				t.Fatal(err)
			}
			path = identity.Path
			rawFields, declaration := identity.Fields, identity.Declaration
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
		if !strings.HasPrefix(local, "result.") || strings.Contains(local, "/") || r.kind != kind || route.FlowID != flow || route.FlowInstance != path || (mode == "root" && route.EntityID != runID) {
			t.Fatalf("publication not owned by exact source: %+v, want %s %s/%s", r, kind, flow, path)
		}
		got[payload.CaseID+"/"+local] = append(got[payload.CaseID+"/"+local], value)
		family := strings.TrimPrefix(local, "result.")
		if family != "direct" && family != "fanout" {
			producer, err := identity.ParseExecutableNode(flow, family)
			if err != nil {
				t.Fatal(err)
			}
			selection, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (storetest.PublicationRuleSelectionStorage, error) {
				return storetest.ReadPublicationRuleSelectionStorage(ctx, selectedProofReceiverStateReader, runID, r.cause, producer.Key())
			})
			if err != nil {
				t.Fatalf("publication lacks exact selected-rule cause: %v", err)
			}
			selectionContext, disposition, selectedFlow, selectedFamily, semanticPath, label := selection.Context, selection.Disposition, selection.Flow, selection.Family, selection.Path, selection.Label
			placement, context, index, predicate := "rules", "handler_rules", 0, "when payload.choice > 0"
			if family == "completion" || family == "completefanout" {
				placement, context = "on_complete", "handler_on_complete"
				predicate = "condition payload.choice > 0"
			}
			if family == "success" {
				predicate = "when true"
			} else if strings.Contains(family, "fanout") {
				predicate = "default"
			} else if value == 20 {
				index, predicate = 1, "default"
			}
			// Rulings6148375122/6169683113: labels project position and
			// predicate text; only handler.rules has the final default row.
			wantLabel := fmt.Sprintf("%s[%d]: %s", placement, index, predicate)
			if placement == "on_complete" && predicate == "default" {
				wantLabel = fmt.Sprintf("%s[%d]", placement, index)
			}
			wantPath := fmt.Sprintf("nodes[%q].handlers[%q].%s[%d]", family, family+".requested", placement, index)
			if selectionContext != context || disposition != "selected" || selectedFlow != flow || selectedFamily != "handler_rule" || semanticPath != wantPath || label != wantLabel {
				t.Fatalf("selected publication cause: %s %s %s %s %s %s; want %s %s %s", selectionContext, disposition, selectedFlow, selectedFamily, semanticPath, label, context, wantPath, wantLabel)
			}
		}
		var public operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, selectedProofEndpoint, "event.get", map[string]any{"event_id": r.id}, &public)
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
