package serveapp

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestEdgeOwnedConnectionPoliciesSharingInputBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, reverse := range []bool{false, true} {
			t.Run(backend+map[bool]string{false: "/forward", true: "/reverse"}[reverse], func(t *testing.T) {
				root := canonicalrouting.CopyConnectionPolicies(t, reverse)
				repo := canonicalrouting.RepoRoot(t)
				bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
				if err != nil {
					t.Fatal(err)
				}
				plans := pinrouting.CompileConnectGraph(semanticview.Wrap(bundle)).Plans()
				if len(plans) != 2 {
					t.Fatalf("compile edge identity evidence: %d plans", len(plans))
				}
				_, start := lifecycleRestartHarness(t, backend, root)
				process, rt := start()
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "work.requested", "bundle_hash": rt.BundleHash, "idempotency_key": "edge-create",
					"payload": map[string]any{"creation_id": "fresh-one", "reuse_id": "shared"},
				})
				waitPublicationSiteCompletion(t, rt, seed.RunID)
				requireConnectionPolicyRoutes(t, rt, plans, seed.RunID, []string{"fresh-one", "shared"}, 1)
				if code := process.stop(); code != 0 {
					t.Fatalf("first serve shutdown failed: %d\n%s", code, process.outputString())
				}
				_, rt = start()
				reuse := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": "work.requested", "bundle_hash": rt.BundleHash, "run_id": seed.RunID, "idempotency_key": "edge-reuse",
					"payload": map[string]any{"creation_id": "fresh-two", "reuse_id": "shared"},
				})
				if reuse.RunID != seed.RunID {
					t.Fatal("reuse changed the run")
				}
				waitPublicationSiteCompletion(t, rt, seed.RunID)
				requireConnectionPolicyRoutes(t, rt, plans, seed.RunID, []string{"fresh-one", "fresh-two", "shared"}, 2)
			})
		}
	}
}

func TestEdgeOwnedMixedConnectionProjectionsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, intrinsic := range []string{"generated.uuid", "event.id"} {
			for _, reverse := range []bool{false, true} {
				t.Run(backend+"/"+intrinsic+map[bool]string{false: "/forward", true: "/reverse"}[reverse], func(t *testing.T) {
					root := canonicalrouting.CopyMixedConnectionProjections(t, reverse, intrinsic)
					repo := canonicalrouting.RepoRoot(t)
					bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
					if err != nil {
						t.Fatal(err)
					}
					plans := pinrouting.CompileConnectGraph(semanticview.Wrap(bundle)).Plans()
					if len(plans) != 2 {
						t.Fatalf("plans=%d", len(plans))
					}
					_, start := lifecycleRestartHarness(t, backend, root)
					process, rt := start()
					seed := publishMixedConnectionRequest(t, rt, "", "mixed-create")
					waitPublicationSiteCompletion(t, rt, seed)
					before := requireMixedConnectionReadback(t, rt, backend, plans, seed, intrinsic, 1)
					if code := process.stop(); code != 0 {
						t.Fatalf("shutdown=%d\n%s", code, process.outputString())
					}
					process, rt = start()
					if got := requireMixedConnectionReadback(t, rt, backend, plans, seed, intrinsic, 1); !reflect.DeepEqual(got, before) {
						t.Fatalf("restart reminted routes: %v -> %v", before, got)
					}
					publishMixedConnectionRequest(t, rt, seed, "mixed-reuse")
					waitPublicationSiteCompletion(t, rt, seed)
					paths := requireMixedConnectionReadback(t, rt, backend, plans, seed, intrinsic, 2)
					if len(paths) != 3 {
						t.Fatalf("create/reuse paths=%v, want three", paths)
					}
					if code := process.stop(); code != 0 {
						t.Fatalf("final shutdown=%d\n%s", code, process.outputString())
					}
				})
			}
		}
	}
}

func publishMixedConnectionRequest(t *testing.T, rt servedControlProofRuntime, runID, idempotency string) string {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "work.requested", "bundle_hash": rt.BundleHash, "run_id": runID, "idempotency_key": idempotency,
		"payload": map[string]any{"creation_id": "00000000-0000-4000-8000-000000000001", "reuse_id": "00000000-0000-4000-8000-000000000002"},
	})
	if runID != "" && result.RunID != runID {
		t.Fatal("reuse changed run")
	}
	return result.RunID
}

func requireMixedConnectionReadback(t *testing.T, rt servedControlProofRuntime, backend string, plans []pinrouting.ConnectRoutePlan, runID, intrinsic string, publications int) []string {
	t.Helper()
	rows, err := rt.DB.Query("SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.ready' ORDER BY event_id", runID)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != publications {
		t.Fatalf("publications=%d, want%d", len(ids), publications)
	}
	paths := map[string]bool{}
	for _, id := range ids {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": id}, &event)
		if _, ok := event.Payload["worker_id"]; ok {
			t.Fatal("synthetic key leaked into producer payload")
		}
		if len(event.Deliveries) != 2 {
			t.Fatalf("deliveries=%+v", event.Deliveries)
		}
		projected := 0
		for _, delivery := range event.Deliveries {
			if delivery.Status != "delivered" || delivery.Target.FlowID != "worker" {
				t.Fatalf("exact consumer unsettled: %+v", delivery)
			}
			query := "SELECT delivery_payload_projection FROM event_deliveries WHERE delivery_id=$1"
			if backend == "postgres" {
				query = "SELECT delivery_payload_projection::text FROM event_deliveries WHERE delivery_id=$1"
			}
			var raw string
			if err := rt.DB.QueryRow(query, delivery.DeliveryID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var projection events.DeliveryPayloadProjection
			if err := json.Unmarshal([]byte(raw), &projection); err != nil {
				t.Fatal(err)
			}
			key := event.Payload["reuse_id"].(string)
			if !projection.Empty() {
				projected++
				fields := projection.Fields()
				key = fields["worker_id"]
				if len(fields) != 1 {
					t.Fatalf("unexpected projection=%v", fields)
				}
				if _, err := uuid.Parse(key); err != nil {
					t.Fatalf("invalid intrinsic key %q", key)
				}
				if intrinsic == "event.id" && key != event.EventID {
					t.Fatalf("event.id changed: %s != %s", key, event.EventID)
				}
			}
			digest := plans[0].ReceiverKeyDigest([]contracts.TemplateInstanceKeyValue{{Field: plans[0].InstanceKey().Field(), Value: key}})
			want := "worker/ti-" + digest[:24]
			if delivery.Target.FlowInstance != want {
				t.Fatalf("delivery target=%s, want%s", delivery.Target.FlowInstance, want)
			}
			paths[want] = true
		}
		if projected != 1 {
			t.Fatalf("projected deliveries=%d, want1", projected)
		}
	}
	var out []string
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func requireConnectionPolicyRoutes(t *testing.T, rt servedControlProofRuntime, plans []pinrouting.ConnectRoutePlan, runID string, want []string, publications int) {
	t.Helper()
	paths := make([]string, 0, len(want))
	for _, key := range want {
		digest := plans[0].ReceiverKeyDigest([]contracts.TemplateInstanceKeyValue{{Field: plans[0].InstanceKey().Field(), Value: key}})
		if len(digest) < 24 {
			t.Fatalf("invalid key digest %q", digest)
		}
		paths = append(paths, "worker/ti-"+digest[:24])
	}
	sort.Strings(paths)
	rows, err := rt.DB.Query("SELECT instance_path FROM flow_instances WHERE run_id=$1 AND flow_template='worker' ORDER BY instance_path", runID)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			t.Fatal(err)
		}
		got = append(got, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, paths) {
		t.Fatalf("connection routes=%v, want %v for keys %v", got, paths, want)
	}
	rows, err = rt.DB.Query("SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.ready'", runID)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != publications {
		t.Fatalf("work.ready publications=%d, want %d", len(ids), publications)
	}
	for _, id := range ids {
		var event operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": id}, &event)
		if len(event.Deliveries) != 2 {
			t.Fatalf("public delivery readback=%+v, want two edge deliveries", event.Deliveries)
		}
		var wantTargets []string
		for _, plan := range plans {
			field := strings.TrimPrefix(plan.InstanceKey().Readback().SourcePath, "payload.")
			key, ok := event.Payload[field].(string)
			if !ok {
				t.Fatalf("public payload lacks source field %s: %#v", field, event.Payload)
			}
			digest := plan.ReceiverKeyDigest([]contracts.TemplateInstanceKeyValue{{Field: plan.InstanceKey().Field(), Value: key}})
			wantTargets = append(wantTargets, "worker/ti-"+digest[:24])
		}
		var gotTargets []string
		for _, delivery := range event.Deliveries {
			if delivery.Status != "delivered" {
				t.Fatalf("unsettled edge delivery: %+v", delivery)
			}
			if delivery.Target.FlowID != "worker" {
				t.Fatalf("public readback changed receiver ownership: %+v", delivery.Target)
			}
			gotTargets = append(gotTargets, delivery.Target.FlowInstance)
		}
		sort.Strings(wantTargets)
		sort.Strings(gotTargets)
		if !reflect.DeepEqual(gotTargets, wantTargets) {
			t.Fatalf("public edge targets=%v, want %v", gotTargets, wantTargets)
		}
	}
}
