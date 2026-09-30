package serveapp

import (
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
		for _, delivery := range event.Deliveries {
			if delivery.Status != "delivered" {
				t.Fatalf("unsettled edge delivery: %+v", delivery)
			}
		}
	}
}
