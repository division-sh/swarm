package apiv1

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReviewer2566FeedOnlyRejectsNestedServiceBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		root := t.TempDir()
		files := map[string]string{
			"schema.yaml":            "stages: {ready: {}, done: {final: true}}\npins:\n  inputs: [start.requested]\nconnect:\n  - {event: work.requested, from: producer, to: worker, resolution: create}\n  - {event: work.safe, from: producer, to: safe, resolution: create}\n",
			"events.yaml":            "start.requested:\n",
			"nodes.yaml":             "starter:\n  execution_type: system_node\n  event_handlers:\n    start.requested: {advances_to: done}\n",
			"producer/schema.yaml":   "instance: producer_id\nstages: {done: {final: true}}\npins:\n  outputs: [work.requested, work.safe]\n",
			"producer/entities.yaml": "Producer:\n  producer_id: {type: text, _unused_reason: dormant identity}\n",
			"producer/events.yaml":   "work.requested:\n  worker_id: text\nwork.safe:\n  worker_id: text\n",
			"worker/schema.yaml":     "instance: worker_id\nstages: {active: {}}\npins:\n  inputs: [work.requested]\n",
			"worker/entities.yaml":   "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
			"worker/nodes.yaml":      "worker:\n  execution_type: system_node\n  event_handlers:\n    work.requested: {}\n",
			"safe/schema.yaml":       "instance: worker_id\nstages: {done: {final: true}}\npins:\n  inputs: [work.safe]\n",
			"safe/entities.yaml":     "Worker:\n  worker_id: {type: text, _unused_reason: constructor identity}\n",
			"safe/nodes.yaml":        "worker:\n  execution_type: system_node\n  event_handlers:\n    work.safe: {}\n",
		}
		for name, body := range files {
			path := filepath.Join(root, name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
		}
		repo := canonicalrouting.RepoRoot(t)
		bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
		if err != nil {
			t.Fatal(err)
		}
		source := semanticview.Wrap(bundle)
		catalog, err := contracts.BuildDurableDataCatalog(bundle)
		if err != nil {
			t.Fatal(err)
		}
		bus, err := newScopedAPITestEventBusWithDataCatalog(t, fixture.primary, &catalog, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		ref, err := durabledata.ParseDeclarationRef("producer", "producer/work.requested")
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		bundleHash := runStartTestBundleHashForSource(source)
		imported, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: bundleHash,
			Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"worker_id\":\"one\"}\n"),
		})
		if err != nil || imported.Outcome != "accepted" {
			t.Fatalf("prepare pinned version: %+v, %v", imported, err)
		}
		safeRef, err := durabledata.ParseDeclarationRef("producer", "producer/work.safe")
		if err != nil {
			t.Fatal(err)
		}
		safeVersion, err := fixture.primary.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
			Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: bundleHash,
			Declaration: safeRef, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"worker_id\":\"safe\"}\n"),
		})
		if err != nil || safeVersion.Outcome != "accepted" {
			t.Fatalf("prepare second pin: %+v, %v", safeVersion, err)
		}
		for _, mode := range []string{"feed_only", "event_and_data"} {
			for _, binding := range []string{"import", "pin", "multiple_pins"} {
				t.Run(mode+"/"+binding, func(t *testing.T) {
					data := map[string]any{"imports": []any{}, "pins": []any{}}
					if binding == "import" {
						data["imports"] = []any{dataRunFusedImport(uuid.NewString(), ref, durabledata.VersionHead(imported.Candidate.VersionID), []byte("{\"worker_id\":\"new\"}\n"))}
					} else {
						pins := []any{map[string]any{"declaration": dataRunDeclaration(ref), "version_id": imported.Candidate.VersionID}}
						if binding == "multiple_pins" {
							// A valid first selection must not hide a service reached by the second.
							pins = append([]any{map[string]any{"declaration": dataRunDeclaration(safeRef), "version_id": safeVersion.Candidate.VersionID}}, pins...)
						}
						data["pins"] = pins
					}
					params := map[string]any{"run_id": uuid.NewString(), "bundle_hash": bundleHash, "idempotency_key": uuid.NewString(), "data": data}
					if mode == "event_and_data" {
						params["event_name"], params["payload"] = "start.requested", map[string]any{}
					}
					body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": mode, "method": "run.start", "params": params})
					if err != nil {
						t.Fatal(err)
					}
					before := finiteRunStartDurableCounts(t, fixture)
					for attempt := 0; attempt < 2; attempt++ {
						response := rpcCall(t, handler, string(body))
						if response.Error == nil || asMap(t, response.Error.Data)["code"] != RunNeverCompletesCode {
							t.Fatalf("selected service feed was not refused: %#v", response)
						}
						if details := asMap(t, asMap(t, response.Error.Data)["details"]); details["flow_id"] != "worker" {
							t.Fatalf("wrong selected receiver identity: %+v", details)
						}
						if after := finiteRunStartDurableCounts(t, fixture); !reflect.DeepEqual(before, after) {
							t.Fatalf("refusal mutated durable state: before=%v after=%v", before, after)
						}
					}
				})
			}
		}
		// The same dormant worker is not a veto when none of its feeds is selected.
		runID, key := uuid.NewString(), uuid.NewString()
		body := runStartBodyWithBundleHash(runID, bundleHash, "start.requested", `{}`, key)
		first := rpcCall(t, handler, body)
		if first.Error != nil {
			t.Fatalf("unselected nested service blocked event-only start: %#v", first)
		}
		if replay := rpcCall(t, handler, body); replay.Error != nil || !reflect.DeepEqual(first.Result, replay.Result) {
			t.Fatalf("selected closure changed receipt replay: first=%#v replay=%#v", first, replay)
		}
		if dataRunCount(t, fixture, "events", "run_id", runID) != 1 || dataRunCount(t, fixture, "fan_out_intents", "run_id", runID) != 0 {
			t.Fatal("event-only initiation invented a feed or duplicated the initial event")
		}
	})
}

func finiteRunStartDurableCounts(t *testing.T, fixture dataRunLifecycleFixture) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"runs", "events", "flow_instances", "entity_state", "resource_versions", "resource_heads", "resource_source_invocations", "resource_version_pins", "fan_out_intents", "resource_run_creation_operations", "resource_run_creation_child_evaluations", "resource_run_creation_child_reservations", "api_idempotency"} {
		var count int
		if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	return counts
}
