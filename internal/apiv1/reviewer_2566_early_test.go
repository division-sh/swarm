package apiv1

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
	"reflect"
	"testing"
)

func TestReviewer2566FeedOnlyRejectsNestedServiceBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		root := canonicalrouting.CopyFiniteAPIServiceFeed(t)
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

func finiteRunStartDurableCounts(t *testing.T, fixture dataRunLifecycleFixture) storetest.FiniteRunStartStorageCounts {
	t.Helper()
	counts, err := storetest.ReadFiniteRunStartStorageCounts(context.Background(), fixture.primary)
	if err != nil {
		t.Fatal(err)
	}
	return counts
}
