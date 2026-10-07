package apiv1

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func finiteRunStartLoadedSource(t *testing.T, finite bool) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyFiniteInitiation(t, finite)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func TestFiniteRunStartRefusesServiceBeforeMutationBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		source := finiteRunStartLoadedSource(t, false)
		bundle, _ := semanticview.Bundle(source)
		catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
		if err != nil {
			t.Fatal(err)
		}
		bus, err := newScopedAPITestEventBusWithDataCatalog(t, fixture.primary, &catalog, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		ref, err := durabledata.ParseDeclarationRef(".", "scan.requested")
		if err != nil {
			t.Fatal(err)
		}
		for _, form := range []string{"event", "event_and_data", "feed"} {
			t.Run(form, func(t *testing.T) {
				params := map[string]any{"run_id": uuid.NewString(), "bundle_hash": runStartTestBundleHashForSource(source), "idempotency_key": uuid.NewString()}
				if form != "feed" {
					params["event_name"], params["payload"] = "scan.requested", map[string]any{"topic": "business intent"}
				}
				if form != "event" {
					params["data"] = map[string]any{"imports": []any{dataRunFusedImport(uuid.NewString(), ref, durabledata.AbsentHead(), []byte("{\"topic\":\"one\"}\n"))}, "pins": []any{}}
				}
				body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": form, "method": "run.start", "params": params})
				if err != nil {
					t.Fatal(err)
				}
				before := finiteRunStartDurableCounts(t, fixture)
				for attempt := 0; attempt < 2; attempt++ {
					response := rpcCall(t, handler, string(body))
					if response.Error == nil || asMap(t, response.Error.Data)["code"] != RunNeverCompletesCode {
						t.Fatalf("service finite refusal = %#v", response)
					}
					details := asMap(t, asMap(t, response.Error.Data)["details"])
					if details["flow_id"] != "." || details["detail"] != "flow .: this flow never completes; use `serve`, or mark its end stages `final`" {
						t.Fatalf("wrong exact-flow diagnostic: %#v", details)
					}
					if after := finiteRunStartDurableCounts(t, fixture); !reflect.DeepEqual(before, after) {
						t.Fatalf("refusal created durable effects: before=%v after=%v", before, after)
					}
				}
			})
		}
	})
}

func TestFiniteRunStartPayloadAdmissionPrecedesEligibilityBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		for _, finite := range []bool{false, true} {
			source := finiteRunStartLoadedSource(t, finite)
			bundle, _ := semanticview.Bundle(source)
			catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
			if err != nil {
				t.Fatal(err)
			}
			bus, err := newScopedAPITestEventBusWithDataCatalog(t, fixture.primary, &catalog, runStartTestEventBusOptions(source))
			if err != nil {
				t.Fatal(err)
			}
			handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
			ref, err := durabledata.ParseDeclarationRef(".", "scan.requested")
			if err != nil {
				t.Fatal(err)
			}
			for _, withData := range []bool{false, true} {
				for _, malformed := range []struct {
					name    string
					present bool
					value   any
				}{
					{"missing", false, nil},
					{"null", true, nil},
					{"integer", true, 1},
					{"boolean", true, true},
					{"array", true, []string{"topic"}},
					{"object", true, map[string]any{"value": "topic"}},
				} {
					payload := map[string]any{}
					if malformed.present {
						payload["topic"] = malformed.value
					}
					params := map[string]any{
						"run_id": uuid.NewString(), "bundle_hash": runStartTestBundleHashForSource(source),
						"event_name": "scan.requested", "payload": payload, "idempotency_key": uuid.NewString(),
					}
					if withData {
						params["data"] = map[string]any{"imports": []any{dataRunFusedImport(uuid.NewString(), ref, durabledata.AbsentHead(), []byte("{\"topic\":\"valid feed\"}\n"))}, "pins": []any{}}
					}
					body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": malformed.name, "method": "run.start", "params": params})
					if err != nil {
						t.Fatal(err)
					}
					before := finiteRunStartDurableCounts(t, fixture)
					response := rpcCall(t, handler, string(body))
					if response.Error == nil || asMap(t, response.Error.Data)["code"] != PayloadValidationFailedCode {
						t.Fatalf("finite=%t data=%t %s: typed admission lost precedence: %+v", finite, withData, malformed.name, response.Error)
					}
					if after := finiteRunStartDurableCounts(t, fixture); !reflect.DeepEqual(before, after) {
						t.Fatalf("typed refusal created effects: before=%v after=%v", before, after)
					}
				}
			}
		}
	})
}

func TestFiniteRunStartDoesNotVetoOrdinaryServicePublicationBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		source := finiteRunStartLoadedSource(t, false)
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		created := rpcCall(t, handler, eventPublishBodyWithBundleHash("", runStartTestBundleHashForSource(source), "scan.requested", `{"topic":"service"}`, "", uuid.NewString()))
		if created.Error != nil {
			t.Fatalf("ordinary service publication gained a finite-start veto: %#v", created)
		}
		runID := asMap(t, created.Result)["run_id"].(string)
		published := rpcCall(t, handler, eventPublishBodyWithBundleHash(runID, runStartTestBundleHashForSource(source), "scan.requested", `{"topic":"existing run"}`, "", uuid.NewString()))
		if published.Error != nil {
			t.Fatalf("existing-run service publication gained a finite-start veto: %#v", published)
		}
		if got := dataRunCount(t, fixture, "events", "run_id", runID); got != 2 {
			t.Fatalf("ordinary service publications persisted %d events, want 2", got)
		}
	})
}

func TestFiniteRunStartReceiptReplayAndConflictBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		source := finiteRunStartLoadedSource(t, true)
		bus, err := newScopedAPITestEventBus(t, fixture.primary, runStartTestEventBusOptions(source))
		if err != nil {
			t.Fatal(err)
		}
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		runID, key := uuid.NewString(), uuid.NewString()
		body := runStartBodyWithBundleHash(runID, runStartTestBundleHashForSource(source), "scan.requested", `{"topic":"finite"}`, key)
		first := rpcCall(t, handler, body)
		if first.Error != nil {
			t.Fatalf("finite initiation: %#v", first)
		}
		replay := rpcCall(t, handler, body)
		if replay.Error != nil || !reflect.DeepEqual(first.Result, replay.Result) {
			t.Fatalf("committed response changed on replay: first=%#v replay=%#v", first, replay)
		}
		conflict := rpcCall(t, handler, runStartBodyWithBundleHash(runID, runStartTestBundleHashForSource(source), "scan.requested", `{"topic":"changed"}`, key))
		if conflict.Error == nil || asMap(t, conflict.Error.Data)["code"] != IdempotencyConflictCode {
			t.Fatalf("finite admission replaced committed request conflict: %#v", conflict)
		}
		if got := dataRunCount(t, fixture, "events", "run_id", runID); got != 1 {
			t.Fatalf("replay/conflict persisted %d events, want 1", got)
		}
	})
}
