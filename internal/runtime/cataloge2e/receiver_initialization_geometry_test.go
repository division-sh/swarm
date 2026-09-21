package cataloge2e

import (
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestReceiverInitializationNestedGeometryBothStores(t *testing.T) {
	root := canonicalrouting.CopyReceiverInitializationGeometry(t)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			var groups []catalogTranscriptGroup
			for _, item := range []struct {
				key, label string
				count      int64
			}{{"alpha", "parent-A", 0}, {"beta", "parent-B", 41}} {
				step := catalogTriggerStep{Event: "work.requested", inputKind: catalogReplayInputRootIngress,
					eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", Payload: map[string]any{
						"worker_id": item.key, "label": item.label, "count": item.count,
					}}
				groups = append(groups, catalogTranscriptGroup{steps: []catalogTriggerStep{step}})
				h.publishAndWait(step, catalogRuntimePublishTimeout)
			}
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			assertNestedReceiverInitialization(t, h)
			hash, err := runtimecontracts.BundleHash(h.bundle)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
			if err != nil {
				t.Fatal(err)
			}
			h = h.reopenFromTranscript(&catalogExecutionTranscript{
				version: catalogReplayTranscriptVersion, platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID, groups: groups,
			})
			h.publishAndWait(catalogTriggerStep{Event: "work.requested", Payload: map[string]any{
				"worker_id": "alpha", "label": "must-not-reinitialize", "count": int64(99),
			}}, catalogRuntimePublishTimeout)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			assertNestedReceiverInitialization(t, h)
			lister, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			events, err := loadCatalogOperatorEvents(h.ctx, lister)
			if err != nil {
				t.Fatal(err)
			}
			ready := 0
			for _, event := range events {
				if strings.HasSuffix(event.EventName, "/worker.ready") || strings.HasSuffix(event.EventName, "/leaf.ready") {
					ready++
				}
			}
			if ready != 4 {
				t.Fatalf("creation events=%d after restart and reuse, want exactly four", ready)
			}
		})
	}
}

func TestReceiverInitializationRestartBetweenNestedLevelsBothStores(t *testing.T) {
	root := canonicalrouting.CopyReceiverInitializationAgentGeometry(t)
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			seedCatalogRootStateForRun(t, h, catalogRuntimeRunID)
			started, release := make(chan struct{}, 1), make(chan struct{})
			h.llm.SetManagedRunBarrier(catalogRuntimeRunID, started, release)
			step := catalogTriggerStep{Event: "work.requested", inputKind: catalogReplayInputRootIngress,
				eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e",
				Payload: map[string]any{"worker_id": "alpha", "label": "parent-A", "count": int64(0)}}
			published := make(chan error, 1)
			go func() { published <- h.publishRuntimeEventResultForStep(step, catalogRuntimePublishTimeout, true) }()
			select {
			case <-started:
			case <-time.After(catalogRuntimePublishTimeout):
				t.Fatal("parent agent never reached provider boundary")
			}
			instances, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			parents := 0
			for _, instance := range instances {
				if instance.WorkflowName == "worker/leaf" {
					t.Fatal("grandchild was created before the managed provider ran")
				}
				if instance.WorkflowName == "worker" {
					parents++
					if instance.Config["worker_id"] != "alpha" || instance.Config["label"] != "parent-A" || instance.Config["count"] != int64(0) {
						t.Fatalf("parent config at interrupted boundary: %#v", instance.Config)
					}
				}
			}
			if parents != 1 {
				t.Fatalf("parents at interrupted boundary=%d, want 1", parents)
			}
			h.llm.mu.Lock()
			calls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
			h.llm.mu.Unlock()
			if len(calls) != 1 {
				t.Fatalf("provider calls=%+v, want one interrupted parent", calls)
			}
			lister, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			events, err := loadCatalogOperatorEvents(h.ctx, lister)
			if err != nil {
				t.Fatal(err)
			}
			ready, ok := events[calls[0].EventID]
			if !ok || !strings.HasSuffix(ready.EventName, "/worker.ready") {
				t.Fatalf("interrupted agent did not consume parent creation: %+v", ready)
			}
			hash, err := runtimecontracts.BundleHash(h.bundle)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
			if err != nil {
				t.Fatal(err)
			}
			transcript := &catalogExecutionTranscript{version: catalogReplayTranscriptVersion,
				platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID,
				groups: []catalogTranscriptGroup{{steps: []catalogTriggerStep{step}}},
				agentFixtures: agentFixtureDoc{AgentFixtures: map[string][]agentFixtureStep{
					calls[0].AgentID: {{On: ready.EventName, Emits: []agentFixtureEmit{{Event: "leaf.requested",
						Payload: map[string]any{"worker_id": "alpha-leaf", "label": "leaf-parent-A", "count": int64(1)}}}}},
				}},
			}
			h.cancel()
			select {
			case <-published:
			case <-time.After(catalogRuntimePublishTimeout):
				t.Fatal("interrupted publisher did not exit")
			}
			h = h.reopenFromTranscript(transcript)
			h.waitForCatalogStoreQuiescence(catalogRuntimePublishTimeout)
			instances, err = h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
			if err != nil {
				t.Fatal(err)
			}
			parents, leaves := 0, 0
			for _, instance := range instances {
				switch instance.WorkflowName {
				case "worker":
					parents++
					if instance.Config["worker_id"] != "alpha" || instance.Config["label"] != "parent-A" || instance.Config["count"] != int64(0) {
						t.Fatalf("parent config changed after restart: %#v", instance.Config)
					}
				case "worker/leaf":
					leaves++
					if instance.Config["worker_id"] != "alpha-leaf" || instance.Config["label"] != "leaf-parent-A" || instance.Config["count"] != int64(1) || instance.Fields["label"] != "leaf-parent-A" || instance.Fields["count"] != int64(1) {
						t.Fatalf("grandchild did not consume exact config: config=%#v fields=%#v", instance.Config, instance.Fields)
					}
				}
			}
			if parents != 1 || leaves != 1 {
				t.Fatalf("after restart parents=%d leaves=%d, want one each", parents, leaves)
			}
			lister, err = h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			events, err = loadCatalogOperatorEvents(h.ctx, lister)
			if err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{"/worker.ready": 0, "/leaf.requested": 0, "/leaf.ready": 0}
			for _, event := range events {
				for suffix := range counts {
					if strings.HasSuffix(event.EventName, suffix) {
						counts[suffix]++
					}
				}
			}
			for event, count := range counts {
				if count != 1 {
					t.Fatalf("%s events=%d, want exactly one across restart", event, count)
				}
			}
		})
	}
}

func assertNestedReceiverInitialization(t *testing.T, h *runtimeHarness) {
	t.Helper()
	instances, err := h.workflow.ListWorkflowInstances(h.ctx, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, instance := range instances {
		key, ok := instance.Config["worker_id"].(string)
		if !ok {
			continue // Root flow has no receiver configuration.
		}
		if seen[key] {
			t.Fatalf("duplicate receiver %s", key)
		}
		seen[key] = true
		want := map[string]struct {
			label, flow string
			count       int64
		}{
			"alpha": {"parent-A", "worker", 0}, "beta": {"parent-B", "worker", 41},
			"alpha-leaf": {"leaf-parent-A", "worker/leaf", 1}, "beta-leaf": {"leaf-parent-B", "worker/leaf", 42},
		}
		w, ok := want[key]
		if !ok || instance.WorkflowName != w.flow || instance.Config["label"] != w.label || instance.Config["count"] != w.count {
			t.Fatalf("receiver %s: flow=%s config=%#v want=%+v", key, instance.WorkflowName, instance.Config, w)
		}
		if w.flow == "worker/leaf" && (instance.Fields["label"] != w.label || instance.Fields["count"] != w.count) {
			t.Fatalf("leaf final consumer did not observe initialized config: fields=%#v want=%+v", instance.Fields, w)
		}
	}
	if len(seen) != 4 {
		t.Fatalf("receivers=%v, want two parents and their two children", seen)
	}
}
