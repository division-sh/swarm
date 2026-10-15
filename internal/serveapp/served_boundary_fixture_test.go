package serveapp

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type servedBoundaryFixture struct {
	Endpoint, Backend, BundleHash string
	Selected                      *selectedStoreOwner
	Runtime                       *runtimepkg.Runtime
	Probe                         *lifecycletest.Probe
	ReceiverStateReader           receiverProofStateReader
}

func startServedBoundaryFixture(t *testing.T, backend, sourceRoot string) (*serveRuntimeTestProcess, servedBoundaryFixture) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	var location string
	switch backend {
	case "sqlite":
		location = filepath.Join(t.TempDir(), ".swarm", "boundary.sqlite")
	case "postgres":
		location = testutil.StartEmptyPostgresDSN(t)
	default:
		t.Fatalf("unsupported boundary fixture backend %q", backend)
	}
	var selected *selectedStoreOwner
	previous := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		var err error
		if backend == "postgres" {
			selected, err = storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{
				Selection: selection, PostgresDSN: location, SessionLockTTL: runtimeSessionLockTTL(cfg),
			})
		} else {
			selected, err = previous(ctx, selection, cfg)
		}
		return selected, err
	}
	t.Cleanup(func() { buildStoresForServe = previous })
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	opts := cliapp.ServeOptions{
		SourceRoot: sourceRoot, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
		TestLifecycleProbe: probe, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	// The receiver control already owns its exact publication observer. Let the
	// existing process fixture bind it instead of replacing it with this probe.
	if _, found := ownedMockLifecycleProbes.Load(t); found {
		opts.TestLifecycleProbe = nil
	}
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfig(t, backend, location)
	} else {
		opts.ConfigPath = writeServeRuntimeTestConfig(t)
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	process := startServeRuntimeTestProcess(t, opts)
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("served boundary shutdown returned %d\n%s", code, process.outputString())
		}
	})
	process.waitForReadyLine()
	if selected == nil {
		t.Fatal("served boundary lacks its original selected owner")
	}
	return process, servedBoundaryFixture{
		Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc",
		Backend:  backend, BundleHash: servedEventPublishFixtureBundleHash(t, sourceRoot),
		Selected: selected, Runtime: servedTestProcessRuntime(t, process), Probe: probe,
		ReceiverStateReader: requireReceiverProofStateReader(t, selected.RuntimeDeps().EventStore),
	}
}

func createServedBoundaryWaitingRun(t *testing.T, rt servedBoundaryFixture, suffix string) (string, string) {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "item.received", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{"item_id": suffix}, "idempotency_key": "issue-1864-" + rt.Backend + "-" + suffix,
	})
	if !result.NewRunCreated || result.RunID == "" || result.EventID == "" {
		t.Fatalf("%s control seed = %+v, want new run", rt.Backend, result)
	}
	node := identitytest.RootNode(t, "item-handler").Key()
	rt.Probe.RequireNodePending(result.EventID, node)
	deadline := time.Now().Add(servedEventPublishLifecycleProbeWaitTimeout)
	rt.Probe.Expect(result.EventID).NodeInProgress(node).HandlerStarted(node).HandlerCompleted(node).NodeDelivered(node).Within(servedEventPublishLifecycleProbeWaitTimeout)
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatal("node delivery consumed the complete lifecycle proof deadline")
	}
	rt.Probe.Expect(result.EventID).PostCommitDispatchStarted().PostCommitDispatchCompleted().Within(remaining)
	count, err := storetest.ReadServedDeliveryStatusCount(t.Context(), rt.Selected.RuntimeDeps().EventStore, result.EventID, "node", node)
	if err != nil || count != 1 {
		t.Fatalf("exact seed node delivery count=%d err=%v", count, err)
	}
	deadline = time.Now().Add(15 * time.Second)
	for {
		entities, err := rt.Selected.Entities().ListOperatorEntities(t.Context(), operatorread.OperatorEntityListOptions{RunID: result.RunID, CurrentState: "waiting"})
		if err != nil {
			t.Fatal(err)
		}
		if len(entities.Entities) != 0 && entities.Entities[0].EntityID != "" && entities.Entities[0].RunID == result.RunID {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("exact waiting entity is missing: %+v", entities)
		}
		time.Sleep(50 * time.Millisecond)
	}
	requireServedRunStatus(t, rt.Endpoint, result.RunID, "running")
	return result.RunID, result.EventID
}

func waitServedBoundaryDeliveryCount(t *testing.T, rt servedBoundaryFixture, runID, eventID, nodeID, status string, want int) {
	t.Helper()
	event, found, err := storetest.ReadCanonicalEventRecord(t.Context(), rt.Selected.RuntimeDeps().EventStore, eventID)
	if err != nil || !found || event.RunID() != runID {
		t.Fatalf("delivery count requires its exact run/event: event=%+v found=%t err=%v", event, found, err)
	}
	var got int
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		got, err = storetest.ReadServedDeliveryStatusCount(t.Context(), rt.Selected.RuntimeDeps().EventStore, eventID, "node", nodeID, status)
		if err != nil {
			t.Fatal(err)
		}
		if got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s delivery count for event=%s node=%s status=%s = %d, want %d", rt.Backend, eventID, nodeID, status, got, want)
}

func waitServedBoundaryPublicationCompletion(t *testing.T, rt servedBoundaryFixture, runID string) {
	t.Helper()
	owner := rt.Selected.RuntimeDeps().EventStore
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		summary, err := rt.Selected.RuntimeDeps().DeliveryStore.SummarizeRun(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if summary.RunID != runID || summary.DeadLetter != 0 {
			t.Fatalf("publication delivery failed or changed run: %+v", summary)
		}
		incomplete, err := storetest.ReadServedIncompletePipelineHandoffCount(t.Context(), owner, runID)
		if err != nil {
			t.Fatal(err)
		}
		if incomplete == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	debug, err := storetest.ReadServedRunDebugSummary(t.Context(), owner, runID)
	stack := make([]byte, 512*1024)
	n := runtime.Stack(stack, true)
	t.Fatalf("publication pipeline did not finish: %s (read error=%v)\n%s", debug, err, stack[:n])
}
