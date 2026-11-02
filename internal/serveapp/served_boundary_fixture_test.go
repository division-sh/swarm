package serveapp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type servedBoundaryFixture struct {
	Endpoint, Backend, BundleHash string
	Selected                      *selectedStoreOwner
	Runtime                       *runtimepkg.Runtime
	Probe                         *lifecycletest.Probe
	ReceiverStateReader           receiverProofStateReader
}

func seedServedRunCreatingEvent(t *testing.T, rt *runtimepkg.Runtime, eventName string, payload json.RawMessage, key string) servedEventPublishRPCResult {
	t.Helper()
	source := rt.Pipeline.SemanticSource()
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("creation fixture requires its compiled rooted bundle")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	runID, eventID := uuid.NewString(), uuid.NewString()
	ctx := correlation.WithRootAdmission(correlation.WithRunID(servedRuntimeProofAuthorActivityContext(t, rt, rooted.Admission().BundleHash()), runID), rooted.Admission())
	endpoint, err := bus.NewOrdinaryFlowAPIEventPublicationEndpoint(source, semanticview.RootExecutionFlowID(source), eventName)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngressWithMode(eventID, events.EventType(eventName), "api.v1", "", payload, 0, runID, "", events.EventEnvelope{}, time.Now().UTC(), rt.ExecutionPosture.RootMode())
	initial, err := json.Marshal(map[string]any{"event_name": event.Type(), "payload": payload, "emitter": "api.v1", "entity_id": "", "flow_instance": "", "source_event_id": ""})
	if err != nil {
		t.Fatal(err)
	}
	request := apiidempotency.Request{Method: "event.publish", Actor: apiidempotency.BearerActor("served-fixture"), IdempotencyKey: key, RequestHash: "served-fixture-" + eventID}
	completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
	creation := durabledata.RunCreationCommand{RunID: runID, Actor: "served-fixture", BundleHash: rooted.Admission().BundleHash(), EventID: eventID, InitialEvent: initial}
	result, replay, err := rt.Bus.PublishAPIEventWithRunCreationAcknowledged(ctx, event, &endpoint, request, completion, &creation)
	if err != nil || replay || result.ResourceID != eventID {
		t.Fatalf("native first-turn fixture: result=%+v replay=%t err=%v", result, replay, err)
	}
	return servedEventPublishRPCResult{RunID: runID, EventID: eventID, NewRunCreated: true}
}

func seedServedPublicationRun(t *testing.T, rt *runtimepkg.Runtime) string {
	t.Helper()
	source := rt.Pipeline.SemanticSource()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("publication setup requires its admitted source artifact")
	}
	owner, ok := rt.Bus.Store().(storetest.RunFixtureStore)
	if !ok {
		t.Fatalf("publication setup lacks native run owner: %T", rt.Bus.Store())
	}
	runID := uuid.NewString()
	ctx := effects.WithExecutionMode(correlation.WithRunID(servedRuntimeProofAuthorActivityContext(t, rt, rt.Options.SourceArtifactFact.BundleHash()), runID), rt.ExecutionPosture.RootMode())
	storetest.RequireRun(t, ctx, owner, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, Artifact: bundle.SourceArtifact})
	plan, err := rt.Manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, OccurredAt: time.Now().UTC(),
		Instance: flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, ""),
	})
	if err != nil {
		t.Fatalf("prepare publication setup: %v", err)
	}
	committed, err := rt.Bus.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("commit publication setup: acknowledged=%t created=%t err=%v", committed.Acknowledged, committed.Created, err)
	}
	if err := rt.Manager.FinalizeCommittedFlowInstanceActivation(ctx, committed); err != nil {
		t.Fatalf("finalize publication setup: %v", err)
	}
	return runID
}

func pauseServedLandingForCreation(t *testing.T, endpoint, bundleHash, flowPath string) string {
	t.Helper()
	runID := requireServedLiveLandingRunID(t, endpoint, bundleHash, flowPath)
	requireServedOKJSONRPC(t, endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "creation-fixture-pause-" + runID})
	requireServedRunStatus(t, endpoint, runID, "paused")
	return runID
}

func requireServedLiveLandingRunID(t *testing.T, endpoint, bundleHash, flowPath string) string {
	t.Helper()
	var result struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"bundle_hash": bundleHash, "limit": 500}, &result)
	var runID string
	for _, run := range result.Runs {
		if run.BundleHash != bundleHash || run.FlowPath != flowPath || run.Status != "live" || run.Origin.Kind() != runlifecycle.OriginDeployment {
			continue
		}
		if runID != "" {
			t.Fatalf("flow %s has multiple live deployment landings: %+v", flowPath, result.Runs)
		}
		runID = run.RunID
	}
	if runID == "" {
		t.Fatalf("flow %s lacks its live deployment landing: %+v", flowPath, result.Runs)
	}
	return runID
}

func requireServedBootConstructedRun(t *testing.T, endpoint, bundleHash string, rt *runtimepkg.Runtime) string {
	t.Helper()
	runID := requireServedLiveLandingRunID(t, endpoint, bundleHash, ".")
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(".", "", runID))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := storetest.ReadReceiverConstructionPublication(t.Context(), rt.Bus.Store(), owner, runID)
	if err != nil || receipt.Identity.InstancePath != runID || receipt.Identity.EntityID != runID || receipt.BundleHash != bundleHash || receipt.CreatingInput != (pipeline.FlowConstructionInput{}) {
		t.Fatalf("boot root lost its exact source, identity or empty construction origin: receipt=%+v err=%v", receipt, err)
	}
	return runID
}

func startServedBoundaryFixture(t *testing.T, backend, sourceRoot string) (*serveRuntimeTestProcess, servedBoundaryFixture) {
	t.Helper()
	return newServedBoundaryFixtureLifetime(t, backend, sourceRoot)()
}

func newServedBoundaryFixtureLifetime(t *testing.T, backend, sourceRoot string) func() (*serveRuntimeTestProcess, servedBoundaryFixture) {
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
	opts := cliapp.ServeOptions{
		SourceRoot: sourceRoot, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfig(t, backend, location)
	} else {
		opts.ConfigPath = writeServeRuntimeTestConfig(t)
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	var priorProcess *serveRuntimeTestProcess
	return func() (*serveRuntimeTestProcess, servedBoundaryFixture) {
		if priorProcess != nil {
			priorProcess.mu.Lock()
			stopped := priorProcess.stopped
			priorProcess.mu.Unlock()
			if !stopped {
				t.Fatal("served boundary must join its previous process before reopening")
			}
		}
		probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
		start := opts
		start.TestLifecycleProbe = probe
		// An existing receiver observer remains the exact process publication owner.
		if _, found := ownedMockLifecycleProbes.Load(t); found {
			start.TestLifecycleProbe = nil
		}
		selected = nil
		process := startServeRuntimeTestProcess(t, start)
		priorProcess = process
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
}

func createServedBoundaryWaitingRun(t *testing.T, rt servedBoundaryFixture, suffix string) (string, string) {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name": "item.received", "bundle_hash": rt.BundleHash,
		"payload": map[string]any{"item_id": suffix}, "idempotency_key": "issue-1864-" + rt.Backend + "-" + suffix,
	})
	if result.NewRunCreated || result.RunID == "" || result.EventID == "" {
		t.Fatalf("%s control seed = %+v, want the existing live run", rt.Backend, result)
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
	requireServedRunStatus(t, rt.Endpoint, result.RunID, "live")
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
