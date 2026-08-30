package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimepreservationcleanup "github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type failOncePostCommitBundleDeleteCapability struct {
	runtimestartupownership.ProcessCapability
	finalMutationCommitted atomic.Bool
	refreshAttempts        atomic.Int32
}

type blockingBundleDeleteReplayCapability struct {
	runtimestartupownership.ProcessCapability
	entered chan struct{}
	release chan struct{}
	blocked atomic.Bool
}

func (c *blockingBundleDeleteReplayCapability) ReplayBundleDeleteResult(
	ctx context.Context,
	req runtimebundledelete.FinalMutationRequest,
) (runtimebundledelete.Result, error) {
	if c.blocked.CompareAndSwap(false, true) {
		close(c.entered)
		select {
		case <-c.release:
		case <-ctx.Done():
			return runtimebundledelete.Result{}, ctx.Err()
		}
	}
	return c.ProcessCapability.ReplayBundleDeleteResult(ctx, req)
}

func (c *failOncePostCommitBundleDeleteCapability) ApplyBundleDeleteFinalMutation(
	ctx context.Context,
	req runtimebundledelete.FinalMutationRequest,
	topology *runtimeagenttopology.SourceSetCommitRequest,
) (runtimebundledelete.FinalMutationResult, error) {
	result, err := c.ProcessCapability.ApplyBundleDeleteFinalMutation(ctx, req, topology)
	if err == nil && topology != nil {
		c.finalMutationCommitted.Store(true)
	}
	return result, err
}

func (c *failOncePostCommitBundleDeleteCapability) IssueGenerationGrant(
	ctx context.Context,
	req runtimestartupownership.GrantRequest,
) (runtimestartupownership.GenerationGrant, error) {
	if c.finalMutationCommitted.Load() && c.refreshAttempts.Add(1) == 2 {
		return nil, errors.New("injected post-commit survivor refresh failure")
	}
	return c.ProcessCapability.IssueGenerationGrant(ctx, req)
}

func TestPostgresBundleDeleteSameProcessReplayRestoresProcessPublication(t *testing.T) {
	ctx := context.Background()
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	db := storetest.DatabaseForTest(selected)
	t.Cleanup(func() { _ = db.Close() })
	storetest.BootstrapPostgresRuntimeStore(t, selected)
	cfg := &config.Config{}
	stores := openSelectedPostgresOwner(t, dsn, db, cfg)

	firstRoot := writeServeRuntimeAgentSlugFixture(t, "delete-replay-a", "alpha-worker")
	secondRoot := writeServeRuntimeAgentSlugFixture(t, "delete-replay-b", "beta-worker")
	thirdRoot := writeServeRuntimeAgentSlugFixture(t, "delete-replay-c", "gamma-worker")
	firstHash := seedServeRuntimeBundleCatalogRoot(t, ctx, selected, firstRoot)
	secondHash := seedServeRuntimeBundleCatalogRoot(t, ctx, selected, secondRoot)
	thirdHash := seedServeRuntimeBundleCatalogRoot(t, ctx, selected, thirdRoot)
	processWorkOwner := worklifetime.NewProcess()
	var capability runtimestartupownership.ProcessCapability
	var contexts *runtimepkg.RuntimeContextManager
	var runtimes []*runtimepkg.Runtime
	t.Cleanup(func() {
		shutdownFailed := false
		if contexts != nil {
			for _, result := range contexts.DeactivateAll(runtimepkg.RuntimeContextCauseUnloaded) {
				if result.ShutdownErr != nil {
					t.Errorf("shutdown runtime context %s: %v", result.BundleHash, result.ShutdownErr)
					shutdownFailed = true
				}
			}
		}
		for i := len(runtimes) - 1; i >= 0; i-- {
			if err := runtimes[i].ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second}); err != nil {
				t.Errorf("shutdown bundle-delete replay runtime: %v", err)
				shutdownFailed = true
			}
		}
		if shutdownFailed {
			return
		}
		if err := closeSelectedStoreTestProcess(processWorkOwner, capability); err != nil {
			t.Errorf("close bundle-delete replay generation: %v", err)
		}
	})
	runtimeInstanceID := uuid.NewString()
	providerCatalog := testProviderTriggerCatalog(t)

	type runtimeFixture struct {
		runtime *runtimepkg.Runtime
		source  semanticview.Source
	}
	newRuntime := func(root, bundleHash string) runtimeFixture {
		bundle := loadWorkflowValidationBundleAt(t, root)
		source := semanticview.Wrap(bundle)
		rt, err := runtimepkg.NewRuntime(ctx, runtimeDepsForServeTest(t, stores, cfg, runtimepkg.RuntimeOptions{
			SelfCheck: false, WorkflowModule: stubWorkflowModule{source: source},
			LLMRuntime: servedNoopLLMRuntime{}, DisablePersistentStartupRecovery: true,
			ProviderTriggerCatalog: providerCatalog, ProcessWorkOwner: processWorkOwner,
			BundleSourceFact:  mustServeTestPersistedBundleSourceFact(bundleHash),
			RuntimeInstanceID: runtimeInstanceID,
		}))
		if err != nil {
			t.Fatalf("NewRuntime(%s): %v", bundleHash, err)
		}
		runtimes = append(runtimes, rt)
		return runtimeFixture{runtime: rt, source: source}
	}
	first := newRuntime(firstRoot, firstHash)
	second := newRuntime(secondRoot, secondHash)
	third := newRuntime(thirdRoot, thirdHash)
	coordinates := []runtimeagenttopology.SourceCoordinate{
		{BundleHash: firstHash, BundleSource: "persisted"},
		{BundleHash: secondHash, BundleSource: "persisted"},
		{BundleHash: thirdHash, BundleSource: "persisted"},
	}
	var desired []runtimeagenttopology.DesiredAgent
	for i, fixture := range []runtimeFixture{first, second, third} {
		compiled, err := fixture.runtime.Manager.CompileStaticTopologyDesiredAgents(fixture.source, coordinates[i])
		if err != nil {
			t.Fatalf("compile desired topology %d: %v", i, err)
		}
		desired = append(desired, compiled...)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan(coordinates, desired)
	if err != nil {
		t.Fatalf("NewSourceSetPlan: %v", err)
	}
	capability, err = stores.StartupOwnership().AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "bundle-delete-replay-test", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		t.Fatalf("AcquireProcessCapability: %v", err)
	}
	if err := installServeSourceSet(ctx, capability, plan); err != nil {
		t.Fatalf("install source set: %v", err)
	}
	installSelectedStoreTestGeneration(t, capability, first.runtime, plan, 1)
	installSelectedStoreTestGeneration(t, capability, second.runtime, plan, 1)
	installSelectedStoreTestGeneration(t, capability, third.runtime, plan, 1)
	if err := first.runtime.Start(ctx); err != nil {
		t.Fatalf("start first runtime: %v", err)
	}
	if err := second.runtime.Start(ctx); err != nil {
		t.Fatalf("start second runtime: %v", err)
	}
	if err := third.runtime.Start(ctx); err != nil {
		t.Fatalf("start third runtime: %v", err)
	}
	contexts, err = runtimepkg.NewRuntimeContextManager(nil,
		completeServeTestPackContext(t, runtimepkg.BundleContext{BundleSourceFact: first.runtime.Options.BundleSourceFact, Source: first.source, Runtime: first.runtime, WorkOwner: first.runtime.WorkOccurrence()}),
		completeServeTestPackContext(t, runtimepkg.BundleContext{BundleSourceFact: second.runtime.Options.BundleSourceFact, Source: second.source, Runtime: second.runtime, WorkOwner: second.runtime.WorkOccurrence()}),
		completeServeTestPackContext(t, runtimepkg.BundleContext{BundleSourceFact: third.runtime.Options.BundleSourceFact, Source: third.source, Runtime: third.runtime, WorkOwner: third.runtime.WorkOccurrence()}),
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	failingCapability := &failOncePostCommitBundleDeleteCapability{ProcessCapability: capability}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := newProcessLifecycleSupervisor(projectServeRuntimePersistence(stores), &ready, first.runtime)
	supervisor.SetRuntimeContextManager(contexts, first.runtime.Options.BundleSourceFact)
	supervisor.SetProcessCapability(failingCapability)
	supervisor.shutdownOptions = runtimepkg.DefaultShutdownOptions()
	coordinator := &runtimebundledelete.Coordinator{
		Planner:   selected,
		Finalizer: processOwnedBundleDeleteFinalizer{capability: failingCapability, supervisor: supervisor},
		Locks:     selected,
		RuntimeQuiescer: bundleDeleteRuntimeQuiescer{
			contexts: contexts, supervisor: supervisor,
		},
	}
	req := runtimebundledelete.Request{
		OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "bundle-delete-replay",
		ReplayKeyHash: strings.Repeat("a", 64),
		BundleHash:    firstHash, RequestedAt: time.Now().UTC(),
	}
	deletePlan, err := selected.PlanBundleDelete(ctx, req)
	if err != nil {
		t.Fatalf("plan active fixture runs before replay proof: %v", err)
	}
	stopCtx := runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(runtimeInstanceID))
	for _, run := range deletePlan.ActiveRuns {
		if _, err := selected.StopRunControl(stopCtx, runtimeruncontrol.TransitionRequest{
			RunID: run.RunID, Reason: "bundle delete replay proof", ControlledBy: "test", Now: req.RequestedAt,
		}); err != nil && !errors.Is(err, runtimeruncontrol.ErrAlreadyTerminal) {
			t.Fatalf("stop active fixture run %s: %v", run.RunID, err)
		}
	}
	deletePlan, err = selected.PlanBundleDelete(ctx, req)
	if err != nil {
		t.Fatalf("replan fixture runs before replay proof: %v", err)
	}
	if len(deletePlan.ActiveRuns) != 0 {
		t.Fatalf("active fixture runs before replay proof = %#v, want none", deletePlan.ActiveRuns)
	}
	if _, err := coordinator.Execute(ctx, req); err == nil || !strings.Contains(err.Error(), "injected post-commit survivor refresh failure") {
		t.Fatalf("first delete error = %v, want injected post-commit failure", err)
	}
	for _, bundleHash := range []string{secondHash, thirdHash} {
		if lookup := contexts.LookupBundleHashStatus(bundleHash); lookup.State != runtimepkg.RuntimeContextStateUnloaded || lookup.Cause != runtimepkg.RuntimeContextCauseSourceSetTransition {
			t.Fatalf("survivor %s after post-commit failure = %#v, want pending source-set transition", bundleHash, lookup)
		}
	}
	var bundleRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bundles WHERE bundle_hash = $1`, firstHash).Scan(&bundleRows); err != nil {
		t.Fatalf("count deleted bundle after post-commit failure: %v", err)
	}
	if bundleRows != 0 {
		t.Fatalf("deleted bundle rows after post-commit failure = %d, want 0", bundleRows)
	}
	var replayRecorded bool
	if err := db.QueryRowContext(ctx, `
		SELECT result IS NOT NULL
		FROM bundle_delete_final_mutation_replays
		WHERE operation_id = $1::uuid
	`, req.OperationID).Scan(&replayRecorded); err != nil {
		t.Fatalf("read final mutation replay record: %v", err)
	}
	if !replayRecorded {
		t.Fatal("committed source-set operation omitted final mutation replay record")
	}
	if supervisor.CurrentRuntime() != nil {
		t.Fatal("post-commit survivor failure retained a process runtime before topology converged")
	}
	if ready.Load() {
		t.Fatal("post-commit survivor failure retained process readiness")
	}
	type survivorProgress struct {
		bundleHash string
		agentID    string
		grants     int
		rebinds    int
	}
	survivorsBeforeReplay := []survivorProgress{
		{bundleHash: secondHash, agentID: "beta-worker"},
		{bundleHash: thirdHash, agentID: "gamma-worker"},
	}
	progressedBeforeFailure := 0
	for i := range survivorsBeforeReplay {
		survivor := &survivorsBeforeReplay[i]
		survivor.grants = countBundleDeleteReplayRows(t, db, `SELECT COUNT(DISTINCT grant_id) FROM runtime_generation_grants WHERE bundle_hash = $1`, survivor.bundleHash)
		survivor.rebinds = countBundleDeleteReplayRows(t, db, `SELECT COUNT(*) FROM agent_lifecycle_operations WHERE operation_kind = 'source_set_rebind' AND agent_id = $1`, survivor.agentID)
		switch {
		case survivor.grants == 2 && survivor.rebinds == 1:
			progressedBeforeFailure++
		case survivor.grants == 1 && survivor.rebinds == 0:
		default:
			t.Fatalf("survivor progress before replay = %#v, want either committed or untouched", *survivor)
		}
	}
	if progressedBeforeFailure != 1 {
		t.Fatalf("survivors durably progressed before direct replay = %d, want 1", progressedBeforeFailure)
	}

	replayRequest := req
	replayRequest.OperationID = uuid.NewString()
	replayRequest.RequestedAt = req.RequestedAt.Add(time.Minute)
	replayed, err := coordinator.Execute(ctx, replayRequest)
	if err != nil {
		t.Fatalf("replay committed delete: %v", err)
	}
	if !replayed.OK || !replayed.Deleted {
		t.Fatalf("replayed delete result = %#v, want completed deletion", replayed)
	}
	if replayed.Plan.BundleHash != firstHash || !replayed.Plan.PlannedAt.Equal(req.RequestedAt) {
		t.Fatalf("replayed delete plan = %#v, want original plan identity at %s", replayed.Plan, req.RequestedAt)
	}
	for _, bundleHash := range []string{secondHash, thirdHash} {
		if lookup := contexts.LookupBundleHashStatus(bundleHash); !lookup.Loaded() {
			t.Fatalf("survivor %s after replay = %#v, want loaded", bundleHash, lookup)
		}
	}
	currentRuntime := supervisor.CurrentRuntime()
	if currentRuntime == nil || currentRuntime == first.runtime || !ready.Load() {
		t.Fatalf("direct replay process publication = runtime:%p ready:%v, want surviving runtime ready", currentRuntime, ready.Load())
	}
	publicRuntime, publicContexts := supervisor.PublicIngressState()
	if publicRuntime != currentRuntime || publicContexts != contexts {
		t.Fatalf("direct replay public ingress state = runtime:%p contexts:%p, want %p/%p", publicRuntime, publicContexts, currentRuntime, contexts)
	}
	use, err := supervisor.acquireCurrentRuntime(ctx)
	if err != nil {
		t.Fatalf("direct replay agent-control acquisition: %v", err)
	}
	if use.Runtime() != currentRuntime || use.Runtime().Manager == nil || len(use.Runtime().Manager.ListAgentConfigs()) == 0 {
		t.Fatalf("direct replay agent-control runtime = %#v, want executable surviving agent runtime", use.Runtime())
	}
	if err := use.Done(); err != nil {
		t.Fatalf("release direct replay agent-control acquisition: %v", err)
	}
	for i := range survivorsBeforeReplay {
		survivor := &survivorsBeforeReplay[i]
		afterGrants := countBundleDeleteReplayRows(t, db, `SELECT COUNT(DISTINCT grant_id) FROM runtime_generation_grants WHERE bundle_hash = $1`, survivor.bundleHash)
		afterRebinds := countBundleDeleteReplayRows(t, db, `SELECT COUNT(*) FROM agent_lifecycle_operations WHERE operation_kind = 'source_set_rebind' AND agent_id = $1`, survivor.agentID)
		wantGrants, wantRebinds := survivor.grants, survivor.rebinds
		if survivor.rebinds == 0 {
			wantGrants++
			wantRebinds++
		}
		if afterGrants != wantGrants || afterRebinds != wantRebinds {
			t.Fatalf("survivor replay work for %s = grants %d->%d rebinds %d->%d, want %d/%d", survivor.bundleHash, survivor.grants, afterGrants, survivor.rebinds, afterRebinds, wantGrants, wantRebinds)
		}
		survivor.grants = afterGrants
		survivor.rebinds = afterRebinds
	}
	changedRequest := req
	changedRequest.OperationID = uuid.NewString()
	changedRequest.RequestHash = "changed-bundle-delete-request"
	changedRequest.RequestedAt = req.RequestedAt.Add(2 * time.Minute)
	if _, err := coordinator.Execute(ctx, changedRequest); err == nil || !strings.Contains(err.Error(), "stored request hash") {
		t.Fatalf("changed-request replay error = %v, want stored request hash conflict", err)
	}
	if supervisor.CurrentRuntime() != currentRuntime || !ready.Load() {
		t.Fatalf("changed-request replay altered process publication = runtime:%p ready:%v", supervisor.CurrentRuntime(), ready.Load())
	}

	duplicateRequest := replayRequest
	duplicateRequest.OperationID = uuid.NewString()
	duplicateRequest.RequestedAt = req.RequestedAt.Add(3 * time.Minute)
	supervisor.mu.Lock()
	supervisor.currentRT = nil
	ready.Store(false)
	supervisor.mu.Unlock()
	duplicate, err := coordinator.Execute(ctx, duplicateRequest)
	if err != nil {
		t.Fatalf("duplicate committed delete: %v", err)
	}
	if !reflect.DeepEqual(duplicate, replayed) {
		t.Fatalf("duplicate coordinator result = %#v, want stored %#v", duplicate, replayed)
	}
	if supervisor.CurrentRuntime() != currentRuntime || !ready.Load() {
		t.Fatalf("duplicate replay did not repair process publication = runtime:%p ready:%v", supervisor.CurrentRuntime(), ready.Load())
	}
	for _, survivor := range survivorsBeforeReplay {
		afterGrants := countBundleDeleteReplayRows(t, db, `SELECT COUNT(DISTINCT grant_id) FROM runtime_generation_grants WHERE bundle_hash = $1`, survivor.bundleHash)
		afterRebinds := countBundleDeleteReplayRows(t, db, `SELECT COUNT(*) FROM agent_lifecycle_operations WHERE operation_kind = 'source_set_rebind' AND agent_id = $1`, survivor.agentID)
		if afterGrants != survivor.grants || afterRebinds != survivor.rebinds {
			t.Fatalf("ordinary duplicate mutated survivor %s: grants %d->%d rebinds %d->%d", survivor.bundleHash, survivor.grants, afterGrants, survivor.rebinds, afterRebinds)
		}
	}
	unkeyedRequest := duplicateRequest
	unkeyedRequest.OperationID = uuid.NewString()
	unkeyedRequest.ReplayKeyHash = ""
	unkeyedRequest.RequestedAt = req.RequestedAt.Add(4 * time.Minute)
	if _, err := coordinator.Execute(ctx, unkeyedRequest); !errors.Is(err, runtimebundledelete.ErrBundleNotFound) {
		t.Fatalf("unkeyed replay error = %v, want ErrBundleNotFound", err)
	}
	expiredRequest := duplicateRequest
	expiredRequest.OperationID = uuid.NewString()
	expiredRequest.RequestedAt = req.RequestedAt.Add(runtimebundledelete.FinalMutationReplayWindow)
	if _, err := coordinator.Execute(ctx, expiredRequest); !errors.Is(err, runtimebundledelete.ErrBundleNotFound) {
		t.Fatalf("expired replay error = %v, want ErrBundleNotFound", err)
	}

	blockingCapability := &blockingBundleDeleteReplayCapability{
		ProcessCapability: failingCapability,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	supervisor.SetProcessCapability(blockingCapability)
	serializedCoordinator := &runtimebundledelete.Coordinator{
		Planner: selected,
		Finalizer: processOwnedBundleDeleteFinalizer{
			capability: blockingCapability,
			supervisor: supervisor,
		},
		Locks: selected,
	}
	serializedRequest := duplicateRequest
	serializedRequest.OperationID = uuid.NewString()
	serializedRequest.RequestedAt = req.RequestedAt.Add(5 * time.Minute)
	replayDone := make(chan error, 1)
	go func() {
		_, replayErr := serializedCoordinator.Execute(ctx, serializedRequest)
		replayDone <- replayErr
	}()
	select {
	case <-blockingCapability.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("serialized replay did not reach the process operation boundary")
	}
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- supervisor.ShutdownProcessWithOptions(ctx, runtimepkg.DefaultShutdownOptions())
	}()
	select {
	case err := <-shutdownDone:
		t.Fatalf("process shutdown bypassed an active bundle-delete replay: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(blockingCapability.release)
	if err := <-replayDone; err != nil {
		t.Fatalf("serialized duplicate replay: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown after serialized duplicate replay: %v", err)
	}
	if supervisor.CurrentRuntime() != nil || ready.Load() {
		t.Fatalf("serialized shutdown publication = runtime:%p ready:%v, want detached", supervisor.CurrentRuntime(), ready.Load())
	}
	for _, survivor := range survivorsBeforeReplay {
		afterGrants := countBundleDeleteReplayRows(t, db, `SELECT COUNT(DISTINCT grant_id) FROM runtime_generation_grants WHERE bundle_hash = $1`, survivor.bundleHash)
		afterRebinds := countBundleDeleteReplayRows(t, db, `SELECT COUNT(*) FROM agent_lifecycle_operations WHERE operation_kind = 'source_set_rebind' AND agent_id = $1`, survivor.agentID)
		if afterGrants != survivor.grants || afterRebinds != survivor.rebinds {
			t.Fatalf("serialized replay mutated survivor %s: grants %d->%d rebinds %d->%d", survivor.bundleHash, survivor.grants, afterGrants, survivor.rebinds, afterRebinds)
		}
	}
}

func TestPostgresUnloadedBundleDeleteFinalMutationReplaysWithoutTopologyOperation(t *testing.T) {
	ctx := context.Background()
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	db := storetest.DatabaseForTest(selected)
	t.Cleanup(func() { _ = db.Close() })
	storetest.BootstrapPostgresRuntimeStore(t, selected)

	root := writeServeRuntimeAgentSlugFixture(t, "unloaded-delete-replay", "unloaded-worker")
	bundleHash := seedServeRuntimeBundleCatalogRoot(t, ctx, selected, root)
	unkeyedRoot := writeServeRuntimeAgentSlugFixture(t, "unloaded-delete-unkeyed", "unkeyed-worker")
	unkeyedBundleHash := seedServeRuntimeBundleCatalogRoot(t, ctx, selected, unkeyedRoot)
	runtimeInstanceID := uuid.NewString()
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "unloaded-bundle-delete-replay", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		t.Fatalf("AcquireProcessCapability: %v", err)
	}
	t.Cleanup(func() { _ = capability.Release(context.Background()) })
	emptyPlan, err := runtimeagenttopology.NewSourceSetPlan(nil, nil)
	if err != nil {
		t.Fatalf("construct empty source set: %v", err)
	}
	if err := installServeSourceSet(ctx, capability, emptyPlan); err != nil {
		t.Fatalf("install empty source set: %v", err)
	}

	requestedAt := time.Now().UTC()
	request := runtimebundledelete.FinalMutationRequest{
		OperationID: uuid.NewString(), OperationName: runtimebundledelete.DefaultOperationName,
		RequestHash: "unloaded-bundle-delete-request", ReplayKeyHash: strings.Repeat("b", 64),
		BundleHash: bundleHash, RequestedAt: requestedAt,
		Completion: pendingBundleDeleteCompletion(bundleHash, requestedAt),
	}
	request.Completion.Force = true
	request.Completion.ActiveRunsStopped = 1
	request.Completion.DeliveriesCancelled = 1
	request.Completion.ContainersStopped = 1
	request.Completion.Cleanup = runtimepreservationcleanup.Result{
		OperationName: runtimebundledelete.DefaultOperationName,
		AppliedAt:     requestedAt,
		ControlledBy:  runtimepreservationcleanup.BundleForceDeleteControlledBy,
	}
	request.Completion.Containers = runtimedestructivereset.ContainerResetResult{
		OperationName: runtimebundledelete.DefaultOperationName,
		AppliedAt:     requestedAt,
		Stopped:       []runtimedestructivereset.ContainerRef{{Name: "agent-container", Kind: "agent"}},
	}
	committed, err := capability.ApplyBundleDeleteFinalMutation(ctx, request, nil)
	if err != nil {
		t.Fatalf("commit unloaded bundle deletion: %v", err)
	}
	if !committed.Deleted || committed.BundleRowsDeleted != 1 {
		t.Fatalf("committed unloaded deletion = %#v", committed)
	}
	var topologyOperations int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_topology_source_set_operations WHERE operation_id=$1::uuid`, request.OperationID).Scan(&topologyOperations); err != nil {
		t.Fatalf("count unrelated topology operations: %v", err)
	}
	if topologyOperations != 0 {
		t.Fatalf("unloaded deletion topology operations=%d, want 0", topologyOperations)
	}

	replay := request
	replay.OperationID = uuid.NewString()
	replay.RequestedAt = requestedAt.Add(time.Minute)
	replayed, err := capability.ApplyBundleDeleteFinalMutation(ctx, replay, nil)
	if err != nil {
		t.Fatalf("replay unloaded bundle deletion: %v", err)
	}
	if !reflect.DeepEqual(replayed, committed) {
		t.Fatalf("replayed unloaded deletion = %#v, want %#v", replayed, committed)
	}
	var ready atomic.Bool
	supervisor := newProcessLifecycleSupervisor(serveRuntimePersistence{}, &ready, nil)
	supervisor.SetProcessCapability(capability)
	replayedCompletion, err := (processOwnedBundleDeleteFinalizer{
		capability: capability,
		supervisor: supervisor,
	}).ReplayBundleDeleteFinalMutation(ctx, replay)
	if err != nil {
		t.Fatalf("replay complete unloaded deletion result: %v", err)
	}
	if supervisor.CurrentRuntime() != nil || ready.Load() {
		t.Fatalf("empty source-set replay publication = runtime:%p ready:%v, want detached", supervisor.CurrentRuntime(), ready.Load())
	}
	wantCompletion, err := runtimebundledelete.CompleteFinalMutation(request, committed)
	if err != nil {
		t.Fatalf("complete expected unloaded deletion result: %v", err)
	}
	if !reflect.DeepEqual(replayedCompletion, wantCompletion) {
		t.Fatalf("replayed coordinator result = %#v, want %#v", replayedCompletion, wantCompletion)
	}
	var replayRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bundle_delete_final_mutation_replays WHERE replay_key_hash=$1`, request.ReplayKeyHash).Scan(&replayRows); err != nil {
		t.Fatalf("count unloaded deletion replay facts: %v", err)
	}
	if replayRows != 1 {
		t.Fatalf("unloaded deletion replay facts=%d, want 1", replayRows)
	}

	unkeyedRequest := runtimebundledelete.FinalMutationRequest{
		OperationID: uuid.NewString(), OperationName: runtimebundledelete.DefaultOperationName,
		RequestHash: "unloaded-bundle-delete-unkeyed-request",
		BundleHash:  unkeyedBundleHash, RequestedAt: requestedAt.Add(2 * time.Minute),
		Completion: pendingBundleDeleteCompletion(unkeyedBundleHash, requestedAt.Add(2*time.Minute)),
	}
	unkeyed, err := capability.ApplyBundleDeleteFinalMutation(ctx, unkeyedRequest, nil)
	if err != nil {
		t.Fatalf("commit unkeyed unloaded bundle deletion: %v", err)
	}
	if !unkeyed.Deleted || unkeyed.BundleRowsDeleted != 1 {
		t.Fatalf("committed unkeyed unloaded deletion = %#v", unkeyed)
	}
	var unkeyedRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bundle_delete_final_mutation_replays WHERE operation_id=$1::uuid AND replay_key_hash=''`, unkeyedRequest.OperationID).Scan(&unkeyedRows); err != nil {
		t.Fatalf("count unkeyed unloaded deletion replay fact: %v", err)
	}
	if unkeyedRows != 1 {
		t.Fatalf("unkeyed unloaded deletion replay facts=%d, want 1 exact operation fact", unkeyedRows)
	}
}

func pendingBundleDeleteCompletion(bundleHash string, requestedAt time.Time) runtimebundledelete.Result {
	return runtimebundledelete.Result{
		OK: true, Status: "completed", OperationName: runtimebundledelete.DefaultOperationName,
		BundleHash: bundleHash,
		Plan:       runtimebundledelete.Plan{BundleHash: bundleHash, PlannedAt: requestedAt.UTC()},
	}
}

func countBundleDeleteReplayRows(t testing.TB, db *sql.DB, query, arg string) int {
	t.Helper()
	var count int
	var err error
	if arg == "" {
		err = db.QueryRowContext(context.Background(), query).Scan(&count)
	} else {
		err = db.QueryRowContext(context.Background(), query, arg).Scan(&count)
	}
	if err != nil {
		t.Fatalf("count bundle delete replay rows: %v", err)
	}
	return count
}
