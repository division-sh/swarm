package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type notifyAllChildrenStore interface {
	runtimebus.EventStore
	runtimedelivery.Store
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecyclePersistence
	runtimemanager.AgentLifecycleStateReader
	ListActiveFlowInstanceDescriptors(context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error)
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	ListFlowInstanceRouteRecords(context.Context, runtimeflowidentity.Route) ([]runtimebus.FlowInstanceRouteRecord, error)
	PipelineObligations() runtimepipelineobligation.Store
}

type lifecycleTransitionRecordingStore interface {
	RecordedAuthorizedLifecycleTransition(string, string) (runtimemanager.AgentLifecycleTransition, bool)
}

type lifecycleTransitionRecorder struct {
	mu       sync.Mutex
	requests []runtimemanager.AgentLifecycleTransition
}

func (r *lifecycleTransitionRecorder) record(req runtimemanager.AgentLifecycleTransition) {
	r.mu.Lock()
	r.requests = append(r.requests, req)
	r.mu.Unlock()
}

func (r *lifecycleTransitionRecorder) latestAuthorized(agentID, operationKind string) (runtimemanager.AgentLifecycleTransition, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.requests) - 1; i >= 0; i-- {
		req := r.requests[i]
		if req.AgentID == agentID && req.OperationKind == operationKind && req.DynamicTopology != nil {
			return req, true
		}
	}
	return runtimemanager.AgentLifecycleTransition{}, false
}

type failingNotifyAllChildrenPostgresStore struct {
	*store.PostgresStore
	lifecycle                 lifecycleTransitionRecorder
	failExactRouteReplacement atomic.Bool
	failNextRouteReplacement  atomic.Bool
	transientRouteFailures    atomic.Int32
}

func (s *failingNotifyAllChildrenPostgresStore) CommitAgentLifecycleTransition(
	ctx context.Context,
	req runtimemanager.AgentLifecycleTransition,
) (runtimemanager.AgentLifecycleTransitionResult, error) {
	s.lifecycle.record(req)
	return s.PostgresStore.CommitAgentLifecycleTransition(ctx, req)
}

func (s *failingNotifyAllChildrenPostgresStore) RecordedAuthorizedLifecycleTransition(
	agentID string,
	operationKind string,
) (runtimemanager.AgentLifecycleTransition, bool) {
	return s.lifecycle.latestAuthorized(agentID, operationKind)
}

func (s *failingNotifyAllChildrenPostgresStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.Route,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s.failNextRouteReplacement.Swap(false) {
		s.transientRouteFailures.Add(1)
		return fmt.Errorf("injected transient postgres exact route replacement failure")
	}
	if s.failExactRouteReplacement.Load() {
		return fmt.Errorf("injected postgres exact route replacement failure")
	}
	return s.PostgresStore.ReplaceFlowInstanceRouteRecords(ctx, identity, routes)
}

type failingNotifyAllChildrenSQLiteStore struct {
	*store.SQLiteRuntimeStore
	lifecycle                 lifecycleTransitionRecorder
	failExactRouteReplacement atomic.Bool
	failNextRouteReplacement  atomic.Bool
	transientRouteFailures    atomic.Int32
}

func (s *failingNotifyAllChildrenSQLiteStore) CommitAgentLifecycleTransition(
	ctx context.Context,
	req runtimemanager.AgentLifecycleTransition,
) (runtimemanager.AgentLifecycleTransitionResult, error) {
	s.lifecycle.record(req)
	return s.SQLiteRuntimeStore.CommitAgentLifecycleTransition(ctx, req)
}

func (s *failingNotifyAllChildrenSQLiteStore) RecordedAuthorizedLifecycleTransition(
	agentID string,
	operationKind string,
) (runtimemanager.AgentLifecycleTransition, bool) {
	return s.lifecycle.latestAuthorized(agentID, operationKind)
}

func (s *failingNotifyAllChildrenSQLiteStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.Route,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s.failNextRouteReplacement.Swap(false) {
		s.transientRouteFailures.Add(1)
		return fmt.Errorf("injected transient sqlite exact route replacement failure")
	}
	if s.failExactRouteReplacement.Load() {
		return fmt.Errorf("injected sqlite exact route replacement failure")
	}
	return s.SQLiteRuntimeStore.ReplaceFlowInstanceRouteRecords(ctx, identity, routes)
}

type notifyAllChildrenRuntime struct {
	bus         *runtimebus.EventBus
	diagnostics *fanInBarrierDiagnosticBus
	manager     *runtimemanager.AgentManager
	pipeline    *runtimepipeline.PipelineCoordinator
	workflow    *runtimepipeline.WorkflowInstanceStore
	workOwner   *worklifetime.RuntimeOccurrence
}

func TestDynamicFlowSourceRevisionConvergesExactAgentSetAndFencesPredecessorsOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB, func(), func() int32)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func(), func() int32) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := &failingNotifyAllChildrenPostgresStore{
					PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db),
				}
				return selected, db,
					func() { selected.failNextRouteReplacement.Store(true) },
					selected.transientRouteFailures.Load
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func(), func() int32) {
				base := storetest.StartSQLiteRuntimeStore(t)
				selected := &failingNotifyAllChildrenSQLiteStore{SQLiteRuntimeStore: base}
				return selected, base.DB,
					func() { selected.failNextRouteReplacement.Store(true) },
					selected.transientRouteFailures.Load
			},
		},
	} {
		for _, mode := range []struct {
			name     string
			autoEmit bool
		}{
			{name: "no_auto_emit"},
			{name: "emitted_creation", autoEmit: true},
		} {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				selected, db, failNextRouteReplacement, transientRouteFailures := tc.setup(t)
				proveDynamicFlowSourceRevisionConvergence(
					t,
					selected,
					db,
					failNextRouteReplacement,
					transientRouteFailures,
					mode.autoEmit,
				)
			})
		}
	}
}

func proveDynamicFlowSourceRevisionConvergence(
	t *testing.T,
	selected notifyAllChildrenStore,
	db *sql.DB,
	failNextRouteReplacement func(),
	transientRouteFailures func() int32,
	autoEmit bool,
) {
	t.Helper()
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sourceV1 := notifyallchildren.LoadSource(t, notifyallchildren.Options{
		AgentTopologyRevision: 1,
		AutoEmitOnCreate:      autoEmit,
	})
	scopeV1, ok := semanticview.FlowScopeByID(sourceV1, notifyallchildren.ChildFlowID)
	if !ok || len(scopeV1.Agents) != 2 {
		t.Fatalf("v1 account agent contract = %#v found=%t, want reader/retired", scopeV1.Agents, ok)
	}
	runtimeV1 := newNotifyAllChildrenRuntime(t, selected, db, sourceV1, time.Now)
	if err := runtimeV1.manager.Run(managedConformanceExecutionContext(t, ctx, "dynamic-flow-source-v1")); err != nil {
		t.Fatalf("run v1 manager: %v", err)
	}

	publishNotifyAllChildrenEvent(t, ctx, runtimeV1.bus, sourceV1, runID, "portfolio.opened", map[string]any{
		"portfolio_id": "portfolio-main",
	})
	publishNotifyAllChildrenEvent(t, ctx, runtimeV1.bus, sourceV1, runID, "portfolio.account.register.requested", map[string]any{
		"portfolio_id": "portfolio-main",
		"account_id":   "acct-revision",
	})
	descriptor, ok := notifyAllChildrenAccountDescriptors(t, ctx, selected)["acct-revision"]
	if !ok {
		t.Fatal("created account descriptor is missing")
	}
	if _, err := runtimeV1.manager.HydrateForStartup(ctx); err != nil {
		t.Fatalf("finalize v1 readiness through startup owner: %v", err)
	}
	initialReadiness := waitNotifyAllChildrenRuntimeReadiness(t, ctx, runtimeV1.workflow, runID, descriptor.FlowInstance)
	if got := !initialReadiness.CreationEventEmittedAt.IsZero(); got != autoEmit {
		t.Fatalf("initial creation completion = %t, want %t", got, autoEmit)
	}
	instanceID := descriptor.InstanceID
	readerID := "account-reader-" + instanceID
	retiredID := "account-retired-" + instanceID
	writerID := "account-writer-" + instanceID
	v1Agents := loadNotifyAllChildrenAgentsByID(t, ctx, selected)
	if v1Agents[readerID].Config.Role != "reader-v1" || v1Agents[retiredID].Config.Role != "retired" {
		t.Fatalf("v1 dynamic agents = %#v", v1Agents)
	}
	readerGenerationV1 := v1Agents[readerID].LifecycleGeneration

	sourceV2 := notifyallchildren.LoadSource(t, notifyallchildren.Options{
		AgentTopologyRevision: 2,
		AutoEmitOnCreate:      autoEmit,
	})
	scopeV2, ok := semanticview.FlowScopeByID(sourceV2, notifyallchildren.ChildFlowID)
	if !ok || len(scopeV2.Agents) != 2 {
		t.Fatalf("v2 account agent contract = %#v found=%t, want reader/writer", scopeV2.Agents, ok)
	}
	if _, found := scopeV2.Agents["retired"]; found {
		t.Fatalf("v2 account contract retained removed agent: %#v", scopeV2.Agents)
	}
	runtimeV2 := newNotifyAllChildrenRuntime(t, selected, db, sourceV2, time.Now)
	if err := runtimeV2.manager.Run(managedConformanceExecutionContext(t, ctx, "dynamic-flow-source-v2")); err != nil {
		t.Fatalf("run v2 manager: %v", err)
	}
	reconcileCtx := worklifetime.WithOccurrence(ctx, runtimeV2.workOwner)
	failNextRouteReplacement()
	sourceRevisionErr := make(chan error, 1)
	genericMutationErrs := make(chan error, 4)
	start := make(chan struct{})
	var mutations sync.WaitGroup
	mutations.Add(5)
	go func() {
		defer mutations.Done()
		<-start
		sourceRevisionErr <- runtimeV2.workflow.RunPipelineMutation(reconcileCtx, func(txctx context.Context) error {
			return runtimeV2.manager.ReconcileDynamicFlowRuntimeReadinessPlansForRun(txctx, time.Now().UTC())
		})
	}()
	go func() {
		defer mutations.Done()
		<-start
		rogue := v1Agents[readerID].Config
		rogue.ID = "account-rogue-" + instanceID
		rogue.Role = "rogue"
		genericMutationErrs <- runtimeV2.manager.SpawnAgent(rogue)
	}()
	go func() {
		defer mutations.Done()
		<-start
		generic := v1Agents[readerID].Config
		generic.Role = "generic-reconfigure"
		genericMutationErrs <- runtimeV2.manager.ReconfigureAgent(readerID, generic)
	}()
	go func() {
		defer mutations.Done()
		<-start
		genericMutationErrs <- runtimeV2.manager.TeardownAgent(retiredID)
	}()
	go func() {
		defer mutations.Done()
		<-start
		rogue := v1Agents[readerID]
		rogue.Config.ID = "account-raw-rogue-" + instanceID
		rogue.Config.Role = "raw-rogue"
		rogue.Status = "active"
		genericMutationErrs <- selected.UpsertAgent(ctx, rogue)
	}()
	close(start)
	mutations.Wait()
	close(sourceRevisionErr)
	close(genericMutationErrs)
	if err := <-sourceRevisionErr; err != nil {
		t.Fatalf("reconcile revised source: %v", err)
	}
	for err := range genericMutationErrs {
		if err == nil {
			t.Fatal("generic dynamic-agent mutation bypassed readiness-owned desired topology")
		}
	}
	revisedReadiness := waitNotifyAllChildrenRuntimeReadiness(t, ctx, runtimeV2.workflow, runID, descriptor.FlowInstance)
	if failures := transientRouteFailures(); failures != 1 {
		t.Fatalf("transient revised-route failures = %d, want exactly one automatic-retry trigger", failures)
	}

	v2Agents := loadNotifyAllChildrenAgentsByID(t, ctx, selected)
	rogueID := "account-rogue-" + instanceID
	if _, found := v2Agents[rogueID]; found {
		t.Fatalf("generic hire escaped readiness ownership: %#v", v2Agents[rogueID])
	}
	if _, found := v2Agents[retiredID]; found {
		t.Fatalf("removed agent %s remains active: %#v", retiredID, v2Agents[retiredID])
	}
	if _, found := v2Agents["account-raw-rogue-"+instanceID]; found {
		t.Fatalf("raw upsert escaped readiness ownership: %#v", v2Agents)
	}
	reader := v2Agents[readerID]
	if reader.Config.Role != "reader-v2" || reader.LifecycleGeneration <= readerGenerationV1 {
		t.Fatalf("changed reader = %#v, want reader-v2 generation after %d", reader, readerGenerationV1)
	}
	if writer := v2Agents[writerID]; writer.Config.Role != "writer" || writer.LifecycleGeneration == 0 {
		t.Fatalf("added writer = %#v", writer)
	}
	if _, ok := runtimeV2.manager.GetAgentConfig(retiredID); ok {
		t.Fatalf("removed agent %s remains process-visible", retiredID)
	}
	if cfg, ok := runtimeV2.manager.GetAgentConfig(readerID); !ok || cfg.Role != "reader-v2" {
		t.Fatalf("changed reader process config = %#v found=%t", cfg, ok)
	}
	if cfg, ok := runtimeV2.manager.GetAgentConfig(writerID); !ok || cfg.Role != "writer" {
		t.Fatalf("added writer process config = %#v found=%t", cfg, ok)
	}
	rogue := v1Agents[readerID].Config
	rogue.ID = rogueID
	rogue.Role = "rogue"
	assertNotifyAllChildrenReadinessOwnershipFailure(t, "generic hire", runtimeV2.manager.SpawnAgent(rogue))
	genericReconfigure := reader.Config
	genericReconfigure.Role = "generic-reconfigure"
	assertNotifyAllChildrenReadinessOwnershipFailure(
		t,
		"generic reconfigure",
		runtimeV2.manager.ReconfigureAgent(readerID, genericReconfigure),
	)
	assertNotifyAllChildrenReadinessOwnershipFailure(
		t,
		"generic teardown",
		runtimeV2.manager.TeardownAgent(writerID),
	)
	recordingStore, ok := selected.(lifecycleTransitionRecordingStore)
	if !ok {
		t.Fatalf("selected store %T does not record lifecycle transitions", selected)
	}
	canonicalReconfigure, found := recordingStore.RecordedAuthorizedLifecycleTransition(readerID, "reconfigure")
	if !found || canonicalReconfigure.DynamicTopology == nil {
		t.Fatalf("canonical readiness reconfigure was not recorded: %#v found=%t", canonicalReconfigure, found)
	}
	replayed, err := selected.CommitAgentLifecycleTransition(ctx, canonicalReconfigure)
	if err != nil || !replayed.Replayed {
		t.Fatalf("authorized lifecycle replay = %#v err=%v, want exact replay", replayed, err)
	}
	unauthorizedReplay := canonicalReconfigure
	unauthorizedReplay.DynamicTopology = nil
	if _, err := selected.CommitAgentLifecycleTransition(ctx, unauthorizedReplay); err == nil {
		t.Fatal("stored lifecycle result replay bypassed current readiness authority")
	} else {
		assertNotifyAllChildrenReadinessOwnershipFailure(t, "unauthorized stored-result replay", err)
	}
	if err := runtimeV1.manager.ReconfigureAgent(readerID, reader.Config); err == nil {
		t.Fatal("stale predecessor manager republished canonical source-owned successor")
	} else {
		assertNotifyAllChildrenReadinessOwnershipFailure(t, "stale predecessor exact replay", err)
	}
	if cfg, ok := runtimeV1.manager.GetAgentConfig(readerID); !ok || cfg.Role != "reader-v1" {
		t.Fatalf("stale predecessor process projection changed after replay rejection: %#v found=%t", cfg, ok)
	}
	proveNotifyAllChildrenRawAgentTopologyAdmission(
		t,
		ctx,
		selected,
		&runtimeV2,
		runID,
		descriptor,
		reader,
		instanceID,
	)
	proveNotifyAllChildrenTerminalTopologyAdmission(
		t,
		ctx,
		selected,
		&runtimeV2,
		runID,
		descriptor,
		readerID,
	)
	if revisedReadiness.Plan.WorkflowVersion != sourceV2.WorkflowVersion() {
		t.Fatalf("revised runtime readiness = %#v", revisedReadiness)
	}
	if revisedReadiness.CreationEventEmittedAt != initialReadiness.CreationEventEmittedAt {
		t.Fatalf("creation completion changed across source revision: before=%s after=%s", initialReadiness.CreationEventEmittedAt, revisedReadiness.CreationEventEmittedAt)
	}
	staleMutation := v1Agents[readerID].Config
	staleMutation.Role = "stale-predecessor-write"
	if err := runtimeV1.manager.ReconfigureAgent(readerID, staleMutation); err == nil {
		t.Fatal("stale predecessor generation mutated after successor convergence")
	}
	terminated, found, err := selected.LoadAgentLifecycleState(ctx, retiredID)
	if err != nil || !found || terminated.Phase != runtimemanager.AgentLifecycleTerminated {
		t.Fatalf("removed lifecycle state = %#v found=%t err=%v", terminated, found, err)
	}

	runtimeV3 := newNotifyAllChildrenRuntime(t, selected, db, sourceV2, time.Now)
	if _, err := runtimeV3.manager.HydrateForStartup(ctx); err != nil {
		t.Fatalf("restart hydration: %v", err)
	}
	for _, agentID := range []string{readerID, writerID} {
		if _, ok := runtimeV3.manager.GetAgentConfig(agentID); !ok {
			t.Fatalf("restart omitted exact active agent %s", agentID)
		}
	}
	if _, ok := runtimeV3.manager.GetAgentConfig(retiredID); ok {
		t.Fatalf("restart resurrected removed agent %s", retiredID)
	}

	sourceV3 := notifyallchildren.LoadSource(t, notifyallchildren.Options{
		AgentTopologyRevision: 3,
		AutoEmitOnCreate:      autoEmit,
	})
	runtimeV4 := newNotifyAllChildrenRuntime(t, selected, db, sourceV3, time.Now)
	if err := runtimeV4.manager.Run(managedConformanceExecutionContext(t, ctx, "dynamic-flow-source-v3")); err != nil {
		t.Fatalf("run v3 manager: %v", err)
	}
	failNextRouteReplacement()
	if err := runtimeV4.workflow.RunPipelineMutation(
		worklifetime.WithOccurrence(ctx, runtimeV4.workOwner),
		func(txctx context.Context) error {
			return runtimeV4.manager.ReconcileDynamicFlowRuntimeReadinessPlansForRun(txctx, time.Now().UTC())
		},
	); err != nil {
		t.Fatalf("reconcile reintroduced source: %v", err)
	}
	waitNotifyAllChildrenRuntimeReadiness(t, ctx, runtimeV4.workflow, runID, descriptor.FlowInstance)
	if failures := transientRouteFailures(); failures != 2 {
		t.Fatalf("transient revised-route failures = %d, want one per revised source", failures)
	}
	v3Agents := loadNotifyAllChildrenAgentsByID(t, ctx, selected)
	reintroduced := v3Agents[retiredID]
	if reintroduced.Config.Role != "returned" || reintroduced.LifecycleGeneration <= terminated.Generation {
		t.Fatalf("reintroduced lifecycle = %#v, want successor after %#v", reintroduced, terminated)
	}
	if transitions := countNotifyAllChildrenLifecycleTransitions(
		t,
		ctx,
		selected,
		db,
		retiredID,
		runtimemanager.AgentLifecycleTerminated,
		runtimemanager.AgentLifecycleRegistered,
	); transitions != 1 {
		t.Fatalf("terminated-to-registered transitions = %d, want exactly one", transitions)
	}
	if _, err := selected.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "restart", RequestHash: "stale-predecessor-restart",
		AgentID: retiredID, Trigger: "restart",
		ExpectedEpoch: terminated.RuntimeEpoch, ExpectedGeneration: terminated.Generation, ExpectedPhase: terminated.Phase,
		TargetEpoch: terminated.RuntimeEpoch, TargetGeneration: terminated.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: terminated.ConfigRevision,
		RunMode: runtimemanager.AgentRunModeStopped, Now: time.Now().UTC(),
	}); err == nil {
		t.Fatal("stale terminated predecessor CAS mutated reintroduced successor")
	}
	if reader := v3Agents[readerID]; reader.Config.Role != "reader-v3" {
		t.Fatalf("v3 reader = %#v", reader)
	}
	if cfg, ok := runtimeV4.manager.GetAgentConfig(retiredID); !ok || cfg.Role != "returned" {
		t.Fatalf("reintroduced process config = %#v found=%t", cfg, ok)
	}
	if _, err := selected.CommitAgentLifecycleTransition(ctx, canonicalReconfigure); err == nil {
		t.Fatal("stale authorized replay survived readiness-plan revision")
	} else {
		assertNotifyAllChildrenReadinessOwnershipFailure(t, "stale authorized replay", err)
	}

	runtimeV5 := newNotifyAllChildrenRuntime(t, selected, db, sourceV3, time.Now)
	if _, err := runtimeV5.manager.HydrateForStartup(ctx); err != nil {
		t.Fatalf("reintroduced restart hydration: %v", err)
	}
	for _, agentID := range []string{readerID, writerID, retiredID} {
		if _, ok := runtimeV5.manager.GetAgentConfig(agentID); !ok {
			t.Fatalf("reintroduced restart omitted active agent %s", agentID)
		}
	}
}

func assertNotifyAllChildrenReadinessOwnershipFailure(t *testing.T, action string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "dynamic_agent_topology_owned_by_readiness") {
		t.Fatalf("%s error = %v, want dynamic readiness ownership rejection", action, err)
	}
}

type notifyAllChildrenTopologySnapshot struct {
	agents    string
	readiness string
	routes    string
	process   string
}

func proveNotifyAllChildrenRawAgentTopologyAdmission(
	t *testing.T,
	ctx context.Context,
	selected notifyAllChildrenStore,
	runtime *notifyAllChildrenRuntime,
	runID string,
	descriptor runtimebus.ActiveFlowInstanceDescriptor,
	reader runtimemanager.PersistedAgent,
	instanceID string,
) {
	t.Helper()
	outside := reader
	outside.Config.ID = "outside-agent-" + instanceID
	outside.Config.Role = "outside"
	outside.Config.FlowID = ""
	outside.Config.FlowPath = "outside/" + instanceID
	outside.Config.Subscriptions = nil
	outside.Config.EmitEvents = nil
	outside.Status = "active"
	if err := selected.UpsertAgent(ctx, outside); err != nil {
		t.Fatalf("seed non-readiness-owned raw agent: %v", err)
	}
	before := snapshotNotifyAllChildrenTopology(t, ctx, selected, runtime, runID, descriptor)

	rawAdd := reader
	rawAdd.Config.ID = "raw-add-" + instanceID
	rawAdd.Config.Role = "raw-add"
	rawAdd.Status = "active"
	configRewrite := reader
	configRewrite.Config.Role = "raw-config-rewrite"
	configRewrite.Status = "active"
	moveOut := reader
	moveOut.Config.FlowPath = ""
	moveOut.Status = "active"
	moveIn := outside
	moveIn.Config.FlowPath = descriptor.FlowInstance
	moveIn.Status = "active"
	activeRewrite := reader
	activeRewrite.Status = "active"
	failedRewrite := reader
	failedRewrite.Status = "failed"
	terminatedRewrite := reader
	terminatedRewrite.Status = "terminated"

	for _, tc := range []struct {
		name string
		rec  runtimemanager.PersistedAgent
	}{
		{name: "raw_add", rec: rawAdd},
		{name: "declared_config_rewrite", rec: configRewrite},
		{name: "path_move_out", rec: moveOut},
		{name: "path_move_in", rec: moveIn},
		{name: "active_status", rec: activeRewrite},
		{name: "failed_status", rec: failedRewrite},
		{name: "terminated_status", rec: terminatedRewrite},
	} {
		err := selected.UpsertAgent(ctx, tc.rec)
		assertNotifyAllChildrenReadinessOwnershipFailure(t, "raw upsert "+tc.name, err)
	}
	after := snapshotNotifyAllChildrenTopology(t, ctx, selected, runtime, runID, descriptor)
	if after != before {
		t.Fatalf("raw topology rejection changed selected/runtime state:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func proveNotifyAllChildrenTerminalTopologyAdmission(
	t *testing.T,
	ctx context.Context,
	selected notifyAllChildrenStore,
	runtime *notifyAllChildrenRuntime,
	runID string,
	descriptor runtimebus.ActiveFlowInstanceDescriptor,
	agentID string,
) {
	t.Helper()
	state, found, err := selected.LoadAgentLifecycleState(ctx, agentID)
	if err != nil || !found {
		t.Fatalf("load terminal-matrix lifecycle state: state=%#v found=%t err=%v", state, found, err)
	}
	before := snapshotNotifyAllChildrenTopology(t, ctx, selected, runtime, runID, descriptor)
	for _, operationKind := range []string{"stop", "teardown", "future_terminal_operation"} {
		for _, trigger := range []string{"stop", "teardown", "future_terminal_trigger"} {
			_, err := selected.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
				OperationID: uuid.NewString(), OperationKind: operationKind,
				RequestHash: "terminal-matrix-" + operationKind + "-" + trigger,
				AgentID:     agentID, Trigger: trigger,
				ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
				TargetEpoch: state.RuntimeEpoch + 1, TargetGeneration: state.Generation + 1,
				TargetPhase: runtimemanager.AgentLifecycleTerminated, ConfigRevision: state.ConfigRevision,
				RunMode: runtimemanager.AgentRunModeStopped, Now: time.Now().UTC(),
			})
			assertNotifyAllChildrenReadinessOwnershipFailure(
				t,
				"terminal operation="+operationKind+" trigger="+trigger,
				err,
			)
		}
	}
	after := snapshotNotifyAllChildrenTopology(t, ctx, selected, runtime, runID, descriptor)
	if after != before {
		t.Fatalf("terminal topology rejection changed selected/runtime state:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func snapshotNotifyAllChildrenTopology(
	t *testing.T,
	ctx context.Context,
	selected notifyAllChildrenStore,
	runtime *notifyAllChildrenRuntime,
	runID string,
	descriptor runtimebus.ActiveFlowInstanceDescriptor,
) notifyAllChildrenTopologySnapshot {
	t.Helper()
	readiness, found, err := runtime.workflow.LoadDynamicFlowRuntimeReadiness(ctx, runID, descriptor.FlowInstance)
	if err != nil || !found {
		t.Fatalf("snapshot readiness: readiness=%#v found=%t err=%v", readiness, found, err)
	}
	route := runtimeflowidentity.StoredRoute(
		notifyallchildren.ChildFlowID,
		descriptor.InstanceID,
		descriptor.FlowInstance,
	)
	routes, err := selected.ListFlowInstanceRouteRecords(ctx, route)
	if err != nil {
		t.Fatalf("snapshot route records: %v", err)
	}
	marshal := func(name string, value any) string {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("snapshot %s: %v", name, err)
		}
		return string(raw)
	}
	processAgents := runtime.manager.ListAgentConfigs()
	slices.SortFunc(processAgents, func(a, b runtimeactors.AgentConfig) int {
		return strings.Compare(a.ID, b.ID)
	})
	return notifyAllChildrenTopologySnapshot{
		agents:    marshal("agents", loadNotifyAllChildrenAgentsByID(t, ctx, selected)),
		readiness: marshal("readiness", readiness),
		routes:    marshal("routes", routes),
		process:   marshal("process agents", processAgents),
	}
}

func countNotifyAllChildrenLifecycleTransitions(
	t *testing.T,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	agentID string,
	previous runtimemanager.AgentLifecyclePhase,
	next runtimemanager.AgentLifecyclePhase,
) int {
	t.Helper()
	query := `
		SELECT COUNT(*)
		FROM agent_lifecycle_transition_facts
		WHERE agent_id = $1 AND previous_phase = $2 AND next_phase = $3
	`
	if _, ok := backend.(*failingNotifyAllChildrenSQLiteStore); ok {
		query = `
			SELECT COUNT(*)
			FROM agent_lifecycle_transition_facts
			WHERE agent_id = ? AND previous_phase = ? AND next_phase = ?
		`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, agentID, string(previous), string(next)).Scan(&count); err != nil {
		t.Fatalf("count lifecycle transitions for %s: %v", agentID, err)
	}
	return count
}

func TestDynamicFlowTerminalizationAndRouteReplacementRollbackTogetherOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB, func())
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func()) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := &failingNotifyAllChildrenPostgresStore{
					PostgresStore: storetest.AdmitPostgresRuntimeStore(t, db),
				}
				return selected, db, func() { selected.failExactRouteReplacement.Store(true) }
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB, func()) {
				base := storetest.StartSQLiteRuntimeStore(t)
				selected := &failingNotifyAllChildrenSQLiteStore{SQLiteRuntimeStore: base}
				return selected, base.DB, func() { selected.failExactRouteReplacement.Store(true) }
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, db, failReplacement := tc.setup(t)
			runID := uuid.NewString()
			ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
			runtime := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now)

			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-main",
			})
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.account.register.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_id":   "acct-rollback",
			})
			descriptor, ok := notifyAllChildrenAccountDescriptors(t, ctx, selected)["acct-rollback"]
			if !ok {
				t.Fatal("created account descriptor is missing")
			}
			route := runtimeflowidentity.StoredRoute(
				notifyallchildren.ChildFlowID,
				descriptor.InstanceID,
				descriptor.FlowInstance,
			)
			before, err := selected.ListFlowInstanceRouteRecords(ctx, route)
			if err != nil || len(before) == 0 {
				t.Fatalf("load prior exact route set: routes=%#v err=%v", before, err)
			}
			if !runtime.bus.HasFlowInstanceRoute(route) {
				t.Fatal("created process route is not active before terminalization")
			}

			failReplacement()
			err = runtime.manager.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
				ContractBundle: source,
				Instance: runtimeflowidentity.Stored(
					source,
					notifyallchildren.ChildFlowID,
					descriptor.FlowInstance,
					descriptor.InstanceID,
					descriptor.EntityID,
					"",
				),
				FinalState: "active",
			})
			if err == nil || !strings.Contains(err.Error(), "exact route replacement failure") {
				t.Fatalf("DeactivateFlowInstanceModel error = %v, want injected route replacement failure", err)
			}

			if got := loadNotifyAllChildrenFlowInstanceStatus(t, ctx, selected, db, descriptor.FlowInstance); got != "active" {
				t.Fatalf("flow instance status after replacement rollback = %q, want active", got)
			}
			after, err := selected.ListFlowInstanceRouteRecords(ctx, route)
			if err != nil || !slices.EqualFunc(after, before, func(a, b runtimebus.FlowInstanceRouteRecord) bool {
				return a == b
			}) {
				t.Fatalf("exact route set after replacement rollback: before=%#v after=%#v err=%v", before, after, err)
			}
			if !runtime.bus.HasFlowInstanceRoute(route) {
				t.Fatal("process route retired despite selected mutation rollback")
			}
		})
	}
}

func TestNotifyAllChildrenRuntimeConformance_MixedValidAndStaleRoutesPersistAndReplayOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (notifyAllChildrenStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				return storetest.AdmitPostgresRuntimeStore(t, db), db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (notifyAllChildrenStore, *sql.DB) {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return backend, backend.DB
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			backend, db := tc.setup(t)
			runID := uuid.NewString()
			fixedEngineNow := time.Date(2026, time.July, 12, 12, 0, 0, 1, time.UTC)
			ctx = runtimecorrelation.WithRunID(ctx, runID)
			source := notifyallchildren.LoadSource(t, notifyallchildren.Options{})
			runtime := newNotifyAllChildrenRuntime(t, backend, db, source, func() time.Time { return fixedEngineNow })

			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.opened", map[string]any{
				"portfolio_id": "portfolio-main",
			})
			assertNotifyAllChildrenRunPersisted(t, ctx, backend, db, runID)
			for _, accountID := range []string{"acct-a", "acct-b", "acct-stale"} {
				publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.account.register.requested", map[string]any{
					"portfolio_id": "portfolio-main",
					"account_id":   accountID,
				})
			}

			descriptors := notifyAllChildrenAccountDescriptors(t, ctx, backend)
			if len(descriptors) != 3 {
				dumpNotifyAllChildrenRuntimeState(t, ctx, backend, db)
				t.Logf("notify-all-children runtime diagnostics: %#v", runtime.diagnostics.snapshot())
				t.Fatalf("active account descriptors = %#v, want A/B/stale", descriptors)
			}
			for _, accountID := range []string{"acct-a", "acct-b", "acct-stale"} {
				if _, ok := descriptors[accountID]; !ok {
					t.Fatalf("active account descriptor %q missing from %#v", accountID, descriptors)
				}
			}

			orderedMembership := []string{"acct-b", "acct-a", "acct-b"}
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  orderedMembership,
			})
			orderedNotifyID := publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "ordered-duplicate",
			})
			orderedItems := loadNotifyAllChildrenItemEvents(t, ctx, backend, db, runID, orderedNotifyID)
			assertNotifyAllChildrenItemSequence(t, orderedItems, orderedMembership)
			assertNotifyAllChildrenDistinctItemTimestamps(t, ctx, backend, db, runID, orderedNotifyID, len(orderedMembership))
			for index, item := range orderedItems {
				routes, err := backend.ListEventDeliveryRoutes(ctx, item.ID)
				if err != nil {
					t.Fatalf("ordered item %d ListEventDeliveryRoutes(%s): %v", index, item.AccountID, err)
				}
				want := descriptors[item.AccountID]
				if len(routes) != 1 || routes[0].Target.FlowInstance != want.FlowInstance || routes[0].Target.EntityID != want.EntityID {
					t.Fatalf("ordered item %d persisted routes = %#v, want only %s/%s", index, routes, want.FlowInstance, want.EntityID)
				}
			}

			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-a", "acct-b", "acct-stale"},
			})
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, "portfolio/portfolio", "account_ids", []any{"acct-a", "acct-b", "acct-stale"})

			stale := descriptors["acct-stale"]
			if err := runtime.manager.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
				ContractBundle: source,
				Instance: runtimeflowidentity.Stored(
					source,
					notifyallchildren.ChildFlowID,
					stale.FlowInstance,
					stale.InstanceID,
					stale.EntityID,
					"",
				),
				FinalState: "active",
			}); err != nil {
				t.Fatalf("deactivate stale account: %v", err)
			}

			notifyID := publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "refresh",
			})
			itemEvents := loadNotifyAllChildrenItemEvents(t, ctx, backend, db, runID, notifyID)
			if len(itemEvents) != 3 {
				t.Fatalf("fan-out item events = %#v, want exactly A/B/stale", itemEvents)
			}
			items := notifyAllChildrenItemIDsByAccount(t, itemEvents)

			for _, accountID := range []string{"acct-a", "acct-b"} {
				itemID := items[accountID]
				routes, err := backend.ListEventDeliveryRoutes(ctx, itemID)
				if err != nil {
					t.Fatalf("ListEventDeliveryRoutes(%s): %v", accountID, err)
				}
				want := descriptors[accountID]
				if len(routes) != 1 || routes[0].Target.FlowInstance != want.FlowInstance || routes[0].Target.EntityID != want.EntityID {
					t.Fatalf("persisted %s routes = %#v, want only %s/%s", accountID, routes, want.FlowInstance, want.EntityID)
				}
				assertNotifyAllChildrenMetadata(t, ctx, backend, db, want.FlowInstance, "last_command", "refresh")
			}

			staleID := items["acct-stale"]
			if routes, err := backend.ListEventDeliveryRoutes(ctx, staleID); err != nil || len(routes) != 0 {
				t.Fatalf("stale routes = %#v err=%v, want none", routes, err)
			}
			failure := loadNotifyAllChildrenFailure(t, ctx, backend, db, staleID)
			if failure.Class != runtimefailures.ClassTargetUnreachable || !strings.Contains(failure.Detail.Code, "target") {
				t.Fatalf("stale failure = %#v, want platform.target_unreachable with route detail", failure)
			}
			assertNotifyAllChildrenFlowInstanceCount(t, ctx, backend, db, 3)

			// A later supported write changes current membership and state. Replaying
			// the original A item must still use its persisted route and payload.
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-a"},
			})
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.notify.requested", map[string]any{
				"portfolio_id": "portfolio-main",
				"command":      "newer",
			})
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, descriptors["acct-a"].FlowInstance, "last_command", "newer")
			publishNotifyAllChildrenEvent(t, ctx, runtime.bus, source, runID, "portfolio.membership.seeded", map[string]any{
				"portfolio_id": "portfolio-main",
				"account_ids":  []string{"acct-b"},
			})

			originalA := items["acct-a"]
			deleteNotifyAllChildrenPipelineReceipt(t, ctx, backend, db, originalA)
			claimed, err := backend.PipelineObligations().ClaimEvent(ctx, originalA, runtimepipelineobligation.PurposeRecovery)
			if err != nil {
				t.Fatalf("claim original pipeline obligation: %v", err)
			}
			replay := claimed.Event
			if err := backend.PipelineObligations().Release(ctx, claimed.Claim); err != nil {
				t.Fatalf("release original pipeline obligation: %v", err)
			}
			routes, err := backend.ListEventDeliveryRoutes(ctx, originalA)
			if err != nil || len(routes) != 1 {
				t.Fatalf("original A persisted routes = %#v err=%v, want one exact route", routes, err)
			}
			recoveryEvent := eventtest.RuntimeControl(
				uuid.NewString(), replay.Type(), "workflow-runtime", "", replay.Payload(), replay.ChainDepth()+1,
				runID, replay.ID(), replay.Envelope(), fixedEngineNow.Add(time.Second),
			)
			storetest.CommitSemanticEventWithRoutes(t, ctx, backend, recoveryEvent, routes, runtimepipelineobligation.ScopeSubscribed)
			eventCountBefore := countNotifyAllChildrenItemEvents(t, ctx, backend, db, runID)
			restarted := newNotifyAllChildrenRuntime(t, backend, db, source, func() time.Time { return fixedEngineNow })
			if err := restarted.pipeline.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("RecoverNodeDeliveries: %v", err)
			}
			waitNotifyAllChildrenBus(t, restarted.bus)
			assertNotifyAllChildrenMetadata(t, ctx, backend, db, descriptors["acct-a"].FlowInstance, "last_command", "refresh")
			if got := countNotifyAllChildrenItemEvents(t, ctx, backend, db, runID); got != eventCountBefore {
				t.Fatalf("item event count after replay = %d, want %d; replay must not re-expand current membership", got, eventCountBefore)
			}
			routes, err = backend.ListEventDeliveryRoutes(ctx, recoveryEvent.ID())
			if err != nil || len(routes) != 1 || routes[0].Target.FlowInstance != descriptors["acct-a"].FlowInstance {
				t.Fatalf("recovered persisted A route = %#v err=%v", routes, err)
			}
		})
	}
}

func newNotifyAllChildrenRuntime(t *testing.T, backend notifyAllChildrenStore, db *sql.DB, source semanticview.Source, engineNow func() time.Time) notifyAllChildrenRuntime {
	t.Helper()
	var coordinator *runtimepipeline.PipelineCoordinator
	var manager *runtimemanager.AgentManager
	workOwner := conformanceTestRuntimeOccurrence(t, authorActivityTestBundleSourceFact.BundleHash())
	eventBus, err := newScopedTestEventBus(t, backend, runtimebus.EventBusOptions{
		ContractBundle: source,
		WorkOwner:      workOwner,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if coordinator == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{coordinator}
		},
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return fmt.Errorf("agent manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if routeStore, ok := backend.(runtimebus.FlowInstanceRoutePersistence); ok {
		routes, err := routeStore.ListFlowInstanceRoutes(testAuthorActivityContext(context.Background()))
		if err != nil {
			t.Fatalf("ListFlowInstanceRoutes: %v", err)
		}
		for _, route := range routes {
			if err := eventBus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{Identity: route}); err != nil {
				t.Fatalf("restore flow-instance route %s: %v", route.InstancePath, err)
			}
		}
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	switch sqliteStore := backend.(type) {
	case *store.SQLiteRuntimeStore:
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, sqliteStore)
	case *failingNotifyAllChildrenSQLiteStore:
		workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, sqliteStore)
	}
	manager = ownConformanceTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(eventBus, nil, runtimemanager.AgentManagerOptions{
		BaseContext:       testAuthorActivityContext(context.Background()),
		BundleSourceFact:  authorActivityTestBundleSourceFact,
		WorkflowInstances: workflowStore,
		WorkOwner:         workOwner,
		DeliveryStore:     backend,
		LifecycleStore:    backend,
		SemanticSource:    source,
	}, backend))
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		t.Fatalf("LoadWorkflowDefinition: %v", err)
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes: %v", err)
	}
	module := conformanceLoadedWorkflowModule{
		source:   source,
		workflow: workflow,
		nodes:    nodes,
		guards:   runtimepipeline.NewContractGuardRegistry(source),
		actions:  runtimepipeline.NewContractActionRegistry(source),
	}
	diagnosticBus := &fanInBarrierDiagnosticBus{EventBus: eventBus}
	coordinator = runtimepipeline.NewPipelineCoordinatorWithOptions(diagnosticBus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:            module,
		InstanceActivator: manager.ActivateFlowInstance,
		WorkflowStore:     workflowStore,
		DeliveryStore:     backend,
		TestEngineEmitNow: engineNow,
		WorkOwner:         workOwner,
	})
	return notifyAllChildrenRuntime{
		bus: eventBus, diagnostics: diagnosticBus, manager: manager, pipeline: coordinator,
		workflow: workflowStore, workOwner: workOwner,
	}
}

func loadNotifyAllChildrenAgentsByID(
	t testing.TB,
	ctx context.Context,
	selected notifyAllChildrenStore,
) map[string]runtimemanager.PersistedAgent {
	t.Helper()
	agents, err := selected.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents: %v", err)
	}
	out := make(map[string]runtimemanager.PersistedAgent, len(agents))
	for _, agent := range agents {
		out[strings.TrimSpace(agent.Config.ID)] = agent
	}
	return out
}

func waitNotifyAllChildrenRuntimeReadiness(
	t testing.TB,
	ctx context.Context,
	workflow *runtimepipeline.WorkflowInstanceStore,
	runID string,
	instancePath string,
) runtimepipeline.DynamicFlowRuntimeReadiness {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var (
		last  runtimepipeline.DynamicFlowRuntimeReadiness
		found bool
		err   error
	)
	for time.Now().Before(deadline) {
		last, found, err = workflow.LoadDynamicFlowRuntimeReadiness(ctx, runID, instancePath)
		if err == nil && found && !last.TopologyReadyAt.IsZero() {
			return last
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("runtime readiness did not converge: found=%v readiness=%#v err=%v", found, last, err)
	return runtimepipeline.DynamicFlowRuntimeReadiness{}
}

func loadNotifyAllChildrenFlowInstanceStatus(
	t *testing.T,
	ctx context.Context,
	backend notifyAllChildrenStore,
	db *sql.DB,
	instancePath string,
) string {
	t.Helper()
	query := `SELECT status FROM flow_instances WHERE instance_id = $1`
	if _, ok := backend.(*failingNotifyAllChildrenSQLiteStore); ok {
		query = `SELECT status FROM flow_instances WHERE instance_id = ?`
	}
	var status string
	if err := db.QueryRowContext(ctx, query, instancePath).Scan(&status); err != nil {
		t.Fatalf("load flow instance status %s: %v", instancePath, err)
	}
	return strings.TrimSpace(status)
}

func publishNotifyAllChildrenEvent(t *testing.T, ctx context.Context, eventBus *runtimebus.EventBus, source semanticview.Source, runID, localEvent string, payload map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s payload: %v", localEvent, err)
	}
	id := uuid.NewString()
	evt := eventtest.RunCreatingRootIngress(
		id,
		events.EventType(source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, localEvent)),
		notifyallchildren.OwnerFlowID,
		"",
		raw,
		0,
		runID,
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	if err := eventBus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("PublishAcknowledged(%s): %v", localEvent, err)
	}
	waitNotifyAllChildrenBus(t, eventBus)
	return id
}

func waitNotifyAllChildrenBus(t *testing.T, eventBus *runtimebus.EventBus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 30*time.Second)
	defer cancel()
	if err := eventBus.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("WaitForQuiescence: %v", err)
	}
}

func assertNotifyAllChildrenRunPersisted(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID string) {
	t.Helper()
	query := `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(*) FROM runs WHERE run_id = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
		t.Fatalf("query notify-all-children run: %v", err)
	}
	if count != 1 {
		t.Fatalf("persisted notify-all-children run count = %d, want 1 from supported event admission", count)
	}
}

func notifyAllChildrenAccountDescriptors(t *testing.T, ctx context.Context, backend notifyAllChildrenStore) map[string]runtimebus.ActiveFlowInstanceDescriptor {
	t.Helper()
	descriptors, err := backend.ListActiveFlowInstanceDescriptors(ctx)
	if err != nil {
		t.Fatalf("ListActiveFlowInstanceDescriptors: %v", err)
	}
	out := map[string]runtimebus.ActiveFlowInstanceDescriptor{}
	for _, descriptor := range descriptors {
		if descriptor.FlowTemplate != notifyallchildren.ChildFlowID {
			continue
		}
		if accountID := descriptor.AddressFields["entity.account_id"]; accountID != "" {
			out[accountID] = descriptor
		}
	}
	return out
}

type notifyAllChildrenItemEvent struct {
	ID        string
	AccountID string
	CreatedAt string
}

func loadNotifyAllChildrenItemEvents(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID, sourceEventID string) []notifyAllChildrenItemEvent {
	t.Helper()
	query := `SELECT event_id::text, payload, created_at FROM events WHERE run_id = $1::uuid AND event_name = $2 AND source_event_id = $3::uuid ORDER BY created_at, event_id`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT event_id, payload, created_at FROM events WHERE run_id = ? AND event_name = ? AND source_event_id = ? ORDER BY created_at, event_id`
	}
	rows, err := db.QueryContext(ctx, query, runID, "portfolio/account.notify.requested", sourceEventID)
	if err != nil {
		t.Fatalf("query fan-out item events: %v", err)
	}
	defer rows.Close()
	out := []notifyAllChildrenItemEvent{}
	for rows.Next() {
		var id string
		var raw, createdAt any
		if err := rows.Scan(&id, &raw, &createdAt); err != nil {
			t.Fatalf("scan fan-out item event: %v", err)
		}
		payload := map[string]any{}
		if err := json.Unmarshal(notifyAllChildrenJSONBytes(raw), &payload); err != nil {
			t.Fatalf("decode fan-out item payload: %v", err)
		}
		accountID, _ := payload["account_id"].(string)
		out = append(out, notifyAllChildrenItemEvent{ID: id, AccountID: accountID, CreatedAt: fmt.Sprint(createdAt)})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read fan-out item events: %v", err)
	}
	return out
}

func assertNotifyAllChildrenItemSequence(t *testing.T, items []notifyAllChildrenItemEvent, want []string) {
	t.Helper()
	got := make([]string, 0, len(items))
	for _, item := range items {
		got = append(got, item.AccountID)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("persisted fan-out item sequence = %#v (%#v), want %#v with order and duplicates preserved", got, items, want)
	}
}

func assertNotifyAllChildrenDistinctItemTimestamps(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID, sourceEventID string, want int) {
	t.Helper()
	query := `SELECT COUNT(DISTINCT created_at) FROM events WHERE run_id = $1::uuid AND event_name = $2 AND source_event_id = $3::uuid`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(DISTINCT created_at) FROM events WHERE run_id = ? AND event_name = ? AND source_event_id = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, runID, "portfolio/account.notify.requested", sourceEventID).Scan(&count); err != nil {
		t.Fatalf("count distinct persisted fan-out timestamps: %v", err)
	}
	if count != want {
		t.Fatalf("distinct persisted fan-out timestamps = %d, want %d from equal engine clock ticks", count, want)
	}
}

func notifyAllChildrenItemIDsByAccount(t *testing.T, items []notifyAllChildrenItemEvent) map[string]string {
	t.Helper()
	out := make(map[string]string, len(items))
	for _, item := range items {
		if _, exists := out[item.AccountID]; exists {
			t.Fatalf("fan-out item events contain duplicate account %q where unique membership was required: %#v", item.AccountID, items)
		}
		out[item.AccountID] = item.ID
	}
	return out
}

func countNotifyAllChildrenItemEvents(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, runID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = $2`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, runID, "portfolio/account.notify.requested").Scan(&count); err != nil {
		t.Fatalf("count fan-out item events: %v", err)
	}
	return count
}

func assertNotifyAllChildrenMetadata(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, flowInstance, field string, want any) {
	t.Helper()
	query := `SELECT fields FROM entity_state WHERE flow_instance = $1 ORDER BY updated_at DESC LIMIT 1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT fields FROM entity_state WHERE flow_instance = ? ORDER BY updated_at DESC LIMIT 1`
	}
	wantJSON, _ := json.Marshal(want)
	deadline := time.Now().Add(5 * time.Second)
	var (
		fields  map[string]any
		gotJSON []byte
		lastErr error
	)
	for time.Now().Before(deadline) {
		var raw any
		if err := db.QueryRowContext(ctx, query, flowInstance).Scan(&raw); err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		fields = map[string]any{}
		if err := json.Unmarshal(notifyAllChildrenJSONBytes(raw), &fields); err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		gotJSON, _ = json.Marshal(fields[field])
		if string(gotJSON) == string(wantJSON) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	dumpNotifyAllChildrenRuntimeState(t, ctx, backend, db)
	t.Fatalf("%s.%s = %s, want %s (all fields %#v, last error %v)", flowInstance, field, gotJSON, wantJSON, fields, lastErr)
}

func loadNotifyAllChildrenFailure(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, eventID string) runtimefailures.Envelope {
	t.Helper()
	query := `SELECT failure::text FROM dead_letters WHERE original_event_id = $1::uuid ORDER BY created_at DESC LIMIT 1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT failure FROM dead_letters WHERE original_event_id = ? ORDER BY created_at DESC LIMIT 1`
	}
	var raw any
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&raw); err != nil {
		t.Fatalf("load stale target failure: %v", err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(notifyAllChildrenJSONBytes(raw))
	if err != nil {
		t.Fatalf("decode stale target failure: %v", err)
	}
	return failure
}

func assertNotifyAllChildrenFlowInstanceCount(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM flow_instances WHERE flow_template = $1`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `SELECT COUNT(*) FROM flow_instances WHERE flow_template = ?`
	}
	var count int
	if err := db.QueryRowContext(ctx, query, notifyallchildren.ChildFlowID).Scan(&count); err != nil {
		t.Fatalf("count account flow instances: %v", err)
	}
	if count != want {
		t.Fatalf("account flow instances = %d, want %d", count, want)
	}
}

func deleteNotifyAllChildrenPipelineReceipt(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB, eventID string) {
	t.Helper()
	query := `DELETE FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if _, ok := backend.(*store.SQLiteRuntimeStore); ok {
		query = `DELETE FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	if _, err := db.ExecContext(ctx, query, eventID); err != nil {
		t.Fatalf("delete replay receipts: %v", err)
	}
}

func notifyAllChildrenJSONBytes(raw any) []byte {
	switch typed := raw.(type) {
	case []byte:
		return typed
	case string:
		return []byte(typed)
	default:
		return []byte(fmt.Sprint(raw))
	}
}

func dumpNotifyAllChildrenRuntimeState(t *testing.T, ctx context.Context, backend notifyAllChildrenStore, db *sql.DB) {
	t.Helper()
	queries := []string{
		`SELECT event_name, event_id, payload FROM events ORDER BY created_at, event_id`,
		`SELECT event_id, subscriber_type, subscriber_id, outcome, COALESCE(reason_code, ''), COALESCE(failure, '') FROM event_receipts ORDER BY event_id, subscriber_type, subscriber_id`,
		`SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, ''), COALESCE(failure, ''), COALESCE(delivery_target_route, '') FROM event_deliveries ORDER BY event_id, subscriber_type, subscriber_id`,
		`SELECT flow_instance, current_state, fields FROM entity_state ORDER BY flow_instance`,
		`SELECT instance_id, flow_template, status, config FROM flow_instances ORDER BY instance_id`,
		`SELECT original_event_id, failure FROM dead_letters ORDER BY created_at`,
	}
	for _, query := range queries {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			t.Logf("notify-all-children diagnostic query failed: %v", err)
			continue
		}
		columns, _ := rows.Columns()
		for rows.Next() {
			values := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for i := range values {
				destinations[i] = &values[i]
			}
			if err := rows.Scan(destinations...); err != nil {
				t.Logf("notify-all-children diagnostic scan failed: %v", err)
				break
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = string(raw)
				}
			}
			t.Logf("notify-all-children %v: %v", columns, values)
		}
		_ = rows.Close()
	}
	_ = backend
}
