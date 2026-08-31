package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
	"github.com/google/uuid"
)

func deactivateFlowInstanceForTest(am *AgentManager, ctx context.Context, templateID, instanceID, flowPath, entityID string) error {
	return am.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
		Instance: runtimeflowidentity.Stored(nil, templateID, flowPath, instanceID, entityID, ""),
	})
}

func TestRebuildPendingDynamicFlowRuntimeCreationEventPlanUsesRevisedCanonicalSchema(t *testing.T) {
	sourceV2 := notifyallchildren.LoadSource(t, notifyallchildren.Options{
		AgentTopologyRevision: 2,
		AutoEmitOnCreate:      true,
		AutoEmitEventRevision: 2,
	})
	schemaV2, ok := sourceV2.FlowSchemaByID(notifyallchildren.ChildFlowID)
	if !ok {
		t.Fatal("revised account schema is missing")
	}
	identity := runtimeflowidentity.Instance{
		TemplateID:    notifyallchildren.ChildFlowID,
		ScopeKey:      notifyallchildren.ChildFlowID,
		InstanceID:    "acct-1",
		InstancePath:  "account/acct-1",
		EntityID:      uuid.NewString(),
		HasStoredPath: true,
	}
	occurredAt := time.Date(2026, time.July, 27, 22, 0, 0, 0, time.UTC)
	current := &runtimepipeline.DynamicFlowRuntimeCreationEventPlan{
		EventID: uuid.NewString(), EventType: "account/acct-1/account.created",
		RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live,
		Payload: []byte(`{"account_id":"acct-1"}`), CreatedAt: occurredAt,
	}
	revised, err := rebuildPendingDynamicFlowRuntimeCreationEventPlan(
		current,
		false,
		sourceV2,
		schemaV2,
		identity,
		map[string]any{"account_id": "acct-1"},
	)
	if err != nil {
		t.Fatalf("rebuild pending creation plan: %v", err)
	}
	if revised == nil || !strings.HasSuffix(revised.EventType, "/account.revised") {
		t.Fatalf("revised creation event = %#v", revised)
	}
	if revised.EventID == current.EventID {
		t.Fatal("revised creation event reused stale event identity")
	}
	if revised.RunID != current.RunID || revised.ParentEventID != current.ParentEventID ||
		revised.ExecutionMode != current.ExecutionMode || !revised.CreatedAt.Equal(current.CreatedAt) {
		t.Fatalf("revised creation lineage changed: current=%#v revised=%#v", current, revised)
	}

	sourceNoAuto := notifyallchildren.LoadSource(t, notifyallchildren.Options{AgentTopologyRevision: 2})
	schemaNoAuto, ok := sourceNoAuto.FlowSchemaByID(notifyallchildren.ChildFlowID)
	if !ok {
		t.Fatal("no-auto account schema is missing")
	}
	removed, err := rebuildPendingDynamicFlowRuntimeCreationEventPlan(
		current,
		false,
		sourceNoAuto,
		schemaNoAuto,
		identity,
		map[string]any{"account_id": "acct-1"},
	)
	if err != nil || removed != nil {
		t.Fatalf("removed auto-emit plan = %#v err=%v", removed, err)
	}
	if _, err := rebuildPendingDynamicFlowRuntimeCreationEventPlan(
		nil,
		false,
		sourceV2,
		schemaV2,
		identity,
		map[string]any{"account_id": "acct-1"},
	); err == nil {
		t.Fatal("introduced auto-emit without persisted lineage")
	}
	preserved, err := rebuildPendingDynamicFlowRuntimeCreationEventPlan(
		current,
		true,
		sourceV2,
		schemaV2,
		identity,
		map[string]any{"account_id": "acct-1"},
	)
	if err != nil || preserved != current {
		t.Fatalf("emitted creation plan was not preserved: plan=%#v err=%v", preserved, err)
	}
}

type flowActivationRouteStore interface {
	UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error
	DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error
	ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error)
}

type flowActivationTestBus struct {
	addedPaths         []string
	addedRouteRequests []runtimebus.FlowInstanceRouteMaterializationRequest
	removedPairs       []string
	published          []events.Event
	publishedContexts  []events.DeliveryContext
	runtimeLogs        []runtimepipeline.RuntimeLogEntry
	unsubscribed       []string
	removeErr          error
	addErr             error
	stageRoute         func(runtimebus.FlowInstanceRouteMaterializationRequest) error
	publishErr         error
	routeStore         flowActivationRouteStore
	creationStore      *flowActivationTestInstanceStore
}

type flowActivationSemanticRouteBus struct {
	*flowActivationTestBus
	durable       *runtimebus.RouteTable
	process       *runtimebus.RouteTable
	durableRoutes map[string][]runtimebus.FlowInstanceRouteRecord
}

type flowActivationTestAgentRoutePreparation struct {
	deliveries chan *worklifetime.EventDelivery
}

func (p *flowActivationTestAgentRoutePreparation) Deliveries() <-chan *worklifetime.EventDelivery {
	return p.deliveries
}

func (*flowActivationTestAgentRoutePreparation) Publish() error { return nil }
func (*flowActivationTestAgentRoutePreparation) Discard() error { return nil }

type flowActivationTestRouteStore struct {
	statusByPath map[string]string
}

type flowActivationTestInstanceStore struct {
	readinessMu             sync.Mutex
	creates                 []runtimepipeline.WorkflowInstance
	upserts                 []runtimepipeline.WorkflowInstance
	terminatedPaths         []string
	terminatedAtSeen        []time.Time
	byStorageRef            map[string]runtimepipeline.WorkflowInstance
	routeLoads              []runtimeflowidentity.Route
	materialization         runtimepipeline.WorkflowInitialMaterializationResult
	armedEntries            []string
	armInitialEntry         func(string) error
	retiredTimerEntries     []string
	retireInitialEntry      func(string) error
	readiness               map[string]runtimepipeline.DynamicFlowRuntimeReadiness
	readinessLoadErr        error
	creationMarkErr         error
	topologyMarkErr         error
	creationMarked          func()
	topologyMarked          func()
	beforeTopologyMark      func(runtimepipeline.DynamicFlowRuntimeReadinessPlan)
	afterTopologyMark       func(runtimepipeline.DynamicFlowRuntimeReadinessPlan)
	beforeCreation          func()
	respectReadinessContext bool
	lifecycleModes          []executionmode.Mode
}

type flowActivationStandingRestarts map[string]runtimepipeline.StandingRestartDispositionKind

func (s flowActivationStandingRestarts) StandingRunRestartDisposition(_ context.Context, runID string) (runtimepipeline.StandingRestartDisposition, error) {
	return runtimepipeline.StandingRestartDisposition{Kind: s[strings.TrimSpace(runID)]}, nil
}

type flowActivationTestStore struct {
	upserts      []PersistedAgent
	terminated   []string
	terminal     map[string]bool
	terminateErr error
	failAgentID  string
}

func flowActivationPersistedAgentKey(rec PersistedAgent) string {
	identity, err := rec.Config.ConcreteIdentity()
	if err != nil {
		return strings.TrimSpace(rec.Config.ID)
	}
	fingerprint, err := identity.Fingerprint()
	if err != nil {
		return strings.TrimSpace(rec.Config.ID)
	}
	return fingerprint
}

func flowActivationLifecycleKey(req AgentLifecycleTransition) string {
	if !req.Identity.IsZero() {
		if fingerprint, err := req.Identity.Fingerprint(); err == nil {
			return fingerprint
		}
	}
	if req.Agent != nil {
		return flowActivationPersistedAgentKey(*req.Agent)
	}
	return strings.TrimSpace(req.AgentID)
}

type flowActivationTestTerminationOwner struct {
	instances *flowActivationTestInstanceStore
	routes    FlowInstanceRouteContextRemover
}

func (o flowActivationTestTerminationOwner) CommitFlowInstanceTermination(ctx context.Context, req runtimepipeline.FlowInstanceTerminationRequest) (runtimepipeline.FlowInstanceTermination, error) {
	if o.instances == nil || o.routes == nil {
		return runtimepipeline.FlowInstanceTermination{}, errors.New("flow activation test termination owner is incomplete")
	}
	route := runtimeflowidentity.StoredRoute(req.Route.ScopeKey, req.Route.InstanceID, req.Route.InstancePath)
	storageRef := strings.TrimSpace(route.InstancePath)
	if !route.Valid() || strings.TrimSpace(req.RunID) == "" {
		return runtimepipeline.FlowInstanceTermination{}, errors.New("flow activation test termination requires an exact route and run_id")
	}
	priorInstance, found, err := o.instances.Load(ctx, runtimeflowidentity.RouteForInstancePath(storageRef))
	if err != nil || !found {
		return runtimepipeline.FlowInstanceTermination{}, errors.Join(err, fmt.Errorf("flow activation test instance %s not found", storageRef))
	}
	priorPaths := append([]string(nil), o.instances.terminatedPaths...)
	priorTimes := append([]time.Time(nil), o.instances.terminatedAtSeen...)
	o.instances.readinessMu.Lock()
	priorReadiness := make(map[string]runtimepipeline.DynamicFlowRuntimeReadiness, len(o.instances.readiness))
	for key, readiness := range o.instances.readiness {
		priorReadiness[key] = readiness
	}
	o.instances.readinessMu.Unlock()
	rollback := func() {
		o.instances.terminatedPaths = priorPaths
		o.instances.terminatedAtSeen = priorTimes
		o.instances.byStorageRef[storageRef] = priorInstance
		o.instances.readinessMu.Lock()
		o.instances.readiness = priorReadiness
		o.instances.readinessMu.Unlock()
	}
	if err := o.instances.MarkTerminated(ctx, route, req.EntityID, req.TerminatedAt); err != nil {
		return runtimepipeline.FlowInstanceTermination{}, err
	}
	instance, found, err := o.instances.Load(ctx, runtimeflowidentity.RouteForInstancePath(storageRef))
	if err != nil || !found || strings.TrimSpace(instance.Status) != "terminated" || instance.TerminatedAt.IsZero() {
		rollback()
		return runtimepipeline.FlowInstanceTermination{}, errors.Join(err, fmt.Errorf("canonical terminal flow instance %s was not persisted", storageRef))
	}
	canonicalRoute := runtimeflowidentity.StoredRoute("", "", instance.StorageRef)
	if !canonicalRoute.Valid() {
		rollback()
		return runtimepipeline.FlowInstanceTermination{}, fmt.Errorf("derive canonical route identity for flow path %s", instance.StorageRef)
	}
	if err := o.routes.RemoveFlowInstanceRouteContext(ctx, canonicalRoute); err != nil {
		rollback()
		return runtimepipeline.FlowInstanceTermination{}, err
	}
	return runtimepipeline.FlowInstanceTermination{Instance: instance, Route: canonicalRoute}, nil
}

type flowActivationTestCommitter struct {
	instances flowInstancePersistence
	routes    FlowInstanceRouteContextInstaller
}

func (o flowActivationTestCommitter) CommitFlowInstanceActivation(
	ctx context.Context,
	plan runtimepipeline.FlowInstanceActivationPlan,
) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	if o.instances == nil || o.routes == nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, errors.New("test flow activation commit owners are required")
	}
	result, err := o.instances.MaterializeInitialEntry(ctx, plan.Instance, plan.OccurredAt)
	if err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	if result != runtimepipeline.WorkflowInitialMaterializationCreated && result != runtimepipeline.WorkflowInitialMaterializationAlreadyExists {
		return runtimepipeline.CommittedFlowInstanceActivation{}, fmt.Errorf("unknown test flow activation result %d", result)
	}
	if err := o.routes.StageFlowInstanceRouteContext(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: plan.Identity.Route(), ActivationVariables: plan.ActivationVariables,
	}); err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	return runtimepipeline.CommittedFlowInstanceActivation{
		Plan: plan, Created: result == runtimepipeline.WorkflowInitialMaterializationCreated,
	}, nil
}

func newFlowActivationManager(t *testing.T, bus Bus, instances flowInstancePersistence, stores ...ManagerPersistence) *AgentManager {
	t.Helper()
	if len(stores) == 0 {
		stores = []ManagerPersistence{&flowActivationTestStore{}}
	}
	var lifecycleStore AgentLifecyclePersistence
	lifecycleStore, _ = stores[0].(AgentLifecyclePersistence)
	var terminalOwner FlowInstanceTerminalMutationOwner
	terminalOwner, _ = instances.(FlowInstanceTerminalMutationOwner)
	if terminalOwner == nil {
		if testInstances, ok := instances.(*flowActivationTestInstanceStore); ok {
			routes, _ := bus.(FlowInstanceRouteContextRemover)
			terminalOwner = flowActivationTestTerminationOwner{instances: testInstances, routes: routes}
		}
	}
	if testInstances, ok := instances.(*flowActivationTestInstanceStore); ok {
		switch typed := bus.(type) {
		case *flowActivationTestBus:
			typed.creationStore = testInstances
		case *flowActivationSemanticRouteBus:
			typed.flowActivationTestBus.creationStore = testInstances
		}
	}
	routes, _ := bus.(FlowInstanceRouteContextInstaller)
	activationOwner, _ := bus.(FlowInstanceActivationCommitter)
	if activationOwner == nil {
		activationOwner = flowActivationTestCommitter{instances: instances, routes: routes}
	}
	manager := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{
		WorkflowInstances:  instances,
		LifecycleStore:     lifecycleStore,
		WorkOwner:          newTestManagerWorkOwner(t),
		BaseContext:        testAuthorActivityContext(context.Background()),
		SourceArtifactFact: authorActivityTestSourceArtifactFact,
		PersistenceRoles: PersistenceRoles{
			FlowActivation:  activationOwner,
			FlowTermination: terminalOwner,
		},
	}, stores...)
	manager.semanticSource = nil
	manager.semanticReadinessSource = dynamicFlowRuntimeReadinessSource{}
	return manager
}

func setFlowActivationManagerSemanticSource(
	am *AgentManager,
	source semanticview.Source,
	facts ...runtimecorrelation.SourceArtifactFact,
) {
	fact := authorActivityTestSourceArtifactFact
	if len(facts) > 0 {
		fact = facts[0]
	}
	am.semanticSource = source
	am.semanticReadinessSource = dynamicFlowRuntimeReadinessSource{fact: fact, source: source}
	if instances, ok := am.workflowInstances.(*flowActivationTestInstanceStore); ok {
		instances.readinessMu.Lock()
		for key, item := range instances.readiness {
			item.OwningRunSource = fact
			instances.readiness[key] = item
		}
		instances.readinessMu.Unlock()
	}
}

func activateFlowInstanceForTest(
	am *AgentManager,
	ctx context.Context,
	req runtimepipeline.FlowInstanceActivationRequest,
) error {
	if am.semanticReadinessSource.source == nil {
		fact := authorActivityTestSourceArtifactFact
		if contextFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx); ok {
			fact = contextFact
		}
		setFlowActivationManagerSemanticSource(am, req.ContractBundle, fact)
	}
	return am.ActivateFlowInstance(ctx, req)
}

func withFlowActivationPostCommit(ctx context.Context, actions *[]runtimepipelinefixture.OwnerAction) context.Context {
	rollback := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	ctx = runtimepipelinefixture.WithPostCommitActions(ctx, actions)
	return runtimepipelinefixture.WithRollbackActions(ctx, &rollback)
}

func (s *flowActivationTestRouteStore) UpsertFlowInstanceRoute(_ context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s.statusByPath == nil {
		s.statusByPath = map[string]string{}
	}
	s.statusByPath[strings.TrimSpace(route.Identity.InstancePath)] = "active"
	return nil
}

func (s *flowActivationTestRouteStore) DeleteFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.Route) error {
	if s.statusByPath == nil {
		s.statusByPath = map[string]string{}
	}
	s.statusByPath[strings.TrimSpace(identity.InstancePath)] = "inactive"
	return nil
}

func (s *flowActivationTestRouteStore) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	var routes []runtimeflowidentity.Route
	for path, status := range s.statusByPath {
		if status != "active" {
			continue
		}
		parts := strings.SplitN(path, "/", 2)
		if len(parts) == 2 {
			routes = append(routes, runtimeflowidentity.StoredRoute(parts[0], parts[1], path))
		}
	}
	return routes, nil
}

func (s *flowActivationTestRouteStore) ListFlowInstanceRouteRecords(_ context.Context, identity runtimeflowidentity.Route) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s.statusByPath[strings.TrimSpace(identity.InstancePath)] != "active" {
		return nil, nil
	}
	return []runtimebus.FlowInstanceRouteRecord{{
		Identity: identity, EventPattern: identity.InstancePath + "/task.started",
		SubscriberType: "agent", SubscriberID: "reviewer-" + identity.InstanceID, SourceFlow: identity.ScopeKey,
	}}, nil
}

func (s *flowActivationTestInstanceStore) Upsert(_ context.Context, instance runtimepipeline.WorkflowInstance) error {
	s.upserts = append(s.upserts, instance)
	s.storeInstance(instance)
	return nil
}

func (s *flowActivationTestInstanceStore) Create(_ context.Context, instance runtimepipeline.WorkflowInstance) error {
	if s.byStorageRef == nil {
		s.byStorageRef = map[string]runtimepipeline.WorkflowInstance{}
	}
	ref := strings.TrimSpace(instance.StorageRef)
	if ref != "" {
		if _, ok := s.byStorageRef[ref]; ok {
			return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "flow-activation-test", "create", map[string]any{"flow_instance": ref})
		}
	}
	s.creates = append(s.creates, instance)
	s.storeInstance(instance)
	return nil
}

func (s *flowActivationTestInstanceStore) MaterializeInitialEntry(_ context.Context, instance runtimepipeline.WorkflowInstance, occurredAt time.Time) (runtimepipeline.WorkflowInitialMaterializationResult, error) {
	if s.materialization != runtimepipeline.WorkflowInitialMaterializationUnknown {
		return s.materialization, nil
	}
	instance.CreatedAt = occurredAt.UTC()
	instance.EnteredStageAt = occurredAt.UTC()
	if err := s.Create(context.Background(), instance); err != nil {
		return runtimepipeline.WorkflowInitialMaterializationUnknown, err
	}
	if instance.RuntimeReadiness != nil {
		plan, err := instance.RuntimeReadiness.Normalized()
		if err != nil {
			return runtimepipeline.WorkflowInitialMaterializationUnknown, err
		}
		s.readinessMu.Lock()
		if s.readiness == nil {
			s.readiness = map[string]runtimepipeline.DynamicFlowRuntimeReadiness{}
		}
		owningSource, err := runtimecorrelation.DecodeSourceArtifactFact(plan.BundleHash)
		if err != nil {
			s.readinessMu.Unlock()
			return runtimepipeline.WorkflowInitialMaterializationUnknown, err
		}
		s.readiness[flowActivationReadinessKey(plan.RunID, instance.StorageRef)] = runtimepipeline.DynamicFlowRuntimeReadiness{
			InstancePath:    instance.StorageRef,
			Plan:            plan,
			OwningRunSource: owningSource,
			RunStatus:       "running",
			InstanceStatus:  "active",
		}
		s.readinessMu.Unlock()
	}
	return runtimepipeline.WorkflowInitialMaterializationCreated, nil
}

func (s *flowActivationTestInstanceStore) PrepareInitialEntryLifecycle(ctx context.Context, instance runtimepipeline.WorkflowInstance, occurredAt time.Time) (runtimepipeline.WorkflowInstance, runtimepipeline.WorkflowLifecycleMutationPlan, error) {
	mode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		return runtimepipeline.WorkflowInstance{}, runtimepipeline.WorkflowLifecycleMutationPlan{}, errors.New("test flow activation lifecycle requires execution mode")
	}
	s.lifecycleModes = append(s.lifecycleModes, mode)
	instance.CreatedAt = occurredAt.UTC()
	instance.EnteredStageAt = occurredAt.UTC()
	return instance, runtimepipeline.WorkflowLifecycleMutationPlan{}, nil
}

func (s *flowActivationTestInstanceStore) FinalizeInitialEntryLifecycle(context.Context, runtimepipeline.CommittedWorkflowLifecycleMutation) error {
	return nil
}

func (s *flowActivationTestInstanceStore) ArmInitialEntryTimers(_ context.Context, route runtimeflowidentity.Route) error {
	instanceID := strings.TrimSpace(route.InstancePath)
	s.armedEntries = append(s.armedEntries, instanceID)
	if s.armInitialEntry != nil {
		return s.armInitialEntry(instanceID)
	}
	return nil
}

func (s *flowActivationTestInstanceStore) ReconcileInitialEntryTimers(ctx context.Context, route runtimeflowidentity.Route) error {
	return s.ArmInitialEntryTimers(ctx, route)
}

func (s *flowActivationTestInstanceStore) RetireInitialEntryTimerWakeups(_ context.Context, route runtimeflowidentity.Route) error {
	instanceID := strings.TrimSpace(route.InstancePath)
	s.retiredTimerEntries = append(s.retiredTimerEntries, instanceID)
	if s.retireInitialEntry != nil {
		return s.retireInitialEntry(instanceID)
	}
	return nil
}

func flowActivationReadinessKey(runID, instanceID string) string {
	return strings.TrimSpace(runID) + "\x00" + strings.TrimSpace(instanceID)
}

func reconcileDynamicFlowRuntimeStartupForTest(am *AgentManager, ctx context.Context, source runtimecorrelation.SourceArtifactFact, replayAllowed bool) error {
	startup, err := am.CanonicalizeDynamicFlowRuntimeStartupReadiness(ctx, source, replayAllowed)
	if err != nil {
		return err
	}
	return am.CompleteDynamicFlowRuntimeStartupTopology(ctx, startup)
}

func (s *flowActivationTestInstanceStore) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	if s.respectReadinessContext {
		if err := ctx.Err(); err != nil {
			return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, err
		}
	}
	if s.readinessLoadErr != nil {
		err := s.readinessLoadErr
		s.readinessLoadErr = nil
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	item, ok := s.readiness[flowActivationReadinessKey(runID, route.InstancePath)]
	return item, ok, nil
}

func (s *flowActivationTestInstanceStore) ReconcileDynamicFlowRuntimeReadinessPlan(
	_ context.Context,
	observed runtimepipeline.DynamicFlowRuntimeReadiness,
	plan runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	_ time.Time,
) (bool, error) {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	return reconcileFlowActivationTestReadinessPlan(s.readiness, observed, plan)
}

func reconcileFlowActivationTestReadinessPlan(readiness map[string]runtimepipeline.DynamicFlowRuntimeReadiness, observed runtimepipeline.DynamicFlowRuntimeReadiness, plan runtimepipeline.DynamicFlowRuntimeReadinessPlan) (bool, error) {
	normalized, err := plan.Normalized()
	if err != nil {
		return false, err
	}
	key := flowActivationReadinessKey(normalized.RunID, normalized.Identity.InstancePath)
	current, ok := readiness[key]
	if !ok || !current.Eligible() {
		return false, fmt.Errorf("readiness not found")
	}
	if !reflect.DeepEqual(current, observed) {
		return false, &runtimepipeline.DynamicFlowRuntimeReadinessObservationConflict{
			RunID: normalized.RunID, InstancePath: normalized.Identity.InstancePath, Coordinate: "test_observation",
		}
	}
	if current.Plan.ExecutionMode != normalized.ExecutionMode {
		return false, fmt.Errorf("cannot revise readiness execution mode")
	}
	actualJSON, err := json.Marshal(current.Plan)
	if err != nil {
		return false, err
	}
	expectedJSON, err := json.Marshal(normalized)
	if err != nil {
		return false, err
	}
	if string(actualJSON) == string(expectedJSON) {
		return false, nil
	}
	if !current.CreationEventEmittedAt.IsZero() {
		actualCreationJSON, err := json.Marshal(current.Plan.CreationEvent)
		if err != nil {
			return false, err
		}
		expectedCreationJSON, err := json.Marshal(normalized.CreationEvent)
		if err != nil {
			return false, err
		}
		if string(actualCreationJSON) != string(expectedCreationJSON) {
			return false, fmt.Errorf("cannot revise emitted creation occurrence")
		}
	}
	current.Plan = normalized
	current.TopologyReadyAt = time.Time{}
	readiness[key] = current
	return true, nil
}

func (s *flowActivationTestInstanceStore) ReconcileDynamicFlowRuntimeReadinessPlans(
	ctx context.Context,
	requests []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation,
	observedAt time.Time,
) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error) {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	next := make(map[string]runtimepipeline.DynamicFlowRuntimeReadiness, len(s.readiness))
	for key, readiness := range s.readiness {
		next[key] = readiness
	}
	results := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, 0, len(requests))
	for _, request := range requests {
		changed, err := reconcileFlowActivationTestReadinessPlan(next, request.Observed, request.Expected)
		if err != nil {
			return nil, err
		}
		results = append(results, runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult{
			RunID: request.Expected.RunID, InstancePath: request.Expected.Identity.InstancePath, Changed: changed,
		})
	}
	s.readiness = next
	return results, nil
}

func (s *flowActivationTestInstanceStore) dynamicFlowRuntimeReadinessForSource(source runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	out := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0, len(s.readiness))
	for _, item := range s.readiness {
		if item.OwningRunSource.Matches(source) && item.Eligible() {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Plan.RunID != out[j].Plan.RunID {
			return out[i].Plan.RunID < out[j].Plan.RunID
		}
		return out[i].InstancePath < out[j].InstancePath
	})
	return out, nil
}

func seedForeignMalformedDynamicFlowRuntimeReadiness(t *testing.T, store *flowActivationTestInstanceStore) string {
	t.Helper()
	foreignSource, err := runtimecorrelation.NewSourceArtifactFact(
		"bundle-v2:sha256:" + strings.Repeat("f", 64),
	)
	if err != nil {
		t.Fatalf("construct foreign readiness source: %v", err)
	}
	key := flowActivationReadinessKey(uuid.NewString(), "review/foreign-"+uuid.NewString())
	store.readinessMu.Lock()
	if store.readiness == nil {
		store.readiness = map[string]runtimepipeline.DynamicFlowRuntimeReadiness{}
	}
	store.readiness[key] = runtimepipeline.DynamicFlowRuntimeReadiness{
		InstancePath: "review/foreign", OwningRunSource: foreignSource,
		RunStatus: "running", InstanceStatus: "active",
	}
	store.readinessMu.Unlock()
	return key
}

func (s *flowActivationTestInstanceStore) InspectDynamicFlowRuntimeReadinessForSource(_ context.Context, source runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	items, err := s.dynamicFlowRuntimeReadinessForSource(source)
	projection := runtimepipeline.DynamicFlowRuntimeReadinessProjection{}
	for _, item := range items {
		planSource, sourceErr := runtimecorrelation.DecodeSourceArtifactFact(item.Plan.BundleHash)
		if sourceErr != nil {
			return projection, sourceErr
		}
		if !planSource.Matches(source) {
			projection.SourceTransitionRequired = append(projection.SourceTransitionRequired, item)
			continue
		}
		if item.Pending() {
			projection.CurrentPending = append(projection.CurrentPending, item)
		} else {
			projection.CurrentCompleted = append(projection.CurrentCompleted, item)
		}
	}
	return projection, err
}

func (s *flowActivationTestInstanceStore) InspectDynamicFlowRuntimeReadinessForRun(_ context.Context, runID string, source runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	items, err := s.dynamicFlowRuntimeReadinessForSource(source)
	if err != nil {
		return nil, err
	}
	result := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0, len(items))
	for _, item := range items {
		if item.Plan.RunID == strings.TrimSpace(runID) {
			result = append(result, item)
		}
	}
	return result, nil
}

func (s *flowActivationTestInstanceStore) MarkDynamicFlowRuntimeTopologyReady(
	_ context.Context,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	readyAt time.Time,
) error {
	normalized, err := expected.Normalized()
	if err != nil {
		return err
	}
	if s.topologyMarkErr != nil {
		return s.topologyMarkErr
	}
	if hook := s.beforeTopologyMark; hook != nil {
		s.beforeTopologyMark = nil
		hook(normalized)
	}
	s.readinessMu.Lock()
	key := flowActivationReadinessKey(normalized.RunID, normalized.Identity.InstancePath)
	item, ok := s.readiness[key]
	if !ok || !item.Eligible() {
		s.readinessMu.Unlock()
		return fmt.Errorf("readiness not found")
	}
	actualJSON, err := json.Marshal(item.Plan)
	if err != nil {
		s.readinessMu.Unlock()
		return err
	}
	expectedJSON, err := json.Marshal(normalized)
	if err != nil {
		s.readinessMu.Unlock()
		return err
	}
	if string(actualJSON) != string(expectedJSON) {
		s.readinessMu.Unlock()
		return fmt.Errorf("readiness plan changed")
	}
	if item.TopologyReadyAt.IsZero() {
		item.TopologyReadyAt = readyAt
	}
	s.readiness[key] = item
	s.readinessMu.Unlock()
	if s.topologyMarked != nil {
		s.topologyMarked()
	}
	if hook := s.afterTopologyMark; hook != nil {
		s.afterTopologyMark = nil
		hook(normalized)
	}
	return nil
}

func (b *flowActivationTestBus) CommitDynamicFlowRuntimeCreationOccurrence(
	ctx context.Context,
	req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest,
) error {
	s := b.creationStore
	if s == nil {
		return b.Publish(ctx, req.Event)
	}
	if s.beforeCreation != nil {
		s.beforeCreation()
	}
	s.readinessMu.Lock()
	if s.creationMarkErr != nil {
		s.readinessMu.Unlock()
		return s.creationMarkErr
	}
	key := flowActivationReadinessKey(req.RunID, req.InstancePath)
	item, ok := s.readiness[key]
	if !ok || !item.Eligible() || item.TopologyReadyAt.IsZero() {
		s.readinessMu.Unlock()
		return fmt.Errorf("readiness not ready")
	}
	if !item.CreationEventEmittedAt.IsZero() {
		s.readinessMu.Unlock()
		return nil
	}
	if err := b.Publish(ctx, req.Event); err != nil {
		s.readinessMu.Unlock()
		return err
	}
	if item.CreationEventEmittedAt.IsZero() {
		item.CreationEventEmittedAt = req.OccurredAt
	}
	s.readiness[key] = item
	s.readinessMu.Unlock()
	if s.creationMarked != nil {
		s.creationMarked()
	}
	return nil
}

func (s *flowActivationTestInstanceStore) storeInstance(instance runtimepipeline.WorkflowInstance) {
	if s.byStorageRef == nil {
		s.byStorageRef = map[string]runtimepipeline.WorkflowInstance{}
	}
	stored := instance
	stored.StorageRef = strings.TrimSpace(stored.StorageRef)
	if stored.StorageRef != "" {
		stored.Status = "active"
		s.byStorageRef[stored.StorageRef] = stored
	}
}

func (s *flowActivationTestInstanceStore) MarkTerminated(_ context.Context, route runtimeflowidentity.Route, _ identity.EntityID, terminatedAt time.Time) error {
	storageRef := route.InstancePath
	s.terminatedPaths = append(s.terminatedPaths, strings.TrimSpace(storageRef))
	s.terminatedAtSeen = append(s.terminatedAtSeen, terminatedAt)
	if s.byStorageRef != nil {
		instance := s.byStorageRef[strings.TrimSpace(storageRef)]
		instance.Status = "terminated"
		instance.TerminatedAt = terminatedAt
		s.byStorageRef[strings.TrimSpace(storageRef)] = instance
	}
	s.readinessMu.Lock()
	for key, readiness := range s.readiness {
		if strings.Trim(strings.TrimSpace(readiness.InstancePath), "/") != strings.Trim(strings.TrimSpace(storageRef), "/") {
			continue
		}
		readiness.InstanceStatus = "terminated"
		readiness.InstanceTerminatedAt = terminatedAt
		s.readiness[key] = readiness
	}
	s.readinessMu.Unlock()
	return nil
}

func (s *flowActivationTestInstanceStore) Load(_ context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error) {
	if s.byStorageRef == nil {
		return runtimepipeline.WorkflowInstance{}, false, nil
	}
	instance, ok := s.byStorageRef[strings.TrimSpace(route.InstancePath)]
	return instance, ok, nil
}

func (s *flowActivationTestInstanceStore) LoadRouteRecoveryProjection(_ context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstanceRouteRecoveryProjection, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	s.routeLoads = append(s.routeLoads, route)
	instance, ok := s.byStorageRef[route.InstancePath]
	if !ok {
		return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("active flow instance not found for route recovery: %s", route.InstancePath)
	}
	entityID := identity.NormalizeEntityID(instance.EntityID)
	if entityID.IsZero() {
		return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("flow instance route recovery entity identity is missing")
	}
	instanceIdentity := runtimeflowidentity.Instance{
		TemplateID: instance.WorkflowName, ScopeKey: route.ScopeKey, InstanceID: route.InstanceID,
		InstancePath: route.InstancePath, EntityID: entityID.String(), HasStoredPath: true,
	}
	if instanceIdentity.Route() != route {
		return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("flow instance route recovery identity mismatch")
	}
	return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{Identity: instanceIdentity, Config: instance.Config}, nil
}

func (s *flowActivationTestStore) UpsertAgent(_ context.Context, rec PersistedAgent) error {
	s.upserts = append(s.upserts, rec)
	if s.terminal != nil {
		delete(s.terminal, flowActivationPersistedAgentKey(rec))
	}
	return nil
}

func (s *flowActivationTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	latest := make(map[string]PersistedAgent, len(s.upserts))
	order := make([]string, 0, len(s.upserts))
	for _, rec := range s.upserts {
		key := flowActivationPersistedAgentKey(rec)
		if _, found := latest[key]; !found {
			order = append(order, key)
		}
		latest[key] = rec
	}
	out := make([]PersistedAgent, 0, len(latest))
	for _, key := range order {
		if s.terminal[key] {
			continue
		}
		rec := latest[key]
		rec.ProcessBinding = lifecycleProbeProcessBinding()
		out = append(out, rec)
	}
	return out, nil
}
func (*flowActivationTestStore) ProcessExecutionBinding() (ProcessExecutionBinding, error) {
	binding := lifecycleProbeProcessBinding()
	return binding, binding.Validate()
}
func (s *flowActivationTestStore) CommitAgentLifecycleTransition(_ context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	lifecycleKey := flowActivationLifecycleKey(req)
	if req.TargetPhase == AgentLifecycleTerminated {
		if s.terminateErr != nil {
			return AgentLifecycleTransitionResult{}, s.terminateErr
		}
		s.terminated = append(s.terminated, strings.TrimSpace(req.AgentID))
		if s.terminal == nil {
			s.terminal = map[string]bool{}
		}
		s.terminal[lifecycleKey] = true
	} else if strings.TrimSpace(req.AgentID) == strings.TrimSpace(s.failAgentID) {
		return AgentLifecycleTransitionResult{}, errors.New("injected agent registration failure")
	} else if req.Agent != nil {
		rec := *req.Agent
		rec.LifecycleEpoch = req.TargetEpoch
		rec.LifecycleGeneration = req.TargetGeneration
		rec.LifecyclePhase = req.TargetPhase
		rec.LifecycleRunMode = req.RunMode
		rec.ProcessBinding = lifecycleProbeProcessBinding()
		s.upserts = append(s.upserts, rec)
		if s.terminal != nil {
			delete(s.terminal, lifecycleKey)
		}
	}
	return AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: uuid.NewString(), AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration, Generation: req.TargetGeneration,
		PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
		ProcessBinding: lifecycleProbeProcessBinding(),
		Subordinate:    sessions.LifecycleMutationOutcome{Action: req.Subordinate.Action},
	}, nil
}
func (*flowActivationTestStore) EnsureEntitySchema(context.Context, string) error { return nil }

func (*flowActivationTestBus) AdmitSourceArtifactFact(ctx context.Context) (context.Context, error) {
	return admitManagerTestBusContext(ctx)
}

func (b *flowActivationTestBus) Publish(ctx context.Context, evt events.Event) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	for _, published := range b.published {
		if published.ID() == evt.ID() {
			return nil
		}
	}
	b.published = append(b.published, evt)
	b.publishedContexts = append(b.publishedContexts, events.DeliveryContextFromContext(ctx))
	return nil
}

func (b *flowActivationTestBus) PublishInMutation(ctx context.Context, evt events.Event) error {
	return b.Publish(ctx, evt)
}

func (*flowActivationTestBus) ResolveSubscribedRecipients(string) []string { return nil }
func (*flowActivationTestBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return nil
}
func (*flowActivationTestBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	return unavailableFlowActivationPipelineObligations{}
}

type unavailableFlowActivationPipelineObligations struct{}

func (unavailableFlowActivationPipelineObligations) ClaimPublication(context.Context, string) (runtimepipelineobligation.Claim, error) {
	return runtimepipelineobligation.Claim{}, errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) ClaimEvent(context.Context, string, runtimepipelineobligation.Purpose) (runtimepipelineobligation.ClaimedWork, error) {
	return runtimepipelineobligation.ClaimedWork{}, errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) OpenScan(context.Context, runtimepipelineobligation.ScanRequest) (runtimepipelineobligation.Scan, error) {
	return runtimepipelineobligation.Scan{}, errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) ClaimBatch(context.Context, runtimepipelineobligation.Scan, int) (runtimepipelineobligation.ScanBatch, error) {
	return runtimepipelineobligation.ScanBatch{}, errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) CloseScan(context.Context, runtimepipelineobligation.Scan) error {
	return errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) MarkDecisionProcessed(context.Context, runtimepipelineobligation.Claim) error {
	return errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) Settle(context.Context, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) (runtimepipelineobligation.SettlementOutcome, error) {
	return runtimepipelineobligation.SettlementOutcome{}, errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) Release(context.Context, runtimepipelineobligation.Claim) error {
	return errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) GlobalWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) SummarizeRun(context.Context, string) (runtimepipelineobligation.RunSummary, error) {
	return runtimepipelineobligation.RunSummary{}, errors.New("flow activation fixture has no pipeline obligations")
}

func (unavailableFlowActivationPipelineObligations) TerminalizeRun(context.Context, string, runtimepipelineobligation.Disposition, time.Time) (int, error) {
	return 0, errors.New("flow activation fixture has no pipeline obligations")
}

func (*flowActivationTestBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*flowActivationTestBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (*flowActivationTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (b *flowActivationTestBus) Unsubscribe(agentID string) {
	b.unsubscribed = append(b.unsubscribed, agentID)
}
func (*flowActivationTestBus) Store() runtimebus.EventStore { return nil }
func (*flowActivationTestBus) ResetInMemoryState() error    { return nil }
func (*flowActivationTestBus) PrepareAgentRoute(
	runtimeeffects.LifecycleToken,
	semanticview.FlowOwnedAgentSubscriptionAdmission,
) runtimebus.AgentRoutePreparation {
	return &flowActivationTestAgentRoutePreparation{
		deliveries: make(chan *worklifetime.EventDelivery),
	}
}
func (*flowActivationTestBus) RemoveAgentRoute(runtimeeffects.LifecycleToken) {}
func (*flowActivationTestBus) FenceAgentRoute(runtimeeffects.LifecycleToken)  {}
func (b *flowActivationTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return nil
}

func (b *flowActivationTestBus) AddFlowInstanceRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	return b.AddFlowInstanceRouteContext(context.Background(), req)
}

func (b *flowActivationSemanticRouteBus) StageFlowInstanceRouteContext(
	_ context.Context,
	req runtimebus.FlowInstanceRouteMaterializationRequest,
) error {
	if b == nil || b.durable == nil {
		return errors.New("semantic durable route table is required")
	}
	req = req.Normalized()
	if err := b.durable.AddFlowInstanceRoute(req); err != nil {
		return err
	}
	if b.durableRoutes == nil {
		b.durableRoutes = map[string][]runtimebus.FlowInstanceRouteRecord{}
	}
	b.durableRoutes[req.Identity.InstancePath] = b.durable.MaterializedRoutes(req.Identity)
	return nil
}

func (b *flowActivationSemanticRouteBus) PublishPersistedFlowInstanceRoute(
	req runtimebus.FlowInstanceRouteMaterializationRequest,
) error {
	if b == nil || b.process == nil {
		return errors.New("semantic process route table is required")
	}
	req = req.Normalized()
	if b.process.HasFlowInstanceRoute(req.Identity) {
		return nil
	}
	return b.process.AddFlowInstanceRoute(req)
}

func (b *flowActivationSemanticRouteBus) RetirePublishedFlowInstanceRoute(
	identity runtimeflowidentity.Route,
) error {
	if b == nil || b.process == nil {
		return errors.New("semantic process route table is required")
	}
	return b.process.RemoveFlowInstanceRoute(identity)
}

func (b *flowActivationSemanticRouteBus) HasFlowInstanceRoute(identity runtimeflowidentity.Route) bool {
	return b != nil && b.process != nil && b.process.HasFlowInstanceRoute(identity)
}

func (b *flowActivationSemanticRouteBus) VerifyFlowInstanceRoute(
	_ context.Context,
	identity runtimeflowidentity.Route,
) error {
	if !b.HasFlowInstanceRoute(identity) {
		return errors.New("semantic route is not process-ready")
	}
	durable := b.durableRoutes[identity.InstancePath]
	process := b.process.MaterializedRoutes(identity)
	if !reflect.DeepEqual(durable, process) {
		return fmt.Errorf("semantic durable/process route mismatch: durable=%#v process=%#v", durable, process)
	}
	return nil
}

func (b *flowActivationTestBus) StageFlowInstanceRouteContext(ctx context.Context, req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	req = req.Normalized()
	if b.addErr != nil {
		return b.addErr
	}
	if b.stageRoute != nil {
		if err := b.stageRoute(req); err != nil {
			return err
		}
	}
	if b.routeStore == nil {
		return nil
	}
	identity := req.Identity
	return b.routeStore.UpsertFlowInstanceRoute(ctx, runtimebus.FlowInstanceRouteRecord{
		Identity:       identity,
		EventPattern:   identity.InstancePath + "/task.started",
		SubscriberType: "agent",
		SubscriberID:   "reviewer-" + identity.InstanceID,
		SourceFlow:     identity.ScopeKey,
	})
}

func (b *flowActivationTestBus) PublishPersistedFlowInstanceRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	req = req.Normalized()
	if b.HasFlowInstanceRoute(req.Identity) {
		return nil
	}
	if b.addErr != nil {
		return b.addErr
	}
	b.addedPaths = append(b.addedPaths, req.Identity.InstancePath)
	b.addedRouteRequests = append(b.addedRouteRequests, req)
	return nil
}

func (b *flowActivationTestBus) RetirePublishedFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	keptPaths := b.addedPaths[:0]
	keptRequests := b.addedRouteRequests[:0]
	for idx, req := range b.addedRouteRequests {
		if req.Identity == identity {
			continue
		}
		keptRequests = append(keptRequests, req)
		if idx < len(b.addedPaths) {
			keptPaths = append(keptPaths, b.addedPaths[idx])
		}
	}
	b.addedPaths = keptPaths
	b.addedRouteRequests = keptRequests
	return nil
}

func (b *flowActivationTestBus) AddFlowInstanceRouteContext(ctx context.Context, req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	req = req.Normalized()
	identity := req.Identity
	if b.HasFlowInstanceRoute(identity) {
		return nil
	}
	if b.addErr != nil {
		return b.addErr
	}
	if _, transactional := runtimepipelinefixture.SQLTx(ctx); transactional {
		if err := b.StageFlowInstanceRouteContext(ctx, req); err != nil {
			return err
		}
		if !runtimepipelinefixture.QueuePostCommitAction(ctx, func(context.Context) {
			_ = b.PublishPersistedFlowInstanceRoute(req)
		}) {
			return errors.New("transactional route requires post-commit process publication")
		}
		return nil
	}
	if err := b.StageFlowInstanceRouteContext(ctx, req); err != nil {
		return err
	}
	return b.PublishPersistedFlowInstanceRoute(req)
}

func (b *flowActivationTestBus) HasFlowInstanceRoute(identity runtimeflowidentity.Route) bool {
	for _, req := range b.addedRouteRequests {
		if req.Identity == identity {
			return true
		}
	}
	return false
}

func (b *flowActivationTestBus) VerifyFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if !b.HasFlowInstanceRoute(identity) {
		return errors.New("route not process-ready")
	}
	if b.routeStore != nil {
		routes, err := b.routeStore.ListFlowInstanceRoutes(ctx)
		if err != nil {
			return err
		}
		for _, route := range routes {
			if route == identity {
				return nil
			}
		}
		return errors.New("route not durably registered")
	}
	return nil
}

func (b *flowActivationTestBus) RemoveFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	return b.RemoveFlowInstanceRouteContext(context.Background(), identity)
}

func (b *flowActivationTestBus) RetireCommittedFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	b.removedPairs = append(b.removedPairs, identity.ScopeKey+"/"+identity.InstanceID)
	return b.RetirePublishedFlowInstanceRoute(identity)
}

func (b *flowActivationTestBus) RemoveFlowInstanceRouteContext(ctx context.Context, identity runtimeflowidentity.Route) error {
	if b.removeErr != nil {
		return b.removeErr
	}
	if b.routeStore != nil {
		if err := b.routeStore.DeleteFlowInstanceRoute(ctx, identity); err != nil {
			return err
		}
	}
	recordRemoval := func() {
		b.removedPairs = append(b.removedPairs, identity.ScopeKey+"/"+identity.InstanceID)
	}
	if runtimepipelinefixture.QueuePostCommitAction(ctx, func(context.Context) {
		recordRemoval()
	}) {
		return nil
	}
	recordRemoval()
	return nil
}

func flowActivationRunContext() context.Context {
	return runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
}

func testFlowAgentURIRef(flowID, logicalID string) runtimecontracts.ContractURIRef {
	return runtimecontracts.ContractURIRef{
		Kind: "agent", FlowID: flowID, LocalID: logicalID,
		Full: "test://flow-activation/" + flowID + "/" + logicalID,
	}
}

func registerTestFlowAgentOwner(bundle *runtimecontracts.WorkflowContractBundle, flowID, logicalID string) {
	if bundle.URIRegistry.Agents == nil {
		bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{}
	}
	if bundle.URIRegistry.ByURI == nil {
		bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{}
	}
	ref := testFlowAgentURIRef(flowID, logicalID)
	bundle.URIRegistry.Agents[flowID+"/"+logicalID] = ref
	bundle.URIRegistry.ByURI[ref.Full] = ref
}

func testFlowBundle(t *testing.T, autoEmit string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	reviewFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "review"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.started": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{},
				},
			},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"reviewer": {
				ID:             "reviewer",
				Type:           "generic",
				Role:           "reviewer",
				ResolvedIntent: managerTestResolvedIntent("reviewer"),
				Subscriptions:  []string{"task.started"},
			},
		},
	}
	root := &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{reviewFlow}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"review/reviewer": testFlowAgentURIRef("review", "reviewer"),
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{
				testFlowAgentURIRef("review", "reviewer").Full: testFlowAgentURIRef("review", "reviewer"),
			},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": &root.Children[0],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "task.started"}}},
				},
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{Event: autoEmit},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
	return admitFlowActivationEntityContracts(t, bundle, map[string]string{"review": "review_entity"})
}

func testFlowRouteRevisionBundle(t *testing.T, nodeEvent string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := testFlowBundle(t, "")
	review := bundle.FlowTree.ByID["review"]
	review.Schema = bundle.FlowSchemas["review"]
	review.Path = "review"
	review.Events["task.revised"] = runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{},
		},
	}
	review.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"route-observer": {
			ID:           "route-observer",
			SubscribesTo: []string{nodeEvent},
		},
	}
	bundle.FlowTree.Root.Children[0] = *review
	return bundle
}

func testFlowBundleWithTwoAgents(t *testing.T, autoEmit string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := testFlowBundle(t, autoEmit)
	bundle.FlowTree.ByID["review"].Agents["writer"] = runtimecontracts.AgentRegistryEntry{
		ID: "writer", Type: "generic", Role: "writer", ResolvedIntent: managerTestResolvedIntent("writer"), Subscriptions: []string{"task.started"},
	}
	registerTestFlowAgentOwner(bundle, "review", "writer")
	return bundle
}

func testFlowBundleWithAutoEmitEntry(t *testing.T, autoEmit string, entry runtimecontracts.EventCatalogEntry) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle := testFlowBundle(t, autoEmit)
	reviewFlow := bundle.FlowTree.ByID["review"]
	if reviewFlow == nil {
		return bundle
	}
	if reviewFlow.Events == nil {
		reviewFlow.Events = map[string]runtimecontracts.EventCatalogEntry{}
	}
	reviewFlow.Events[strings.TrimSpace(autoEmit)] = entry
	return bundle
}

func testNestedFlowBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	workerRef := testFlowAgentURIRef("grandchild", "worker")
	grandchild := &runtimecontracts.FlowContractView{
		Path:  "child/grandchild",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "grandchild"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"micro.started": {},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				ID:             "worker",
				Type:           "generic",
				Role:           "worker",
				ResolvedIntent: managerTestResolvedIntent("worker"),
				Subscriptions:  []string{"micro.started"},
			},
		},
		AgentURIs: map[string]string{"worker": workerRef.Full},
	}
	child := &runtimecontracts.FlowContractView{
		Path:     "child",
		Paths:    runtimecontracts.FlowContractPaths{FlowPath: "child"},
		Children: []runtimecontracts.FlowContractView{*grandchild},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"grandchild/worker": workerRef,
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{workerRef.Full: workerRef},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*child},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      child,
				"grandchild": grandchild,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"grandchild": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "micro.started"}}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
	return admitFlowActivationEntityContracts(t, bundle, map[string]string{"grandchild": "grandchild_entity"})
}

func admitFlowActivationEntityContracts(
	t *testing.T,
	base *runtimecontracts.WorkflowContractBundle,
	flowEntityTypes map[string]string,
) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	flowIDs := make([]string, 0, len(flowEntityTypes))
	for flowID := range flowEntityTypes {
		flowIDs = append(flowIDs, strings.TrimSpace(flowID))
	}
	sort.Strings(flowIDs)
	for _, flowID := range flowIDs {
		entityType := strings.TrimSpace(flowEntityTypes[flowID])
		if flowID == "" || entityType == "" {
			t.Fatalf("flow activation entity contract requires nonblank flow and entity type: flow=%q type=%q", flowID, entityType)
		}
		writeFlowActivationFixtureFile(t, filepath.Join(root, flowID, "schema.yaml"), fmt.Sprintf("name: %s\nmode: template\ninitial_state: active\nstates: [active]\n", flowID))
		writeFlowActivationFixtureFile(t, filepath.Join(root, flowID, "entities.yaml"), fmt.Sprintf("%s: {}\n", entityType))
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: flow-activation-test\n")
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	admitted, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("load flow activation entity contracts: %v", err)
	}
	admitted.Semantics = base.Semantics
	admitted.FlowTree = base.FlowTree
	admitted.FlowSchemas = base.FlowSchemas
	admitted.URIRegistry = base.URIRegistry
	admitted.Nodes = base.Nodes
	admitted.Events = base.Events
	admitted.Agents = base.Agents
	admitted.Tools = base.Tools
	admitted.Policy = base.Policy
	return admitted
}

func testStaticFlowBundle() *runtimecontracts.WorkflowContractBundle {
	analyzerRef := testFlowAgentURIRef("analyzer-flow", "analyzer")
	analysisFlow := &runtimecontracts.FlowContractView{
		Path:  "analyzer-flow",
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "analyzer-flow"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {
				Type:           "generic",
				Role:           "analyzer",
				ResolvedIntent: managerTestResolvedIntent("analyzer"),
				Subscriptions:  []string{"analysis.requested"},
				EmitEvents:     []string{"analysis.done"},
			},
		},
		AgentURIs: map[string]string{"analyzer": analyzerRef.Full},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "manager-test", Version: "v-test"},
		URIRegistry: runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{
				"analyzer-flow/analyzer": analyzerRef,
			},
			ByURI: map[string]runtimecontracts.ContractURIRef{analyzerRef.Full: analyzerRef},
		},
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*analysisFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"analyzer-flow": analysisFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"analyzer-flow": {
				RequiredAgents: []runtimecontracts.FlowRequiredAgent{{
					Role:         "analyzer",
					SubscribesTo: []string{"analysis.requested"},
					Emits:        []string{"analysis.done"},
				}},
				Pins: runtimecontracts.FlowPins{
					Inputs:  runtimecontracts.FlowInputPins{EventPins: []runtimecontracts.FlowInputEventPin{{Event: "analysis.requested"}}},
					Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{{Event: "analysis.done"}}},
				},
			},
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(fmt.Sprintf("compile static flow test semantics: %v", err))
	}
	return bundle
}

func testActivationRequest(bundle *runtimecontracts.WorkflowContractBundle, templateID, instanceID, sourceEntityID, flowPath string) runtimepipeline.FlowInstanceActivationRequest {
	instance := runtimeflowidentity.Stored(
		semanticview.Wrap(bundle),
		templateID,
		flowPath,
		instanceID,
		runtimepipeline.FlowInstanceEntityID(flowPath),
		sourceEntityID,
	)
	return runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		Instance:       instance,
		TriggerEvent: eventtest.RunCreatingRootIngress(
			"77777777-7777-4777-8777-777777777777", events.EventType("spawn.requested"),
			"spawner", "", json.RawMessage(`{}`), 0,
			"77777777-7777-4777-8777-777777777778", "", events.EventEnvelope{}, time.Now().UTC(),
		),
	}
}

func testFlowActivationTriggerEvent(eventID string, runIDs ...string) events.Event {
	return testFlowActivationTriggerEventWithMode(eventID, executionmode.Live, runIDs...)
}

func testFlowActivationTriggerEventWithMode(eventID string, mode executionmode.Mode, runIDs ...string) events.Event {
	runID := "77777777-7777-4777-8777-777777777778"
	if len(runIDs) > 0 {
		runID = strings.TrimSpace(runIDs[0])
	}
	return eventtest.RunCreatingRootIngressWithMode(strings.TrimSpace(eventID),
		events.EventType("spawn.requested"),
		"spawner", "", json.RawMessage(`{}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC(), mode)

}

func TestFlowInstanceActivationExecutionModeAuthority(t *testing.T) {
	req := runtimepipeline.FlowInstanceActivationRequest{TriggerEvent: testFlowActivationTriggerEventWithMode(uuid.NewString(), executionmode.Mock)}
	if mode, err := flowInstanceActivationExecutionMode(context.Background(), req); err != nil || mode != executionmode.Mock {
		t.Fatalf("trigger mode = %q, %v, want mock", mode, err)
	}
	if _, err := flowInstanceActivationExecutionMode(runtimeeffects.WithExecutionMode(context.Background(), executionmode.Live), req); err == nil {
		t.Fatal("conflicting context and trigger modes were accepted")
	}
	if mode, err := flowInstanceActivationExecutionMode(runtimeeffects.WithExecutionMode(context.Background(), executionmode.Live), runtimepipeline.FlowInstanceActivationRequest{}); err != nil || mode != executionmode.Live {
		t.Fatalf("explicit root mode = %q, %v, want live", mode, err)
	}
	if _, err := flowInstanceActivationExecutionMode(context.Background(), runtimepipeline.FlowInstanceActivationRequest{}); err == nil {
		t.Fatal("missing execution mode authority was accepted")
	}
}

func decodeFlowActivationEventPayload(t *testing.T, event events.Event) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(event.Payload(), &payload); err != nil {
		t.Fatalf("decode event payload: %v", err)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return payload
}

func findPublishedFlowActivationEvent(t *testing.T, bus *flowActivationTestBus, eventType string) events.Event {
	t.Helper()
	for _, event := range bus.published {
		if string(event.Type()) == eventType {
			return event
		}
	}
	t.Fatalf("published events = %#v, want %s", bus.published, eventType)
	return eventtest.RunCreatingRootIngress("", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
}

func TestActivateFlowInstanceAddsDerivedRouteTableInstance(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.InitialState = "queued"
	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.addedPaths) != 1 || bus.addedPaths[0] != "review/inst-1" {
		t.Fatalf("added paths = %#v, want [review/inst-1]", bus.addedPaths)
	}
	if len(instances.creates) != 1 || instances.creates[0].EntityType != "review_entity" {
		t.Fatalf("activation entity contract = %#v, want canonical review_entity and no schema.Entity interpretation", instances.creates)
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); !ok {
		t.Fatal("expected activated flow agent config")
	}
	cfg, _ := testAgentConfig(t, am, "reviewer", "review/inst-1")
	if got := strings.TrimSpace(cfg.EntityID); got != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("agent entity_id = %q, want %q", got, runtimepipeline.FlowInstanceEntityID("review/inst-1"))
	}
}

func TestActivateFlowInstanceRejectsRetiredSchemaEntityContradiction(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	schema := bundle.FlowSchemas["review"]
	schema.Entity = "retired_schema_selector"
	bundle.FlowSchemas["review"] = schema

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err == nil || !strings.Contains(err.Error(), "requires one canonical entity contract") {
		t.Fatalf("retired schema entity contradiction error = %v", err)
	}
	if len(instances.creates) != 0 || len(bus.addedPaths) != 0 {
		t.Fatalf("retired schema entity contradiction mutated activation: instances=%#v routes=%#v", instances.creates, bus.addedPaths)
	}
}

func TestActivateFlowInstanceDoesNotConsumeAmbientTransactionAuthority(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle(t, "")
	postCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	ctx := runtimepipelinefixture.WithSQLTx(testAuthorActivityContext(context.Background()), &sql.Tx{})
	ctx = withFlowActivationPostCommit(ctx, &postCommit)

	if err := activateFlowInstanceForTest(am, ctx, testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); !ok {
		t.Fatal("flow agent did not start from the closed activation result")
	}
	if len(bus.addedPaths) != 1 || bus.addedPaths[0] != "review/inst-1" {
		t.Fatalf("committed route materialization = %#v, want review/inst-1", bus.addedPaths)
	}
	if len(postCommit) != 0 {
		t.Fatalf("ambient post-commit actions = %d, want zero", len(postCommit))
	}
}

func TestActivateFlowInstanceArmsInitialTimersOnlyAfterRuntimeInstallation(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	var am *AgentManager
	instances.armInitialEntry = func(instanceID string) error {
		if instanceID != "review/inst-1" {
			return fmt.Errorf("armed instance = %q, want review/inst-1", instanceID)
		}
		if len(bus.addedPaths) != 1 || bus.addedPaths[0] != "review/inst-1" {
			return fmt.Errorf("timer armed before route installation: %#v", bus.addedPaths)
		}
		if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); !ok {
			return errors.New("timer armed before agent installation")
		}
		return nil
	}
	am = newFlowActivationManager(t, bus, instances)
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(testFlowBundle(t, ""), "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(instances.armedEntries) != 1 || instances.armedEntries[0] != "review/inst-1" {
		t.Fatalf("armed entries = %#v, want [review/inst-1]", instances.armedEntries)
	}
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want no activation failure", bus.runtimeLogs)
	}
}

func TestDynamicFlowRuntimeReadinessRecoversEveryFinalizationBoundary(t *testing.T) {
	for _, boundary := range []string{
		"partial_agent",
		"arm",
		"topology_mark",
		"topology_reload",
		"creation_event",
		"creation_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			routeStore := &flowActivationTestRouteStore{}
			bus := &flowActivationTestBus{routeStore: routeStore}
			completed := make(chan struct{}, 1)
			instances := &flowActivationTestInstanceStore{
				creationMarked: func() {
					select {
					case completed <- struct{}{}:
					default:
					}
				},
			}
			agentStore := &flowActivationTestStore{}
			am := newFlowActivationManager(t, bus, instances, agentStore)
			bundle := testFlowBundleWithTwoAgents(t, "task.started")
			setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
			req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

			switch boundary {
			case "partial_agent":
				agentStore.failAgentID = "writer"
			case "arm":
				instances.armInitialEntry = func(string) error { return errors.New("injected arm failure") }
			case "topology_mark":
				instances.topologyMarkErr = errors.New("injected topology mark failure")
			case "topology_reload":
				instances.afterTopologyMark = func(runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
					instances.readinessLoadErr = errors.New("injected topology readback failure")
				}
			case "creation_event":
				bus.publishErr = errors.New("injected creation event failure")
			case "creation_commit":
				instances.creationMarkErr = errors.New("injected completion mark failure")
			}

			err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
			if err == nil {
				t.Fatal("direct activation succeeded across injected finalization failure")
			}
			if len(bus.published) != 0 {
				t.Fatalf(
					"creation events before readiness recovery = %d, want %d",
					len(bus.published),
					0,
				)
			}
			if bus.HasFlowInstanceRoute(req.Instance.Route()) {
				t.Fatal("failed readiness attempt left the route process-visible")
			}
			wantTimerRetirements := 0
			switch boundary {
			case "arm", "topology_mark", "topology_reload", "creation_event", "creation_commit":
				wantTimerRetirements = 1
			}
			if got := len(instances.retiredTimerEntries); got != wantTimerRetirements {
				t.Fatalf(
					"timer projection retirements after %s failure = %d, want %d: %#v",
					boundary,
					got,
					wantTimerRetirements,
					instances.retiredTimerEntries,
				)
			}
			if boundary == "partial_agent" && len(agentStore.upserts) != 1 {
				t.Fatalf("partial agent registrations = %d, want one", len(agentStore.upserts))
			}

			agentStore.failAgentID = ""
			bus.addErr = nil
			instances.armInitialEntry = nil
			instances.topologyMarkErr = nil
			instances.readinessLoadErr = nil
			bus.publishErr = nil
			instances.creationMarkErr = nil
			if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
				t.Fatalf("Run automatic readiness owner: %v", err)
			}
			select {
			case <-completed:
			case <-time.After(5 * time.Second):
				t.Fatal("automatic readiness retry did not complete")
			}
			if len(bus.published) != 1 {
				t.Fatalf("creation events after recovery = %d, want exactly one", len(bus.published))
			}
			if len(instances.armedEntries) == 0 {
				t.Fatal("initial timers were not armed after readiness recovery")
			}
			recoveryCtx := testAuthorActivityContext(context.Background())
			readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(recoveryCtx, req.TriggerEvent.RunID(), req.Instance.Route())
			if err != nil || !found || readiness.TopologyReadyAt.IsZero() || readiness.CreationEventEmittedAt.IsZero() {
				t.Fatalf("completed readiness: found=%v readiness=%#v err=%v", found, readiness, err)
			}
			if _, err := am.EnsureFlowInstance(recoveryCtx, req); err != nil {
				t.Fatalf("second EnsureFlowInstance: %v", err)
			}
			if len(bus.published) != 1 {
				t.Fatalf("creation events after exact replay = %d, want one", len(bus.published))
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessAutomaticRetryCompletesWithoutEnsureOrRestart(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	completed := make(chan struct{}, 1)
	instances.creationMarked = func() {
		select {
		case completed <- struct{}{}:
		default:
		}
	}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	armCalls := 0
	instances.armInitialEntry = func(string) error {
		armCalls++
		if armCalls == 1 {
			return errors.New("injected first readiness attempt failure")
		}
		return nil
	}
	am := newFlowActivationManager(t, bus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err == nil {
		t.Fatal("ActivateFlowInstance succeeded across injected first readiness failure")
	}
	if armCalls != 1 || len(bus.published) != 0 {
		t.Fatalf("first committed readiness attempt: arms=%d published=%d", armCalls, len(bus.published))
	}
	if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("automatic readiness retry did not complete durable readiness")
	}
	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		context.Background(),
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found || readiness.TopologyReadyAt.IsZero() || readiness.CreationEventEmittedAt.IsZero() {
		t.Fatalf("automatic retry readiness: found=%v readiness=%#v err=%v", found, readiness, err)
	}
	if armCalls != 2 || len(bus.published) != 1 {
		t.Fatalf("automatic retry side effects: arms=%d published=%d, want 2/1", armCalls, len(bus.published))
	}
}

func TestDynamicFlowRuntimeTopologyReadyRejectsConcurrentPlanRevision(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	var revisionErr error
	instances.beforeTopologyMark = func(expected runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		observed, found, err := instances.LoadDynamicFlowRuntimeReadiness(context.Background(), expected.RunID, expected.Identity.Route())
		if err != nil || !found {
			revisionErr = errors.Join(err, errors.New("concurrent readiness observation not found"))
			return
		}
		revised := expected
		revised.WorkflowVersion += "-concurrent-revision"
		changed, err := instances.ReconcileDynamicFlowRuntimeReadinessPlan(
			context.Background(),
			observed,
			revised,
			time.Now().UTC(),
		)
		if err != nil {
			revisionErr = err
			return
		}
		if !changed {
			revisionErr = errors.New("concurrent readiness plan was not revised")
		}
	}

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
	if revisionErr != nil {
		t.Fatalf("inject concurrent readiness revision: %v", revisionErr)
	}
	if err == nil || !strings.Contains(err.Error(), "readiness plan changed") {
		t.Fatalf("ActivateFlowInstance error = %v, want exact-plan topology completion rejection", err)
	}
	readiness, found, loadErr := instances.LoadDynamicFlowRuntimeReadiness(
		context.Background(),
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if loadErr != nil || !found {
		t.Fatalf("load concurrently revised readiness: found=%v err=%v", found, loadErr)
	}
	if readiness.Plan.WorkflowVersion == strings.TrimSpace(bundle.WorkflowVersion()) ||
		!readiness.TopologyReadyAt.IsZero() ||
		!readiness.Pending() {
		t.Fatalf("concurrently revised readiness was falsely completed: %#v", readiness)
	}
	if bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("stale topology attempt left its process route published")
	}
}

func TestDynamicFlowRuntimeReadinessRejectsRevisionAfterAdmissionBeforeExecution(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}

	admitted := make(chan struct{})
	release := make(chan struct{})
	am.testAfterDynamicFlowReadinessAdmission = func() {
		close(admitted)
		<-release
	}
	reconciled := make(chan error, 1)
	go func() {
		reconciled <- am.reconcileDynamicFlowRuntimeReadiness(
			ctx,
			req.TriggerEvent.RunID(),
			req.Instance.InstancePath,
		)
	}()
	<-admitted

	current, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		ctx,
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found {
		t.Fatalf("load admitted readiness: found=%v err=%v", found, err)
	}
	revised := current.Plan
	revised.WorkflowVersion = "v-revised-after-admission"
	if changed, err := instances.ReconcileDynamicFlowRuntimeReadinessPlan(
		ctx,
		current,
		revised,
		time.Now().UTC(),
	); err != nil || !changed {
		t.Fatalf("revise admitted readiness: changed=%v err=%v", changed, err)
	}
	close(release)
	if err := <-reconciled; !errors.Is(err, errDynamicFlowRuntimeReadinessPlanStale) {
		t.Fatalf("post-admission revision error = %v, want stale-plan rejection", err)
	}
	if !bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("stale admitted callback retired the already-published route")
	}
}

func TestDynamicFlowRuntimeReadinessRejectsCurrentFactWithOldSemanticSource(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	oldBundle := testFlowBundle(t, "")
	currentBundle := testFlowBundle(t, "")
	currentBundle.FlowTree.ByID["review"].Agents["editor"] = runtimecontracts.AgentRegistryEntry{
		ID: "editor", Type: "generic", Role: "editor", ResolvedIntent: managerTestResolvedIntent("editor"),
	}
	registerTestFlowAgentOwner(currentBundle, "review", "editor")
	oldSource := semanticview.Wrap(oldBundle)
	currentSource := semanticview.Wrap(currentBundle)
	currentFact, err := runtimecorrelation.NewSourceArtifactFact(
		"bundle-v2:sha256:" + strings.Repeat("d", 64),
	)
	if err != nil {
		t.Fatalf("current bundle source fact: %v", err)
	}
	setFlowActivationManagerSemanticSource(am, currentSource, currentFact)
	ctx := runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), currentFact)
	req := testActivationRequest(currentBundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance current source: %v", err)
	}
	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		ctx,
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found {
		t.Fatalf("load current readiness: found=%v err=%v", found, err)
	}
	err = am.reconcileDynamicFlowRuntimeReadinessPlan(ctx, readiness.Plan, oldSource)
	if !errors.Is(err, errDynamicFlowRuntimeReadinessSourceStale) {
		t.Fatalf("old semantic source error = %v, want stale-source rejection", err)
	}
}

func TestFlowActivationRejectsForeignSemanticSourceBeforeAnyMutation(t *testing.T) {
	for _, operation := range []string{"activate", "ensure"} {
		for _, transactional := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/transactional=%t", operation, transactional), func(t *testing.T) {
				instances := &flowActivationTestInstanceStore{}
				agents := &flowActivationTestStore{}
				bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
				manager := newFlowActivationManager(t, bus, instances, agents)
				currentBundle := testFlowBundle(t, "")
				foreignBundle := testFlowBundle(t, "")
				foreignBundle.FlowTree.ByID["review"].Agents["editor"] = runtimecontracts.AgentRegistryEntry{
					ID: "editor", Type: "generic", Role: "editor",
				}
				registerTestFlowAgentOwner(foreignBundle, "review", "editor")
				setFlowActivationManagerSemanticSource(manager, semanticview.Wrap(currentBundle))
				ctx := testAuthorActivityContext(context.Background())
				currentReq := testActivationRequest(
					currentBundle,
					"review",
					"inst-1",
					"ent-1",
					"review/inst-1",
				)
				if operation == "ensure" {
					if err := activateFlowInstanceForTest(manager, ctx, currentReq); err != nil {
						t.Fatalf("seed current source: %v", err)
					}
				}

				creates := len(instances.creates)
				readiness := len(instances.readiness)
				upserts := len(agents.upserts)
				armed := len(instances.armedEntries)
				published := len(bus.published)
				addedPaths := len(bus.addedPaths)
				postCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)
				attemptCtx := ctx
				if transactional {
					attemptCtx = runtimepipelinefixture.WithSQLTx(attemptCtx, &sql.Tx{})
					attemptCtx = withFlowActivationPostCommit(attemptCtx, &postCommit)
				}
				foreignReq := testActivationRequest(
					foreignBundle,
					"review",
					"inst-1",
					"ent-1",
					"review/inst-1",
				)
				var err error
				switch operation {
				case "activate":
					err = manager.ActivateFlowInstance(attemptCtx, foreignReq)
				case "ensure":
					_, err = manager.EnsureFlowInstance(attemptCtx, foreignReq)
				default:
					t.Fatalf("unsupported operation %q", operation)
				}
				if !errors.Is(err, errDynamicFlowRuntimeReadinessSourceStale) {
					t.Fatalf("foreign semantic source error = %v, want stale-source rejection", err)
				}
				if len(instances.creates) != creates ||
					len(instances.readiness) != readiness ||
					len(agents.upserts) != upserts ||
					len(instances.armedEntries) != armed ||
					len(bus.published) != published ||
					len(bus.addedPaths) != addedPaths ||
					len(postCommit) != 0 {
					t.Fatalf(
						"foreign source crossed mutation boundary: creates=%d/%d readiness=%d/%d upserts=%d/%d arms=%d/%d events=%d/%d routes=%d/%d post_commit=%d",
						len(instances.creates),
						creates,
						len(instances.readiness),
						readiness,
						len(agents.upserts),
						upserts,
						len(instances.armedEntries),
						armed,
						len(bus.published),
						published,
						len(bus.addedPaths),
						addedPaths,
						len(postCommit),
					)
				}
			})
		}
	}
}

func TestDynamicFlowRuntimeTopologyReadyRejectsPostCASPlanRevision(t *testing.T) {
	instances := &flowActivationTestInstanceStore{respectReadinessContext: true}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	intermediateSource := semanticview.Wrap(bundle)
	revisedSource := semanticview.Wrap(bundle)
	intermediateFact, err := runtimecorrelation.NewSourceArtifactFact(
		"bundle-v2:sha256:" + strings.Repeat("b", 64),
	)
	if err != nil {
		t.Fatalf("intermediate bundle source fact: %v", err)
	}
	revisedFact, err := runtimecorrelation.NewSourceArtifactFact(
		"bundle-v2:sha256:" + strings.Repeat("c", 64),
	)
	if err != nil {
		t.Fatalf("revised bundle source fact: %v", err)
	}
	successorErr := make(chan error, 1)
	key, err := newDynamicFlowRuntimeReadinessKey(req.TriggerEvent.RunID(), req.Instance.InstancePath)
	if err != nil {
		t.Fatalf("readiness key: %v", err)
	}
	activationCtx, cancelActivation := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancelActivation()
	intermediateCtx := runtimecorrelation.WithSourceArtifactFact(
		testAuthorActivityContext(context.Background()),
		intermediateFact,
	)
	successorCtx := runtimecorrelation.WithSourceArtifactFact(
		testAuthorActivityContext(context.Background()),
		revisedFact,
	)

	var revisionErr error
	var staleErr error
	var staleSourceErr error
	instances.afterTopologyMark = func(completed runtimepipeline.DynamicFlowRuntimeReadinessPlan) {
		observed, found, err := instances.LoadDynamicFlowRuntimeReadiness(intermediateCtx, completed.RunID, completed.Identity.Route())
		if err != nil || !found {
			revisionErr = errors.Join(err, errors.New("intermediate readiness observation not found"))
			return
		}
		intermediate := completed
		intermediate.BundleHash = intermediateFact.BundleHash()
		changed, err := instances.ReconcileDynamicFlowRuntimeReadinessPlan(
			intermediateCtx,
			observed,
			intermediate,
			time.Now().UTC(),
		)
		if err != nil {
			revisionErr = err
			return
		}
		if !changed {
			revisionErr = errors.New("post-CAS intermediate readiness plan was not revised")
			return
		}
		observed, found, err = instances.LoadDynamicFlowRuntimeReadiness(successorCtx, completed.RunID, completed.Identity.Route())
		if err != nil || !found {
			revisionErr = errors.Join(err, errors.New("successor readiness observation not found"))
			return
		}
		revised := intermediate
		revised.BundleHash = revisedFact.BundleHash()
		changed, err = instances.ReconcileDynamicFlowRuntimeReadinessPlan(
			successorCtx,
			observed,
			revised,
			time.Now().UTC(),
		)
		if err != nil {
			revisionErr = err
			return
		}
		if !changed {
			revisionErr = errors.New("post-CAS readiness plan was not revised")
			return
		}
		setFlowActivationManagerSemanticSource(am, revisedSource, revisedFact)
		go func() {
			successorErr <- am.reconcileDynamicFlowRuntimeReadinessPlan(
				successorCtx,
				revised,
				revisedSource,
			)
		}()
		deadline := time.Now().Add(5 * time.Second)
		for {
			am.dynamicFlowReadinessMu.Lock()
			attempt := am.dynamicFlowReadinessAttempts[key]
			successorQueued := attempt != nil &&
				attempt.successorRequired &&
				attempt.successor != nil &&
				attempt.successor.planCoordinate != attempt.planCoordinate
			am.dynamicFlowReadinessMu.Unlock()
			if successorQueued {
				break
			}
			if time.Now().After(deadline) {
				revisionErr = errors.New("post-CAS readiness successor was not queued")
				return
			}
			runtime.Gosched()
		}
		staleSourceErr = am.reconcileDynamicFlowRuntimeReadinessPlan(
			intermediateCtx,
			revised,
			revisedSource,
		)
		staleErr = am.reconcileDynamicFlowRuntimeReadinessPlan(
			intermediateCtx,
			intermediate,
			intermediateSource,
		)
		cancelActivation()
	}

	err = activateFlowInstanceForTest(am, activationCtx, req)
	if revisionErr != nil {
		t.Fatalf("inject post-CAS readiness revision: %v", revisionErr)
	}
	if !errors.Is(staleErr, errDynamicFlowRuntimeReadinessSourceStale) {
		t.Fatalf("delayed intermediate plan error = %v, want stale-source rejection", staleErr)
	}
	if !errors.Is(staleSourceErr, errDynamicFlowRuntimeReadinessSourceStale) {
		t.Fatalf("previous-source callback error = %v, want stale-source rejection", staleSourceErr)
	}
	if err != nil {
		t.Fatalf("ActivateFlowInstance with canceled predecessor and queued revised-plan successor: %v", err)
	}
	if err := <-successorErr; err != nil {
		t.Fatalf("post-CAS revised-plan successor: %v", err)
	}
	readiness, found, loadErr := instances.LoadDynamicFlowRuntimeReadiness(
		context.Background(),
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if loadErr != nil || !found {
		t.Fatalf("load post-CAS revised readiness: found=%v err=%v", found, loadErr)
	}
	revisedHash := revisedFact.BundleHash()
	if readiness.Plan.WorkflowVersion != strings.TrimSpace(bundle.WorkflowVersion()) ||
		readiness.Plan.BundleHash != revisedHash ||
		readiness.TopologyReadyAt.IsZero() ||
		readiness.Pending() {
		t.Fatalf("post-CAS revised readiness successor did not complete: %#v", readiness)
	}
	if !bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("post-CAS revised topology route was not published")
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); !ok {
		t.Fatal("post-CAS revised topology agent was not published")
	}
}

func TestDynamicFlowRuntimeReadinessSameVersionSourceReplacementQueuesExactPlan(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	source := semanticview.Wrap(bundle)
	setFlowActivationManagerSemanticSource(am, source)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEventWithMode(req.TriggerEvent.ID(), executionmode.Mock, req.TriggerEvent.RunID())
	initialCtx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, initialCtx, req); err != nil {
		t.Fatalf("activate initial source: %v", err)
	}
	initial, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		initialCtx,
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found || initial.TopologyReadyAt.IsZero() || initial.Plan.ExecutionMode != executionmode.Mock {
		t.Fatalf("initial readiness: found=%v err=%v readiness=%#v", found, err, initial)
	}

	revisedFact, err := runtimecorrelation.NewSourceArtifactFact(
		"bundle-v2:sha256:" + strings.Repeat("d", 64),
	)
	if err != nil {
		t.Fatalf("revised bundle source fact: %v", err)
	}
	setFlowActivationManagerSemanticSource(am, source, revisedFact)
	foreignKey := seedForeignMalformedDynamicFlowRuntimeReadiness(t, instances)
	revisedCtx := runtimecorrelation.WithRunID(
		runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), revisedFact),
		req.TriggerEvent.RunID(),
	)
	revisedCtx = worklifetime.WithOccurrence(revisedCtx, am.workOwner)
	if err := am.ReconcileDynamicFlowRuntimeReadinessPlansForRun(
		revisedCtx,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("reconcile same-version revised source: %v", err)
	}
	completed, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		revisedCtx,
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found {
		t.Fatalf("load completed revised source: found=%v err=%v", found, err)
	}
	revisedHash := revisedFact.BundleHash()
	if completed.Plan.WorkflowVersion != initial.Plan.WorkflowVersion ||
		completed.Plan.BundleHash != revisedHash ||
		completed.Plan.ExecutionMode != executionmode.Mock ||
		completed.TopologyReadyAt.IsZero() ||
		completed.Pending() {
		t.Fatalf("same-version revised source did not recomplete exact plan: %#v", completed)
	}
	instances.readinessMu.Lock()
	_, foreignSurvived := instances.readiness[foreignKey]
	instances.readinessMu.Unlock()
	if !foreignSurvived {
		t.Fatal("live source revision mutated foreign malformed readiness")
	}
}

func TestDynamicFlowRuntimeReadinessSiblingAdditionReconcilesUnchangedAgentTopology(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances, agents)
	initialBundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(initialBundle))
	req := testActivationRequest(initialBundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEventWithMode(req.TriggerEvent.ID(), executionmode.Mock, req.TriggerEvent.RunID())
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
		t.Fatalf("activate initial source: %v", err)
	}
	reviewerIdentity := testAgentIdentity(t, am, "reviewer", "review/inst-1")
	initialState, ok := am.lifecycle.stateByIdentity(reviewerIdentity)
	if !ok {
		t.Fatal("initial reviewer lifecycle state is missing")
	}
	initialConfig, ok := testAgentConfig(t, am, "reviewer", "review/inst-1")
	if !ok {
		t.Fatal("initial reviewer config is missing")
	}
	initialConfigRevision, err := lifecycleConfigRevision(PersistedAgent{Config: initialConfig})
	if err != nil {
		t.Fatalf("initial reviewer config revision: %v", err)
	}

	revisedBundle := testFlowBundleWithTwoAgents(t, "")
	revisedSource := semanticview.Wrap(revisedBundle)
	revisedFact, err := runtimecorrelation.NewSourceArtifactFact(
		"bundle-v2:sha256:" + strings.Repeat("e", 64),
	)
	if err != nil {
		t.Fatalf("revised bundle source fact: %v", err)
	}
	setFlowActivationManagerSemanticSource(am, revisedSource, revisedFact)
	revisedCtx := runtimecorrelation.WithRunID(
		runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), revisedFact),
		req.TriggerEvent.RunID(),
	)
	revisedCtx = worklifetime.WithOccurrence(revisedCtx, am.workOwner)
	if err := am.ReconcileDynamicFlowRuntimeReadinessPlansForRun(revisedCtx, time.Now().UTC()); err != nil {
		t.Fatalf("reconcile sibling addition: %v", err)
	}

	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		revisedCtx,
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found || readiness.TopologyReadyAt.IsZero() || readiness.Pending() {
		t.Fatalf("load sibling-addition readiness: found=%v err=%v readiness=%#v", found, err, readiness)
	}
	expectedTopology, err := dynamicFlowAgentTopologyAuthority(readiness.Plan)
	if err != nil {
		t.Fatalf("derive revised topology admission: %v", err)
	}
	revisedState, ok := am.lifecycle.stateByIdentity(reviewerIdentity)
	if !ok {
		t.Fatal("revised reviewer lifecycle state is missing")
	}
	if revisedState.Topology.Equal(initialState.Topology) {
		t.Fatal("unchanged reviewer retained the initial readiness topology")
	}
	if !revisedState.Topology.Equal(expectedTopology) {
		t.Fatalf("reviewer process topology = %#v, want %#v", revisedState.Topology, expectedTopology)
	}
	if revisedState.Generation <= initialState.Generation {
		t.Fatalf("reviewer lifecycle generation = %d, want later than %d", revisedState.Generation, initialState.Generation)
	}
	revisedConfig, ok := testAgentConfig(t, am, "reviewer", "review/inst-1")
	if !ok {
		t.Fatal("revised reviewer config is missing")
	}
	revisedConfigRevision, err := lifecycleConfigRevision(PersistedAgent{Config: revisedConfig})
	if err != nil {
		t.Fatalf("revised reviewer config revision: %v", err)
	}
	if revisedConfigRevision != initialConfigRevision {
		t.Fatalf("reviewer config revision changed: before=%s after=%s", initialConfigRevision, revisedConfigRevision)
	}
	if _, ok := testAgentConfig(t, am, "writer", "review/inst-1"); !ok {
		t.Fatal("added sibling writer is not process-ready")
	}
	persisted, err := agents.LoadAgents(revisedCtx)
	if err != nil {
		t.Fatalf("load reconciled agents: %v", err)
	}
	var reviewerPersisted *PersistedAgent
	for i := range persisted {
		identity, identityErr := persisted[i].Config.ConcreteIdentity()
		if identityErr != nil {
			t.Fatalf("persisted agent identity: %v", identityErr)
		}
		if identity == reviewerIdentity {
			reviewerPersisted = &persisted[i]
			break
		}
	}
	if reviewerPersisted == nil {
		t.Fatal("reconciled reviewer is not durably registered")
	}
	if !reviewerPersisted.Topology.Equal(expectedTopology) {
		t.Fatalf("reviewer durable topology = %#v, want %#v", reviewerPersisted.Topology, expectedTopology)
	}
}

func TestDynamicFlowRuntimeReadinessSameVersionRouteRevisionReplacesExactTopology(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	durableRoutes := map[string][]runtimebus.FlowInstanceRouteRecord{}

	sourceABundle := testFlowRouteRevisionBundle(t, "task.started")
	sourceA := semanticview.Wrap(sourceABundle)
	durableA, err := runtimebus.DeriveRouteTable(sourceA)
	if err != nil {
		t.Fatalf("derive source A durable routes: %v", err)
	}
	processA, err := runtimebus.DeriveRouteTable(sourceA)
	if err != nil {
		t.Fatalf("derive source A process routes: %v", err)
	}
	busA := &flowActivationSemanticRouteBus{
		flowActivationTestBus: &flowActivationTestBus{},
		durable:               durableA,
		process:               processA,
		durableRoutes:         durableRoutes,
	}
	managerA := newFlowActivationManager(t, busA, instances, agents)
	setFlowActivationManagerSemanticSource(managerA, sourceA)
	ctxA := testAuthorActivityContext(context.Background())
	reqA := testActivationRequest(sourceABundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := activateFlowInstanceForTest(managerA, ctxA, reqA); err != nil {
		t.Fatalf("activate source A: %v", err)
	}
	initial, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		ctxA,
		reqA.TriggerEvent.RunID(),
		reqA.Instance.Route(),
	)
	if err != nil || !found || initial.TopologyReadyAt.IsZero() {
		t.Fatalf("source A readiness: found=%v err=%v readiness=%#v", found, err, initial)
	}

	hasRecord := func(records []runtimebus.FlowInstanceRouteRecord, pattern, subscriber string) bool {
		for _, record := range records {
			if record.EventPattern == pattern && record.SubscriberID == subscriber {
				return true
			}
		}
		return false
	}
	hasSubscriber := func(subscribers []runtimebus.Subscriber, id string) bool {
		for _, subscriber := range subscribers {
			if subscriber.Recipient.ID() == id {
				return true
			}
		}
		return false
	}
	oldEvent := "review/inst-1/task.started"
	newEvent := "review/inst-1/task.revised"
	routeObserver := identitytest.FlowNode(t, "review", "route-observer").Key()
	if !hasRecord(durableRoutes[reqA.Instance.InstancePath], oldEvent, routeObserver) ||
		!hasSubscriber(processA.Resolve(oldEvent), routeObserver) {
		t.Fatalf(
			"source A route-only facts missing: durable=%#v process=%#v",
			durableRoutes[reqA.Instance.InstancePath],
			processA.Resolve(oldEvent),
		)
	}

	sourceBBundle := testFlowRouteRevisionBundle(t, "task.revised")
	sourceB := semanticview.Wrap(sourceBBundle)
	revisedFact, err := runtimecorrelation.NewSourceArtifactFact(
		"bundle-v2:sha256:" + strings.Repeat("d", 64),
	)
	if err != nil {
		t.Fatalf("revised bundle source fact: %v", err)
	}
	durableB, err := runtimebus.DeriveRouteTable(sourceB)
	if err != nil {
		t.Fatalf("derive source B durable routes: %v", err)
	}
	processB, err := runtimebus.DeriveRouteTable(sourceB)
	if err != nil {
		t.Fatalf("derive source B process routes: %v", err)
	}
	busB := &flowActivationSemanticRouteBus{
		flowActivationTestBus: &flowActivationTestBus{},
		durable:               durableB,
		process:               processB,
		durableRoutes:         durableRoutes,
	}
	managerB := newFlowActivationManager(t, busB, instances, agents)
	setFlowActivationManagerSemanticSource(managerB, sourceB, revisedFact)
	ctxB := runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), revisedFact)
	reqB := testActivationRequest(sourceBBundle, "review", "inst-1", "ent-1", "review/inst-1")
	if created, err := managerB.EnsureFlowInstance(ctxB, reqB); err != nil {
		t.Fatalf("ensure source B: %v", err)
	} else if created {
		t.Fatal("same-version route revision reported a new instance")
	}
	revised, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		ctxB,
		reqB.TriggerEvent.RunID(),
		reqB.Instance.Route(),
	)
	if err != nil || !found || revised.TopologyReadyAt.IsZero() {
		t.Fatalf("source B readiness: found=%v err=%v readiness=%#v", found, err, revised)
	}
	if revised.Plan.WorkflowVersion != initial.Plan.WorkflowVersion ||
		revised.Plan.BundleHash == initial.Plan.BundleHash ||
		!reflect.DeepEqual(revised.Plan.Agents, initial.Plan.Agents) {
		t.Fatalf("route-only revision changed wrong plan facts: before=%#v after=%#v", initial.Plan, revised.Plan)
	}
	revisedRecords := durableRoutes[reqB.Instance.InstancePath]
	if hasRecord(revisedRecords, oldEvent, routeObserver) ||
		hasSubscriber(processB.Resolve(oldEvent), routeObserver) {
		t.Fatalf(
			"source B retained stale route-only facts: durable=%#v process=%#v",
			revisedRecords,
			processB.Resolve(oldEvent),
		)
	}
	if !hasRecord(revisedRecords, newEvent, routeObserver) ||
		!hasSubscriber(processB.Resolve(newEvent), routeObserver) {
		t.Fatalf(
			"source B route-only facts missing: durable=%#v process=%#v",
			revisedRecords,
			processB.Resolve(newEvent),
		)
	}
}

func TestDynamicFlowRuntimeReadinessNoAutoEmitArmFailureConvergesAfterMissedSignal(t *testing.T) {
	completed := make(chan struct{}, 1)
	instances := &flowActivationTestInstanceStore{
		topologyMarked: func() {
			select {
			case completed <- struct{}{}:
			default:
			}
		},
	}
	armCalls := 0
	instances.armInitialEntry = func(string) error {
		armCalls++
		if armCalls == 1 {
			return errors.New("injected first timer arm failure")
		}
		return nil
	}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	foreignKey := seedForeignMalformedDynamicFlowRuntimeReadiness(t, instances)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err == nil {
		t.Fatal("ActivateFlowInstance succeeded across injected timer arm failure")
	}
	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		context.Background(),
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found || !readiness.Pending() || !readiness.TopologyReadyAt.IsZero() {
		t.Fatalf("failed no-auto readiness: found=%v readiness=%#v err=%v", found, readiness, err)
	}
	if bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("failed no-auto readiness left process route published")
	}
	for {
		select {
		case <-am.dynamicFlowReadinessSignal:
			continue
		default:
		}
		break
	}
	am.dynamicFlowReadinessRetryInterval = 10 * time.Millisecond

	if err := am.Run(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Run automatic readiness owner: %v", err)
	}
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("automatic no-auto readiness retry did not complete")
	}
	readiness, found, err = instances.LoadDynamicFlowRuntimeReadiness(
		context.Background(),
		req.TriggerEvent.RunID(),
		req.Instance.Route(),
	)
	if err != nil || !found || readiness.Pending() || readiness.TopologyReadyAt.IsZero() {
		t.Fatalf("completed no-auto readiness: found=%v readiness=%#v err=%v", found, readiness, err)
	}
	if armCalls != 2 || len(bus.published) != 0 {
		t.Fatalf("no-auto retry side effects: arms=%d events=%d, want 2/0", armCalls, len(bus.published))
	}
	instances.readinessMu.Lock()
	_, foreignSurvived := instances.readiness[foreignKey]
	instances.readinessMu.Unlock()
	if !foreignSurvived {
		t.Fatal("automatic retry mutated foreign malformed readiness")
	}
}

func TestDynamicFlowRuntimeReadinessCoalescesConcurrentAttemptsByRunAndInstance(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	am.mu.Lock()
	am.startupAgentsHydrated = true
	am.mu.Unlock()
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	admissionEntered := make(chan struct{}, 1)
	admissionRelease := make(chan struct{})
	defer func() {
		select {
		case <-admissionRelease:
		default:
			close(admissionRelease)
		}
	}()
	am.testAfterDynamicFlowReadinessAdmission = func() {
		admissionEntered <- struct{}{}
		<-admissionRelease
	}
	leaderErr := make(chan error, 1)
	go func() {
		leaderErr <- activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
	}()
	<-admissionEntered

	followerErrs := make(chan error, 3)
	ensureCtx, cancelEnsure := context.WithCancel(testAuthorActivityContext(context.Background()))
	pendingCtx, cancelPending := context.WithCancel(testAuthorActivityContext(context.Background()))
	startupCtx, cancelStartup := context.WithCancel(testAuthorActivityContext(context.Background()))
	go func() {
		_, err := am.EnsureFlowInstance(ensureCtx, req)
		followerErrs <- err
	}()
	go func() {
		followerErrs <- am.reconcilePendingDynamicFlowRuntimeReadiness(pendingCtx)
	}()
	go func() {
		followerErrs <- reconcileDynamicFlowRuntimeStartupForTest(am, startupCtx, authorActivityTestSourceArtifactFact, true)
	}()
	cancelEnsure()
	cancelPending()
	cancelStartup()
	for range 3 {
		if err := <-followerErrs; !errors.Is(err, context.Canceled) {
			t.Fatalf("coalesced follower: %v, want context cancellation from in-flight attempt", err)
		}
	}
	close(admissionRelease)
	if err := <-leaderErr; err != nil {
		t.Fatalf("coalesced leader: %v", err)
	}
	if len(bus.addedPaths) != 1 || len(instances.armedEntries) != 1 || len(bus.published) != 1 {
		t.Fatalf(
			"coalesced side effects: stage=%d arm=%d publish=%d, want 1/1/1",
			len(bus.addedPaths),
			len(instances.armedEntries),
			len(bus.published),
		)
	}
}

func TestDynamicFlowRuntimeReadinessTerminalRaceRetiresProcessRoute(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}, addErr: errors.New("seed pending readiness")}
	am := newFlowActivationManager(t, bus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err == nil {
		t.Fatal("activation unexpectedly completed the readiness plan")
	}
	bus.addErr = nil
	if err := bus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: req.Instance.Route(),
	}); err != nil {
		t.Fatalf("seed stale process route: %v", err)
	}
	stageEntered := make(chan struct{})
	stageRelease := make(chan struct{})
	bus.stageRoute = func(runtimebus.FlowInstanceRouteMaterializationRequest) error {
		close(stageEntered)
		<-stageRelease
		return nil
	}
	reconciled := make(chan error, 1)
	go func() {
		reconciled <- am.reconcileDynamicFlowRuntimeReadiness(
			testAuthorActivityContext(context.Background()),
			req.TriggerEvent.RunID(),
			req.Instance.InstancePath,
		)
	}()
	<-stageEntered
	if err := instances.MarkTerminated(context.Background(), req.Instance.Route(), identity.NormalizeEntityID(req.Instance.EntityID), time.Now().UTC()); err != nil {
		t.Fatalf("MarkTerminated: %v", err)
	}
	close(stageRelease)
	if err := <-reconciled; err != nil {
		t.Fatalf("terminal reconciliation: %v", err)
	}
	if bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("terminal readiness left a process-visible route")
	}
	if len(instances.armedEntries) != 0 || len(bus.published) != 0 {
		t.Fatalf("terminal readiness side effects: armed=%#v published=%#v", instances.armedEntries, bus.published)
	}
}

func TestDynamicFlowRuntimeReadinessTerminalBeforeCreationCommitRetiresMaterializedTopology(t *testing.T) {
	instances := &flowActivationTestInstanceStore{creationMarkErr: errors.New("hold creation commit pending")}
	agents := &flowActivationTestStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())

	if err := activateFlowInstanceForTest(am, ctx, req); err == nil {
		t.Fatal("activation unexpectedly completed the creation occurrence")
	}
	instances.creationMarkErr = nil
	instances.beforeCreation = func() {
		instances.beforeCreation = nil
		if err := instances.MarkTerminated(context.Background(), req.Instance.Route(), identity.NormalizeEntityID(req.Instance.EntityID), time.Now().UTC()); err != nil {
			t.Fatalf("terminalize at creation boundary: %v", err)
		}
	}
	err := am.reconcileDynamicFlowRuntimeReadiness(
		ctx,
		req.TriggerEvent.RunID(),
		req.Instance.InstancePath,
	)
	if err != nil {
		t.Fatalf("terminal creation reconciliation: %v", err)
	}
	if bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("terminal creation boundary left a process-visible route")
	}
	if len(bus.published) != 0 {
		t.Fatalf("terminal creation boundary published events: %#v", bus.published)
	}
	for _, agentID := range []string{"reviewer", "writer"} {
		if _, ok := testAgentConfig(t, am, agentID, "review/inst-1"); ok {
			t.Fatalf("terminal creation boundary left process agent %s", agentID)
		}
	}
}

func TestRecoverableStateSnapshotIncludesReadinessOnlyPendingWork(t *testing.T) {
	runID := uuid.NewString()
	path := "review/inst-1"
	bundleHash := authorActivityTestSourceArtifactFact.BundleHash()
	instances := &flowActivationTestInstanceStore{
		readiness: map[string]runtimepipeline.DynamicFlowRuntimeReadiness{
			flowActivationReadinessKey(runID, path): {
				InstancePath:    path,
				OwningRunSource: authorActivityTestSourceArtifactFact,
				RunStatus:       "running", InstanceStatus: "active",
				Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
					RunID: runID, BundleHash: bundleHash, WorkflowVersion: "1.0.0", ExecutionMode: executionmode.Live,
					Identity: runtimeflowidentity.Instance{
						TemplateID: "review", ScopeKey: "review", InstanceID: "inst-1",
						InstancePath: path, EntityID: uuid.NewString(), HasStoredPath: true,
					},
				},
			},
		},
	}
	am := newFlowActivationManager(t, &flowActivationTestBus{}, instances)
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(testFlowBundle(t, "")))
	seedForeignMalformedDynamicFlowRuntimeReadiness(t, instances)
	snapshot, err := am.RecoverableStateSnapshot(testAuthorActivityContext(context.Background()))
	if err != nil {
		t.Fatalf("RecoverableStateSnapshot: %v", err)
	}
	if !snapshot.HasRecoverableWork() || snapshot.PendingDynamicFlowRuntimeReadinessCount != 1 {
		t.Fatalf("readiness-only snapshot = %#v", snapshot)
	}
	if got := snapshot.Detail()["pending_dynamic_flow_runtime_readiness_count"]; got != 1 {
		t.Fatalf("readiness-only detail = %#v", snapshot.Detail())
	}
}

func TestDynamicFlowRuntimeReadinessExcludesNonExecutableStandingRuns(t *testing.T) {
	bundleHash := authorActivityTestSourceArtifactFact.BundleHash()
	runKinds := flowActivationStandingRestarts{
		uuid.NewString(): runtimepipeline.StandingRestartOrdinary,
		uuid.NewString(): runtimepipeline.StandingRestartActiveIntrinsic,
		uuid.NewString(): runtimepipeline.StandingRestartSuspended,
		uuid.NewString(): runtimepipeline.StandingRestartOrphaned,
	}
	instances := &flowActivationTestInstanceStore{readiness: map[string]runtimepipeline.DynamicFlowRuntimeReadiness{}}
	index := 0
	for runID := range runKinds {
		index++
		path := fmt.Sprintf("review/standing-%d", index)
		instances.readiness[flowActivationReadinessKey(runID, path)] = runtimepipeline.DynamicFlowRuntimeReadiness{
			InstancePath: path, OwningRunSource: authorActivityTestSourceArtifactFact,
			RunStatus: "running", InstanceStatus: "active",
			Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
				RunID: runID, BundleHash: bundleHash,
				WorkflowVersion: "1.0.0", ExecutionMode: executionmode.Live,
				Identity: runtimeflowidentity.Instance{
					TemplateID: "review", ScopeKey: "review", InstanceID: fmt.Sprintf("standing-%d", index),
					InstancePath: path, EntityID: uuid.NewString(), HasStoredPath: true,
				},
			},
		}
	}
	am := newFlowActivationManager(t, &flowActivationTestBus{}, instances)
	am.roles.StandingRestarts = runKinds
	projection, err := am.InspectDynamicFlowRuntimeReadinessForSource(context.Background(), authorActivityTestSourceArtifactFact)
	if err != nil {
		t.Fatalf("inspect standing readiness: %v", err)
	}
	if len(projection.CurrentPending) != 2 {
		t.Fatalf("executable readiness count = %d, want ordinary plus active intrinsic", len(projection.CurrentPending))
	}
	for _, item := range projection.CurrentPending {
		kind := runKinds[item.Plan.RunID]
		if kind != runtimepipeline.StandingRestartOrdinary && kind != runtimepipeline.StandingRestartActiveIntrinsic {
			t.Fatalf("non-executable readiness escaped filter: run=%s kind=%s", item.Plan.RunID, kind)
		}
	}
}

func TestSourceScopedStartupFinalizesIncompleteDynamicFlowRuntimeReadiness(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{failAgentID: "writer"}
	firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	first := newFlowActivationManager(t, firstBus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())

	if err := activateFlowInstanceForTest(first, ctx, req); err == nil {
		t.Fatal("activation succeeded across injected partial agent failure")
	}
	if len(agents.upserts) != 1 || len(firstBus.published) != 0 || len(instances.armedEntries) != 0 {
		t.Fatalf(
			"pre-restart state: agents=%d events=%d armed=%d",
			len(agents.upserts),
			len(firstBus.published),
			len(instances.armedEntries),
		)
	}

	agents.failAgentID = ""
	restartBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	if err := reconcileDynamicFlowRuntimeStartupForTest(restarted, ctx, authorActivityTestSourceArtifactFact, true); err != nil {
		t.Fatalf("ReconcileDynamicFlowRuntimeStartupReadiness: %v", err)
	}
	if _, ok := testAgentConfig(t, restarted, "reviewer", "review/inst-1"); !ok {
		t.Fatal("restarted manager did not restore first declared agent")
	}
	if _, ok := testAgentConfig(t, restarted, "writer", "review/inst-1"); !ok {
		t.Fatal("restarted manager did not reconcile missing declared agent")
	}
	if len(instances.armedEntries) != 1 {
		t.Fatalf("startup timer arm count = %d, want one", len(instances.armedEntries))
	}
	if len(restartBus.published) != 1 {
		t.Fatalf("startup creation event count = %d, want one", len(restartBus.published))
	}
	if err := reconcileDynamicFlowRuntimeStartupForTest(restarted, ctx, authorActivityTestSourceArtifactFact, true); err != nil {
		t.Fatalf("second ReconcileDynamicFlowRuntimeStartupReadiness: %v", err)
	}
	if len(restartBus.published) != 1 {
		t.Fatalf("creation events after repeated startup recovery = %d, want one", len(restartBus.published))
	}
}

func TestSourceScopedStartupCanonicalizesCompletedPredecessorWithoutReplay(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	initialBundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(initialBundle, "review", "inst-1", "ent-1", "review/inst-1")
	initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	if err := activateFlowInstanceForTest(initial, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("activate predecessor source: %v", err)
	}
	before, found, err := instances.LoadDynamicFlowRuntimeReadiness(context.Background(), req.TriggerEvent.RunID(), req.Instance.Route())
	if err != nil || !found || before.Pending() {
		t.Fatalf("completed predecessor readiness: found=%v readiness=%#v err=%v", found, before, err)
	}

	revisedFact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("d", 64))
	if err != nil {
		t.Fatal(err)
	}
	revisedBundle := testFlowBundleWithTwoAgents(t, "task.started")
	revisedBundle.Semantics.Version = "v-revised"
	restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(revisedBundle), revisedFact)
	revisedCtx := runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), revisedFact)
	startup, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(revisedCtx, revisedFact, false)
	if err != nil {
		t.Fatalf("canonicalize completed predecessor without replay: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(revisedCtx)
	if err := restarted.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		cancelRun()
		t.Fatalf("start successor manager: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		_ = restarted.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	})
	if err := restarted.CompleteDynamicFlowRuntimeStartupTopology(revisedCtx, startup); err != nil {
		t.Fatalf("complete successor topology without replay: %v", err)
	}
	after, found, err := instances.LoadDynamicFlowRuntimeReadiness(revisedCtx, req.TriggerEvent.RunID(), req.Instance.Route())
	wantHash := revisedFact.BundleHash()
	if err != nil || !found || after.Pending() || after.Plan.BundleHash != wantHash || after.Plan.WorkflowVersion != "v-revised" {
		t.Fatalf("canonical completed successor: found=%v readiness=%#v err=%v", found, after, err)
	}
	if !after.CreationEventEmittedAt.Equal(before.CreationEventEmittedAt) {
		t.Fatalf("creation completion changed across startup source transition: before=%s after=%s", before.CreationEventEmittedAt, after.CreationEventEmittedAt)
	}
}

func TestSourceScopedStartupDerivesEntireTransitionSetBeforeWriting(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	initialBundle := testFlowBundleWithTwoAgents(t, "task.started")
	requests := []runtimepipeline.FlowInstanceActivationRequest{
		testActivationRequest(initialBundle, "review", "inst-1", "ent-1", "review/inst-1"),
		testActivationRequest(initialBundle, "review", "inst-2", "ent-2", "review/inst-2"),
	}
	initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	ctx := testAuthorActivityContext(context.Background())
	for _, req := range requests {
		if err := activateFlowInstanceForTest(initial, ctx, req); err != nil {
			t.Fatalf("activate predecessor source %s: %v", req.Instance.InstancePath, err)
		}
	}

	revisedFact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	revisedBundle := testFlowBundleWithTwoAgents(t, "task.started")
	revisedBundle.Semantics.Version = "v-transition-set"
	restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(revisedBundle), revisedFact)
	revisedCtx := runtimecorrelation.WithSourceArtifactFact(ctx, revisedFact)

	secondKey := flowActivationReadinessKey(requests[1].TriggerEvent.RunID(), requests[1].Instance.InstancePath)
	instances.readinessMu.Lock()
	invalid := instances.readiness[secondKey]
	invalid.Plan.Identity.TemplateID = "missing-review"
	instances.readiness[secondKey] = invalid
	before := make(map[string]runtimepipeline.DynamicFlowRuntimeReadiness, len(instances.readiness))
	for key, readiness := range instances.readiness {
		before[key] = readiness
	}
	instances.readinessMu.Unlock()

	if _, err := restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(revisedCtx, revisedFact, false); err == nil || !strings.Contains(err.Error(), "persisted identity changed") {
		t.Fatalf("transition-set derivation error = %v", err)
	}
	instances.readinessMu.Lock()
	defer instances.readinessMu.Unlock()
	if !reflect.DeepEqual(instances.readiness, before) {
		t.Fatalf("failed transition-set derivation wrote a prefix:\nbefore=%#v\nafter=%#v", before, instances.readiness)
	}
}

func TestSourceScopedStartupPostBatchPendingRequiresReplayAfterRestart(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	routeStore := &flowActivationTestRouteStore{}
	initialBundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(initialBundle, "review", "inst-1", "ent-1", "review/inst-1")
	initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: routeStore}, instances, agents)
	if err := activateFlowInstanceForTest(initial, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("activate predecessor source: %v", err)
	}

	revisedFact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	revisedBundle := testFlowBundleWithTwoAgents(t, "task.started")
	revisedBundle.Semantics.Version = "v-post-batch"
	revisedCtx := runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), revisedFact)
	canonicalizer := newFlowActivationManager(t, &flowActivationTestBus{routeStore: routeStore}, instances, agents)
	setFlowActivationManagerSemanticSource(canonicalizer, semanticview.Wrap(revisedBundle), revisedFact)
	if _, err := canonicalizer.CanonicalizeDynamicFlowRuntimeStartupReadiness(revisedCtx, revisedFact, false); err != nil {
		t.Fatalf("commit completed-predecessor transition batch: %v", err)
	}
	committed, found, err := instances.LoadDynamicFlowRuntimeReadiness(revisedCtx, req.TriggerEvent.RunID(), req.Instance.Route())
	if err != nil || !found || !committed.Pending() || committed.Plan.BundleHash != revisedFact.BundleHash() {
		t.Fatalf("post-batch readiness: found=%v readiness=%#v err=%v", found, committed, err)
	}

	blocked := newFlowActivationManager(t, &flowActivationTestBus{routeStore: routeStore}, instances, agents)
	setFlowActivationManagerSemanticSource(blocked, semanticview.Wrap(revisedBundle), revisedFact)
	if _, err := blocked.CanonicalizeDynamicFlowRuntimeStartupReadiness(revisedCtx, revisedFact, false); err == nil || !strings.Contains(err.Error(), "requires recovery") {
		t.Fatalf("recovery-disabled post-batch startup error = %v", err)
	}
	afterRefusal, found, err := instances.LoadDynamicFlowRuntimeReadiness(revisedCtx, req.TriggerEvent.RunID(), req.Instance.Route())
	if err != nil || !found || !reflect.DeepEqual(afterRefusal, committed) {
		t.Fatalf("recovery-disabled refusal mutated readiness: found=%v before=%#v after=%#v err=%v", found, committed, afterRefusal, err)
	}

	recoveryBus := &flowActivationTestBus{routeStore: routeStore}
	recovered := newFlowActivationManager(t, recoveryBus, instances, agents)
	setFlowActivationManagerSemanticSource(recovered, semanticview.Wrap(revisedBundle), revisedFact)
	startup, err := recovered.CanonicalizeDynamicFlowRuntimeStartupReadiness(revisedCtx, revisedFact, true)
	if err != nil {
		t.Fatalf("admit explicit post-batch recovery: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(revisedCtx)
	if err := recovered.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		cancelRun()
		t.Fatalf("start recovery manager: %v", err)
	}
	t.Cleanup(func() {
		cancelRun()
		_ = recovered.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	})
	if err := recovered.CompleteDynamicFlowRuntimeStartupTopology(revisedCtx, startup); err != nil {
		t.Fatalf("complete explicit post-batch recovery: %v", err)
	}
	completed, found, err := instances.LoadDynamicFlowRuntimeReadiness(revisedCtx, req.TriggerEvent.RunID(), req.Instance.Route())
	if err != nil || !found || completed.Pending() || completed.Plan.BundleHash != revisedFact.BundleHash() {
		t.Fatalf("completed post-batch recovery: found=%v readiness=%#v err=%v", found, completed, err)
	}
}

func TestSourceScopedStartupRequiresReplayForPendingPredecessor(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{failAgentID: "writer"}
	initialBundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(initialBundle, "review", "inst-1", "ent-1", "review/inst-1")
	initial := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	if err := activateFlowInstanceForTest(initial, testAuthorActivityContext(context.Background()), req); err == nil {
		t.Fatal("predecessor activation unexpectedly completed")
	}

	revisedFact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	revisedBundle := testFlowBundleWithTwoAgents(t, "task.started")
	revisedBundle.Semantics.Version = "v-revised"
	agents.failAgentID = ""
	restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(revisedBundle), revisedFact)
	revisedCtx := runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), revisedFact)
	_, err = restarted.CanonicalizeDynamicFlowRuntimeStartupReadiness(revisedCtx, revisedFact, false)
	if !IsDynamicFlowRuntimeReadinessFinalizationError(err) || !strings.Contains(err.Error(), "requires recovery") {
		t.Fatalf("pending predecessor without replay error = %v", err)
	}
	blocked, found, loadErr := instances.LoadDynamicFlowRuntimeReadiness(revisedCtx, req.TriggerEvent.RunID(), req.Instance.Route())
	if loadErr != nil || !found || !blocked.Pending() || blocked.Plan.BundleHash == revisedFact.BundleHash() {
		t.Fatalf("blocked predecessor readiness: found=%v readiness=%#v err=%v", found, blocked, loadErr)
	}
	if err := reconcileDynamicFlowRuntimeStartupForTest(restarted, revisedCtx, revisedFact, true); err != nil {
		t.Fatalf("recover pending predecessor: %v", err)
	}
	completed, found, loadErr := instances.LoadDynamicFlowRuntimeReadiness(revisedCtx, req.TriggerEvent.RunID(), req.Instance.Route())
	if loadErr != nil || !found || completed.Pending() || completed.Plan.BundleHash != revisedFact.BundleHash() {
		t.Fatalf("recovered successor readiness: found=%v readiness=%#v err=%v", found, completed, loadErr)
	}
}

func TestMockOnlyPostureRejectsLiveDynamicReadinessBeforeTopologyMutation(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{failAgentID: "writer"}
	firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	first := newFlowActivationManager(t, firstBus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	if err := activateFlowInstanceForTest(first, ctx, req); err == nil {
		t.Fatal("activation succeeded across injected partial agent failure")
	}
	baselineAgents := len(agents.upserts)

	agents.failAgentID = ""
	restartBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	restarted.executionPosture = executionposture.MockOnly
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	if err := restarted.reconcilePendingDynamicFlowRuntimeReadiness(ctx); err == nil || !strings.Contains(err.Error(), "runtime.execution_posture=mock_only") {
		t.Fatalf("readiness reconciliation error = %v, want live-plan rejection", err)
	}
	if len(agents.upserts) != baselineAgents || len(restartBus.addedPaths) != 0 || len(restartBus.published) != 0 || len(instances.armedEntries) != 0 {
		t.Fatalf("readiness mutations agents=%d/%d routes=%d events=%d timers=%d", len(agents.upserts), baselineAgents, len(restartBus.addedPaths), len(restartBus.published), len(instances.armedEntries))
	}
	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
	if err != nil || !found || !readiness.Pending() || readiness.Plan.ExecutionMode != executionmode.Live {
		t.Fatalf("readiness after rejection = %#v found=%v err=%v", readiness, found, err)
	}
}

func TestSourceScopedStartupExcludesTerminalDynamicFlowTopology(t *testing.T) {
	bundle := testFlowBundle(t, "")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	bundleHash := authorActivityTestSourceArtifactFact.BundleHash()
	plan, err := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Identity: req.Instance,
		RunID:    req.TriggerEvent.RunID(), BundleHash: bundleHash,
		WorkflowVersion: bundle.WorkflowVersion(), ExecutionMode: executionmode.Live,
		Agents: []runtimepipeline.DynamicFlowRuntimeAgentExpectation{{
			Identity: runtimeagentidentity.Identity{
				Name: runtimeagentidentity.Name{
					AgentID: "reviewer",
					Owner:   testFlowAgentURIRef("review", "reviewer").Full,
					Source:  runtimeagentidentity.NameSourceDeclared,
				},
				Route: runtimeagentidentity.Route{
					Presence:     runtimeagentidentity.RoutePresent,
					ScopeKey:     req.Instance.ScopeKey,
					InstanceID:   req.Instance.InstanceID,
					InstancePath: req.Instance.InstancePath,
				},
			},
			ConfigRevision: strings.Repeat("a", 64),
		}},
	}.Normalized()
	if err != nil {
		t.Fatalf("normalize readiness plan: %v", err)
	}
	terminatedAt := time.Now().UTC()
	instances := &flowActivationTestInstanceStore{
		byStorageRef: map[string]runtimepipeline.WorkflowInstance{
			req.Instance.InstancePath: {
				InstanceID: req.Instance.InstanceID, StorageRef: req.Instance.InstancePath,
				WorkflowName: req.Instance.TemplateID, Status: "terminated", TerminatedAt: terminatedAt,
			},
		},
		readiness: map[string]runtimepipeline.DynamicFlowRuntimeReadiness{
			flowActivationReadinessKey(plan.RunID, req.Instance.InstancePath): {
				InstancePath: req.Instance.InstancePath, Plan: plan,
				OwningRunSource: authorActivityTestSourceArtifactFact,
				RunStatus:       "cancelled", InstanceStatus: "terminated", InstanceTerminatedAt: terminatedAt,
			},
		},
	}
	agents := &flowActivationTestStore{upserts: []PersistedAgent{{
		Config: managerTestAgentConfig(models.AgentConfig{
			ID:       "reviewer",
			Identity: plan.Agents[0].Identity,
			FlowPath: req.Instance.InstancePath,
		}),
		Status: "active",
	}}}
	routeStore := &flowActivationTestRouteStore{statusByPath: map[string]string{
		req.Instance.InstancePath: "active",
	}}
	bus := &flowActivationTestBus{routeStore: routeStore}
	am := newFlowActivationManager(t, bus, instances, agents)
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	if err := reconcileDynamicFlowRuntimeStartupForTest(am, testAuthorActivityContext(context.Background()), authorActivityTestSourceArtifactFact, false); err != nil {
		t.Fatalf("ReconstructDynamicFlowRuntimeStartupTopology: %v", err)
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); ok {
		t.Fatal("terminal readiness retained a process agent")
	}
	if bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("terminal readiness retained or restored a process route")
	}
	if len(instances.armedEntries) != 0 || len(bus.published) != 0 {
		t.Fatalf("terminal startup side effects: armed=%#v published=%#v", instances.armedEntries, bus.published)
	}
}

func TestEnsureFlowInstanceRestoresPersistedDeclaredAgentsWithoutNewLifecycleTransition(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	first := newFlowActivationManager(t, firstBus, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())

	if err := activateFlowInstanceForTest(first, ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	persistedCount := len(agents.upserts)
	if persistedCount != 2 {
		t.Fatalf("persisted agents = %d, want 2", persistedCount)
	}

	restartBus := &flowActivationTestBus{routeStore: firstBus.routeStore}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	if created, err := restarted.EnsureFlowInstance(ctx, req); err != nil {
		t.Fatalf("EnsureFlowInstance: %v", err)
	} else if created {
		t.Fatal("EnsureFlowInstance reported a new instance")
	}
	if len(agents.upserts) != persistedCount {
		t.Fatalf("persisted agent transitions = %d, want unchanged %d", len(agents.upserts), persistedCount)
	}
	for _, agentID := range []string{"reviewer", "writer"} {
		if _, ok := testAgentConfig(t, restarted, agentID, "review/inst-1"); !ok {
			t.Fatalf("persisted declared agent %s was not restored into process state", agentID)
		}
	}
}

func TestEnsureFlowInstanceReconcilesRevisedSemanticSourceIntoReadinessOwner(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	firstBundle := testFlowBundleWithTwoAgents(t, "")
	firstReq := testActivationRequest(firstBundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())
	first := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	if err := activateFlowInstanceForTest(first, ctx, firstReq); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}

	revisedBundle := testFlowBundleWithTwoAgents(t, "")
	revisedBundle.Semantics.Version = "v-revised"
	delete(revisedBundle.FlowTree.ByID["review"].Agents, "reviewer")
	writer := revisedBundle.FlowTree.ByID["review"].Agents["writer"]
	writer.Role = "writer-v2"
	revisedBundle.FlowTree.ByID["review"].Agents["writer"] = writer
	revisedBundle.FlowTree.ByID["review"].Agents["editor"] = managerTestAgentEntry("editor", runtimecontracts.AgentRegistryEntry{
		ID: "editor", Type: "generic", Role: "editor", Subscriptions: []string{"task.started"},
	})
	registerTestFlowAgentOwner(revisedBundle, "review", "editor")
	revisedReq := testActivationRequest(revisedBundle, "review", "inst-1", "ent-1", "review/inst-1")
	restarted := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(revisedBundle))
	if created, err := restarted.EnsureFlowInstance(ctx, revisedReq); err != nil {
		t.Fatalf("EnsureFlowInstance revised source: %v", err)
	} else if created {
		t.Fatal("EnsureFlowInstance reported a new instance for source revision")
	}
	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		ctx,
		revisedReq.TriggerEvent.RunID(),
		revisedReq.Instance.Route(),
	)
	if err != nil || !found {
		t.Fatalf("load revised readiness: found=%v err=%v", found, err)
	}
	if readiness.Plan.WorkflowVersion != "v-revised" || readiness.TopologyReadyAt.IsZero() {
		t.Fatalf("revised readiness = %#v", readiness)
	}
	if _, ok := testAgentConfig(t, restarted, "reviewer", "review/inst-1"); ok {
		t.Fatal("removed reviewer remains process-visible")
	}
	if cfg, ok := testAgentConfig(t, restarted, "writer", "review/inst-1"); !ok || cfg.Role != "writer-v2" {
		t.Fatalf("changed writer config = %#v found=%t", cfg, ok)
	}
	if cfg, ok := testAgentConfig(t, restarted, "editor", "review/inst-1"); !ok || cfg.Role != "editor" {
		t.Fatalf("added editor config = %#v found=%t", cfg, ok)
	}
	if len(agents.terminated) != 1 || agents.terminated[0] != "reviewer" {
		t.Fatalf("retired agents = %#v, want reviewer", agents.terminated)
	}
	persisted, err := agents.LoadAgents(ctx)
	if err != nil {
		t.Fatalf("LoadAgents after source revision: %v", err)
	}
	if len(persisted) != 2 {
		t.Fatalf("persisted revised agent set = %#v, want writer/editor", persisted)
	}
}

func TestEnsureFlowInstanceVerifiesReadinessAfterNamedMutationCommit(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	first := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	bundle := testFlowBundleWithTwoAgents(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := activateFlowInstanceForTest(first, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}

	restartBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	setFlowActivationManagerSemanticSource(restarted, semanticview.Wrap(bundle))
	ctx := testAuthorActivityContext(context.Background())
	ctx = worklifetime.WithOccurrence(ctx, restarted.workOwner)
	if created, err := restarted.EnsureFlowInstance(ctx, req); err != nil {
		t.Fatalf("EnsureFlowInstance: %v", err)
	} else if created {
		t.Fatal("EnsureFlowInstance reported a new instance")
	}
	if !restartBus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("flow route was not process-ready after named mutation commit")
	}
	for _, agentID := range []string{"reviewer", "writer"} {
		if _, ok := testAgentConfig(t, restarted, agentID, "review/inst-1"); !ok {
			t.Fatalf("persisted declared agent %s was not restored after commit", agentID)
		}
	}
}

func TestActivateFlowInstanceUsesStagedInitialState(t *testing.T) {
	bus := &flowActivationTestBus{}
	store := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, store)
	bundle := testFlowBundle(t, "")
	schema := bundle.FlowSchemas["review"]
	schema.StageDeclarations = runtimecontracts.FlowStageDeclarations{
		Declared: true,
		Entries: []runtimecontracts.FlowStageDeclaration{
			{ID: "queued", Initial: true},
			{ID: "done", Terminal: true},
		},
	}
	bundle.FlowSchemas["review"] = schema

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(store.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(store.creates))
	}
	if got := store.creates[0].CurrentState; got != "queued" {
		t.Fatalf("created instance current state = %q, want queued", got)
	}
}

func TestActivateFlowInstancePassesActivationConfigToRouteMaterialization(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle(t, "")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"vertical_id": "11111111-1111-4111-8111-111111111111",
	}
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.addedRouteRequests) != 1 {
		t.Fatalf("route materialization requests = %#v, want one", bus.addedRouteRequests)
	}
	got := bus.addedRouteRequests[0].ActivationVariables["vertical_id"]
	if got != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("route activation variable vertical_id = %q, want config value", got)
	}
	if got := bus.addedRouteRequests[0].ActivationVariables["instance_id"]; got != "inst-1" {
		t.Fatalf("route activation variable instance_id = %q, want inst-1", got)
	}
}

func TestActivateFlowInstanceRejectsAgentNameInterpolationBeforeMutation(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle(t, "")
	bundle.FlowTree.ByID["review"].Agents["reviewer"] = runtimecontracts.AgentRegistryEntry{
		ID:             "reviewer-{flow_instance_path}",
		Type:           "generic",
		Role:           "reviewer",
		ResolvedIntent: managerTestResolvedIntent("reviewer"),
		Subscriptions:  []string{"task.started"},
	}

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"flow_instance_path": "wrong-config-path",
		"flow_scope_key":     "wrong-config-scope",
		"instance_id":        "wrong-config-instance",
		"template_id":        "wrong-config-template",
	}
	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
	if err == nil || !strings.Contains(err.Error(), "contains interpolation") || !strings.Contains(err.Error(), "instance.by") {
		t.Fatalf("ActivateFlowInstance error = %v, want agent-name interpolation teaching error", err)
	}
	if len(bus.addedRouteRequests) != 0 || len(am.ListAgentConfigs()) != 0 {
		t.Fatalf("rejected interpolation mutated activation: routes=%#v agents=%#v", bus.addedRouteRequests, am.ListAgentConfigs())
	}
}

func TestActivateFlowInstancePublishesAutoEmitEvent(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "task.started")
	const runID = "11111111-1111-1111-1111-111111111115"
	const triggerEventID = "33333333-3333-3333-3333-333333333333"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEventWithMode(triggerEventID, executionmode.Mock, runID)

	err := activateFlowInstanceForTest(am, ctx, req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	var autoEmit *events.Event
	for idx := range bus.published {
		if string(bus.published[idx].Type()) == "review/inst-1/task.started" {
			autoEmit = &bus.published[idx]
			break
		}
	}
	if autoEmit == nil {
		t.Fatalf("published events = %#v, want review/inst-1/task.started", bus.published)
	}
	if got := autoEmit.EntityID(); got != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("published entity_id = %q, want %q", got, runtimepipeline.FlowInstanceEntityID("review/inst-1"))
	}
	if got := strings.TrimSpace(autoEmit.RunID()); got != runID {
		t.Fatalf("published run_id = %q, want active run %q", got, runID)
	}
	if got := strings.TrimSpace(autoEmit.ParentEventID()); got != triggerEventID {
		t.Fatalf("published parent_event_id = %q, want trigger event %q", got, triggerEventID)
	}
	if got := autoEmit.ExecutionMode(); got != executionmode.Mock {
		t.Fatalf("published execution mode = %q, want mock", got)
	}
	if len(instances.lifecycleModes) != 1 || instances.lifecycleModes[0] != executionmode.Mock {
		t.Fatalf("initial lifecycle modes = %#v, want [mock]", instances.lifecycleModes)
	}
	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, runID, req.Instance.Route())
	if err != nil || !found || readiness.Plan.ExecutionMode != executionmode.Mock {
		t.Fatalf("durable activation execution mode: found=%v err=%v readiness=%#v", found, err, readiness)
	}
	if got, _ := autoEmit.ContextMap("")["source_event_id"].(string); got != triggerEventID {
		t.Fatalf("event context source_event_id = %q, want trigger event %q", got, triggerEventID)
	}
}

func TestActivateFlowInstancePreservesReplyContextIntoAutoEmit(t *testing.T) {
	bundle := testFlowBundle(t, "review.created")
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:child-activation"}}
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.publishedContexts) != 1 || bus.publishedContexts[0].ReplyContextID() != req.Context.ReplyContextID() {
		t.Fatalf("child auto-emit contexts = %#v, want %q", bus.publishedContexts, req.Context.ReplyContextID())
	}
	if !bus.published[0].DeliveryContext().Empty() {
		t.Fatalf("child auto-emit exposed reply context on business event: %#v", bus.published[0].DeliveryContext())
	}
}

func TestActivateFlowInstanceAutoEmitRejectsMissingTriggerLineage(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = events.Event{}

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
	if err == nil || !strings.Contains(err.Error(), "requires exact trigger run_id and parent_event_id") {
		t.Fatalf("ActivateFlowInstance error = %v, want exact trigger lineage failure", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none", bus.published)
	}
}

func TestActivateFlowInstanceAutoEmitPublishesConfigPayloadWithoutActivationContext(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry(t, "component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"component_id":   {Type: "string"},
				"component_type": {Type: "string"},
			},
			Required: []string{"component_id", "component_type"},
		},
	})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"component_id":   "component-1",
		"component_type": "api",
	}

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/component.scaffold.start")
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["component_id"]; got != "component-1" {
		t.Fatalf("component_id payload = %#v, want component-1", got)
	}
	if got := payload["component_type"]; got != "api" {
		t.Fatalf("component_type payload = %#v, want api", got)
	}
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("auto-emit payload included activation context %q: %#v", key, payload)
		}
	}
	if got := event.EntityID(); got != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("auto-emit entity_id = %q, want flow instance entity", got)
	}
	if got := event.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("auto-emit flow_instance = %q, want review/inst-1", got)
	}
}

func TestActivateFlowInstanceAutoEmitKeepsPayloadSourceEventIDNonAuthoritative(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry(t, "component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"source_event_id": {Type: "string"},
			},
			Required: []string{"source_event_id"},
		},
	})
	const triggerEventID = "44444444-4444-4444-4444-444444444444"
	const payloadSourceEventID = "business-payload-source"
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(triggerEventID)
	req.Config = map[string]any{"source_event_id": payloadSourceEventID}

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/component.scaffold.start")
	if got := strings.TrimSpace(event.ParentEventID()); got != triggerEventID {
		t.Fatalf("parent_event_id = %q, want trigger event %q", got, triggerEventID)
	}
	if got, _ := event.ContextMap("")["source_event_id"].(string); got != triggerEventID {
		t.Fatalf("event context source_event_id = %q, want trigger event %q", got, triggerEventID)
	}
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["source_event_id"]; got != payloadSourceEventID {
		t.Fatalf("payload source_event_id = %#v, want business payload value %q", got, payloadSourceEventID)
	}
}

func TestActivateFlowInstanceCommittedAutoEmitUsesProjectedConfigPayload(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry(t, "component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"component_id": {Type: "string"},
			},
			Required: []string{"component_id"},
		},
	})
	ctx := testAuthorActivityContext(context.Background())
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{"component_id": "component-1"}

	if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/component.scaffold.start")
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["component_id"]; got != "component-1" {
		t.Fatalf("component_id payload = %#v, want component-1", got)
	}
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("queued auto-emit payload included activation context %q: %#v", key, payload)
		}
	}
}

func TestActivateFlowInstanceAutoEmitAllowsDeclaredTemplateIDBusinessField(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry(t, "repo.template.selected", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"template_id": {Type: "string"},
			},
			Required: []string{"template_id"},
		},
	})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{"template_id": "application-basic-v1"}

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/repo.template.selected")
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["template_id"]; got != "application-basic-v1" {
		t.Fatalf("template_id payload = %#v, want config business value", got)
	}
}

func TestActivateFlowInstancePublishesAutoEmitAfterNamedCommit(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle(t, "task.started")
	const runID = "22222222-2222-2222-2222-222222222215"
	const triggerEventID = "55555555-5555-5555-5555-555555555555"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(triggerEventID, runID)

	err := activateFlowInstanceForTest(am, ctx, req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.published) != 1 {
		t.Fatalf("published events after named commit = %d, want 1", len(bus.published))
	}
	if got := string(bus.published[0].Type()); got != "review/inst-1/task.started" {
		t.Fatalf("auto-emitted type = %q, want review/inst-1/task.started", got)
	}
	if got := strings.TrimSpace(bus.published[0].RunID()); got != runID {
		t.Fatalf("auto-emitted run_id = %q, want active run %q", got, runID)
	}
	if got := strings.TrimSpace(bus.published[0].ParentEventID()); got != triggerEventID {
		t.Fatalf("auto-emitted parent_event_id = %q, want trigger event %q", got, triggerEventID)
	}
}

func TestActivateFlowInstanceFinalizesIdenticalReplayWithoutDuplicateCreationSideEffects(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	store := &flowActivationTestStore{}
	am := newFlowActivationManager(t, bus, instances, store)
	bundle := testFlowBundle(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("first ActivateFlowInstance: %v", err)
	}
	firstCreates := len(instances.creates)
	firstRoutes := len(bus.addedPaths)
	firstPublished := len(bus.published)
	firstAgents := len(store.upserts)

	instances.materialization = runtimepipeline.WorkflowInitialMaterializationAlreadyExists
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("identical replay ActivateFlowInstance: %v", err)
	}
	if len(instances.creates) != firstCreates {
		t.Fatalf("creates = %d, want unchanged %d", len(instances.creates), firstCreates)
	}
	if len(bus.addedPaths) != firstRoutes {
		t.Fatalf("added paths = %#v, want unchanged route side effects", bus.addedPaths)
	}
	if len(bus.published) != firstPublished {
		t.Fatalf("published events = %#v, want unchanged auto-emit side effects", bus.published)
	}
	if len(store.upserts) != firstAgents {
		t.Fatalf("persisted agents = %#v, want unchanged agent side effects", store.upserts)
	}
}

func TestActivateFlowInstanceFailsClosedOnAutoEmitMissingRequiredField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundleWithAutoEmitEntry(t, "task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"reason": {Type: "string"},
			},
			Required: []string{"reason"},
		},
	})

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err == nil || !strings.Contains(err.Error(), "auto-emit task.started") || !strings.Contains(err.Error(), "reason is required") {
		t.Fatalf("ActivateFlowInstance error = %v, want missing required auto-emit schema failure", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none", bus.published)
	}
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", bus.runtimeLogs)
	}
	if len(instances.creates) != 0 {
		t.Fatalf("instance creates = %#v, want none", instances.creates)
	}
	if len(bus.addedPaths) != 0 {
		t.Fatalf("added paths = %#v, want none", bus.addedPaths)
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); ok {
		t.Fatal("unexpected activated agent config after auto-emit schema failure")
	}
}

func TestActivateFlowInstanceQueuedAutoEmitFailsClosedOnUndeclaredConfigField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "task.started")
	postCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	ctx := withFlowActivationPostCommit(testAuthorActivityContext(context.Background()), &postCommit)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"unexpected": "value",
	}

	err := activateFlowInstanceForTest(am, ctx, req)
	if err == nil || !strings.Contains(err.Error(), "auto-emit task.started") || !strings.Contains(err.Error(), "unexpected is not allowed") {
		t.Fatalf("ActivateFlowInstance error = %v, want undeclared auto-emit schema failure", err)
	}
	if len(postCommit) != 0 {
		t.Fatalf("post-commit actions = %d, want 0", len(postCommit))
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none", bus.published)
	}
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", bus.runtimeLogs)
	}
	if len(instances.creates) != 0 {
		t.Fatalf("instance creates = %#v, want none", instances.creates)
	}
	if len(bus.addedPaths) != 0 {
		t.Fatalf("added paths = %#v, want none", bus.addedPaths)
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); ok {
		t.Fatal("unexpected activated agent config after queued auto-emit schema failure")
	}
}

func TestActivateFlowInstanceAutoEmitFailsClosedOnUndeclaredEnvelopeLikeConfigField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"entity_id": "business-value",
	}

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
	if err == nil || !strings.Contains(err.Error(), "auto-emit task.started") || !strings.Contains(err.Error(), "entity_id is not allowed") {
		t.Fatalf("ActivateFlowInstance error = %v, want undeclared envelope-like config field failure", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none", bus.published)
	}
	if len(instances.creates) != 0 {
		t.Fatalf("instance creates = %#v, want none", instances.creates)
	}
}

func TestValidateAutoEmitPayload_RejectsListTypeViolation(t *testing.T) {
	bundle := testFlowBundleWithAutoEmitEntry(t, "task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"instance_id":      {Type: "string"},
				"template_id":      {Type: "string"},
				"flow_path":        {Type: "string"},
				"parent_entity_id": {Type: "string"},
				"sources":          {Type: "[SourceID]"},
			},
			Required: []string{"instance_id", "template_id", "flow_path", "parent_entity_id", "sources"},
		},
	})
	bundle.RootTypes = runtimecontracts.TypeCatalogDocument{
		Scalars: map[string]runtimecontracts.ScalarTypeDecl{
			"SourceID": {Base: "text"},
		},
	}

	err := validateAutoEmitPayload(semanticview.Wrap(bundle), "review", "task.started", map[string]any{
		"instance_id":      "inst-1",
		"template_id":      "review",
		"flow_path":        "review/inst-1",
		"parent_entity_id": "ent-parent",
		"sources":          "not-a-list",
	})
	if err == nil {
		t.Fatal("expected list-type auto-emit failure")
	}
	if !strings.Contains(err.Error(), "$.sources must be array") {
		t.Fatalf("validateAutoEmitPayload error = %v, want list-type detail", err)
	}
}

func TestValidateAutoEmitPayload_AllowsNamedTypeThroughCanonicalSchema(t *testing.T) {
	bundle := testFlowBundleWithAutoEmitEntry(t, "task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"instance_id":      {Type: "string"},
				"template_id":      {Type: "string"},
				"flow_path":        {Type: "string"},
				"parent_entity_id": {Type: "string"},
				"details":          {Type: "ReviewDetails"},
			},
			Required: []string{"instance_id", "template_id", "flow_path", "parent_entity_id", "details"},
		},
	})
	bundle.RootTypes = runtimecontracts.TypeCatalogDocument{
		Types: map[string]runtimecontracts.NamedTypeDecl{
			"ReviewDetails": {
				Fields: map[string]runtimecontracts.TypeFieldSpec{
					"summary": {Type: "text"},
				},
			},
		},
	}

	err := validateAutoEmitPayload(semanticview.Wrap(bundle), "review", "task.started", map[string]any{
		"instance_id":      "inst-1",
		"template_id":      "review",
		"flow_path":        "review/inst-1",
		"parent_entity_id": "ent-parent",
		"details":          map[string]any{"summary": "ready"},
	})
	if err != nil {
		t.Fatalf("validateAutoEmitPayload valid named type: %v", err)
	}

	err = validateAutoEmitPayload(semanticview.Wrap(bundle), "review", "task.started", map[string]any{
		"instance_id":      "inst-1",
		"template_id":      "review",
		"flow_path":        "review/inst-1",
		"parent_entity_id": "ent-parent",
		"details":          "not-object",
	})
	if err == nil {
		t.Fatal("expected named-type auto-emit violation")
	}
	if !strings.Contains(err.Error(), "$.details must be object") {
		t.Fatalf("validateAutoEmitPayload error = %v, want named-type detail", err)
	}
}

func TestNormalizedStaticFlowEmitEvents_ExternalizesLocalEvents(t *testing.T) {
	got := normalizedStaticFlowEmitEvents(
		[]string{"analysis.done", "shared.event"},
		nil,
		map[string]struct{}{"analysis.done": {}},
		"analyzer-flow",
	)
	if len(got) != 2 || got[0] != "analyzer-flow/analysis.done" || got[1] != "shared.event" {
		t.Fatalf("normalizedStaticFlowEmitEvents = %#v", got)
	}
}

func TestNormalizedFlowAgentEmitEvents_ExternalizesInstanceLocalEvents(t *testing.T) {
	got := normalizedFlowAgentEmitEvents(
		[]string{"task.started", "shared.event"},
		nil,
		map[string]struct{}{"task.started": {}},
		"parent/review/inst-1",
	)
	if len(got) != 2 || got[0] != "parent/review/inst-1/task.started" || got[1] != "shared.event" {
		t.Fatalf("normalizedFlowAgentEmitEvents = %#v", got)
	}
}

func TestActivateFlowInstancePersistsFlowInstanceConfig(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundleWithAutoEmitEntry(t, "task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"name":     {Type: "string"},
				"priority": {Type: "integer"},
			},
		},
	})

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"name":     "alpha",
		"priority": 1,
	}
	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(instances.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(instances.creates))
	}
	got := instances.creates[0]
	if got.StorageRef != "review/inst-1" {
		t.Fatalf("storage_ref = %q, want review/inst-1", got.StorageRef)
	}
	if got.EntityID != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("entity_id = %#v, want %q", got.EntityID, runtimepipeline.FlowInstanceEntityID("review/inst-1"))
	}
	if got.Config["name"] != "alpha" {
		t.Fatalf("config name = %#v, want alpha", got.Config["name"])
	}
	if got.Config["priority"] != 1 {
		t.Fatalf("config priority = %#v, want 1", got.Config["priority"])
	}
}

func TestActivateFlowInstancePersistsFullParentRouteMetadata(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-legacy", "review/inst-1")
	req.Instance.ParentRoute = runtimeflowidentity.ParentRoute{
		FlowID:       "operating",
		FlowInstance: "operating/root",
		EntityID:     "parent-ent",
	}
	req.Instance.ParentEntityID = "parent-ent"
	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(instances.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(instances.creates))
	}
	created := instances.creates[0]
	if got := created.ParentFlowID; got != "operating" {
		t.Fatalf("parent_flow_id = %#v, want operating", got)
	}
	if got := created.ParentFlowInstance; got != "operating/root" {
		t.Fatalf("parent_flow_instance = %#v, want operating/root", got)
	}
	if got := created.ParentEntityID; got != "parent-ent" {
		t.Fatalf("parent_entity_id = %#v, want parent-ent", got)
	}
}

func TestActivateFlowInstanceResolvesAgentPermissions(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle(t, "")
	reviewFlow := bundle.FlowTree.ByID["review"]
	reviewFlow.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"permission_bundles": {
			Value: map[string]any{
				"ops": map[string]any{
					"permissions": []any{"ask_human"},
				},
			},
		},
	}}
	bundle.FlowTree.Root.Children[0].Policy = reviewFlow.Policy
	entry := reviewFlow.Agents["reviewer"]
	entry.PermissionsBundle = "ops"
	entry.Permissions = []string{"schedule"}
	reviewFlow.Agents["reviewer"] = entry
	bundle.FlowTree.Root.Children[0].Agents["reviewer"] = entry

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	cfg, ok := testAgentConfig(t, am, "reviewer", "review/inst-1")
	if !ok {
		t.Fatal("expected activated flow agent config")
	}
	if len(cfg.Permissions) != 2 || cfg.Permissions[0] != "ask_human" || cfg.Permissions[1] != "schedule" {
		t.Fatalf("permissions = %#v, want [ask_human schedule]", cfg.Permissions)
	}
}

func TestDeactivateFlowInstanceRemovesAgentsAndRoutes(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle(t, "")

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := deactivateFlowInstanceForTest(am, flowActivationRunContext(), "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); ok {
		t.Fatal("expected flow agent teardown")
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed pairs = %#v, want [review/inst-1]", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("terminated paths = %#v, want [review/inst-1]", instances.terminatedPaths)
	}
	if len(instances.terminatedAtSeen) != 1 || instances.terminatedAtSeen[0].IsZero() {
		t.Fatalf("terminated_at seen = %#v, want one non-zero timestamp", instances.terminatedAtSeen)
	}
}

func TestDeactivateFlowInstanceConsumesTerminalEvidenceAfterNamedCommit(t *testing.T) {
	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{}
	am := newFlowActivationManager(t, bus, instances, managerStore)
	bundle := testFlowBundle(t, "")

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	quiescenceCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := am.lifecycle.waitForWork(quiescenceCtx); err != nil {
		t.Fatalf("wait for activation backlog replay: %v", err)
	}
	if err := deactivateFlowInstanceForTest(am, flowActivationRunContext(), "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}

	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("flow instance terminal state = %#v, want committed owner entry", instances.terminatedPaths)
	}
	if _, ok := testAgentConfig(t, am, "reviewer", "review/inst-1"); ok {
		t.Fatal("expected flow agent teardown after named commit")
	}
	if len(bus.unsubscribed) != 0 {
		t.Fatalf("generic unsubscribe used after post-commit flush: %#v", bus.unsubscribed)
	}
	if len(managerStore.terminated) != 1 || managerStore.terminated[0] != "reviewer" {
		t.Fatalf("agent terminations after post-commit flush = %#v, want reviewer", managerStore.terminated)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed routes after post-commit flush = %#v, want [review/inst-1]", bus.removedPairs)
	}
	if got := routeStore.statusByPath["review/inst-1"]; got != "inactive" {
		t.Fatalf("route status after post-commit flush = %q, want inactive", got)
	}
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", bus.runtimeLogs)
	}
}

func TestDeactivateFlowInstanceReportsPostCommitAgentFailureAfterRouteRetirement(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{terminateErr: errors.New("agent terminate failed")}
	am := newFlowActivationManager(t, bus, instances, managerStore)
	bundle := testFlowBundle(t, "")

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := deactivateFlowInstanceForTest(am, flowActivationRunContext(), "review", "inst-1", "review/inst-1", "ent-1"); err == nil || !strings.Contains(err.Error(), "agent terminate failed") {
		t.Fatalf("DeactivateFlowInstance error = %v, want committed teardown failure", err)
	}
	if len(bus.runtimeLogs) != 1 {
		t.Fatalf("runtime logs = %#v, want one post-commit failure log", bus.runtimeLogs)
	}
	log := bus.runtimeLogs[0]
	if log.Action != "terminal_flow_instance_side_effects_failed" || log.Level != "warn" {
		t.Fatalf("runtime log = %#v, want warning terminal_flow_instance_side_effects_failed", log)
	}
	if log.Failure == nil || log.Failure.Class != runtimefailures.ClassInternalFailure || log.Failure.Detail.Code != "unclassified_runtime_error" {
		t.Fatalf("runtime log failure = %#v, want canonical internal failure", log.Failure)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed routes after agent failure = %#v, want committed route retirement", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("flow terminal state = %#v, want preserved terminal transition", instances.terminatedPaths)
	}
}

func TestDeactivateFlowInstanceFailsBeforePostCommitSideEffectsWhenRoutePersistenceFails(t *testing.T) {
	bus := &flowActivationTestBus{removeErr: errors.New("route removal failed")}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{}
	am := newFlowActivationManager(t, bus, instances, managerStore)
	bundle := testFlowBundle(t, "")

	if err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	postCommit := make([]runtimepipelinefixture.OwnerAction, 0, 1)
	ctx := withFlowActivationPostCommit(flowActivationRunContext(), &postCommit)
	err := deactivateFlowInstanceForTest(am, ctx, "review", "inst-1", "review/inst-1", "ent-1")
	if err == nil || !strings.Contains(err.Error(), "persist flow instance terminalization") || !strings.Contains(err.Error(), "route removal failed") {
		t.Fatalf("DeactivateFlowInstance error = %v, want exact route persistence failure", err)
	}

	runtimepipelinefixture.FlushPostCommitActions(postCommit)
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want no post-commit side-effect failure", bus.runtimeLogs)
	}
	if len(managerStore.terminated) != 0 {
		t.Fatalf("agent terminations after route failure = %#v, want none", managerStore.terminated)
	}
	if len(bus.removedPairs) != 0 {
		t.Fatalf("process route retirements after route failure = %#v, want none", bus.removedPairs)
	}
	if len(postCommit) != 0 {
		t.Fatalf("post-commit actions after route failure = %d, want none", len(postCommit))
	}
	if len(instances.terminatedPaths) != 0 {
		t.Fatalf("terminal state survived route replacement rollback: %#v", instances.terminatedPaths)
	}
	instance, ok, loadErr := instances.Load(context.Background(), runtimeflowidentity.RouteForInstancePath("review/inst-1"))
	if loadErr != nil || !ok || instance.Status != "active" {
		t.Fatalf("flow instance after route replacement rollback: found=%t instance=%#v err=%v", ok, instance, loadErr)
	}
}

func TestDeactivateFlowInstanceUsesExactResolvedFlowPathForNestedTemplate(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testNestedFlowBundle(t)

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "grandchild", "inst-1", "ent-1", "child/grandchild/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := deactivateFlowInstanceForTest(am, flowActivationRunContext(), "grandchild", "inst-1", "child/grandchild/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "child/grandchild/inst-1" {
		t.Fatalf("removed pairs = %#v, want [child/grandchild/inst-1]", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "child/grandchild/inst-1" {
		t.Fatalf("terminated paths = %#v, want [child/grandchild/inst-1]", instances.terminatedPaths)
	}
}

func TestBuildFlowAgentConfig_ExternalizesLocalSubscriptionsAndEmitEventsFromExactFlowPath(t *testing.T) {
	source := semanticview.Wrap(testNestedFlowBundle(t))
	cfg, err := buildFlowAgentConfig(
		source,
		managerTestFlowAgentNamePlan(t, source, "grandchild", "worker"),
		"grandchild",
		"inst-1",
		"ent-1",
		"child/grandchild/inst-1",
		"worker",
		managerTestAgentEntry("worker", runtimecontracts.AgentRegistryEntry{
			ID:            "worker",
			Type:          "generic",
			Role:          "worker",
			Subscriptions: []string{"micro.started"},
			EmitEvents:    []string{"micro.started"},
		}),
		map[string]string{"instance_id": "inst-1"},
		map[string]struct{}{"micro.started": {}},
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "child/grandchild/inst-1/micro.started" {
		t.Fatalf("subscriptions = %#v, want [child/grandchild/inst-1/micro.started]", cfg.Subscriptions)
	}
	if len(cfg.EmitEvents) != 1 || cfg.EmitEvents[0] != "child/grandchild/inst-1/micro.started" {
		t.Fatalf("emit_events = %#v, want [child/grandchild/inst-1/micro.started]", cfg.EmitEvents)
	}
}

func TestStaticAndTemplateAgentMaterializationDefaultRoleToEffectiveName(t *testing.T) {
	bundle := testFlowBundle(t, "")
	review := bundle.FlowTree.ByID["review"]
	entry := review.Agents["reviewer"]
	entry.ID = "public-reviewer"
	entry.Role = ""
	review.Agents["reviewer"] = entry
	bundle.FlowTree.Root.Children[0] = *review
	source := semanticview.Wrap(bundle)
	namePlan := managerTestFlowAgentNamePlan(t, source, "review", "reviewer")

	staticCfg, err := buildStaticFlowAgentConfig(source, namePlan, "review", "review", "reviewer", entry, staticFlowLocalEventSetForTest(review.Agents))
	if err != nil {
		t.Fatalf("buildStaticFlowAgentConfig: %v", err)
	}
	templateCfg, err := buildFlowAgentConfig(
		source,
		namePlan,
		"review",
		"inst-1",
		"ent-1",
		"review/inst-1",
		"reviewer",
		entry,
		nil,
		staticFlowLocalEventSetForTest(review.Agents),
		nil,
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	for _, cfg := range []models.AgentConfig{staticCfg, templateCfg} {
		if cfg.ID != "public-reviewer" || cfg.Role != "public-reviewer" {
			t.Fatalf("materialized actor = id %q role %q, want effective public role", cfg.ID, cfg.Role)
		}
		if err := runtimeauthority.NewSourceProvider(source).AuthorizeNotifyHuman(cfg); err != nil {
			t.Fatalf("materialized actor mailbox authority: %v", err)
		}
	}
}

func TestStaticFlowRequiredAgentMaterializationRegistersSubscriptions(t *testing.T) {
	bundle := testStaticFlowBundle()
	records, err := StaticFlowRequiredAgentMaterializationRecords(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("StaticFlowRequiredAgentMaterializationRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("materialized agents = %#v, want analyzer", records)
	}
	cfg := records[0].Config
	if got := cfg.FlowID; got != "analyzer-flow" {
		t.Fatalf("flow_id = %q, want analyzer-flow", got)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "analyzer-flow/analysis.requested" {
		t.Fatalf("subscriptions = %#v, want [analyzer-flow/analysis.requested]", cfg.Subscriptions)
	}
	if got := cfg.EmitEvents; len(got) != 1 || got[0] != "analyzer-flow/analysis.done" {
		t.Fatalf("emit_events = %#v, want [analyzer-flow/analysis.done]", got)
	}
}

func TestStandingActivatedFlowAgentsAreOwnedOnlyByFlowInstanceActivation(t *testing.T) {
	bundle := testStaticFlowBundle()
	schema := bundle.FlowSchemas["analyzer-flow"]
	schema.Mode = runtimecontracts.FlowModeSingleton
	schema.Activation = runtimecontracts.FlowActivationStanding
	bundle.FlowSchemas["analyzer-flow"] = schema
	bundle.FlowTree.ByID["analyzer-flow"].Schema = schema
	bundle.FlowTree.Root.Children[0].Schema = schema
	source := semanticview.Wrap(bundle)

	staticRecords, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		t.Fatalf("StaticAgentMaterializationRecords: %v", err)
	}
	if len(staticRecords) != 0 {
		t.Fatalf("standing static agent records = %#v, want none before materialized activation", staticRecords)
	}
	requiredRecords, err := StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		t.Fatalf("StaticFlowRequiredAgentMaterializationRecords: %v", err)
	}
	if len(requiredRecords) != 0 {
		t.Fatalf("standing required-agent records = %#v, want none before materialized activation", requiredRecords)
	}
}

func TestStaticAgentMaterializationKeepsDistinctSameIDPhysicalDeclarations(t *testing.T) {
	projectOwner := "test://agent-name/packages/support-extension/worker"
	flowOwner := "test://agent-name/support/worker"
	projectEntry := managerTestAgentEntry("worker", runtimecontracts.AgentRegistryEntry{ID: "project-worker", Role: "project-worker"})
	flowEntry := managerTestAgentEntry("worker", runtimecontracts.AgentRegistryEntry{ID: "flow-worker", Role: "flow-worker"})
	root := &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{
		{
			Paths: runtimecontracts.FlowContractPaths{FlowPath: "support-extension", AgentsFile: "packages/support-extension/agents.yaml"},
			Path:  "support-extension", Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
			Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": projectEntry}, AgentURIs: map[string]string{"worker": projectOwner},
		},
		{
			Paths: runtimecontracts.FlowContractPaths{FlowPath: "support", AgentsFile: "support/agents.yaml"},
			Path:  "support", Schema: runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
			Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": flowEntry}, AgentURIs: map[string]string{"worker": flowOwner},
		},
	}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{Root: root, ByID: map[string]*runtimecontracts.FlowContractView{
			"support-extension": &root.Children[0], "support": &root.Children[1],
		}},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			projectOwner: {Kind: "agent", FlowID: "support-extension", LocalID: "worker", Full: projectOwner},
			flowOwner:    {Kind: "agent", FlowID: "support", LocalID: "worker", Full: flowOwner},
		}},
	}
	source := semanticview.Wrap(bundle)

	records, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, record := range records {
		got[record.Config.ID] = record.Config.Identity.Name.Owner
	}
	if len(got) != 2 || got["project-worker"] != projectOwner || got["flow-worker"] != flowOwner {
		t.Fatalf("materialized owners = %#v, want both exact declarations", got)
	}
}

func TestStaticFlowRequiredAgentMaterializationInfersFromOmittedRequiredAgents(t *testing.T) {
	bundle := testStaticFlowBundle()
	schema := bundle.FlowSchemas["analyzer-flow"]
	schema.RequiredAgents = nil
	schema.RequiredAgentsDeclared = false
	bundle.FlowSchemas["analyzer-flow"] = schema
	bundle.Semantics.FlowAgents = map[string][]runtimecontracts.FlowRequiredAgent{}
	bundle.Semantics.FlowAgentFacts = map[string][]runtimecontracts.RequiredAgentFact{}

	records, err := StaticFlowRequiredAgentMaterializationRecords(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("StaticFlowRequiredAgentMaterializationRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("materialized agents = %#v, want analyzer", records)
	}
	cfg := records[0].Config
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "analyzer-flow/analysis.requested" {
		t.Fatalf("subscriptions = %#v, want [analyzer-flow/analysis.requested]", cfg.Subscriptions)
	}
}

func TestStaticFlowRequiredAgentMaterializationUsesFlowOwnedDeclaration(t *testing.T) {
	source := loadNestedProjectStaticAgentSource(t)
	declarations := semanticview.AgentDeclarationsForOwner(source, "support")
	if len(declarations) != 1 || declarations[0].Source.FlowPath != "support" {
		t.Fatalf("support declarations = %#v, want one flow-owned declaration", declarations)
	}
	records, err := StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		t.Fatalf("StaticFlowRequiredAgentMaterializationRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("materialized agents = %#v, want flow-owned project agent", records)
	}
	cfg := records[0].Config
	if cfg.ID != "backend" || cfg.FlowID != "support" || cfg.FlowPath != "support" || cfg.Identity.Name.Owner != declarations[0].OwnerURI {
		t.Fatalf("materialized agent = %#v, want exact canonical support declaration", cfg)
	}
}

func TestStaticRequiredAgentsForScopeRejectsRoleFallbackWithoutMapKey(t *testing.T) {
	bundle := testStaticFlowBundle()
	flow := bundle.FlowTree.ByID["analyzer-flow"]
	entry := flow.Agents["analyzer"]
	delete(flow.Agents, "analyzer")
	delete(flow.AgentURIs, "analyzer")
	flow.Agents["worker-alias"] = entry
	ref := testFlowAgentURIRef("analyzer-flow", "worker-alias")
	flow.AgentURIs["worker-alias"] = ref.Full
	bundle.URIRegistry.Agents = map[string]runtimecontracts.ContractURIRef{"analyzer-flow/worker-alias": ref}
	bundle.URIRegistry.ByURI = map[string]runtimecontracts.ContractURIRef{ref.Full: ref}
	schema := bundle.FlowSchemas["analyzer-flow"]
	schema.RequiredAgents = []runtimecontracts.FlowRequiredAgent{{
		Role:         "worker",
		SubscribesTo: []string{"analysis.requested"},
		Emits:        []string{"analysis.done"},
	}}
	bundle.FlowSchemas["analyzer-flow"] = schema

	records, err := StaticFlowRequiredAgentMaterializationRecords(semanticview.Wrap(bundle))

	if err == nil || !strings.Contains(err.Error(), `required agent "worker"`) {
		t.Fatalf("expected required-agent map-key error, records=%#v err=%v", records, err)
	}
}

func TestStaticAgentMaterializationRecordsRegistersRootAndFlowSubscriptions(t *testing.T) {
	source := loadRootAndFlowStaticAgentSource(t)
	records, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		t.Fatalf("StaticAgentMaterializationRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("materialized records = %#v, want root and flow declarations exactly once", records)
	}
	configs := map[string]models.AgentConfig{}
	for _, record := range records {
		configs[record.Config.ID] = record.Config
	}
	rootCfg := configs["test-agent"]
	if len(rootCfg.Subscriptions) != 1 || rootCfg.Subscriptions[0] != "task.assigned" {
		t.Fatalf("root subscriptions = %#v, want [task.assigned]", rootCfg.Subscriptions)
	}
	flowCfg := configs["operator"]
	if len(flowCfg.Subscriptions) != 1 || flowCfg.Subscriptions[0] != "ops-flow/work.requested" {
		t.Fatalf("flow subscriptions = %#v, want [ops-flow/work.requested]", flowCfg.Subscriptions)
	}
}

func TestStaticAgentMaterialization_FilesystemFlowOwnedAgentsCarryCanonicalFlowPath(t *testing.T) {
	source := loadFilesystemStaticAgentSource(t)
	records, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		t.Fatalf("StaticAgentMaterializationRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("materialized agents = %#v, want 1", records)
	}
	cfg := records[0].Config
	if cfg.FlowPath != "parent/child/support" {
		t.Fatalf("FlowPath = %q, want parent/child/support", cfg.FlowPath)
	}
	if cfg.FlowID != "parent/child/support" {
		t.Fatalf("FlowID = %q, want parent/child/support", cfg.FlowID)
	}
	if cfg.ID != "backend" {
		t.Fatalf("ID = %q, want backend", cfg.ID)
	}
}

func TestStaticAgentMaterialization_StructurallyNestedProjectAgentsStartWithOwningFlowPath(t *testing.T) {
	source := loadNestedProjectStaticAgentSource(t)
	records, err := StaticAgentMaterializationRecords(source)
	if err != nil {
		t.Fatalf("StaticAgentMaterializationRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("materialized agents = %#v, want 1", records)
	}
	cfg := records[0].Config
	if cfg.FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", cfg.FlowPath)
	}
	if cfg.FlowID != "support" {
		t.Fatalf("FlowID = %q, want support", cfg.FlowID)
	}
	if cfg.ID != "backend" {
		t.Fatalf("ID = %q, want backend", cfg.ID)
	}
}

func TestFlowInstanceAgentRecordsMaterializeProjectDeclarationOwnedByFlow(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mode         string
		instanceID   string
		instancePath string
	}{
		{name: "template", mode: runtimecontracts.FlowModeTemplate, instanceID: "inst-1", instancePath: "support/inst-1"},
		{name: "singleton", mode: runtimecontracts.FlowModeSingleton, instanceID: "support", instancePath: "support"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := loadNestedProjectAgentSource(t, tc.mode)
			declarations := semanticview.AgentDeclarations(source)
			if len(declarations) != 1 || declarations[0].Source.FlowPath != "support" || declarations[0].OwnerFlowID != "support" {
				t.Fatalf("declarations = %#v, want one declaration owned by support", declarations)
			}
			bundle, ok := semanticview.Bundle(source)
			if !ok {
				t.Fatal("loaded source does not expose its contract bundle")
			}
			scope, ok := source.FlowScopeByID("support")
			if !ok {
				t.Fatal("support flow scope missing")
			}
			schema, ok := source.FlowSchemaByID("support")
			if !ok {
				t.Fatal("support flow schema missing")
			}
			req := testActivationRequest(bundle, "support", tc.instanceID, "ent-1", tc.instancePath)
			am := newFlowActivationManager(t, &flowActivationTestBus{}, &flowActivationTestInstanceStore{})
			records, err := am.flowInstanceAgentRecords(req, schema, scope)
			if err != nil {
				t.Fatalf("flowInstanceAgentRecords: %v", err)
			}
			if len(records) != 1 {
				t.Fatalf("materialized records = %#v, want project-owned agent exactly once", records)
			}
			cfg := records[0].Config
			if cfg.ID != "backend" || cfg.FlowID != "support" || cfg.FlowPath != tc.instancePath {
				t.Fatalf("materialized config = %#v, want backend on exact support instance %q", cfg, tc.instancePath)
			}
			if cfg.Identity.Name.Owner != declarations[0].OwnerURI {
				t.Fatalf("materialized owner = %q, want canonical declaration owner %q", cfg.Identity.Name.Owner, declarations[0].OwnerURI)
			}
		})
	}
}

func TestActivateFlowInstanceFailsWithoutWorkflowInstanceStore(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{WorkOwner: newTestManagerWorkOwner(t)})

	err := activateFlowInstanceForTest(am, testAuthorActivityContext(context.Background()), testActivationRequest(testFlowBundle(t, ""), "review", "inst-1", "ent-1", "review/inst-1"))
	if err == nil || !strings.Contains(err.Error(), "workflow instance store is required") {
		t.Fatalf("ActivateFlowInstance err = %v, want workflow instance store error", err)
	}
}

func loadRootAndFlowStaticAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: root-and-flow-static-agents\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), "task.assigned: {}\ntask.completed: {}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), `
test-agent:
  type: generic
  role: test-agent
  intent: {inline: "Handle root work."}
  model: regular
  memory: true
  subscriptions: [task.assigned]
  emit_events: [task.completed]
`)

	writeFlowActivationFixtureFile(t, filepath.Join(root, "ops-flow", "schema.yaml"), "name: ops-flow\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "ops-flow", "events.yaml"), "work.requested: {}\nwork.completed: {}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "ops-flow", "agents.yaml"), `
operator:
  type: generic
  role: operator
  intent: {inline: "Handle operations work."}
  model: regular
  memory: true
  subscriptions: [work.requested]
  emit_events: [work.completed]
`)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadFilesystemStaticAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)

	flowRoot := filepath.Join(root, "parent", "child", "support")

	writeFlowActivationFixtureFile(t, filepath.Join(flowRoot, "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeFlowActivationFixtureFile(t, filepath.Join(flowRoot, "events.yaml"), `
item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(flowRoot, "agents.yaml"), `
backend:
  type: generic
  role: backend
  intent: {inline: "Handle backend work for this flow instance."}
  model: regular
  memory: true
  subscriptions:
    - item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadNestedProjectStaticAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	return loadNestedProjectAgentSource(t, runtimecontracts.FlowModeStatic)
}

func loadNestedProjectAgentSource(t *testing.T, mode string) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "support", "schema.yaml"), fmt.Sprintf(`
name: support
mode: %s
initial_state: waiting
states:
  - waiting
  - done
`, mode))
	writeFlowActivationFixtureFile(t, filepath.Join(root, "support", "events.yaml"), `
item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "support", "agents.yaml"), `
backend:
  type: generic
  role: backend
  intent: {inline: "Handle backend work for this flow instance."}
  model: regular
  memory: true
  subscriptions:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeFlowActivationFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func staticFlowLocalEventSetForTest(agents map[string]runtimecontracts.AgentRegistryEntry) map[string]struct{} {
	entries := make([]runtimecontracts.AgentRegistryEntry, 0, len(agents))
	for _, entry := range agents {
		entries = append(entries, entry)
	}
	return staticFlowLocalEventSetForEntries(entries)
}

func managerTestFlowAgentNamePlan(t *testing.T, source semanticview.Source, flowID, logicalID string) semanticview.AgentNamePlan {
	t.Helper()
	scope, ok := source.FlowScopeByID(flowID)
	if !ok {
		t.Fatalf("flow scope %q not found", flowID)
	}
	plan, err := semanticview.FlowAgentNamePlan(source, scope, logicalID)
	if err != nil {
		t.Fatalf("flow scope %q agent %q name plan: %v", flowID, logicalID, err)
	}
	return plan
}

func TestBuildFlowAgentConfig_PassesContractToolsAndEmitEvents(t *testing.T) {
	source := semanticview.Wrap(testFlowBundle(t, ""))
	cfg, err := buildFlowAgentConfig(
		source,
		managerTestFlowAgentNamePlan(t, source, "review", "reviewer"),
		"review",
		"inst-1",
		"ent-1",
		"review/inst-1",
		"reviewer",
		managerTestAgentEntry("reviewer", runtimecontracts.AgentRegistryEntry{
			ID:              "reviewer",
			Type:            "generic",
			Role:            "reviewer",
			Tools:           []string{"schedule", "check_status"},
			NativeTools:     map[string]any{"bash": true, "file_io": true},
			EmitEvents:      []string{"task.completed", "task.completed", "review.failed"},
			MaxTurnsPerTask: 7,
		}),
		map[string]string{"instance_id": "inst-1"},
		map[string]struct{}{"task.completed": {}, "review.failed": {}},
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	if got := cfg.MaxTurnsPerTask; got != 7 {
		t.Fatalf("max_turns_per_task = %d, want 7", got)
	}
	if got := cfg.Tools; len(got) != 2 || got[0] != "check_status" || got[1] != "schedule" {
		t.Fatalf("tools = %#v, want [check_status schedule]", got)
	}
	if got := cfg.EmitEvents; len(got) != 2 || got[0] != "review/inst-1/review.failed" || got[1] != "review/inst-1/task.completed" {
		t.Fatalf("emit_events = %#v, want [review/inst-1/review.failed review/inst-1/task.completed]", got)
	}
	if !cfg.NativeTools.Bash || !cfg.NativeTools.FileIO {
		t.Fatalf("native_tools = %#v, want bash/file_io true", cfg.NativeTools)
	}
}

func TestBuildFlowAgentConfigRejectsPayloadDerivedNestedSystemPromptBeforeMaterialization(t *testing.T) {
	source := semanticview.Wrap(testFlowBundle(t, ""))
	_, err := buildFlowAgentConfig(
		source,
		managerTestFlowAgentNamePlan(t, source, "review", "reviewer"),
		"review",
		"inst-1",
		"ent-1",
		"review/inst-1",
		"reviewer",
		managerTestAgentEntry("reviewer", runtimecontracts.AgentRegistryEntry{
			ID:   "reviewer",
			Type: "generic",
			Role: "reviewer",
		}),
		map[string]string{"instance_id": "inst-1"},
		map[string]struct{}{},
		map[string]any{"opaque": []any{map[string]any{"system_prompt": "obsolete"}}},
	)
	if err == nil || !strings.Contains(err.Error(), "RETIRED") || !strings.Contains(err.Error(), "config.opaque[0].system_prompt") {
		t.Fatalf("buildFlowAgentConfig error = %v, want nested authored system_prompt rejection", err)
	}
}

func TestStaticAndDynamicFlowAgentConfigRejectForeignExactAndPattern(t *testing.T) {
	source := semanticview.Wrap(testFlowBundle(t, ""))
	for _, subscription := range []string{"foreign/task.ready", "foreign/**/task.ready"} {
		t.Run(strings.ReplaceAll(subscription, "/", "_"), func(t *testing.T) {
			entry := managerTestAgentEntry("reviewer", runtimecontracts.AgentRegistryEntry{ID: "reviewer", Type: "generic", Subscriptions: []string{subscription}})
			namePlan := managerTestFlowAgentNamePlan(t, source, "review", "reviewer")
			if _, err := buildStaticFlowAgentConfig(source, namePlan, "review", "review", "reviewer", entry, map[string]struct{}{"task.started": {}}); err == nil || !strings.Contains(err.Error(), "nearest common ancestor schema.yaml") {
				t.Fatalf("buildStaticFlowAgentConfig error = %v, want admission rejection", err)
			}
			if _, err := buildFlowAgentConfig(source, namePlan, "review", "inst-1", "ent-1", "review/inst-1", "reviewer", entry, nil, map[string]struct{}{"task.started": {}}, nil); err == nil || !strings.Contains(err.Error(), "nearest common ancestor schema.yaml") {
				t.Fatalf("buildFlowAgentConfig error = %v, want admission rejection", err)
			}
		})
	}
}

func TestBuildFlowAgentConfigRebasesAdmittedSameScopeExactToConcreteInstance(t *testing.T) {
	source := semanticview.Wrap(testFlowBundle(t, ""))
	cfg, err := buildFlowAgentConfig(
		source,
		managerTestFlowAgentNamePlan(t, source, "review", "reviewer"),
		"review",
		"inst-1",
		"ent-1",
		"review/inst-1",
		"reviewer",
		managerTestAgentEntry("reviewer", runtimecontracts.AgentRegistryEntry{
			ID:            "reviewer",
			Type:          "generic",
			Subscriptions: []string{"review/task.started"},
		}),
		nil,
		map[string]struct{}{"task.started": {}},
		nil,
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "review/inst-1/task.started" {
		t.Fatalf("subscriptions = %#v, want concrete instance subscription", cfg.Subscriptions)
	}
}

func TestStaticAndTemplateAgentMaterializationConsumeEffectivePlatformDefaults(t *testing.T) {
	source := loadAgentPlatformDefaultsMaterializationSource(t)

	staticScope, ok := source.FlowScopeByID("static_support")
	if !ok {
		t.Fatal("static_support flow scope missing")
	}
	staticEntry := staticScope.Agents["worker"]
	staticCfg, err := buildStaticFlowAgentConfig(source, managerTestFlowAgentNamePlan(t, source, "static_support", "worker"), "static_support", "static_support", "worker", staticEntry, staticFlowLocalEventSetForTest(staticScope.Agents))
	if err != nil {
		t.Fatalf("buildStaticFlowAgentConfig: %v", err)
	}
	assertMaterializedAgentPlatformDefaults(t, staticCfg)

	templateScope, ok := source.FlowScopeByID("template_support")
	if !ok {
		t.Fatal("template_support flow scope missing")
	}
	templateEntry := templateScope.Agents["worker"]
	templateCfg, err := buildFlowAgentConfig(
		source,
		managerTestFlowAgentNamePlan(t, source, "template_support", "worker"),
		"template_support",
		"inst-1",
		"entity-1",
		"template_support/inst-1",
		"worker",
		templateEntry,
		map[string]string{"instance_id": "inst-1"},
		staticFlowLocalEventSetForTest(templateScope.Agents),
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	assertMaterializedAgentPlatformDefaults(t, templateCfg)
}

func assertMaterializedAgentPlatformDefaults(t *testing.T, cfg models.AgentConfig) {
	t.Helper()
	if cfg.Type != runtimecontracts.DefaultAgentType {
		t.Fatalf("Type = %q, want %q", cfg.Type, runtimecontracts.DefaultAgentType)
	}
	if cfg.Memory != agentmemory.PlatformDefault() {
		t.Fatalf("Memory = %+v, want platform default false", cfg.Memory)
	}
	if cfg.MaxTurnsPerTask != runtimecontracts.DefaultAgentMaxTurnsPerTask {
		t.Fatalf("MaxTurnsPerTask = %d, want %d", cfg.MaxTurnsPerTask, runtimecontracts.DefaultAgentMaxTurnsPerTask)
	}
	if cfg.WorkspaceClass != "" {
		t.Fatalf("WorkspaceClass = %q, want empty default", cfg.WorkspaceClass)
	}
}

func loadAgentPlatformDefaultsMaterializationSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: agent-defaults-materialization\n")

	for _, flowID := range []string{"static_support", "template_support"} {
		writeFlowActivationFixtureFile(t, filepath.Join(root, flowID, "schema.yaml"), "name: "+flowID+"\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flowID, "events.yaml"), flowID+".requested:\n  entity_id: string\n")
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "static_support", "agents.yaml"), `
worker:
  intent: {inline: "Handle static support requests."}
  model: regular
  subscriptions:
    - static_support.requested
  emit_events: []
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "template_support", "agents.yaml"), `
worker:
  id: worker
  intent: {inline: "Handle template support requests."}
  model: regular
  subscriptions:
    - template_support.requested
  emit_events: []
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
