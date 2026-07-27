package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

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
}

type flowActivationWorkflowModule struct {
	source  semanticview.Source
	guards  runtimepipeline.GuardRegistry
	actions runtimepipeline.ActionRegistry
}

type flowActivationTestAgentRoutePreparation struct {
	deliveries chan *worklifetime.EventDelivery
}

func (p *flowActivationTestAgentRoutePreparation) Deliveries() <-chan *worklifetime.EventDelivery {
	return p.deliveries
}

func (*flowActivationTestAgentRoutePreparation) Publish() error { return nil }
func (*flowActivationTestAgentRoutePreparation) Discard() error { return nil }

func newFlowActivationWorkflowModule(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimepipeline.WorkflowModule {
	t.Helper()
	source := semanticview.Wrap(bundle)
	return flowActivationWorkflowModule{
		source:  source,
		guards:  runtimepipeline.NewContractGuardRegistry(source),
		actions: runtimepipeline.NewContractActionRegistry(source),
	}
}

func (m flowActivationWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m flowActivationWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return nil
}

func (m flowActivationWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return nil
}

func (m flowActivationWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guards
}

func (m flowActivationWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actions
}

type flowActivationTestRouteStore struct {
	statusByPath map[string]string
}

type flowActivationTestInstanceStore struct {
	readinessMu      sync.Mutex
	creates          []runtimepipeline.WorkflowInstance
	upserts          []runtimepipeline.WorkflowInstance
	terminatedPaths  []string
	terminatedAtSeen []time.Time
	byStorageRef     map[string]runtimepipeline.WorkflowInstance
	routeLoads       []runtimeflowidentity.Route
	materialization  runtimepipeline.WorkflowInitialMaterializationResult
	armedEntries     []string
	armInitialEntry  func(string) error
	readiness        map[string]runtimepipeline.DynamicFlowRuntimeReadiness
	creationMarkErr  error
	creationMarked   func()
	topologyMarked   func()
	beforeCreation   func()
}

type flowActivationTestStore struct {
	upserts      []PersistedAgent
	terminated   []string
	terminateErr error
	failAgentID  string
}

func newFlowActivationManager(t *testing.T, bus Bus, instances flowInstancePersistence, stores ...ManagerPersistence) *AgentManager {
	t.Helper()
	if len(stores) == 0 {
		stores = []ManagerPersistence{&flowActivationTestStore{}}
	}
	var lifecycleStore AgentLifecyclePersistence
	lifecycleStore, _ = stores[0].(AgentLifecyclePersistence)
	return newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{
		WorkflowInstances: instances,
		LifecycleStore:    lifecycleStore,
		WorkOwner:         newTestManagerWorkOwner(t),
	}, stores...)
}

func withFlowActivationPostCommit(ctx context.Context, actions *[]runtimepipeline.OwnerAction) context.Context {
	rollback := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, actions)
	return runtimepipeline.WithPipelineRollbackActions(ctx, &rollback)
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
		s.readiness[flowActivationReadinessKey(plan.RunID, instance.StorageRef)] = runtimepipeline.DynamicFlowRuntimeReadiness{
			InstancePath:   instance.StorageRef,
			Plan:           plan,
			RunStatus:      "running",
			InstanceStatus: "active",
		}
		s.readinessMu.Unlock()
	}
	return runtimepipeline.WorkflowInitialMaterializationCreated, nil
}

func (s *flowActivationTestInstanceStore) ArmInitialEntryTimers(_ context.Context, instanceID string) error {
	instanceID = strings.TrimSpace(instanceID)
	s.armedEntries = append(s.armedEntries, instanceID)
	if s.armInitialEntry != nil {
		return s.armInitialEntry(instanceID)
	}
	return nil
}

func flowActivationReadinessKey(runID, instanceID string) string {
	return strings.TrimSpace(runID) + "\x00" + strings.TrimSpace(instanceID)
}

func (s *flowActivationTestInstanceStore) LoadDynamicFlowRuntimeReadiness(_ context.Context, runID, instanceID string) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	item, ok := s.readiness[flowActivationReadinessKey(runID, instanceID)]
	return item, ok, nil
}

func (s *flowActivationTestInstanceStore) ListDynamicFlowRuntimeReadiness(context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	out := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0, len(s.readiness))
	for _, item := range s.readiness {
		if item.Eligible() {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *flowActivationTestInstanceStore) ListDynamicFlowRuntimeReadinessKeys(context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadinessKey, error) {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	var keys []runtimepipeline.DynamicFlowRuntimeReadinessKey
	for _, item := range s.readiness {
		keys = append(keys, runtimepipeline.DynamicFlowRuntimeReadinessKey{
			RunID:        item.Plan.RunID,
			InstancePath: item.InstancePath,
		})
	}
	return keys, nil
}

func (s *flowActivationTestInstanceStore) MarkDynamicFlowRuntimeTopologyReady(_ context.Context, runID, instanceID string, readyAt time.Time) error {
	s.readinessMu.Lock()
	defer s.readinessMu.Unlock()
	key := flowActivationReadinessKey(runID, instanceID)
	item, ok := s.readiness[key]
	if !ok || !item.Eligible() {
		return fmt.Errorf("readiness not found")
	}
	if item.TopologyReadyAt.IsZero() {
		item.TopologyReadyAt = readyAt
	}
	s.readiness[key] = item
	if s.topologyMarked != nil {
		s.topologyMarked()
	}
	return nil
}

func (s *flowActivationTestInstanceStore) CommitDynamicFlowRuntimeCreationOccurrence(
	ctx context.Context,
	req runtimepipeline.DynamicFlowRuntimeCreationOccurrenceRequest,
	publisher runtimepipeline.DynamicFlowRuntimeCreationOccurrencePublisher,
) error {
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
	if err := publisher.PublishInMutation(ctx, req.Event); err != nil {
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

func (s *flowActivationTestInstanceStore) MarkTerminated(_ context.Context, storageRef string, terminatedAt time.Time) error {
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

func (s *flowActivationTestInstanceStore) Load(_ context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error) {
	if s.byStorageRef == nil {
		return runtimepipeline.WorkflowInstance{}, false, nil
	}
	instance, ok := s.byStorageRef[strings.TrimSpace(instanceID)]
	return instance, ok, nil
}

func (s *flowActivationTestInstanceStore) LoadRouteRecoveryProjection(_ context.Context, route runtimeflowidentity.Route) (runtimepipeline.WorkflowInstanceRouteRecoveryProjection, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	s.routeLoads = append(s.routeLoads, route)
	instance, ok := s.byStorageRef[route.InstancePath]
	if !ok {
		return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("active flow instance not found for route recovery: %s", route.InstancePath)
	}
	identity := runtimepipeline.StoredFlowInstance(nil, instance)
	if identity.Route() != route {
		return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("flow instance route recovery identity mismatch")
	}
	return runtimepipeline.WorkflowInstanceRouteRecoveryProjection{Identity: identity, Config: instance.Config}, nil
}

func (s *flowActivationTestStore) UpsertAgent(_ context.Context, rec PersistedAgent) error {
	s.upserts = append(s.upserts, rec)
	return nil
}

func (s *flowActivationTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return append([]PersistedAgent(nil), s.upserts...), nil
}
func (s *flowActivationTestStore) CommitAgentLifecycleTransition(_ context.Context, req AgentLifecycleTransition) (AgentLifecycleTransitionResult, error) {
	if req.TargetPhase == AgentLifecycleTerminated {
		if s.terminateErr != nil {
			return AgentLifecycleTransitionResult{}, s.terminateErr
		}
		s.terminated = append(s.terminated, strings.TrimSpace(req.AgentID))
	} else if strings.TrimSpace(req.AgentID) == strings.TrimSpace(s.failAgentID) {
		return AgentLifecycleTransitionResult{}, errors.New("injected agent registration failure")
	} else if req.Agent != nil {
		s.upserts = append(s.upserts, *req.Agent)
	}
	return AgentLifecycleTransitionResult{
		OperationID: req.OperationID, TransitionID: uuid.NewString(), AgentID: req.AgentID,
		PreviousEpoch: req.ExpectedEpoch, RuntimeEpoch: req.TargetEpoch,
		PreviousGeneration: req.ExpectedGeneration, Generation: req.TargetGeneration,
		PreviousPhase: req.ExpectedPhase, Phase: req.TargetPhase,
		ConfigRevision: req.ConfigRevision, RunMode: req.RunMode,
		Subordinate: sessions.LifecycleMutationOutcome{Action: req.Subordinate.Action},
	}, nil
}
func (*flowActivationTestStore) EnsureEntitySchema(context.Context, string) error { return nil }

func (*flowActivationTestBus) AdmitBundleSourceFact(ctx context.Context) (context.Context, error) {
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
func (*flowActivationTestBus) EngineOutbox() runtimeengine.OutboxWriter    { return nil }
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

func (unavailableFlowActivationPipelineObligations) Settle(context.Context, runtimepipelineobligation.Claim, runtimepipelineobligation.Disposition) error {
	return errors.New("flow activation fixture has no pipeline obligations")
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
func (b *flowActivationTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return nil
}

func (b *flowActivationTestBus) AddFlowInstanceRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	return b.AddFlowInstanceRouteContext(context.Background(), req)
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
	if _, transactional := runtimepipeline.PipelineSQLTxFromContext(ctx); transactional {
		if err := b.StageFlowInstanceRouteContext(ctx, req); err != nil {
			return err
		}
		if !runtimepipeline.QueuePipelinePostCommitAction(ctx, func(context.Context) {
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
	if runtimepipeline.QueuePipelinePostCommitAction(ctx, func(context.Context) {
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

type flowActivationStubAgent struct{ id string }

func (a flowActivationStubAgent) ID() string                      { return a.id }
func (flowActivationStubAgent) Type() string                      { return "generic" }
func (flowActivationStubAgent) Subscriptions() []events.EventType { return nil }
func (flowActivationStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func testFlowBundle(autoEmit string) *runtimecontracts.WorkflowContractBundle {
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.started": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{},
				},
			},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"reviewer": {
				ID:            "reviewer-{instance_id}",
				Type:          "generic",
				Role:          "reviewer",
				Subscriptions: []string{"task.started"},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*reviewFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": reviewFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"task.started"}},
				},
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{Event: autoEmit},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
}

func testFlowBundleWithTwoAgents(autoEmit string) *runtimecontracts.WorkflowContractBundle {
	bundle := testFlowBundle(autoEmit)
	bundle.FlowTree.ByID["review"].Agents["writer"] = runtimecontracts.AgentRegistryEntry{
		ID: "writer-{instance_id}", Type: "generic", Role: "writer", Subscriptions: []string{"task.started"},
	}
	return bundle
}

func testFlowBundleWithAutoEmitEntry(autoEmit string, entry runtimecontracts.EventCatalogEntry) *runtimecontracts.WorkflowContractBundle {
	bundle := testFlowBundle(autoEmit)
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

func testNestedFlowBundle() *runtimecontracts.WorkflowContractBundle {
	grandchild := &runtimecontracts.FlowContractView{
		Path:  "child/grandchild",
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				ID:            "worker-{instance_id}",
				Type:          "generic",
				Role:          "worker",
				Subscriptions: []string{"micro.started"},
			},
		},
	}
	child := &runtimecontracts.FlowContractView{
		Path:     "child",
		Paths:    runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Children: []runtimecontracts.FlowContractView{*grandchild},
	}
	return &runtimecontracts.WorkflowContractBundle{
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
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"micro.started"}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
}

func testStaticFlowBundle() *runtimecontracts.WorkflowContractBundle {
	analysisFlow := &runtimecontracts.FlowContractView{
		Path:  "analyzer-flow",
		Paths: runtimecontracts.FlowContractPaths{ID: "analyzer-flow"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {
				Type:          "generic",
				Role:          "analyzer",
				Subscriptions: []string{"analysis.requested"},
				EmitEvents:    []string{"analysis.done"},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
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
					Inputs:  runtimecontracts.FlowInputPins{Events: []string{"analysis.requested"}},
					Outputs: runtimecontracts.FlowOutputPins{Events: []string{"analysis.done"}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Version: "v-test",
			FlowAgents: map[string][]runtimecontracts.FlowRequiredAgent{
				"analyzer-flow": {{
					Role:         "analyzer",
					SubscribesTo: []string{"analysis.requested"},
					Emits:        []string{"analysis.done"},
				}},
			},
			FlowInputs: map[string][]string{
				"analyzer-flow": {"analysis.requested"},
			},
			FlowOutputs: map[string][]string{
				"analyzer-flow": {"analysis.done"},
			},
			FlowPrefix: map[string]string{
				"analyzer-flow": "analyzer-flow",
			},
		},
	}
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
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.InitialState = "queued"
	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.addedPaths) != 1 || bus.addedPaths[0] != "review/inst-1" {
		t.Fatalf("added paths = %#v, want [review/inst-1]", bus.addedPaths)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
		t.Fatal("expected activated flow agent config")
	}
	cfg, _ := am.GetAgentConfig("reviewer-inst-1")
	if got := strings.TrimSpace(cfg.EntityID); got != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("agent entity_id = %q, want %q", got, runtimepipeline.FlowInstanceEntityID("review/inst-1"))
	}
}

func TestActivateFlowInstanceDefersAgentStartupUntilMutationCommit(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := runtimepipeline.WithPipelineSQLTxContext(testAuthorActivityContext(context.Background()), &sql.Tx{})
	ctx = withFlowActivationPostCommit(ctx, &postCommit)

	if err := am.ActivateFlowInstance(ctx, testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.addedPaths) != 0 {
		t.Fatalf("transactional route materialized before commit: %#v", bus.addedPaths)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("flow agent started before the activation transaction committed")
	}
	if len(postCommit) != 1 {
		t.Fatalf("post-commit actions = %d, want one keyed readiness reconciliation", len(postCommit))
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
		t.Fatal("flow agent did not start after activation commit")
	}
	if len(bus.addedPaths) != 1 || bus.addedPaths[0] != "review/inst-1" {
		t.Fatalf("post-commit route materialization = %#v, want review/inst-1", bus.addedPaths)
	}
}

func TestActivateFlowInstanceArmsInitialTimersOnlyAfterRuntimeInstallation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		inMutation bool
	}{
		{name: "direct"},
		{name: "post_commit", inMutation: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
					return errors.New("timer armed before agent installation")
				}
				return nil
			}
			am = newFlowActivationManager(t, bus, instances)
			ctx := testAuthorActivityContext(context.Background())
			postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
			if tc.inMutation {
				ctx = runtimepipeline.WithPipelineSQLTxContext(ctx, &sql.Tx{})
				ctx = withFlowActivationPostCommit(ctx, &postCommit)
			}

			if err := am.ActivateFlowInstance(ctx, testActivationRequest(testFlowBundle(""), "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
				t.Fatalf("ActivateFlowInstance: %v", err)
			}
			if tc.inMutation {
				if len(instances.armedEntries) != 0 {
					t.Fatalf("armed entries before commit = %#v, want none", instances.armedEntries)
				}
				if len(postCommit) != 1 {
					t.Fatalf("post-commit actions = %d, want one keyed readiness reconciliation", len(postCommit))
				}
				runtimepipeline.FlushPipelinePostCommitActions(postCommit)
			}
			if len(instances.armedEntries) != 1 || instances.armedEntries[0] != "review/inst-1" {
				t.Fatalf("armed entries = %#v, want [review/inst-1]", instances.armedEntries)
			}
			if len(bus.runtimeLogs) != 0 {
				t.Fatalf("runtime logs = %#v, want no activation failure", bus.runtimeLogs)
			}
		})
	}
}

func TestDynamicFlowRuntimeReadinessRecoversEveryFinalizationBoundary(t *testing.T) {
	for _, mode := range []struct {
		name       string
		postCommit bool
	}{
		{name: "direct"},
		{name: "post_commit", postCommit: true},
	} {
		for _, boundary := range []string{"partial_agent", "extra_agent", "route", "arm", "creation_event", "creation_commit"} {
			t.Run(mode.name+"/"+boundary, func(t *testing.T) {
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
				bundle := testFlowBundleWithTwoAgents("task.started")
				am.semanticSource = semanticview.Wrap(bundle)
				req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

				switch boundary {
				case "partial_agent":
					agentStore.failAgentID = "writer-inst-1"
				case "extra_agent":
					agentStore.upserts = append(agentStore.upserts, PersistedAgent{
						Config: models.AgentConfig{ID: "stale-inst-1", FlowPath: "review/inst-1"},
						Status: "active",
					})
				case "route":
					if mode.postCommit {
						break
					}
					bus.addErr = errors.New("injected route failure")
				case "arm":
					instances.armInitialEntry = func(string) error { return errors.New("injected arm failure") }
				case "creation_event":
					bus.publishErr = errors.New("injected creation event failure")
				case "creation_commit":
					instances.creationMarkErr = errors.New("injected completion mark failure")
				}

				ctx := testAuthorActivityContext(context.Background())
				var postCommit []runtimepipeline.OwnerAction
				if mode.postCommit {
					postCommit = make([]runtimepipeline.OwnerAction, 0, 1)
					ctx = runtimepipeline.WithPipelineSQLTxContext(ctx, &sql.Tx{})
					ctx = withFlowActivationPostCommit(ctx, &postCommit)
				}
				err := am.ActivateFlowInstance(ctx, req)
				if mode.postCommit {
					if err != nil {
						t.Fatalf("queued ActivateFlowInstance: %v", err)
					}
					if len(postCommit) != 1 {
						t.Fatalf("post-commit actions = %d, want one keyed readiness reconciliation", len(postCommit))
					}
					if boundary == "route" {
						bus.addErr = errors.New("injected route failure")
					}
					runtimepipeline.FlushPipelinePostCommitActions(postCommit)
				} else if err == nil {
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
				if boundary == "partial_agent" && len(agentStore.upserts) != 1 {
					t.Fatalf("partial agent registrations = %d, want one", len(agentStore.upserts))
				}

				agentStore.failAgentID = ""
				if boundary == "extra_agent" {
					filtered := agentStore.upserts[:0]
					for _, rec := range agentStore.upserts {
						if rec.Config.ID != "stale-inst-1" {
							filtered = append(filtered, rec)
						}
					}
					agentStore.upserts = filtered
				}
				bus.addErr = nil
				instances.armInitialEntry = nil
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
				readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(recoveryCtx, req.TriggerEvent.RunID(), req.Instance.InstancePath)
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
	bundle := testFlowBundleWithTwoAgents("task.started")
	am.semanticSource = semanticview.Wrap(bundle)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := runtimepipeline.WithPipelineSQLTxContext(testAuthorActivityContext(context.Background()), &sql.Tx{})
	ctx = withFlowActivationPostCommit(ctx, &postCommit)

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if armCalls != 1 || len(bus.published) != 0 {
		t.Fatalf("first post-commit attempt: arms=%d published=%d", armCalls, len(bus.published))
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
		req.Instance.InstancePath,
	)
	if err != nil || !found || readiness.TopologyReadyAt.IsZero() || readiness.CreationEventEmittedAt.IsZero() {
		t.Fatalf("automatic retry readiness: found=%v readiness=%#v err=%v", found, readiness, err)
	}
	if armCalls != 2 || len(bus.published) != 1 {
		t.Fatalf("automatic retry side effects: arms=%d published=%d, want 2/1", armCalls, len(bus.published))
	}
}

func TestDynamicFlowRuntimeReadinessNoAutoEmitArmFailureRemainsPendingUntilAutomaticRetry(t *testing.T) {
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
	bundle := testFlowBundle("")
	am.semanticSource = semanticview.Wrap(bundle)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err == nil {
		t.Fatal("ActivateFlowInstance succeeded across injected timer arm failure")
	}
	readiness, found, err := instances.LoadDynamicFlowRuntimeReadiness(
		context.Background(),
		req.TriggerEvent.RunID(),
		req.Instance.InstancePath,
	)
	if err != nil || !found || !readiness.Pending() || !readiness.TopologyReadyAt.IsZero() {
		t.Fatalf("failed no-auto readiness: found=%v readiness=%#v err=%v", found, readiness, err)
	}
	if bus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("failed no-auto readiness left process route published")
	}

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
		req.Instance.InstancePath,
	)
	if err != nil || !found || readiness.Pending() || readiness.TopologyReadyAt.IsZero() {
		t.Fatalf("completed no-auto readiness: found=%v readiness=%#v err=%v", found, readiness, err)
	}
	if armCalls != 2 || len(bus.published) != 0 {
		t.Fatalf("no-auto retry side effects: arms=%d events=%d, want 2/0", armCalls, len(bus.published))
	}
}

func TestDynamicFlowRuntimeReadinessCoalescesConcurrentAttemptsByRunAndInstance(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances, agents)
	bundle := testFlowBundleWithTwoAgents("task.started")
	am.semanticSource = semanticview.Wrap(bundle)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	stageEntered := make(chan struct{}, 1)
	stageRelease := make(chan struct{})
	var stageCalls atomic.Int32
	bus.stageRoute = func(runtimebus.FlowInstanceRouteMaterializationRequest) error {
		stageCalls.Add(1)
		stageEntered <- struct{}{}
		<-stageRelease
		return nil
	}
	leaderErr := make(chan error, 1)
	go func() {
		leaderErr <- am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req)
	}()
	<-stageEntered

	followerErrs := make(chan error, 3)
	ensureCtx, cancelEnsure := context.WithCancel(testAuthorActivityContext(context.Background()))
	pendingCtx, cancelPending := context.WithCancel(testAuthorActivityContext(context.Background()))
	startupCtx, cancelStartup := context.WithCancel(testAuthorActivityContext(context.Background()))
	go func() {
		_, err := am.EnsureFlowInstance(ensureCtx, req)
		followerErrs <- err
	}()
	go func() {
		followerErrs <- am.reconcilePendingDynamicFlowRuntimeReadiness(
			pendingCtx,
			semanticview.Wrap(bundle),
		)
	}()
	go func() {
		_, err := am.HydrateForStartup(startupCtx)
		followerErrs <- err
	}()
	cancelEnsure()
	cancelPending()
	cancelStartup()
	for range 3 {
		if err := <-followerErrs; !errors.Is(err, context.Canceled) {
			t.Fatalf("coalesced follower: %v, want context cancellation from in-flight attempt", err)
		}
	}
	close(stageRelease)
	if err := <-leaderErr; err != nil {
		t.Fatalf("coalesced leader: %v", err)
	}
	if stageCalls.Load() != 1 || len(instances.armedEntries) != 1 || len(bus.published) != 1 {
		t.Fatalf(
			"coalesced side effects: stage=%d arm=%d publish=%d, want 1/1/1",
			stageCalls.Load(),
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
	bundle := testFlowBundleWithTwoAgents("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err == nil {
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
			semanticview.Wrap(bundle),
		)
	}()
	<-stageEntered
	if err := instances.MarkTerminated(context.Background(), req.Instance.InstancePath, time.Now().UTC()); err != nil {
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
	bundle := testFlowBundleWithTwoAgents("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())

	if err := am.ActivateFlowInstance(ctx, req); err == nil {
		t.Fatal("activation unexpectedly completed the creation occurrence")
	}
	instances.creationMarkErr = nil
	instances.beforeCreation = func() {
		instances.beforeCreation = nil
		if err := instances.MarkTerminated(context.Background(), req.Instance.InstancePath, time.Now().UTC()); err != nil {
			t.Fatalf("terminalize at creation boundary: %v", err)
		}
	}
	err := am.reconcileDynamicFlowRuntimeReadiness(
		ctx,
		req.TriggerEvent.RunID(),
		req.Instance.InstancePath,
		semanticview.Wrap(bundle),
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
	for _, agentID := range []string{"reviewer-inst-1", "writer-inst-1"} {
		if _, ok := am.GetAgentConfig(agentID); ok {
			t.Fatalf("terminal creation boundary left process agent %s", agentID)
		}
	}
}

func TestRecoverableStateSnapshotIncludesReadinessOnlyPendingWork(t *testing.T) {
	runID := uuid.NewString()
	path := "review/inst-1"
	instances := &flowActivationTestInstanceStore{
		readiness: map[string]runtimepipeline.DynamicFlowRuntimeReadiness{
			flowActivationReadinessKey(runID, path): {
				InstancePath: path,
				RunStatus:    "running", InstanceStatus: "active",
				Plan: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
					Version: 1, RunID: runID, WorkflowVersion: "1.0.0",
					Identity: runtimeflowidentity.Instance{
						TemplateID: "review", ScopeKey: "review", InstanceID: "inst-1",
						InstancePath: path, EntityID: uuid.NewString(), HasStoredPath: true,
					},
				},
			},
		},
	}
	am := newFlowActivationManager(t, &flowActivationTestBus{}, instances)
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

func TestHydrateForStartupFinalizesIncompleteDynamicFlowRuntimeReadiness(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{failAgentID: "writer-inst-1"}
	firstBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	first := newFlowActivationManager(t, firstBus, instances, agents)
	bundle := testFlowBundleWithTwoAgents("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())

	if err := first.ActivateFlowInstance(ctx, req); err == nil {
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
	restarted.semanticSource = semanticview.Wrap(bundle)
	if _, err := restarted.HydrateForStartup(ctx); err != nil {
		t.Fatalf("HydrateForStartup: %v", err)
	}
	if _, ok := restarted.GetAgentConfig("reviewer-inst-1"); !ok {
		t.Fatal("restarted manager did not restore first declared agent")
	}
	if _, ok := restarted.GetAgentConfig("writer-inst-1"); !ok {
		t.Fatal("restarted manager did not reconcile missing declared agent")
	}
	if len(instances.armedEntries) != 1 {
		t.Fatalf("startup timer arm count = %d, want one", len(instances.armedEntries))
	}
	if len(restartBus.published) != 1 {
		t.Fatalf("startup creation event count = %d, want one", len(restartBus.published))
	}
	if _, err := restarted.HydrateForStartup(ctx); err != nil {
		t.Fatalf("second HydrateForStartup: %v", err)
	}
	if len(restartBus.published) != 1 {
		t.Fatalf("creation events after repeated startup recovery = %d, want one", len(restartBus.published))
	}
}

func TestHydrateForStartupRetiresTerminalDynamicFlowProcessTopology(t *testing.T) {
	bundle := testFlowBundle("")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	plan, err := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
		Version:  1,
		Identity: req.Instance,
		RunID:    req.TriggerEvent.RunID(), WorkflowVersion: bundle.WorkflowVersion(),
		Agents: []runtimepipeline.DynamicFlowRuntimeAgentExpectation{{
			AgentID: "reviewer-inst-1", ConfigRevision: strings.Repeat("a", 64),
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
				RunStatus: "cancelled", InstanceStatus: "terminated", InstanceTerminatedAt: terminatedAt,
			},
		},
	}
	agents := &flowActivationTestStore{upserts: []PersistedAgent{{
		Config: models.AgentConfig{ID: "reviewer-inst-1", FlowPath: req.Instance.InstancePath},
		Status: "active",
	}}}
	routeStore := &flowActivationTestRouteStore{statusByPath: map[string]string{
		req.Instance.InstancePath: "active",
	}}
	bus := &flowActivationTestBus{routeStore: routeStore}
	if err := bus.PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest{
		Identity: req.Instance.Route(),
	}); err != nil {
		t.Fatalf("seed process route: %v", err)
	}
	am := newFlowActivationManager(t, bus, instances, agents)
	am.semanticSource = semanticview.Wrap(bundle)
	if _, err := am.HydrateForStartup(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("HydrateForStartup: %v", err)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
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
	bundle := testFlowBundleWithTwoAgents("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx := testAuthorActivityContext(context.Background())

	if err := first.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	persistedCount := len(agents.upserts)
	if persistedCount != 2 {
		t.Fatalf("persisted agents = %d, want 2", persistedCount)
	}

	restartBus := &flowActivationTestBus{routeStore: firstBus.routeStore}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	restarted.semanticSource = semanticview.Wrap(bundle)
	if created, err := restarted.EnsureFlowInstance(ctx, req); err != nil {
		t.Fatalf("EnsureFlowInstance: %v", err)
	} else if created {
		t.Fatal("EnsureFlowInstance reported a new instance")
	}
	if len(agents.upserts) != persistedCount {
		t.Fatalf("persisted agent transitions = %d, want unchanged %d", len(agents.upserts), persistedCount)
	}
	for _, agentID := range []string{"reviewer-inst-1", "writer-inst-1"} {
		if _, ok := restarted.GetAgentConfig(agentID); !ok {
			t.Fatalf("persisted declared agent %s was not restored into process state", agentID)
		}
	}
}

func TestEnsureFlowInstanceDefersReadinessVerificationUntilMutationCommit(t *testing.T) {
	instances := &flowActivationTestInstanceStore{}
	agents := &flowActivationTestStore{}
	first := newFlowActivationManager(t, &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}, instances, agents)
	bundle := testFlowBundleWithTwoAgents("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	if err := first.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}

	restartBus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	restarted := newFlowActivationManager(t, restartBus, instances, agents)
	restarted.semanticSource = semanticview.Wrap(bundle)
	postCommit := make([]runtimepipeline.OwnerAction, 0, 2)
	ctx := runtimepipeline.WithPipelineSQLTxContext(testAuthorActivityContext(context.Background()), &sql.Tx{})
	ctx = withFlowActivationPostCommit(ctx, &postCommit)
	ctx = worklifetime.WithOccurrence(ctx, restarted.workOwner)
	if created, err := restarted.EnsureFlowInstance(ctx, req); err != nil {
		t.Fatalf("EnsureFlowInstance: %v", err)
	} else if created {
		t.Fatal("EnsureFlowInstance reported a new instance")
	}
	if len(postCommit) != 1 {
		t.Fatalf("post-commit actions = %d, want one keyed readiness reconciliation", len(postCommit))
	}
	if restartBus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("flow route became process-ready before mutation commit")
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if !restartBus.HasFlowInstanceRoute(req.Instance.Route()) {
		t.Fatal("flow route was not process-ready before readiness verification")
	}
	for _, agentID := range []string{"reviewer-inst-1", "writer-inst-1"} {
		if _, ok := restarted.GetAgentConfig(agentID); !ok {
			t.Fatalf("persisted declared agent %s was not restored after commit", agentID)
		}
	}
}

func TestActivateFlowInstanceUsesStagedInitialState(t *testing.T) {
	bus := &flowActivationTestBus{}
	store := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, store)
	bundle := testFlowBundle("")
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
	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
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
	bundle := testFlowBundle("")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"vertical_id": "11111111-1111-4111-8111-111111111111",
	}
	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
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

func TestActivateFlowInstanceUsesSameBuiltinsForAgentAndRouteMaterialization(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")
	bundle.FlowTree.ByID["review"].Agents["reviewer"] = runtimecontracts.AgentRegistryEntry{
		ID:            "reviewer-{flow_instance_path}",
		Type:          "generic",
		Role:          "reviewer",
		Subscriptions: []string{"task.started"},
	}

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"flow_instance_path": "wrong-config-path",
		"flow_scope_key":     "wrong-config-scope",
		"instance_id":        "wrong-config-instance",
		"template_id":        "wrong-config-template",
	}
	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if _, ok := am.GetAgentConfig("reviewer-review/inst-1"); !ok {
		t.Fatalf("expected flow agent rendered with built-in flow_instance_path, configs=%#v", am.ListAgentConfigs())
	}
	if len(bus.addedRouteRequests) != 1 {
		t.Fatalf("route materialization requests = %#v, want one", bus.addedRouteRequests)
	}
	vars := bus.addedRouteRequests[0].ActivationVariables
	if got := vars["flow_instance_path"]; got != "review/inst-1" {
		t.Fatalf("route activation variable flow_instance_path = %q, want review/inst-1", got)
	}
	if got := vars["flow_scope_key"]; got != "review" {
		t.Fatalf("route activation variable flow_scope_key = %q, want review", got)
	}
	if got := vars["instance_id"]; got != "inst-1" {
		t.Fatalf("route activation variable instance_id = %q, want inst-1", got)
	}
	if got := vars["template_id"]; got != "review" {
		t.Fatalf("route activation variable template_id = %q, want review", got)
	}
}

func TestActivateFlowInstancePublishesAutoEmitEvent(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("task.started")
	const runID = "11111111-1111-1111-1111-111111111115"
	const triggerEventID = "33333333-3333-3333-3333-333333333333"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEventWithMode(triggerEventID, executionmode.Mock, runID)

	err := am.ActivateFlowInstance(ctx, req)
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
	if got, _ := autoEmit.ContextMap("")["source_event_id"].(string); got != triggerEventID {
		t.Fatalf("event context source_event_id = %q, want trigger event %q", got, triggerEventID)
	}
}

func TestActivateFlowInstancePreservesReplyContextIntoAutoEmit(t *testing.T) {
	bundle := testFlowBundle("review.created")
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:child-activation"}}
	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
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
	bundle := testFlowBundle("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = events.Event{}

	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req)
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
	bundle := testFlowBundleWithAutoEmitEntry("component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"component_id":   {Type: "string"},
				"component_type": {Type: "string"},
			},
		},
		Required: []string{"component_id", "component_type"},
	})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"component_id":   "component-1",
		"component_type": "api",
	}

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
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
	bundle := testFlowBundleWithAutoEmitEntry("component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"source_event_id": {Type: "string"},
			},
		},
		Required: []string{"source_event_id"},
	})
	const triggerEventID = "44444444-4444-4444-4444-444444444444"
	const payloadSourceEventID = "business-payload-source"
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(triggerEventID)
	req.Config = map[string]any{"source_event_id": payloadSourceEventID}

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
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

func TestActivateFlowInstanceQueuedAutoEmitUsesProjectedConfigPayload(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry("component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"component_id": {Type: "string"},
			},
		},
		Required: []string{"component_id"},
	})
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := withFlowActivationPostCommit(testAuthorActivityContext(context.Background()), &postCommit)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{"component_id": "component-1"}

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("auto-emit published before post-commit flush: %#v", bus.published)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
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
	bundle := testFlowBundleWithAutoEmitEntry("repo.template.selected", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"template_id": {Type: "string"},
			},
		},
		Required: []string{"template_id"},
	})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{"template_id": "application-basic-v1"}

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/repo.template.selected")
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["template_id"]; got != "application-basic-v1" {
		t.Fatalf("template_id payload = %#v, want config business value", got)
	}
}

func TestActivateFlowInstanceQueuesAutoEmitUntilPostCommitWhenAvailable(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("task.started")
	const runID = "22222222-2222-2222-2222-222222222215"
	const triggerEventID = "55555555-5555-5555-5555-555555555555"
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	ctx = withFlowActivationPostCommit(ctx, &postCommit)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(triggerEventID, runID)

	err := am.ActivateFlowInstance(ctx, req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("auto-emit published before post-commit flush: %#v", bus.published)
	}
	if len(postCommit) != 1 {
		t.Fatalf("post-commit actions = %d, want 1", len(postCommit))
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if len(bus.published) != 1 {
		t.Fatalf("published events after post-commit = %d, want 1", len(bus.published))
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
	bundle := testFlowBundle("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("first ActivateFlowInstance: %v", err)
	}
	firstCreates := len(instances.creates)
	firstRoutes := len(bus.addedPaths)
	firstPublished := len(bus.published)
	firstAgents := len(store.upserts)

	instances.materialization = runtimepipeline.WorkflowInitialMaterializationAlreadyExists
	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
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

func TestActivateFlowInstanceCanonicalStoreFinalizesIdenticalReplayWithoutDuplicateCreationSideEffects(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	const runID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	sourceFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		t.Fatal("canonical readiness test context missing bundle source fact")
	}
	bundleHash, bundleSource := sourceFact.StorageValues()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source)
		VALUES ($1::uuid, 'running', $2, $3)
		ON CONFLICT (run_id) DO NOTHING
	`, runID, bundleHash, bundleSource); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	agentStore := &flowActivationTestStore{}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	bundle := testFlowBundle("task.started")
	runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        newFlowActivationWorkflowModule(t, bundle),
		WorkflowStore: workflowStore,
		WorkOwner:     newTestManagerWorkOwner(t),
	})
	am := newFlowActivationManager(t, bus, workflowStore, agentStore)
	req := testActivationRequest(bundle, "review", "inst-1", "11111111-1111-1111-1111-111111111111", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(req.TriggerEvent.ID(), runID)

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("first ActivateFlowInstance: %v", err)
	}
	firstRoutes := len(bus.addedPaths)
	firstPublished := len(bus.published)
	firstAgents := len(agentStore.upserts)

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("identical replay ActivateFlowInstance: %v", err)
	}
	if len(bus.addedPaths) != firstRoutes {
		t.Fatalf("added paths = %#v, want unchanged route side effects", bus.addedPaths)
	}
	if len(bus.published) != firstPublished {
		t.Fatalf("published events = %#v, want unchanged auto-emit side effects", bus.published)
	}
	if len(agentStore.upserts) != firstAgents {
		t.Fatalf("persisted agents = %#v, want unchanged agent side effects", agentStore.upserts)
	}
}

func TestActivateFlowInstanceFailsClosedOnAutoEmitMissingRequiredField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"reason": {Type: "string"},
			},
		},
		Required: []string{"reason"},
	})

	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
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
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("unexpected activated agent config after auto-emit schema failure")
	}
}

func TestActivateFlowInstanceQueuedAutoEmitFailsClosedOnUndeclaredConfigField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle("task.started")
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := withFlowActivationPostCommit(testAuthorActivityContext(context.Background()), &postCommit)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"unexpected": "value",
	}

	err := am.ActivateFlowInstance(ctx, req)
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
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("unexpected activated agent config after queued auto-emit schema failure")
	}
}

func TestActivateFlowInstanceAutoEmitFailsClosedOnUndeclaredEnvelopeLikeConfigField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"entity_id": "business-value",
	}

	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req)
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
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"instance_id":      {Type: "string"},
				"template_id":      {Type: "string"},
				"flow_path":        {Type: "string"},
				"parent_entity_id": {Type: "string"},
				"sources":          {Type: "[SourceID]"},
			},
		},
		Required: []string{"instance_id", "template_id", "flow_path", "parent_entity_id", "sources"},
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
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"instance_id":      {Type: "string"},
				"template_id":      {Type: "string"},
				"flow_path":        {Type: "string"},
				"parent_entity_id": {Type: "string"},
				"details":          {Type: "ReviewDetails"},
			},
		},
		Required: []string{"instance_id", "template_id", "flow_path", "parent_entity_id", "details"},
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
		"review/inst-1",
		"review",
		"inst-1",
	)
	if len(got) != 2 || got[0] != "review/inst-1/task.started" || got[1] != "shared.event" {
		t.Fatalf("normalizedFlowAgentEmitEvents = %#v", got)
	}
}

func TestActivateFlowInstancePersistsFlowInstanceConfig(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
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
	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req)
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
	if got.Metadata["entity_id"] != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("metadata entity_id = %#v, want %q", got.Metadata["entity_id"], runtimepipeline.FlowInstanceEntityID("review/inst-1"))
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
	bundle := testFlowBundle("")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-legacy", "review/inst-1")
	req.Instance.ParentRoute = runtimeflowidentity.ParentRoute{
		FlowID:       "operating",
		FlowInstance: "operating/root",
		EntityID:     "parent-ent",
	}
	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(instances.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(instances.creates))
	}
	metadata := instances.creates[0].Metadata
	if got := metadata["parent_flow_id"]; got != "operating" {
		t.Fatalf("parent_flow_id = %#v, want operating", got)
	}
	if got := metadata["parent_flow_instance"]; got != "operating/root" {
		t.Fatalf("parent_flow_instance = %#v, want operating/root", got)
	}
	if got := metadata["parent_entity_id"]; got != "parent-ent" {
		t.Fatalf("parent_entity_id = %#v, want parent-ent", got)
	}
}

func TestActivateFlowInstanceResolvesAgentPermissions(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")
	reviewFlow := bundle.FlowTree.ByID["review"]
	reviewFlow.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"permission_bundles": {
			Value: map[string]any{
				"ops": map[string]any{
					"permissions": []any{"agent_fire"},
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

	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	cfg, ok := am.GetAgentConfig("reviewer-inst-1")
	if !ok {
		t.Fatal("expected activated flow agent config")
	}
	if len(cfg.Permissions) != 2 || cfg.Permissions[0] != "agent_fire" || cfg.Permissions[1] != "schedule" {
		t.Fatalf("permissions = %#v, want [agent_fire schedule]", cfg.Permissions)
	}
}

func TestDeactivateFlowInstanceRemovesAgentsAndRoutes(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testFlowBundle("")

	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := am.DeactivateFlowInstance(flowActivationRunContext(), "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
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

func TestDeactivateFlowInstanceQueuesTerminalSideEffectsUntilPostCommitWhenAvailable(t *testing.T) {
	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{}
	am := newFlowActivationManager(t, bus, instances, managerStore)
	bundle := testFlowBundle("")

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := withFlowActivationPostCommit(flowActivationRunContext(), &postCommit)
	if err := am.DeactivateFlowInstance(ctx, "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}

	if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
		t.Fatal("flow agent was torn down before post-commit flush")
	}
	if len(bus.unsubscribed) != 0 {
		t.Fatalf("unsubscribed before post-commit flush = %#v, want none", bus.unsubscribed)
	}
	if len(managerStore.terminated) != 0 {
		t.Fatalf("agent terminations before post-commit flush = %#v, want none", managerStore.terminated)
	}
	if len(bus.removedPairs) != 0 {
		t.Fatalf("removed routes before post-commit flush = %#v, want none", bus.removedPairs)
	}
	if got := routeStore.statusByPath["review/inst-1"]; got != "inactive" {
		t.Fatalf("route status before post-commit flush = %q, want transactionally inactive", got)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("flow instance terminal state = %#v, want committed owner entry", instances.terminatedPaths)
	}
	if len(postCommit) != 2 {
		t.Fatalf("post-commit actions = %d, want route retirement and terminal side effects", len(postCommit))
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow agent teardown after post-commit flush")
	}
	if len(bus.unsubscribed) != 0 {
		t.Fatalf("generic unsubscribe used after post-commit flush: %#v", bus.unsubscribed)
	}
	if len(managerStore.terminated) != 1 || managerStore.terminated[0] != "reviewer-inst-1" {
		t.Fatalf("agent terminations after post-commit flush = %#v, want reviewer-inst-1", managerStore.terminated)
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

func TestDeactivateFlowInstanceLogsPostCommitAgentFailureAfterRouteRetirement(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{terminateErr: errors.New("agent terminate failed")}
	am := newFlowActivationManager(t, bus, instances, managerStore)
	bundle := testFlowBundle("")

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := withFlowActivationPostCommit(flowActivationRunContext(), &postCommit)
	if err := am.DeactivateFlowInstance(ctx, "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance returned pre-commit error: %v", err)
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
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
	bundle := testFlowBundle("")

	if err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	postCommit := make([]runtimepipeline.OwnerAction, 0, 1)
	ctx := withFlowActivationPostCommit(flowActivationRunContext(), &postCommit)
	err := am.DeactivateFlowInstance(ctx, "review", "inst-1", "review/inst-1", "ent-1")
	if err == nil || !strings.Contains(err.Error(), "stage exact terminal flow-instance route topology") {
		t.Fatalf("DeactivateFlowInstance error = %v, want exact route persistence failure", err)
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
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
}

func TestDeactivateFlowInstanceUsesExactResolvedFlowPathForNestedTemplate(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(t, bus, instances)
	bundle := testNestedFlowBundle()

	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(bundle, "grandchild", "inst-1", "ent-1", "child/grandchild/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := am.DeactivateFlowInstance(flowActivationRunContext(), "grandchild", "inst-1", "child/grandchild/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "child/grandchild/inst-1" {
		t.Fatalf("removed pairs = %#v, want [child/grandchild/inst-1]", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "child/grandchild/inst-1" {
		t.Fatalf("terminated paths = %#v, want [child/grandchild/inst-1]", instances.terminatedPaths)
	}
}

func TestDeactivateFlowInstanceModel_PersistsTerminalStateInFlowInstances(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	const runID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source)
		VALUES ($1::uuid, 'running', 'bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'ephemeral')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	bundle := testFlowBundle("")
	runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        newFlowActivationWorkflowModule(t, bundle),
		WorkflowStore: store,
		WorkOwner:     newTestManagerWorkOwner(t),
	})
	am := newFlowActivationManager(t, bus, store)
	const subjectID = "11111111-1111-1111-1111-111111111111"
	req := testActivationRequest(bundle, "review", "inst-1", subjectID, "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(req.TriggerEvent.ID(), runID)

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := store.Mutate(ctx, req.Instance.EntityID, func(instance *runtimepipeline.WorkflowInstance) {
		instance.CurrentState = "completed"
	}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	sharedAgent := flowActivationStubAgent{id: "shared-subject-agent"}
	sharedConfig := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "shared-subject-agent",
		EntityID:      req.Instance.EntityID,
		FlowPath:      "review/other-inst",
	}
	if err := am.lifecycle.registerExecution(ctx, PersistedAgent{Config: sharedConfig, Status: "active", HiredBy: "test"}, false, sharedAgent, testManagerSubscriptionAdmission(t, sharedConfig)); err != nil {
		t.Fatalf("register shared-subject-agent: %v", err)
	}

	if err := am.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		Instance:       req.Instance,
		FinalState:     "completed",
	}); err != nil {
		t.Fatalf("DeactivateFlowInstanceModel: %v", err)
	}

	var (
		status       string
		terminatedAt time.Time
	)
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT status, terminated_at
		FROM flow_instances
		WHERE instance_id = $1
	`, "review/inst-1").Scan(&status, &terminatedAt); err != nil {
		t.Fatalf("query flow_instances: %v", err)
	}
	if strings.TrimSpace(status) != "terminated" {
		t.Fatalf("flow_instances.status = %q, want terminated", status)
	}
	if terminatedAt.IsZero() {
		t.Fatal("flow_instances.terminated_at is zero")
	}
	routeStatus := routeStore.statusByPath["review/inst-1"]
	if strings.TrimSpace(routeStatus) != "inactive" {
		t.Fatalf("routing_rules.status = %q, want inactive", routeStatus)
	}

	instance, ok, err := store.Load(ctx, req.Instance.EntityID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "completed" {
		t.Fatalf("current_state = %q, want completed", got)
	}
	if strings.TrimSpace(instance.Status) != "terminated" {
		t.Fatalf("loaded workflow instance status = %q, want terminated", instance.Status)
	}
	if instance.TerminatedAt.IsZero() {
		t.Fatal("loaded workflow instance terminated_at is zero")
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow-scoped agent teardown")
	}
	if _, ok := am.GetAgentConfig("shared-subject-agent"); !ok {
		t.Fatal("expected unrelated flow agent to remain active")
	}
}

func TestDeactivateFlowInstanceModel_PostCommitSideEffectsFollowTerminalCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	const runID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), runID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source)
		VALUES ($1::uuid, 'running', 'bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'ephemeral')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	managerStore := &flowActivationTestStore{}
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	bundle := testFlowBundle("")
	runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
		Module:        newFlowActivationWorkflowModule(t, bundle),
		WorkflowStore: store,
		WorkOwner:     newTestManagerWorkOwner(t),
	})
	am := newFlowActivationManager(t, bus, store, managerStore)
	const subjectID = "22222222-2222-2222-2222-222222222222"
	req := testActivationRequest(bundle, "review", "inst-1", subjectID, "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(req.TriggerEvent.ID(), runID)

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if err := store.Mutate(txctx, req.Instance.EntityID, func(instance *runtimepipeline.WorkflowInstance) {
			instance.CurrentState = "completed"
		}); err != nil {
			return err
		}
		if err := am.DeactivateFlowInstanceModel(txctx, runtimepipeline.FlowInstanceDeactivationRequest{
			ContractBundle: semanticview.Wrap(bundle),
			Instance:       req.Instance,
			FinalState:     "completed",
		}); err != nil {
			return err
		}
		if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
			return errors.New("flow agent was torn down before terminal transaction commit")
		}
		if len(managerStore.terminated) != 0 || len(bus.unsubscribed) != 0 || len(bus.removedPairs) != 0 {
			return fmt.Errorf(
				"side effects before commit: terminated=%#v unsubscribed=%#v routes=%#v",
				managerStore.terminated,
				bus.unsubscribed,
				bus.removedPairs,
			)
		}
		if got := routeStore.statusByPath["review/inst-1"]; got != "inactive" {
			return fmt.Errorf("route status inside terminal mutation = %q, want inactive", got)
		}
		var externalStatus string
		if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
			SELECT status
			FROM flow_instances
			WHERE instance_id = $1
		`, "review/inst-1").Scan(&externalStatus); err != nil {
			return fmt.Errorf("external flow_instances status before commit: %w", err)
		}
		if strings.TrimSpace(externalStatus) != "active" {
			return fmt.Errorf("external flow_instances status before commit = %q, want active", externalStatus)
		}
		return nil
	}); err != nil {
		t.Fatalf("RunPipelineMutation: %v", err)
	}

	var status string
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT status
		FROM flow_instances
		WHERE instance_id = $1
	`, "review/inst-1").Scan(&status); err != nil {
		t.Fatalf("external flow_instances status after commit: %v", err)
	}
	if strings.TrimSpace(status) != "terminated" {
		t.Fatalf("external flow_instances status after commit = %q, want terminated", status)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow agent teardown after terminal transaction commit")
	}
	if len(managerStore.terminated) != 1 || managerStore.terminated[0] != "reviewer-inst-1" {
		t.Fatalf("agent terminations after commit = %#v, want reviewer-inst-1", managerStore.terminated)
	}
	if len(bus.unsubscribed) != 0 {
		t.Fatalf("generic unsubscribe used after commit: %#v", bus.unsubscribed)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed routes after commit = %#v, want review/inst-1", bus.removedPairs)
	}
	if got := routeStore.statusByPath["review/inst-1"]; got != "inactive" {
		t.Fatalf("route status after commit = %q, want inactive", got)
	}
}

func TestBuildFlowAgentConfig_ExternalizesLocalSubscriptionsFromExactFlowPath(t *testing.T) {
	cfg, err := buildFlowAgentConfig(
		semanticview.Wrap(testNestedFlowBundle()),
		"grandchild",
		"inst-1",
		"ent-1",
		"child/grandchild/inst-1",
		"worker",
		runtimecontracts.AgentRegistryEntry{
			ID:            "worker-{instance_id}",
			Type:          "generic",
			Role:          "worker",
			Subscriptions: []string{"micro.started"},
		},
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
}

func TestEnsureStaticFlowRequiredAgentsRegistersStaticFlowSubscriptions(t *testing.T) {
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{}, store)
	bundle := testStaticFlowBundle()

	if err := am.EnsureStaticFlowRequiredAgents(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle)); err != nil {
		t.Fatalf("EnsureStaticFlowRequiredAgents: %v", err)
	}
	cfg, ok := am.GetAgentConfig("analyzer")
	if !ok {
		t.Fatal("expected static flow required agent config")
	}
	if got := cfg.FlowID; got != "analyzer-flow" {
		t.Fatalf("flow_id = %q, want analyzer-flow", got)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "analyzer-flow/analysis.requested" {
		t.Fatalf("subscriptions = %#v, want [analyzer-flow/analysis.requested]", cfg.Subscriptions)
	}
	if got := cfg.EmitEvents; len(got) != 1 || got[0] != "analyzer-flow/analysis.done" {
		t.Fatalf("emit_events = %#v, want [analyzer-flow/analysis.done]", got)
	}
	if len(store.upserts) != 1 || store.upserts[0].Config.ID != "analyzer" {
		t.Fatalf("persisted agents = %#v, want analyzer", store.upserts)
	}
}

func TestStandingActivatedFlowAgentsAreOwnedOnlyByFlowInstanceActivation(t *testing.T) {
	bundle := testStaticFlowBundle()
	bundle.PackageTree = []runtimecontracts.LoadedProjectPackage{{
		Key: "root",
		Manifest: runtimecontracts.ProjectPackageDocument{Flows: []runtimecontracts.ProjectFlowRef{{
			ID: "analyzer-flow", Flow: "analyzer-flow", Mode: runtimecontracts.FlowModeSingleton,
			Activation: runtimecontracts.ProjectFlowActivationStanding,
		}}},
	}}
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

func TestEnsureStaticFlowRequiredAgentsInfersFromOmittedRequiredAgents(t *testing.T) {
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{}, store)
	bundle := testStaticFlowBundle()
	schema := bundle.FlowSchemas["analyzer-flow"]
	schema.RequiredAgents = nil
	schema.RequiredAgentsDeclared = false
	bundle.FlowSchemas["analyzer-flow"] = schema
	bundle.Semantics.FlowAgents = map[string][]runtimecontracts.FlowRequiredAgent{}
	bundle.Semantics.FlowAgentFacts = map[string][]runtimecontracts.RequiredAgentFact{}

	if err := am.EnsureStaticFlowRequiredAgents(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle)); err != nil {
		t.Fatalf("EnsureStaticFlowRequiredAgents: %v", err)
	}
	cfg, ok := am.GetAgentConfig("analyzer")
	if !ok {
		t.Fatal("expected inferred static flow required agent config")
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "analyzer-flow/analysis.requested" {
		t.Fatalf("subscriptions = %#v, want [analyzer-flow/analysis.requested]", cfg.Subscriptions)
	}
	if len(store.upserts) != 1 || store.upserts[0].Config.ID != "analyzer" {
		t.Fatalf("persisted agents = %#v, want analyzer", store.upserts)
	}
}

func TestStaticRequiredAgentsForScopeRejectsRoleFallbackWithoutMapKey(t *testing.T) {
	records, err := staticRequiredAgentsForScope(nil, "analysis", "analysis", map[string]runtimecontracts.AgentRegistryEntry{
		"worker-alias": {
			ID:            "worker",
			Role:          "worker",
			Subscriptions: []string{"analysis.requested"},
			EmitEvents:    []string{"analysis.done"},
		},
	}, []runtimecontracts.FlowRequiredAgent{{
		Role:         "worker",
		SubscribesTo: []string{"analysis.requested"},
		Emits:        []string{"analysis.done"},
	}})

	if err == nil || !strings.Contains(err.Error(), `required agent "worker"`) {
		t.Fatalf("expected required-agent map-key error, records=%#v err=%v", records, err)
	}
}

func TestEnsureStaticAgentsForScopeRegistersRootAndFlowSubscriptions(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(t, bus, &flowActivationTestInstanceStore{})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Version: "v-test",
			FlowPrefix: map[string]string{
				"ops-flow": "ops-flow",
			},
		},
	})

	rootAgents := map[string]runtimecontracts.AgentRegistryEntry{
		"test-agent": {
			ID:            "test-agent",
			Type:          "generic",
			Role:          "test-agent",
			Subscriptions: []string{"task.assigned"},
			EmitEvents:    []string{"task.completed"},
		},
	}
	if err := am.ensureStaticAgentsForScope(testAuthorActivityContext(context.Background()), source, "", "", rootAgents); err != nil {
		t.Fatalf("ensureStaticAgentsForScope(root): %v", err)
	}
	flowAgents := map[string]runtimecontracts.AgentRegistryEntry{
		"operator": {
			ID:            "operator",
			Type:          "generic",
			Role:          "operator",
			Subscriptions: []string{"work.requested"},
			EmitEvents:    []string{"work.completed"},
		},
	}
	if err := am.ensureStaticAgentsForScope(testAuthorActivityContext(context.Background()), source, "ops-flow", "ops-flow", flowAgents); err != nil {
		t.Fatalf("ensureStaticAgentsForScope(flow): %v", err)
	}

	rootCfg, ok := am.GetAgentConfig("test-agent")
	if !ok {
		t.Fatal("expected root static agent config")
	}
	if len(rootCfg.Subscriptions) != 1 || rootCfg.Subscriptions[0] != "task.assigned" {
		t.Fatalf("root subscriptions = %#v, want [task.assigned]", rootCfg.Subscriptions)
	}

	flowCfg, ok := am.GetAgentConfig("operator")
	if !ok {
		t.Fatal("expected flow static agent config")
	}
	if len(flowCfg.Subscriptions) != 1 || flowCfg.Subscriptions[0] != "ops-flow/work.requested" {
		t.Fatalf("flow subscriptions = %#v, want [ops-flow/work.requested]", flowCfg.Subscriptions)
	}
}

func TestEnsureStaticAgents_PackageBackedFlowOwnedAgentsCarryCanonicalFlowPath(t *testing.T) {
	source := loadPackageBackedStaticAgentSource(t)
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	var captured []models.AgentConfig
	am := newTestAgentManagerWithOptions(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		captured = append(captured, cfg)
		return flowActivationStubAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{}, store)

	if err := am.EnsureStaticAgents(testAuthorActivityContext(context.Background()), source); err != nil {
		t.Fatalf("EnsureStaticAgents: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("captured agents = %#v, want 1", captured)
	}
	if captured[0].FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", captured[0].FlowPath)
	}
	if captured[0].FlowID != "support" {
		t.Fatalf("FlowID = %q, want support", captured[0].FlowID)
	}
	if captured[0].ID != "backend-{vertical_id}" {
		t.Fatalf("ID = %q, want backend-{vertical_id}", captured[0].ID)
	}
	if len(store.upserts) != 1 || store.upserts[0].Config.FlowPath != "support" {
		t.Fatalf("persisted agents = %#v, want support flow path", store.upserts)
	}
}

func TestEnsureStaticAgents_SoleParentFlowPackageAgentsStartWithOwningFlowPath(t *testing.T) {
	source := loadSoleParentFlowStaticAgentSource(t)
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	var captured []models.AgentConfig
	am := newTestAgentManagerWithOptions(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		captured = append(captured, cfg)
		return flowActivationStubAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{}, store)

	if err := am.EnsureStaticAgents(testAuthorActivityContext(context.Background()), source); err != nil {
		t.Fatalf("EnsureStaticAgents: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("captured agents = %#v, want 1", captured)
	}
	if captured[0].FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", captured[0].FlowPath)
	}
	if captured[0].FlowID != "support" {
		t.Fatalf("FlowID = %q, want support", captured[0].FlowID)
	}
	if captured[0].ID != "backend-{vertical_id}" {
		t.Fatalf("ID = %q, want backend-{vertical_id}", captured[0].ID)
	}
}

func TestActivateFlowInstanceFailsWithoutWorkflowInstanceStore(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newTestAgentManagerWithOptions(t, bus, nil, AgentManagerOptions{WorkOwner: newTestManagerWorkOwner(t)})

	err := am.ActivateFlowInstance(testAuthorActivityContext(context.Background()), testActivationRequest(testFlowBundle(""), "review", "inst-1", "ent-1", "review/inst-1"))
	if err == nil || !strings.Contains(err.Error(), "workflow instance store is required") {
		t.Fatalf("ActivateFlowInstance err = %v, want workflow instance store error", err)
	}
}

func loadPackageBackedStaticAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  type: generic
  role: backend
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

func loadSoleParentFlowStaticAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: extras
flows:
  - id: support
    flow: support
    mode: static
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
flows: []
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "extras", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  type: generic
  role: backend
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

func TestBuildFlowAgentConfig_PassesContractToolsAndEmitEvents(t *testing.T) {
	cfg, err := buildFlowAgentConfig(
		semanticview.Wrap(testFlowBundle("")),
		"review",
		"inst-1",
		"ent-1",
		"review/inst-1",
		"reviewer",
		runtimecontracts.AgentRegistryEntry{
			ID:              "reviewer-{instance_id}",
			Type:            "generic",
			Role:            "reviewer",
			Tools:           []string{"schedule", "check_status"},
			NativeTools:     map[string]any{"bash": true, "file_io": true},
			EmitEvents:      []string{"task.completed", "task.completed", "review.failed"},
			MaxTurnsPerTask: 7,
		},
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

func TestStaticAndDynamicFlowAgentConfigRejectForeignExactAndPattern(t *testing.T) {
	source := semanticview.Wrap(testFlowBundle(""))
	for _, subscription := range []string{"foreign/task.ready", "foreign/**/task.ready"} {
		t.Run(strings.ReplaceAll(subscription, "/", "_"), func(t *testing.T) {
			entry := runtimecontracts.AgentRegistryEntry{ID: "reviewer", Type: "generic", Subscriptions: []string{subscription}}
			if _, err := buildStaticFlowAgentConfig(source, "review", "review", "reviewer", entry, map[string]struct{}{"task.started": {}}); err == nil || !strings.Contains(err.Error(), "cannot cross a flow boundary") {
				t.Fatalf("buildStaticFlowAgentConfig error = %v, want admission rejection", err)
			}
			if _, err := buildFlowAgentConfig(source, "review", "inst-1", "ent-1", "review/inst-1", "reviewer", entry, nil, map[string]struct{}{"task.started": {}}, nil); err == nil || !strings.Contains(err.Error(), "cannot cross a flow boundary") {
				t.Fatalf("buildFlowAgentConfig error = %v, want admission rejection", err)
			}
		})
	}
}

func TestStaticAndTemplateAgentMaterializationConsumeEffectivePlatformDefaults(t *testing.T) {
	source := loadAgentPlatformDefaultsMaterializationSource(t)

	staticScope, ok := source.FlowScopeByID("static_support")
	if !ok {
		t.Fatal("static_support flow scope missing")
	}
	staticEntry := staticScope.Agents["worker"]
	staticCfg, err := buildStaticFlowAgentConfig(source, "static_support", "static_support", "worker", staticEntry, staticFlowLocalEventSet(staticScope.Agents))
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
		"template_support",
		"inst-1",
		"entity-1",
		"template_support/inst-1",
		"worker",
		templateEntry,
		map[string]string{"instance_id": "inst-1"},
		staticFlowLocalEventSet(templateScope.Agents),
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

	writeFlowActivationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: agent-defaults-materialization
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: static_support
    flow: static_support
    mode: static
  - id: template_support
    flow: template_support
    mode: template
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: agent-defaults-materialization\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	for _, flowID := range []string{"static_support", "template_support"} {
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), "name: "+flowID+"\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "tools.yaml"), "{}\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), flowID+".requested:\n  entity_id: string\n")
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "static_support", "agents.yaml"), `
worker:
  model: regular
  subscriptions:
    - static_support.requested
  emit_events: []
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "template_support", "agents.yaml"), `
worker:
  id: worker-{instance_id}
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
