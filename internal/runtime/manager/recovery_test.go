package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type recoveryTestBus struct {
	storedRoutes            []runtimebus.FlowInstanceRouteRecord
	selectedRouteRecoveries []SelectedContractRouteRecoveryRecord
	routeListQueries        int
	pipelineSweeps          int
	restored                []string
	restoredRequests        []runtimebus.FlowInstanceRouteMaterializationRequest
	replayable              []events.PersistedReplayEvent
	deliveries              map[string][]string
	runtimeLogs             []runtimepipeline.RuntimeLogEntry
	direct                  []events.Event
}

type directiveRecoveryTestBus struct {
	*recoveryTestBus
	runtimeagentcontrol.DirectiveOperationStore
	order []string
}

func (*recoveryTestBus) AdmitBundleSourceFact(ctx context.Context) (context.Context, error) {
	return admitManagerTestBusContext(ctx)
}

type recoveryBudgetGuardStub struct {
	err   error
	calls int
}

func (s *recoveryBudgetGuardStub) ProjectRecoveryBudgetState(context.Context) error {
	s.calls++
	return s.err
}
func (*recoveryBudgetGuardStub) IsEntityEmergency(string) bool { return false }
func (*recoveryBudgetGuardStub) IsEntityThrottle(string) bool  { return false }
func (*recoveryBudgetGuardStub) IsEmergency(string) bool       { return false }
func (*recoveryBudgetGuardStub) IsThrottle(string) bool        { return false }

func TestRecoverReturnsBudgetRecoveryProjectionFailure(t *testing.T) {
	projectionErr := errors.New("budget projection unavailable")
	budget := &recoveryBudgetGuardStub{err: projectionErr}
	am := newTestAgentManagerWithOptions(t, &recordingReceiptBus{}, nil, AgentManagerOptions{Budget: budget}, &receiptReaderStub{})

	err := am.Recover(testAuthorActivityContext(context.Background()))
	if !errors.Is(err, projectionErr) {
		t.Fatalf("Recover error = %v, want wrapped budget projection failure", err)
	}
	if !strings.Contains(err.Error(), "project recovered budget state") {
		t.Fatalf("Recover error = %q, want explicit recovery projection context", err)
	}
	if budget.calls != 1 {
		t.Fatalf("budget recovery projection calls = %d, want 1", budget.calls)
	}
}

func (b *directiveRecoveryTestBus) Store() runtimebus.EventStore { return b }

func (b *directiveRecoveryTestBus) ReconcileDirectiveOperations(ctx context.Context, now time.Time, ttl time.Duration) (runtimeagentcontrol.DirectiveOperationReconcileResult, error) {
	b.order = append(b.order, "directive")
	return b.DirectiveOperationStore.ReconcileDirectiveOperations(ctx, now, ttl)
}

func (b *directiveRecoveryTestBus) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	b.order = append(b.order, "pipeline")
	return b.recoveryTestBus.SweepPipelineObligations(ctx, limit)
}

func (*recoveryTestBus) Publish(context.Context, events.Event) error                 { return nil }
func (*recoveryTestBus) PublishDirect(context.Context, events.Event, []string) error { return nil }
func (*recoveryTestBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (*recoveryTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*recoveryTestBus) Unsubscribe(string)        {}
func (*recoveryTestBus) ResetInMemoryState() error { return nil }
func (b *recoveryTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return nil
}
func (b *recoveryTestBus) Store() runtimebus.EventStore { return b }
func (b *recoveryTestBus) SweepPipelineObligations(context.Context, int) (runtimepipelineobligation.SweepResult, error) {
	b.pipelineSweeps++
	return runtimepipelineobligation.SweepResult{Exhausted: true}, nil
}
func (*recoveryTestBus) PipelineWorkPresence(context.Context) (runtimepipelineobligation.GlobalWorkPresence, error) {
	return runtimepipelineobligation.GlobalWorkPresence{}, nil
}
func (b *recoveryTestBus) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublishNoop(ctx, command)
}
func (b *recoveryTestBus) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	return append([]string(nil), b.deliveries[eventID]...), nil
}
func (b *recoveryTestBus) UpsertFlowInstanceRoute(context.Context, runtimebus.FlowInstanceRouteRecord) error {
	return nil
}
func (b *recoveryTestBus) DeleteFlowInstanceRoute(context.Context, runtimeflowidentity.Route) error {
	return nil
}
func (b *recoveryTestBus) ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.Route, error) {
	b.routeListQueries++
	out := make([]runtimeflowidentity.Route, 0, len(b.storedRoutes))
	for _, route := range b.storedRoutes {
		out = append(out, route.Identity)
	}
	return out, nil
}
func (b *recoveryTestBus) ListSelectedContractRouteRecoveryRecords(context.Context) ([]SelectedContractRouteRecoveryRecord, error) {
	return append([]SelectedContractRouteRecoveryRecord(nil), b.selectedRouteRecoveries...), nil
}
func (b *recoveryTestBus) PublishPersistedFlowInstanceRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	req = req.Normalized()
	identity := req.Identity
	b.restored = append(b.restored, identity.InstancePath)
	b.restoredRequests = append(b.restoredRequests, req)
	return nil
}

type recoveryTestStore struct {
	agents []PersistedAgent
}

func (s *recoveryTestStore) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (s *recoveryTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return append([]PersistedAgent(nil), s.agents...), nil
}
func (s *recoveryTestStore) EnsureEntitySchema(context.Context, string) error { return nil }

func TestRecoverRejectsPersistedForeignExactAndPatternBeforeRouteOrPendingQuery(t *testing.T) {
	for _, subscription := range []string{"foreign/task.ready", "foreign/**/task.ready"} {
		t.Run(strings.ReplaceAll(subscription, "/", "_"), func(t *testing.T) {
			store := &recoveryTestStore{agents: []PersistedAgent{{Config: managerTestAgentConfig(models.AgentConfig{
				ExecutionMode: "live",
				ID:            "reviewer",
				Identity: managerScopedRuntimeAgentIdentity(
					"reviewer", "test://recovery/reviewer", "review", "inst-1", "review/inst-1",
				),
				FlowPath:      "review/inst-1",
				Subscriptions: []string{subscription},
			})}}}
			bus := &recoveryTestBus{}
			am := newTestAgentManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
				return recoveryTestAgent{id: cfg.ID}, nil
			}, store)
			err := am.Recover(context.Background())
			if err == nil || !strings.Contains(err.Error(), "cannot cross a flow boundary") {
				t.Fatalf("Recover error = %v, want admission rejection", err)
			}
			if am.Count() != 0 || bus.routeListQueries != 0 || bus.pipelineSweeps != 0 {
				t.Fatalf("recovery side effects: agents=%d route_queries=%d pipeline_sweeps=%d, want none", am.Count(), bus.routeListQueries, bus.pipelineSweeps)
			}
		})
	}
}

func TestMockOnlyPostureRejectsPersistedLiveAgentBeforeStartupReconstruction(t *testing.T) {
	store := &recoveryTestStore{agents: []PersistedAgent{{Config: models.AgentConfig{
		ExecutionMode: executionmode.Live, ID: "persisted-live", Role: "worker",
		Identity: managerScopedRuntimeAgentIdentity(
			"persisted-live", "test://recovery/persisted-live", "review", "inst-live", "review/inst-live",
		),
		FlowPath: "review/inst-live",
	}, Status: "active", StartedAt: time.Now().UTC()}}}
	bus := &recoveryTestBus{}
	factoryCalls := 0
	am := newTestAgentManagerWithOptions(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		factoryCalls++
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{ExecutionPosture: executionposture.MockOnly}, store)

	if _, err := am.HydrateForStartup(testAuthorActivityContext(context.Background())); err == nil || !strings.Contains(err.Error(), "runtime.execution_posture=mock_only") {
		t.Fatalf("HydrateForStartup error = %v, want live-agent rejection", err)
	}
	if factoryCalls != 0 || am.Count() != 0 || bus.routeListQueries != 0 || bus.pipelineSweeps != 0 {
		t.Fatalf("startup mutations factory=%d agents=%d route_queries=%d sweeps=%d, want zero", factoryCalls, am.Count(), bus.routeListQueries, bus.pipelineSweeps)
	}
}

func TestMockOnlyPostureRejectsLiveAgentRestartBeforeSuccessorFactory(t *testing.T) {
	bus := &recoveryTestBus{}
	factoryCalls := 0
	am := newTestAgentManagerWithOptions(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		factoryCalls++
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{ExecutionPosture: executionposture.Live})
	if err := am.SpawnAgent(managerTestAgentConfig(models.AgentConfig{ExecutionMode: executionmode.Live, ID: "restart-live", Role: "worker"})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	before := factoryCalls
	current, ok := testExecutionSnapshot(t, am, "restart-live", "")
	if !ok {
		t.Fatal("live agent execution snapshot is missing")
	}
	am.executionPosture = executionposture.MockOnly
	if _, err := am.Restart(testAuthorActivityContext(context.Background()), runtimeagentcontrol.RestartRequest{AgentID: "restart-live"}); err == nil || !strings.Contains(err.Error(), "runtime.execution_posture=mock_only") {
		t.Fatalf("Restart error = %v, want live-agent rejection", err)
	}
	after, ok := testExecutionSnapshot(t, am, "restart-live", "")
	if !ok || factoryCalls != before || after.Config.ExecutionMode != current.Config.ExecutionMode {
		t.Fatalf("restart mutated execution ok=%v factory=%d/%d mode=%q/%q", ok, factoryCalls, before, after.Config.ExecutionMode, current.Config.ExecutionMode)
	}
}

type startupReplayTestStore struct {
	recoveryTestStore
	*managerDeliveryTestStore
}

func newStartupReplayTestStore(t *testing.T, persistence recoveryTestStore, pending map[string][]events.Event) *startupReplayTestStore {
	t.Helper()
	deliveryStore := newManagerDeliveryTestStore(t)
	for agentID, events := range pending {
		deliveryStore.seedAgentDeliveries(t, agentID, events)
	}
	return &startupReplayTestStore{recoveryTestStore: persistence, managerDeliveryTestStore: deliveryStore}
}

type startupReplayTestAgent struct{ id string }

func (a startupReplayTestAgent) ID() string                      { return a.id }
func (startupReplayTestAgent) Type() string                      { return "generic" }
func (startupReplayTestAgent) Subscriptions() []events.EventType { return nil }
func (startupReplayTestAgent) OnEvent(_ context.Context, evt events.Event) ([]events.Event, error) {
	switch evt.Type() {
	case events.EventType("system.recover.drop"):
		return nil, errors.New("boom")
	case events.EventType("system.recover.leased"):
		return nil, errors.New("session currently leased")
	default:
		return nil, nil
	}
}

func TestRecoverRestoresPersistedFlowInstanceRoutes(t *testing.T) {
	bus := &recoveryTestBus{
		storedRoutes: []runtimebus.FlowInstanceRouteRecord{{
			Identity: runtimeflowidentity.DeriveRoute("review", "inst-1"),
		}},
	}
	store := &recoveryTestStore{
		agents: []PersistedAgent{{
			Config: managerTestAgentConfig(models.AgentConfig{
				ExecutionMode: "live",
				ID:            "reviewer",
				Identity: managerScopedRuntimeAgentIdentity(
					"reviewer", "test://recovery/reviewer", "review", "inst-1", "review/inst-1",
				),
				Role:     "reviewer",
				EntityID: "ent-1",
				FlowID:   "review",
				FlowPath: "review/inst-1",
				Config:   mustRecoveryJSON(t, map[string]any{"tools": []string{"agent_message"}}),
			}),
			StartedAt: time.Now().UTC(),
		}},
	}
	workflowInstances := &flowActivationTestInstanceStore{
		byStorageRef: map[string]runtimepipeline.WorkflowInstance{
			"review/inst-1": {
				InstanceID:   "inst-1",
				StorageRef:   "review/inst-1",
				WorkflowName: "review",
				Config: map[string]any{
					"vertical_id": "11111111-1111-4111-8111-111111111111",
				},
				Metadata: map[string]any{
					"entity_id":   "ent-1",
					"flow_path":   "review/inst-1",
					"instance_id": "inst-1",
				},
			},
		},
	}
	am := newTestAgentManagerWithOptions(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{WorkflowInstances: workflowInstances}, store)

	if err := am.Recover(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(bus.restored) != 1 || bus.restored[0] != "review/inst-1" {
		t.Fatalf("restored routes = %#v, want [review/inst-1]", bus.restored)
	}
	if got := bus.restoredRequests[0].ActivationVariables["vertical_id"]; got != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("restored activation variable vertical_id = %q, want persisted config value", got)
	}
	if len(workflowInstances.routeLoads) != 1 || workflowInstances.routeLoads[0] != runtimeflowidentity.DeriveRoute("review", "inst-1") {
		t.Fatalf("route recovery projection loads = %#v, want exact review/inst-1 route", workflowInstances.routeLoads)
	}
}

func TestDirectiveReconciliationPrecedesGenericPipelineRecovery(t *testing.T) {
	bus := &directiveRecoveryTestBus{
		recoveryTestBus:         &recoveryTestBus{},
		DirectiveOperationStore: &directiveEventStore{},
	}
	am := newTestAgentManager(t, bus, nil, &recoveryTestStore{})

	if err := am.ReconcileDirectiveOperations(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("ReconcileDirectiveOperations: %v", err)
	}
	if err := am.Recover(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(bus.order) < 2 || bus.order[0] != "directive" || bus.order[1] != "pipeline" {
		t.Fatalf("recovery order = %#v, want directive before pipeline", bus.order)
	}
}

func TestRecoverRestoresSelectedContractRouteRecoveriesFromForkLocalOwner(t *testing.T) {
	forkRunID := "00000000-0000-0000-0000-000000000601"
	bus := &recoveryTestBus{
		selectedRouteRecoveries: []SelectedContractRouteRecoveryRecord{
			selectedContractRouteRecoveryRecord(t, forkRunID),
		},
	}
	am := newTestAgentManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, &recoveryTestStore{})

	if err := am.Recover(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	snapshot := am.SelectedContractRouteRecoverySnapshot()
	if len(snapshot) != 1 {
		t.Fatalf("selected route recovery snapshot len = %d, want 1", len(snapshot))
	}
	got := snapshot[forkRunID]
	if got.Record.Owner != SelectedContractRoutePersistenceOwner ||
		got.Record.RuntimeRecoveryOwner != SelectedContractRouteRecoveryOwner ||
		got.RouteTopology.Owner != selectedContractRouteTopologyOwner ||
		got.RecipientPlanning.Owner != selectedContractRecipientPlanningOwner ||
		len(got.RecipientPlanning.RecipientPlanEvents) != 1 {
		t.Fatalf("selected route recovery truth = %#v, want canonical recovered topology and recipient planning", got)
	}
	classification := struct {
		consumer       string
		owner          string
		classification string
		consumedOwners []string
	}{
		consumer:       "internal/runtime/manager.restoreSelectedContractRouteRecoveries/SelectedContractRouteRecoveryRecipientGuard",
		owner:          got.Record.RuntimeRecoveryOwner,
		classification: "carrier_readiness_consumer",
		consumedOwners: []string{
			got.Record.Owner,
			got.Record.RouteTopologyOwner,
			got.Record.RecipientPlanningOwner,
			got.RecipientPlanning.RouteTopologyOwner,
		},
	}
	if strings.TrimSpace(classification.owner) == "" {
		t.Fatalf("%s has empty owner in classification row %#v", classification.consumer, classification)
	}
	if classification.classification == "route_authority" {
		t.Fatalf("%s incorrectly classified as live EventBus route authority", classification.consumer)
	}
	for _, owner := range classification.consumedOwners {
		if strings.TrimSpace(owner) == "" {
			t.Fatalf("%s has empty consumed owner in classification row %#v", classification.consumer, classification)
		}
	}
	guard, ok := am.SelectedContractRouteRecoveryRecipientGuard(forkRunID)
	if !ok {
		t.Fatalf("missing selected route recovery recipient guard for %s", forkRunID)
	}
	guard.ExpectForkEvent("fork-event-1", "source-event-1")
	lineage, err := events.NewSelectedForkLineage(
		"fork-run-1",
		"source-run-1",
		"source-event-1",
		"selected-contract-recovery-test",
		"",
		executionmode.Live,
	)
	if err != nil {
		t.Fatalf("NewSelectedForkLineage: %v", err)
	}
	evt := eventtest.SelectedForkReplay(
		"fork-event-1",
		events.EventType("work.ready"),
		eventtest.Producer(events.EventProducerPlatform, selectedContractExecutionOwner),
		"",
		nil,
		0,
		lineage,
		events.EventEnvelope{},
		time.Time{},
	)

	if err := guard.AuthorizeEvent(testAuthorActivityContext(context.Background()), evt); err != nil {
		t.Fatalf("AuthorizeEvent recovered guard: %v", err)
	}
	if err := guard.Authorize(testAuthorActivityContext(context.Background()), evt, runtimebus.PublishRecipientPlan{
		RoutedRecipients: []runtimebus.PublishDiagnosticRecipient{{
			Type:        "agent",
			ID:          "agent-a",
			Path:        "review/inst-1",
			RouteSource: "selected_contract_route_topology",
		}},
	}); err != nil {
		t.Fatalf("Authorize recovered recipients: %v", err)
	}
	if err := guard.Authorize(testAuthorActivityContext(context.Background()), evt, runtimebus.PublishRecipientPlan{
		SubscriptionRecipients: []string{"agent-a"},
	}); err == nil || !strings.Contains(err.Error(), "live subscriptions") {
		t.Fatalf("Authorize subscription bypass error = %v, want live subscription rejection", err)
	}
	if len(bus.restored) != 0 {
		t.Fatalf("current route restore was used for selected route recovery: %#v", bus.restored)
	}
	state, err := am.RecoverableStateSnapshot(testAuthorActivityContext(context.Background()))
	if err != nil {
		t.Fatalf("RecoverableStateSnapshot: %v", err)
	}
	if state.PersistedSelectedContractRouteRecoveryCount != 1 {
		t.Fatalf("selected route recovery count = %d, want 1", state.PersistedSelectedContractRouteRecoveryCount)
	}
}

func TestRecoverRejectsSelectedContractRouteRecoveryFromCurrentRouteOwner(t *testing.T) {
	record := selectedContractRouteRecoveryRecord(t, "00000000-0000-0000-0000-000000000602")
	record.Owner = "routing_rules"
	bus := &recoveryTestBus{
		selectedRouteRecoveries: []SelectedContractRouteRecoveryRecord{record},
	}
	am := newTestAgentManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, &recoveryTestStore{})

	err := am.Recover(testAuthorActivityContext(context.Background()))
	if err == nil || !strings.Contains(err.Error(), SelectedContractRoutePersistenceOwner) {
		t.Fatalf("Recover error = %v, want canonical owner rejection", err)
	}
}

func TestRecoverRejectsSelectedContractRouteRecoveryFingerprintMismatch(t *testing.T) {
	record := selectedContractRouteRecoveryRecord(t, "00000000-0000-0000-0000-000000000603")
	record.RecipientPlanning = mustRecoveryJSON(t, map[string]any{
		"owner":                         selectedContractRecipientPlanningOwner,
		"route_topology_owner":          selectedContractRouteTopologyOwner,
		"non_mutating":                  true,
		"recipient_planning_supported":  true,
		"delivery_writes_supported":     false,
		"frontier_evidence_fingerprint": "frontier-fp",
		"recipient_plan_events": []map[string]any{{
			"source_event_id": "source-event-1",
			"event_name":      "work.ready",
			"recipients": []map[string]any{{
				"subscriber_type": "agent",
				"subscriber_id":   "agent-tampered",
				"path":            "review/inst-1",
				"route_source":    "selected_contract_route_topology",
			}},
			"disposition": "selected_contract_recipient_planning",
		}},
	})
	bus := &recoveryTestBus{
		selectedRouteRecoveries: []SelectedContractRouteRecoveryRecord{record},
	}
	am := newTestAgentManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, &recoveryTestStore{})

	err := am.Recover(testAuthorActivityContext(context.Background()))
	if err == nil || !strings.Contains(err.Error(), "recipient planning fingerprint mismatch") {
		t.Fatalf("Recover error = %v, want recipient planning fingerprint mismatch", err)
	}
}

func selectedContractRouteRecoveryRecord(t *testing.T, forkRunID string) SelectedContractRouteRecoveryRecord {
	t.Helper()
	routeTopology := mustRecoveryJSON(t, map[string]any{
		"owner":                           selectedContractRouteTopologyOwner,
		"non_mutating":                    true,
		"route_persistence_supported":     false,
		"executable_recipients_supported": false,
		"frontier_evidence_fingerprint":   "frontier-fp",
		"static_route_events": []map[string]any{{
			"source_event_id": "source-event-1",
			"event_name":      "work.ready",
			"derived_recipients": []map[string]any{{
				"subscriber_type": "agent",
				"subscriber_id":   "agent-a",
				"path":            "review/inst-1",
				"route_source":    "selected_contract_route_topology",
			}},
			"disposition": "selected_contract_route_topology",
		}},
		"dynamic_topology_proofs": []map[string]any{{
			"flow_instance":    "review/inst-1",
			"source_event_ids": []string{"source-event-1"},
			"event_names":      []string{"work.ready"},
			"derived_recipients": []map[string]any{{
				"subscriber_type": "agent",
				"subscriber_id":   "agent-a",
				"path":            "review/inst-1",
				"route_source":    "selected_contract_route_topology",
			}},
			"disposition": "selected_contract_dynamic_route_topology",
		}},
	})
	recipientPlanning := mustRecoveryJSON(t, map[string]any{
		"owner":                         selectedContractRecipientPlanningOwner,
		"route_topology_owner":          selectedContractRouteTopologyOwner,
		"non_mutating":                  true,
		"recipient_planning_supported":  true,
		"delivery_writes_supported":     false,
		"frontier_evidence_fingerprint": "frontier-fp",
		"recipient_plan_events": []map[string]any{{
			"source_event_id": "source-event-1",
			"event_name":      "work.ready",
			"recipients": []map[string]any{{
				"subscriber_type": "agent",
				"subscriber_id":   "agent-a",
				"path":            "review/inst-1",
				"route_source":    "selected_contract_route_topology",
			}},
			"disposition": "selected_contract_recipient_planning",
		}},
	})
	return SelectedContractRouteRecoveryRecord{
		Owner:                        SelectedContractRoutePersistenceOwner,
		RuntimeRecoveryOwner:         SelectedContractRouteRecoveryOwner,
		ForkRunID:                    forkRunID,
		SourceRunID:                  "00000000-0000-0000-0000-000000000501",
		ForkEventID:                  "00000000-0000-0000-0000-000000000701",
		RouteTopologyOwner:           selectedContractRouteTopologyOwner,
		DynamicTopologyOwner:         "runtime.run_fork.selected_contract_dynamic_route_topology",
		RecipientPlanningOwner:       selectedContractRecipientPlanningOwner,
		FrontierEvidenceFingerprint:  "frontier-fp",
		RouteTopologyFingerprint:     recoveryJSONFingerprint(routeTopology),
		RecipientPlanningFingerprint: recoveryJSONFingerprint(recipientPlanning),
		StaticRouteEventCount:        1,
		DynamicTopologyProofCount:    1,
		RecipientPlanEventCount:      1,
		RouteTopology:                routeTopology,
		RecipientPlanning:            recipientPlanning,
		CreatedAt:                    time.Now().UTC(),
	}
}

func recoveryJSONFingerprint(raw json.RawMessage) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestRecover_UsesCanonicalLoadedAgentMetadata(t *testing.T) {
	bus := &recoveryTestBus{}
	store := &recoveryTestStore{
		agents: []PersistedAgent{{
			Config: managerTestAgentConfig(models.AgentConfig{
				ExecutionMode: "live",
				ID:            "reviewer",
				Identity: managerScopedRuntimeAgentIdentity(
					"reviewer", "test://recovery/reviewer", "review", "inst-1", "review/inst-1",
				),
				Type:            "review-worker",
				Role:            "reviewer",
				FlowID:          "review",
				Model:           "regular",
				LLMBackend:      "anthropic",
				Memory:          agentmemory.Authored(true),
				Subscriptions:   []string{"review.ready"},
				EmitEvents:      []string{"review.completed"},
				WorkspaceClass:  "shared_flow",
				ManagerFallback: "control-plane",
				FlowPath:        "review/inst-1",
				EntityID:        "ent-1",
				ParentAgent:     "control-plane",
				Config: mustRecoveryJSON(t, map[string]any{
					"test_marker":        "canonical-loaded-metadata",
					"subscriptions":      []string{"wrong.subscription"},
					"manager_fallback":   "wrong-manager",
					"workspace_class":    "wrong-workspace",
					"max_turns_per_task": 99,
				}),
			}),
			StartedAt: time.Now().UTC(),
		}},
	}
	var hydrated models.AgentConfig
	am := newTestAgentManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		hydrated = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, store)

	if err := am.Recover(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if hydrated.ID != "reviewer" {
		t.Fatalf("hydrated id = %q, want reviewer", hydrated.ID)
	}
	if hydrated.Memory != agentmemory.Authored(true) {
		t.Fatalf("memory = %+v, want authored true", hydrated.Memory)
	}
	if len(hydrated.Subscriptions) != 1 || hydrated.Subscriptions[0] != "review/inst-1/review.ready" {
		t.Fatalf("subscriptions = %#v, want [review/inst-1/review.ready]", hydrated.Subscriptions)
	}
	if hydrated.ManagerFallback != "control-plane" {
		t.Fatalf("manager_fallback = %q, want control-plane", hydrated.ManagerFallback)
	}
	if hydrated.WorkspaceClass != "shared_flow" {
		t.Fatalf("workspace_class = %q, want shared_flow", hydrated.WorkspaceClass)
	}
	if strings.TrimSpace(hydrated.FlowPath) != "review/inst-1" {
		t.Fatalf("flow_path = %q, want review/inst-1", hydrated.FlowPath)
	}
}

func TestRecover_UsesCanonicalPipelineRecoveryOwner(t *testing.T) {
	bus := &recoveryTestBus{}
	am := newTestAgentManager(t, bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, &recoveryTestStore{})

	if err := am.Recover(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if bus.pipelineSweeps != 1 {
		t.Fatalf("canonical pipeline recovery sweeps = %d, want 1", bus.pipelineSweeps)
	}
}

type recoveryTestAgent struct{ id string }

func (a recoveryTestAgent) ID() string                      { return a.id }
func (recoveryTestAgent) Type() string                      { return "generic" }
func (recoveryTestAgent) Subscriptions() []events.EventType { return nil }
func (recoveryTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func mustRecoveryJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
}
