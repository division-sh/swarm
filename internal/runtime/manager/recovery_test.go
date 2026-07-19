package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
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
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type recoveryTestBus struct {
	storedRoutes            []runtimebus.FlowInstanceRouteRecord
	selectedRouteRecoveries []SelectedContractRouteRecoveryRecord
	routeListQueries        int
	replayQueries           int
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
	am := NewAgentManagerWithOptions(&recordingReceiptBus{}, nil, AgentManagerOptions{Budget: budget}, &receiptReaderStub{})

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

func (b *directiveRecoveryTestBus) ListEventsMissingPipelineReceipt(ctx context.Context, before time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	b.order = append(b.order, "pipeline")
	return b.recoveryTestBus.ListEventsMissingPipelineReceipt(ctx, before, limit)
}

func (*recoveryTestBus) Publish(context.Context, events.Event) error                 { return nil }
func (*recoveryTestBus) PublishDirect(context.Context, events.Event, []string) error { return nil }
func (b *recoveryTestBus) PublishPersistedRecipients(_ context.Context, evt events.Event, _ []string) error {
	b.direct = append(b.direct, evt)
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
func (b *recoveryTestBus) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublishNoop(ctx, plan)
}
func (b *recoveryTestBus) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	return append([]string(nil), b.deliveries[eventID]...), nil
}
func (*recoveryTestBus) SupportsPersistedReplay() bool { return true }
func (*recoveryTestBus) ClaimPipelineReplay(context.Context, string) (runtimeownership.Lease, bool, error) {
	return recoveryTestReplayLease{}, true, nil
}
func (b *recoveryTestBus) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.PersistedReplayEvent, error) {
	b.replayQueries++
	return append([]events.PersistedReplayEvent(nil), b.replayable...), nil
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
func (b *recoveryTestBus) RestorePersistedFlowInstanceRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	req = req.Normalized()
	identity := req.Identity
	b.restored = append(b.restored, identity.InstancePath)
	b.restoredRequests = append(b.restoredRequests, req)
	return nil
}

type recoveryTestStore struct {
	agents                  []PersistedAgent
	pendingSubscriptionArgs [][]events.EventType
}

func (s *recoveryTestStore) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (s *recoveryTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return append([]PersistedAgent(nil), s.agents...), nil
}
func (s *recoveryTestStore) EnsureEntitySchema(context.Context, string) error { return nil }
func (s *recoveryTestStore) UpsertEventReceipt(context.Context, string, string, ReceiptStatus, *runtimefailures.Envelope) error {
	return nil
}
func (s *recoveryTestStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func (s *recoveryTestStore) ListPendingSubscribedEvents(_ context.Context, _ string, subscriptions []events.EventType, _ time.Time, _ int) ([]events.Event, error) {
	s.pendingSubscriptionArgs = append(s.pendingSubscriptionArgs, append([]events.EventType(nil), subscriptions...))
	return nil, nil
}

func TestRecoverRejectsPersistedForeignExactAndPatternBeforeRouteOrPendingQuery(t *testing.T) {
	for _, subscription := range []string{"foreign/task.ready", "foreign/**/task.ready"} {
		t.Run(strings.ReplaceAll(subscription, "/", "_"), func(t *testing.T) {
			store := &recoveryTestStore{agents: []PersistedAgent{{Config: models.AgentConfig{
				ExecutionMode: "live",
				ID:            "reviewer",
				FlowPath:      "review/inst-1",
				Subscriptions: []string{subscription},
			}}}}
			bus := &recoveryTestBus{}
			am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
				return recoveryTestAgent{id: cfg.ID}, nil
			}, store)
			err := am.Recover(context.Background())
			if err == nil || !strings.Contains(err.Error(), "cannot cross a flow boundary") {
				t.Fatalf("Recover error = %v, want admission rejection", err)
			}
			if am.Count() != 0 || len(store.pendingSubscriptionArgs) != 0 || bus.routeListQueries != 0 || bus.replayQueries != 0 {
				t.Fatalf("recovery side effects: agents=%d pending_queries=%#v route_queries=%d replay_queries=%d, want none", am.Count(), store.pendingSubscriptionArgs, bus.routeListQueries, bus.replayQueries)
			}
		})
	}
}

func TestPendingSubscribedRecoveryUsesAdmittedSameScopeSubscriptions(t *testing.T) {
	store := &recoveryTestStore{}
	am := NewAgentManager(&recoveryTestBus{}, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, store)
	if err := am.SpawnAgent(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "reviewer",
		FlowPath:      "review/inst-1",
		Subscriptions: []string{"task.ready", "task.*"},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	execution, ok := am.lifecycle.executionSnapshot("reviewer")
	if !ok {
		t.Fatal("admitted execution missing")
	}
	if _, err := am.pendingEventsForAgent(context.Background(), "reviewer", execution.Subscriptions, time.Time{}); err != nil {
		t.Fatalf("pendingEventsForAgent: %v", err)
	}
	if len(store.pendingSubscriptionArgs) != 1 {
		t.Fatalf("pending subscription queries = %#v, want one", store.pendingSubscriptionArgs)
	}
	want := []events.EventType{"review/inst-1/task.*", "review/inst-1/task.ready"}
	if got := store.pendingSubscriptionArgs[0]; !reflect.DeepEqual(got, want) {
		t.Fatalf("pending query subscriptions = %#v, want %#v", got, want)
	}
}

type startupReplayTestStore struct {
	recoveryTestStore
	pending  map[string][]events.Event
	receipts map[string]EventReceipt
}

func (s *startupReplayTestStore) ListPendingEventsForAgent(_ context.Context, agentID string, _ time.Time, _ int) ([]events.Event, error) {
	out := append([]events.Event(nil), s.pending[strings.TrimSpace(agentID)]...)
	return out, nil
}

func (*startupReplayTestStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func (s *startupReplayTestStore) GetEventReceipt(_ context.Context, eventID, agentID string) (EventReceipt, bool, error) {
	key := strings.TrimSpace(eventID) + "|" + strings.TrimSpace(agentID)
	receipt, ok := s.receipts[key]
	return receipt, ok, nil
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
			Config: models.AgentConfig{
				ExecutionMode: "live",
				ID:            "reviewer-inst-1",
				Role:          "reviewer",
				EntityID:      "ent-1",
				Config:        mustRecoveryJSON(t, map[string]any{"tools": []string{"agent_message"}}),
			},
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
	am := NewAgentManagerWithOptions(bus, func(cfg models.AgentConfig) (Agent, error) {
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
	am := NewAgentManager(bus, nil, &recoveryTestStore{})

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
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
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
	evt := eventtest.RootIngress("fork-event-1",
		events.EventType("work.ready"),
		selectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})

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
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
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
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
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
			Config: models.AgentConfig{
				ExecutionMode:   "live",
				ID:              "reviewer-inst-1",
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
					"system_prompt":      "x",
					"subscriptions":      []string{"wrong.subscription"},
					"manager_fallback":   "wrong-manager",
					"workspace_class":    "wrong-workspace",
					"max_turns_per_task": 99,
				}),
			},
			StartedAt: time.Now().UTC(),
		}},
	}
	var hydrated models.AgentConfig
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
		hydrated = cfg
		return recoveryTestAgent{id: cfg.ID}, nil
	}, store)

	if err := am.Recover(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if hydrated.ID != "reviewer-inst-1" {
		t.Fatalf("hydrated id = %q, want reviewer-inst-1", hydrated.ID)
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

func TestRecover_UsesCanonicalPipelineReplayAftermathDiagnostics(t *testing.T) {
	childID := "evt-replay"
	parentID := "evt-parent"
	bus := &recoveryTestBus{
		replayable: []events.PersistedReplayEvent{{
			Event: eventtest.ChildWithLineage(childID,
				events.EventType("system.recover"), "runtime", "", nil, 0,
				events.EventLineage{RunID: "run-1", ParentEventID: parentID, ExecutionMode: executionmode.Live},
				events.EventEnvelope{}, time.Now().UTC()),
		}},
		deliveries: map[string][]string{
			childID: {"agent-a"},
		},
	}
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
		return recoveryTestAgent{id: cfg.ID}, nil
	}, &recoveryTestStore{})

	if err := am.Recover(managedExecutionTestContext(t, testAuthorActivityContext(context.Background()))); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(bus.direct) != 1 || bus.direct[0].ID() != childID {
		t.Fatalf("direct replayed events = %#v, want [%s]", bus.direct, childID)
	}
	entry := findManagerRecoveryAftermathLog(t, bus.runtimeLogs, childID, "replayed", "persisted_recipients_replayed")
	if strings.TrimSpace(entry.Component) != "pipeline-recovery" {
		t.Fatalf("runtime log component = %q, want pipeline-recovery", entry.Component)
	}
}

func TestRecoverWithStartupReplayDiagnostics_LogsCanonicalManagerReplayAftermath(t *testing.T) {
	now := time.Now().UTC()
	store := &startupReplayTestStore{
		recoveryTestStore: recoveryTestStore{
			agents: []PersistedAgent{{
				Config: models.AgentConfig{
					ExecutionMode: "live",
					ID:            "agent-a",
				},
				StartedAt: now,
			}},
		},
		pending: map[string][]events.Event{
			"agent-a": {
				eventtest.RootIngress("evt-replay", events.EventType("system.recover.ok"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-5*time.Minute)),
				eventtest.RootIngress("evt-receipt", events.EventType("system.recover.receipt"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-4*time.Minute)),
				eventtest.RootIngress("evt-inflight", events.EventType("system.recover.inflight"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-3*time.Minute)),
				eventtest.RootIngress("evt-leased", events.EventType("system.recover.leased"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-2*time.Minute)),
				eventtest.RootIngress("evt-drop", events.EventType("system.recover.drop"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-time.Minute)),
			},
		},
		receipts: map[string]EventReceipt{
			"evt-receipt|agent-a": {
				EventID: "evt-receipt",
				AgentID: "agent-a",
				Status:  ReceiptStatusProcessed,
			},
		},
	}
	bus := &recoveryTestBus{}
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
		return startupReplayTestAgent{id: cfg.ID}, nil
	}, store)
	am.inFlight["agent-a|evt-inflight"] = struct{}{}

	summary, err := am.RecoverWithStartupReplayDiagnostics(managedExecutionTestContext(t, testAuthorActivityContext(context.Background())))
	if err != nil {
		t.Fatalf("RecoverWithStartupReplayDiagnostics: %v", err)
	}
	if summary.ReplayedCount != 1 || summary.SkippedCount != 2 || summary.DroppedCount != 2 {
		t.Fatalf("summary = %#v, want replayed=1 skipped=2 dropped=2", summary)
	}
	if summary.FirstDroppedFailure == nil || summary.FirstDroppedFailure.Detail.Code != "unclassified_runtime_error" {
		t.Fatalf("summary.FirstDroppedFailure = %#v, want unclassified_runtime_error", summary.FirstDroppedFailure)
	}
	if len(bus.runtimeLogs) != 5 {
		t.Fatalf("runtime log count = %d, want 5", len(bus.runtimeLogs))
	}
	assertReplayAftermathLog := func(eventID, outcome, reason string) {
		t.Helper()
		for _, entry := range bus.runtimeLogs {
			if strings.TrimSpace(entry.Action) != startupManagerReplayAction {
				continue
			}
			if strings.TrimSpace(entry.EventID) != strings.TrimSpace(eventID) {
				continue
			}
			detail, _ := entry.Detail.(map[string]any)
			if got := strings.TrimSpace(detail["decision_outcome"].(string)); got != outcome {
				t.Fatalf("event %s decision_outcome = %q, want %q", eventID, got, outcome)
			}
			if got := strings.TrimSpace(detail["decision_reason_code"].(string)); got != reason {
				t.Fatalf("event %s decision_reason_code = %q, want %q", eventID, got, reason)
			}
			return
		}
		t.Fatalf("missing startup manager replay log for %s in %#v", eventID, bus.runtimeLogs)
	}
	assertReplayAftermathLog("evt-replay", "replayed", string(startupManagerReplayReasonReplayed))
	assertReplayAftermathLog("evt-receipt", "skipped", string(startupManagerReplayReasonReceiptProcessed))
	assertReplayAftermathLog("evt-inflight", "skipped", string(startupManagerReplayReasonDuplicateInFlight))
	assertReplayAftermathLog("evt-leased", "dropped", string(startupManagerReplayReasonProcessFailed))
	assertReplayAftermathLog("evt-drop", "dropped", string(startupManagerReplayReasonProcessFailed))
	for _, entry := range bus.runtimeLogs {
		if strings.TrimSpace(entry.Action) == "pending_replay_failed" || strings.TrimSpace(entry.Action) == "pending_replay_event_failed" {
			t.Fatalf("unexpected legacy startup replay action %q in %#v", entry.Action, bus.runtimeLogs)
		}
	}
}

func TestReplayAgentBacklog_DoesNotEmitStartupAftermathOutsideStartupRecovery(t *testing.T) {
	now := time.Now().UTC()
	store := &startupReplayTestStore{
		pending: map[string][]events.Event{
			"agent-a": {
				eventtest.RootIngress("evt-drop", events.EventType("system.recover.drop"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-time.Minute)),
			},
		},
	}
	bus := &recoveryTestBus{}
	am := NewAgentManager(bus, func(cfg models.AgentConfig) (Agent, error) {
		return startupReplayTestAgent{id: cfg.ID}, nil
	}, store)
	if err := am.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{
		Config: models.AgentConfig{ExecutionMode: "live", ID: "agent-a"},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	if err := am.ReplayAgentBacklog(testAuthorActivityContext(context.Background()), "agent-a"); err != nil {
		t.Fatalf("ReplayAgentBacklog: %v", err)
	}
	foundLegacyFailure := false
	for _, entry := range bus.runtimeLogs {
		if strings.TrimSpace(entry.Action) == startupManagerReplayAction {
			t.Fatalf("unexpected startup replay aftermath action on direct ReplayAgentBacklog: %#v", bus.runtimeLogs)
		}
		if strings.TrimSpace(entry.Action) == "pending_replay_event_failed" {
			foundLegacyFailure = true
		}
	}
	if !foundLegacyFailure {
		t.Fatalf("runtime logs = %#v, want legacy pending_replay_event_failed outside startup recovery", bus.runtimeLogs)
	}
}

func TestReplayBacklogReportsDirectReplayCount(t *testing.T) {
	now := time.Now().UTC()
	store := &startupReplayTestStore{
		pending: map[string][]events.Event{
			"agent-a": {
				eventtest.RootIngress("evt-1", events.EventType("system.recover.ok"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-2*time.Minute)),
				eventtest.RootIngress("evt-2", events.EventType("system.recover.ok"), "", "", nil, 0, "", "", events.EventEnvelope{}, now.Add(-time.Minute)),
			},
		},
	}
	am := NewAgentManager(nil, func(cfg models.AgentConfig) (Agent, error) {
		return startupReplayTestAgent{id: cfg.ID}, nil
	}, store)
	if err := am.spawnAgentInternal(testAuthorActivityContext(context.Background()), PersistedAgent{
		Config: models.AgentConfig{ExecutionMode: "live", ID: "agent-a"},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	result, err := am.ReplayBacklog(testAuthorActivityContext(context.Background()), runtimeagentcontrol.ReplayBacklogRequest{AgentID: "agent-a"})
	if err != nil {
		t.Fatalf("ReplayBacklog: %v", err)
	}
	if result.ReplayedCount != 2 {
		t.Fatalf("ReplayBacklog replayed_count = %d, want 2", result.ReplayedCount)
	}
}

type recoveryTestAgent struct{ id string }

type recoveryTestReplayLease struct{}

func (recoveryTestReplayLease) Release(context.Context) error { return nil }

func findManagerRecoveryAftermathLog(t *testing.T, logs []runtimepipeline.RuntimeLogEntry, eventID, outcome, reason string) runtimepipeline.RuntimeLogEntry {
	t.Helper()
	for _, entry := range logs {
		if strings.TrimSpace(entry.Action) != "startup_recovery_pipeline_replay_aftermath" {
			continue
		}
		if strings.TrimSpace(entry.EventID) != strings.TrimSpace(eventID) {
			continue
		}
		detail, _ := entry.Detail.(map[string]any)
		outcomeText, _ := detail["decision_outcome"].(string)
		reasonText, _ := detail["decision_reason_code"].(string)
		if strings.TrimSpace(outcomeText) != strings.TrimSpace(outcome) {
			continue
		}
		if strings.TrimSpace(reasonText) != strings.TrimSpace(reason) {
			continue
		}
		return entry
	}
	t.Fatalf("missing manager recovery aftermath log for event=%q outcome=%q reason=%q in %#v", eventID, outcome, reason, logs)
	return runtimepipeline.RuntimeLogEntry{}
}

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
