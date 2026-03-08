package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/config"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	empirepipeline "empireai/internal/runtime/pipeline/empire"
	"empireai/internal/runtime/sessions"
)

func testRuntimeConfig() *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: false,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              "true",
				OutputFormat:         "json",
				NoSessionPersistence: false,
			},
		},
	}
}

func TestNewRuntimeBuildsCoreComponents(t *testing.T) {
	rt, err := NewRuntime(context.Background(), testRuntimeConfig(), Stores{
		EventStore:      runtimebus.InMemoryEventStore{},
		SessionRegistry: sessions.NewInMemoryRegistry(time.Second),
	}, RuntimeOptions{})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if rt.Bus == nil {
		t.Fatal("expected bus")
	}
	if rt.Scheduler == nil {
		t.Fatal("expected scheduler")
	}
	if rt.LLM == nil {
		t.Fatal("expected llm runtime")
	}
	if rt.ToolExecutor == nil {
		t.Fatal("expected tool executor")
	}
	if rt.Manager == nil {
		t.Fatal("expected manager")
	}
}

func TestNewRuntimeCreatesOptionalGateways(t *testing.T) {
	rt, err := NewRuntime(context.Background(), testRuntimeConfig(), Stores{
		EventStore:      runtimebus.InMemoryEventStore{},
		SessionRegistry: sessions.NewInMemoryRegistry(time.Second),
		InboundStore:    noopInboundStore{},
	}, RuntimeOptions{
		EnableToolGateway: true,
		ToolGatewayToken:  "test-token",
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if rt.ToolGateway == nil {
		t.Fatal("expected tool gateway")
	}
	if rt.InboundGateway == nil {
		t.Fatal("expected inbound gateway")
	}
}

func TestRuntimeStartRunsCoreBootstrap(t *testing.T) {
	rt, err := NewRuntime(context.Background(), testRuntimeConfig(), Stores{
		EventStore:      runtimebus.InMemoryEventStore{},
		SessionRegistry: sessions.NewInMemoryRegistry(time.Second),
	}, RuntimeOptions{SelfCheck: true})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if !rt.Manager.IsRunning() {
		t.Fatal("expected manager to be running after Start")
	}
}

func TestRuntimeShutdownStopsOwnedComponents(t *testing.T) {
	rt, err := NewRuntime(context.Background(), testRuntimeConfig(), Stores{
		EventStore:      runtimebus.InMemoryEventStore{},
		SessionRegistry: sessions.NewInMemoryRegistry(time.Second),
	}, RuntimeOptions{})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if rt.Manager.IsRunning() {
		t.Fatal("expected manager stopped after Shutdown")
	}
}

type noopInboundStore struct{}

func (noopInboundStore) RecordInboundEvent(context.Context, string, string, string) (bool, error) {
	return true, nil
}
func (noopInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return InboundTarget{}, nil
}
func (noopInboundStore) PurgeInboundEventsBefore(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

type bootstrapScheduleStore struct {
	upserts []runtimepipeline.Schedule
}

func (s *bootstrapScheduleStore) UpsertSchedule(_ context.Context, sc runtimepipeline.Schedule) error {
	s.upserts = append(s.upserts, sc)
	return nil
}

func (*bootstrapScheduleStore) CancelSchedule(context.Context, string, string) error { return nil }
func (*bootstrapScheduleStore) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	return nil, nil
}
func (*bootstrapScheduleStore) MarkScheduleFired(context.Context, runtimepipeline.Schedule) error { return nil }

type workflowRuntimeStub struct {
	bundle *runtimecontracts.WorkflowContractBundle
}

func (w workflowRuntimeStub) ContractBundle() *runtimecontracts.WorkflowContractBundle {
	return w.bundle
}

func (workflowRuntimeStub) WorkflowDefinition() *runtimepipeline.WorkflowDefinition { return nil }
func (workflowRuntimeStub) WorkflowNodes() []runtimepipeline.WorkflowNode           { return nil }
func (workflowRuntimeStub) WorkflowStateStore() runtimepipeline.WorkflowStateStore  { return nil }
func (workflowRuntimeStub) WorkflowInstanceStore() runtimepipeline.WorkflowInstancePersistence {
	return nil
}
func (workflowRuntimeStub) TransitionEvaluator() runtimepipeline.TransitionEvaluator { return nil }
func (workflowRuntimeStub) GuardRegistry() runtimepipeline.GuardRegistry             { return nil }
func (workflowRuntimeStub) ActionRegistry() runtimepipeline.ActionRegistry           { return nil }

func TestEnsureRecurringWorkflowSchedules_UsesRecurringTimersFromContracts(t *testing.T) {
	store := &bootstrapScheduleStore{}
	module := empirepipeline.NewModule()
	if err := ensureRecurringWorkflowSchedules(context.Background(), store, workflowRuntimeStub{bundle: module.ContractBundle()}); err != nil {
		t.Fatalf("ensureRecurringWorkflowSchedules() error = %v", err)
	}
	if len(store.upserts) != 1 {
		t.Fatalf("expected 1 recurring workflow schedule, got %d", len(store.upserts))
	}
	got := store.upserts[0]
	if got.AgentID != "empire-coordinator" {
		t.Fatalf("expected contract owner empire-coordinator, got %q", got.AgentID)
	}
	if got.EventType != "timer.portfolio_digest" {
		t.Fatalf("expected timer.portfolio_digest schedule, got %q", got.EventType)
	}
	if got.Mode != "cron" || got.Cron != "@every 6h0m0s" {
		t.Fatalf("unexpected recurring schedule: %+v", got)
	}
}

func TestEnsureRecurringWorkflowSchedules_DoesNotProvisionStageTimersAtStartup(t *testing.T) {
	store := &bootstrapScheduleStore{}
	module := empirepipeline.NewModule()
	if err := ensureRecurringWorkflowSchedules(context.Background(), store, workflowRuntimeStub{bundle: module.ContractBundle()}); err != nil {
		t.Fatalf("ensureRecurringWorkflowSchedules() error = %v", err)
	}
	for _, sc := range store.upserts {
		if sc.EventType == "timer.marginal_review" {
			t.Fatalf("did not expect stage-scoped timer.marginal_review startup schedule: %+v", sc)
		}
	}
}
