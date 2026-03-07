package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/config"
	runtimebus "empireai/internal/runtime/bus"
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
