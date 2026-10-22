package manager

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func TestWorkspaceGatewayActivationHoldsPublicationAndSkipsCurrentReplay(t *testing.T) {
	bus := newProjectionTestBus()
	am := newProjectionTestManager(t, bus, (&projectionTestFactory{handled: make(chan int, 1)}).Build)
	entered, release := make(chan effects.LifecycleToken, 1), make(chan struct{})
	var calls atomic.Int32
	am.workspaceGatewayAdmission = func(ctx context.Context, actor models.AgentConfig) error {
		calls.Add(1)
		token, ok := effects.LifecycleTokenFromContext(ctx)
		if !ok || token.Identity != actor.Identity {
			return errors.New("activation has no exact token")
		}
		if err := am.ProveUnpublishedActivation(token); err != nil {
			return err
		}
		entered <- token
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	runCtx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, runCtx)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
			t.Error(err)
		}
	})
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: "gateway-held", Subscriptions: []string{"test.old"},
		Identity: agentidentitytest.RootRuntime(t, "gateway-held", "gateway-held-test"),
	})
	result := make(chan error, 1)
	go func() { result <- spawnManagerTestAgent(am, cfg) }()
	var token effects.LifecycleToken
	select {
	case token = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("activation did not reach gateway observation")
	}
	if routes := len(bus.routeHistory(cfg.ID)); routes != 0 {
		t.Fatalf("published %d routes before gateway observation completed", routes)
	}
	foreign := token
	foreign.Generation++
	if am.ProveUnpublishedActivation(foreign) == nil {
		t.Fatal("foreign generation acquired observation permission")
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if routes := len(bus.routeHistory(cfg.ID)); routes != 1 {
		t.Fatalf("routes after successful observation = %d", routes)
	}
	if am.ProveUnpublishedActivation(token) == nil {
		t.Fatal("published token retained pre-publication permission")
	}
	if _, err := am.replaceExecutionIdentityConfigWithTopology(context.Background(), cfg.Identity, "start", "", nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("current occurrence replay performed %d observations", calls.Load())
	}
}

func TestWorkspaceGatewayActivationFailureCompensatesWithoutPublication(t *testing.T) {
	bus := newProjectionTestBus()
	am := newProjectionTestManager(t, bus, (&projectionTestFactory{handled: make(chan int, 1)}).Build)
	want := errors.New("target gateway unavailable")
	var token effects.LifecycleToken
	am.workspaceGatewayAdmission = func(ctx context.Context, _ models.AgentConfig) error {
		token, _ = effects.LifecycleTokenFromContext(ctx)
		return want
	}
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	defer cancel()
	defer am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	cfg := managerTestAgentConfig(models.AgentConfig{
		ExecutionMode: "live", ID: "gateway-refused", Subscriptions: []string{"test.old"},
		Identity: agentidentitytest.RootRuntime(t, "gateway-refused", "gateway-refused-test"),
	})
	if err := spawnManagerTestAgent(am, cfg); !errors.Is(err, want) {
		t.Fatalf("spawn error = %v, want gateway refusal", err)
	}
	if routes := len(bus.routeHistory(cfg.ID)); routes != 0 {
		t.Fatalf("refused activation published %d routes", routes)
	}
	if am.ProveUnpublishedActivation(token) == nil {
		t.Fatal("compensated activation retained observation permission")
	}
}

func TestWorkspaceGatewayActivationRequiresCurrentActor(t *testing.T) {
	bus := newProjectionTestBus()
	am := newProjectionTestManager(t, bus, (&projectionTestFactory{handled: make(chan int, 1)}).Build)
	type observation struct {
		ctx   context.Context
		token effects.LifecycleToken
		actor models.AgentConfig
	}
	entered, release := make(chan observation, 1), make(chan struct{})
	am.workspaceGatewayAdmission = func(ctx context.Context, actor models.AgentConfig) error {
		token, _ := effects.LifecycleTokenFromContext(ctx)
		entered <- observation{ctx: ctx, token: token, actor: actor}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
			t.Error(err)
		}
	})
	cfg := managerTestAgentConfig(models.AgentConfig{ExecutionMode: "live", ID: "current-gateway", Subscriptions: []string{"test.old"},
		Identity: agentidentitytest.RootRuntime(t, "current-gateway", "current-gateway-run")})
	result := make(chan error, 1)
	go func() { result <- spawnManagerTestAgent(am, cfg) }()
	var observed observation
	select {
	case observed = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("activation did not reach gateway observation")
	}
	if err := am.proveUnpublishedActivationActor(observed.ctx, observed.token, observed.actor); err != nil {
		t.Fatal(err)
	}
	changed := observed.actor
	changed.ExecutionMode = "mock"
	if err := am.proveUnpublishedActivationActor(observed.ctx, observed.token, changed); err == nil {
		t.Fatal("same identity with different configuration borrowed the current activation")
	}
	foreign := observed.token
	foreign.Generation++
	if err := am.proveUnpublishedActivationActor(observed.ctx, foreign, observed.actor); err == nil {
		t.Fatal("different generation borrowed the current activation")
	}
	canceled, stop := context.WithCancel(observed.ctx)
	stop()
	if err := am.proveUnpublishedActivationActor(canceled, observed.token, observed.actor); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe = %v", err)
	}
	if routes := len(bus.routeHistory(cfg.ID)); routes != 0 {
		t.Fatalf("probe validation published %d routes", routes)
	}
	// Keep the candidate held until all negative observations have completed.
	select {
	case err := <-result:
		t.Fatalf("held observation completed before release: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("released observation did not complete")
	}
	if err := am.proveUnpublishedActivationActor(observed.ctx, observed.token, observed.actor); err == nil {
		t.Fatal("published actor retained observation permission")
	}
}

func TestWorkspaceGatewayActivationRejectsConfigurationDriftBeforePublication(t *testing.T) {
	bus := newProjectionTestBus()
	am := newProjectionTestManager(t, bus, (&projectionTestFactory{handled: make(chan int, 1)}).Build)
	am.workspaceGatewayAdmission = func(_ context.Context, actor models.AgentConfig) error {
		actor.Config[12] = '2'
		return nil
	}
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second}); err != nil {
			t.Error(err)
		}
	})
	cfg := managerTestAgentConfig(models.AgentConfig{ExecutionMode: "live", ID: "drift-gateway", Subscriptions: []string{"test.old"},
		Config: json.RawMessage(`{"original":1}`), Identity: agentidentitytest.RootRuntime(t, "drift-gateway", "drift-gateway-run")})
	if err := spawnManagerTestAgent(am, cfg); err == nil {
		t.Fatal("configuration drift during observation published the receiver")
	}
	if routes := len(bus.routeHistory(cfg.ID)); routes != 0 {
		t.Fatalf("changed activation published %d routes", routes)
	}
}
