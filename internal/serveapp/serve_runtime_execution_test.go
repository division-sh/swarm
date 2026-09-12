package serveapp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

func TestSupervisorPreservesSelectedRetirementFailureAcrossShutdownBranches(t *testing.T) {
	for _, managed := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "managed"}[managed], func(t *testing.T) {
			rt := &runtime.Runtime{}
			supervisor := newProcessLifecycleSupervisor(nil, rt)
			// An incomplete construction is an explicit selected-retirement
			// failure; successful normal shutdown cannot erase that evidence.
			supervisor.selected = &storeselected.RunFork{}
			if supervisor.selected.RetireSelectedContexts(context.Background()) == nil {
				t.Fatal("incomplete selected owner unexpectedly proves retirement")
			}
			supervisor.shutdownRuntime = func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error { return nil }
			if managed {
				manager, err := runtime.NewRuntimeContextManager(nil)
				if err != nil {
					t.Fatal(err)
				}
				supervisor.SetRuntimeContextManager(manager, mustServeTestEphemeralSourceArtifactFact(runtimeContextTestHash("a")))
			}
			if err := supervisor.ShutdownProcessWithOptions(context.Background(), runtime.DefaultShutdownOptions()); err == nil {
				t.Fatal("normal shutdown erased selected-retirement failure")
			}
		})
	}
}

func TestSupervisorReleasesConstructedProjectionOnlyAfterSuccessfulJoin(t *testing.T) {
	for _, failJoin := range []bool{false, true} {
		t.Run(map[bool]string{false: "joined", true: "unjoined"}[failJoin], func(t *testing.T) {
			manager, err := runtime.NewRuntimeContextManager(nil)
			if err != nil {
				t.Fatal(err)
			}
			rt := &runtime.Runtime{}
			supervisor := newProcessLifecycleSupervisor(nil, rt)
			supervisor.runtimeContexts = manager
			supervisor.resetRequests = []serveRuntimeBundleContextRequest{{}}
			joined, released := false, false
			supervisor.shutdownRuntime = func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error {
				if failJoin {
					return errors.New("join failed")
				}
				joined = true
				return nil
			}
			supervisor.resetContexts = []serveRuntimeBundleContext{{runtime: rt, workspaces: serveRuntimeWorkspaceStub{release: func(context.Context) error {
				if !joined {
					t.Error("projection released before successful join")
				}
				released = true
				return nil
			}}}}
			err = supervisor.ShutdownProcessWithOptions(context.Background(), runtime.DefaultShutdownOptions())
			if (err != nil) != failJoin || released == failJoin {
				t.Fatalf("shutdown error=%v released=%v, join failed=%v", err, released, failJoin)
			}
		})
	}
}

func TestServeUnloadedResetRegistersExecutionWithoutRuntime(t *testing.T) {
	supervisor := newProcessLifecycleSupervisor(nil, nil)
	handlers := supervisor.executionDispatch()
	if len(handlers) != len(serveRuntimeExecutionMethods) {
		t.Fatal("unloaded execution registration is incomplete or contains duplicates")
	}
	for _, method := range serveRuntimeExecutionMethods {
		_, err := handlers[method](context.Background(), apiv1.Request{Method: method})
		var application *apiv1.ApplicationError
		if !errors.As(err, &application) || application.Code != apiv1.BundleUnavailableCode {
			t.Errorf("%s: %v, want BUNDLE_UNAVAILABLE", method, err)
		}
	}
}

func TestSupervisorDelegatesRegisteredResetContextShutdownOnlyToManager(t *testing.T) {
	hash := runtimeContextTestHash("a")
	owner := newSupervisorTestRuntimeOccurrence(t, hash)
	fact := mustServeTestEphemeralSourceArtifactFact(hash)
	source := semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{})
	eventBus, err := bus.NewEphemeralEventBusWithOptions(nil, bus.EventBusOptions{
		SourceArtifactFact: fact, WorkOwner: owner, ContractBundle: source, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatal(err)
	}
	rt := &runtime.Runtime{Bus: eventBus}
	manager, err := runtime.NewRuntimeContextManager(nil, runtime.BundleContext{
		SourceArtifactFact: fact, Runtime: rt, WorkOwner: owner,
		Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor := newProcessLifecycleSupervisor(nil, rt)
	supervisor.SetRuntimeContextManager(manager, fact)
	supervisor.resetRequests = []serveRuntimeBundleContextRequest{{}}
	supervisor.resetContexts = []serveRuntimeBundleContext{{runtime: rt}}
	supervisor.resetContextsManaged = true
	supervisor.shutdownRuntime = func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error {
		t.Fatal("duplicate direct shutdown bypassed the registered context owner")
		return nil
	}
	if err := supervisor.ShutdownProcessWithOptions(context.Background(), runtime.DefaultShutdownOptions()); err != nil {
		t.Fatal(err)
	}
	if lookup := manager.LookupBundleHashStatus(hash); lookup.Loaded() {
		t.Fatal("shutdown left registered execution selectable")
	}
}

func TestServeExecutionDispatchOwnsExactOccurrenceUntilHandlerSettles(t *testing.T) {
	hash := runtimeContextTestHash("a")
	owner := newSupervisorTestRuntimeOccurrence(t, hash)
	fact := mustServeTestEphemeralSourceArtifactFact(hash)
	rt := &runtime.Runtime{Bus: &bus.EventBus{}}
	manager, err := runtime.NewRuntimeContextManager(nil, runtime.BundleContext{
		SourceArtifactFact: fact, Runtime: rt, WorkOwner: owner,
		Source: semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	supervisor := newProcessLifecycleSupervisor(nil, rt)
	supervisor.SetRuntimeContextManager(manager, fact)
	supervisor.execution = map[string]apiv1.MethodHandler{
		"execution": supervisor.bindPrimaryExecution(rt, func(ctx context.Context, _ apiv1.Request) (any, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil, nil
		}),
	}
	dispatch := supervisor.executionDispatch()["execution"]
	done := make(chan error, 1)
	go func() { _, err := dispatch(context.Background(), apiv1.Request{}); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not dispatched")
	}
	if got := owner.ActiveCount(); got != 1 {
		t.Fatalf("active execution leases = %d, want 1", got)
	}
	supervisor.mu.Lock()
	supervisor.resetting = true
	supervisor.mu.Unlock()
	owner.Retire()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not receive occurrence cancellation")
	}
	if got := owner.ActiveCount(); got != 1 {
		t.Fatalf("handler settled before its persistence completed: %d", got)
	}
	if _, err := dispatch(context.Background(), apiv1.Request{}); err == nil {
		t.Fatal("reset admitted another execution handler")
	}
	// Do not finish this test until dispatch has relinquished its exact lease.
	t.Cleanup(func() {
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("handler did not settle")
		}
		if got := owner.ActiveCount(); got != 0 {
			t.Errorf("settled handler retained %d leases", got)
		}
	})
}

func TestServeResetRetainsControlReadinessButFencesIngress(t *testing.T) {
	ready := &atomic.Bool{}
	supervisor := newProcessLifecycleSupervisor(ready, nil)
	supervisor.apiReady = true
	called := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	server := newAPIServer(serveSupervisorReadiness{ready, supervisor}, called, called)
	for _, test := range []struct {
		path string
		want int
	}{{"/v1/rpc", http.StatusNoContent}, {"/v1/ws", http.StatusNoContent}, {"/webhooks/test", http.StatusServiceUnavailable}, {"/readyz", http.StatusServiceUnavailable}} {
		recorder := httptest.NewRecorder()
		server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, test.path, nil))
		if recorder.Code != test.want {
			t.Errorf("%s: status %d, want %d", test.path, recorder.Code, test.want)
		}
	}
}

func TestServeManagerRoutedExecutionDoesNotRequirePrimaryContext(t *testing.T) {
	primaryHash, siblingHash := runtimeContextTestHash("a"), runtimeContextTestHash("b")
	sibling := &runtime.Runtime{Bus: &bus.EventBus{}}
	manager, err := runtime.NewRuntimeContextManager(nil, runtime.BundleContext{
		SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(siblingHash), Runtime: sibling,
		WorkOwner: newSupervisorTestRuntimeOccurrence(t, siblingHash),
		Source:    semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor := newProcessLifecycleSupervisor(nil, &runtime.Runtime{})
	supervisor.SetRuntimeContextManager(manager, mustServeTestEphemeralSourceArtifactFact(primaryHash))
	supervisor.execution = map[string]apiv1.MethodHandler{
		"routed": func(ctx context.Context, _ apiv1.Request) (any, error) {
			use, _, err := manager.AcquireBundleHash(ctx, siblingHash)
			if err != nil {
				return nil, err
			}
			if use == nil {
				return nil, errors.New("sibling unavailable")
			}
			defer use.Done()
			return use.Runtime(), nil
		},
	}
	got, err := supervisor.executionDispatch()["routed"](context.Background(), apiv1.Request{})
	if err != nil || got != sibling {
		t.Fatalf("sibling execution = %v, %v", got, err)
	}
}

func TestServeDynamicControlRemainsFencedUntilResetConverges(t *testing.T) {
	hash := runtimeContextTestHash("a")
	fact := mustServeTestEphemeralSourceArtifactFact(hash)
	rt := &runtime.Runtime{Bus: &bus.EventBus{}}
	manager, err := runtime.NewRuntimeContextManager(nil, runtime.BundleContext{
		SourceArtifactFact: fact, Runtime: rt,
		WorkOwner: newSupervisorTestRuntimeOccurrence(t, hash),
		Source:    semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor := newProcessLifecycleSupervisor(nil, rt)
	supervisor.SetRuntimeContextManager(manager, fact)
	supervisor.resetting = true
	use, err := supervisor.acquireCurrentRuntime(context.Background())
	if use != nil {
		use.Done()
	}
	if err == nil || use != nil {
		t.Fatal("dynamic control entered published runtime before reset consumers converged")
	}
	supervisor.resetting = false
	use, err = supervisor.acquireCurrentRuntime(context.Background())
	if err != nil || use == nil {
		t.Fatalf("converged dynamic control = %v, %v", use, err)
	}
	use.Done()
}
