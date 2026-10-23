package serveapp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

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
