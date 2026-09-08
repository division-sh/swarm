package runforkexecution

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/tools"
)

func TestPreparedGatewayConstructionSettlesWork(t *testing.T) {
	for _, scenario := range []string{"missing_executor", "canceled", "retired_environment", "success"} {
		t.Run(scenario, func(t *testing.T) {
			t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
			process := worklifetime.NewProcess()
			work, err := process.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			executor := tools.NewExecutorWithOptions(nil, tools.ExecutorOptions{})
			switch scenario {
			case "missing_executor":
				executor = nil
			case "canceled":
				process.Retire()
				<-work.Context().Done()
			case "retired_environment":
				t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "retired")
			}
			binding, cleanup, err := startSelectedContractAgentRuntimeGateway(executor, mcp.NewTurnContextRegistry(nil), work, nil)
			if scenario == "success" {
				if err != nil || binding.Empty() || cleanup == nil {
					t.Fatalf("gateway: %v", err)
				}
				if process.ActiveCount() != 1 {
					t.Fatal("serving gateway lost its process work")
				}
				cleanup()
				cleanup()
			} else if err == nil || !binding.Empty() || cleanup != nil {
				t.Fatalf("failed gateway returned binding/cleanup: %v", err)
			}
			if process.ActiveCount() != 0 {
				t.Fatalf("gateway leaked %d work tokens", process.ActiveCount())
			}
			if err := work.Done(); !errors.Is(err, worklifetime.ErrAlreadySettled) {
				t.Fatalf("gateway did not settle its exact token: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := process.Join(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPreparedGatewayRetirementJoinsAcceptedHTTPHandler(t *testing.T) {
	for _, listenerFailure := range []bool{false, true} {
		name := "shutdown"
		if listenerFailure {
			name = "listener_failure"
		}
		t.Run(name, func(t *testing.T) { testPreparedGatewayHandlerJoin(t, listenerFailure) })
	}
}

type preparedGatewayFailingListener struct {
	net.Listener
	accepted bool
	failed   chan struct{}
	once     sync.Once
}

func (l *preparedGatewayFailingListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.Listener.Accept()
	}
	<-l.failed
	return nil, errors.New("injected permanent listener failure")
}

func (l *preparedGatewayFailingListener) fail() { l.once.Do(func() { close(l.failed) }) }

func (l *preparedGatewayFailingListener) Close() error {
	l.fail()
	return l.Listener.Close()
}

func testPreparedGatewayHandlerJoin(t *testing.T, listenerFailure bool) {
	t.Helper()
	process := worklifetime.NewProcess()
	work, err := process.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var failing *preparedGatewayFailingListener
	if listenerFailure {
		failing = &preparedGatewayFailingListener{Listener: listener, failed: make(chan struct{})}
		listener = failing
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHandler()
	server := serveSelectedContractGateway(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}), work)
	requestDone := make(chan error, 1)
	go func() {
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			err = response.Body.Close()
		}
		requestDone <- err
	}()
	<-entered
	process.Retire()
	if failing != nil {
		failing.fail()
		<-server.stopped
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	// Shutdown has stopped Serve, but the accepted handler still owns work.
	<-server.stopped
	if process.ActiveCount() != 1 {
		t.Error("Serve exit released work before the HTTP handler joined")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := process.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("process joined a blocked HTTP handler: %v", err)
	}
	select {
	case <-closed:
		t.Error("gateway cleanup skipped accepted HTTP handler")
	default:
	}
	releaseHandler()
	if err := <-requestDone; err != nil {
		t.Error(err)
	}
	<-closed
	if process.ActiveCount() != 0 {
		t.Fatal("HTTP handler joined but process work remained")
	}
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
}
