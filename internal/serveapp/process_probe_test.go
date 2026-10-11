package serveapp

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestServingProbeStaysAliveWithoutExecutionBeforeAdmission(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work := worklifetime.NewProcess()
	probe, err := startServeProcessProbe(ctx, work, "127.0.0.1:0", func(subject string, err error) { t.Errorf("%s: %v", subject, err) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := probe.Close(shutdown); err != nil {
			t.Error(err)
		}
	}()
	client := &http.Client{Timeout: time.Second}
	url := "http://" + probe.listener.Addr().String()
	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable, "/v1/rpc": http.StatusServiceUnavailable, "/v1/ws": http.StatusServiceUnavailable} {
		response, err := client.Get(url + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("%s: status %d, want %d", path, response.StatusCode, want)
		}
	}
	var ready, called atomic.Bool
	live := newAPIServer(&ready, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusNoContent)
	}), nil, ctx)
	if err := probe.Install(ctx, live.Handler); err != nil {
		t.Fatal(err)
	}
	if err := probe.Install(ctx, live.Handler); err == nil {
		t.Fatal("duplicate runtime handler replaced the admitted handler")
	}
	response, err := client.Get(url + "/v1/rpc")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || called.Load() {
		t.Fatal("installing the handler bypassed runtime readiness")
	}
	ready.Store(true)
	response, err = client.Get(url + "/v1/rpc")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent || !called.Load() {
		t.Fatal("the same listener did not expose the admitted runtime")
	}
	refused := &serveProcessProbe{handler: &serveProbeHandler{}}
	cancel()
	if err := refused.Install(ctx, live.Handler); !errors.Is(err, context.Canceled) || refused.handler.live.Load() != nil {
		t.Fatal("cancellation published a late runtime handler")
	}
}

func TestServingStandbyPrecedesStoreProjectionBothStores(t *testing.T) {
	for _, backend := range []backendselection.Backend{backendselection.BackendSQLite, backendselection.BackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			request := selected.RuntimeRequest{Selection: backendselection.Selection{Backend: backend}}
			cfg := &config.Config{}
			if backend == backendselection.BackendSQLite {
				request.Selection.SQLitePath = filepath.Join(t.TempDir(), "runtime.db")
			} else {
				var p testpostgres.Parameters
				request.PostgresDSN, p, _ = testutil.StartEmptyPostgresProcessLeaseProof(t)
				t.Setenv("SWARM_PROCESS_WAIT_PASSWORD", p.Password)
				cfg.Database = config.DatabaseConfig{Host: p.Host, Port: int(p.Port), Name: p.Database, User: p.User, PasswordEnv: "SWARM_PROCESS_WAIT_PASSWORD", SSLMode: p.SSLMode}
			}
			holder, err := selected.OpenRuntime(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			defer closeUnactivatedSelectedStore(t, holder)
			originalProjection := projectRuntimePersistenceForServe
			var projections atomic.Int64
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				projections.Add(1)
				return originalProjection(owner)
			}
			defer func() { projectRuntimePersistenceForServe = originalProjection }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bound := make(chan net.Addr, 1)
			var output bytes.Buffer
			opts := cliapp.ServeOptions{APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", ShutdownGrace: time.Second,
				Output: &output, TestAPIListenerBound: func(addr net.Addr) { bound <- addr }}
			presenter := newServeLifecyclePresenter(opts)
			defer presenter.finish()
			returned := make(chan int, 1)
			go func() {
				returned <- buildRuntimeComposition(ctx, runtimeCompositionRequest{Purpose: executionposture.Live,
					Options: opts, Config: cliapp.RuntimeConfigLoadResult{Config: cfg}, StoreSelection: request.Selection,
					Presenter: presenter, Cancel: cancel, RuntimeInstanceID: uuid.NewString()})
			}()
			var addr net.Addr
			select {
			case addr = <-bound:
			case <-time.After(3 * time.Second):
				t.Fatal("serving lease wait prevented early probe publication")
			}
			client := &http.Client{Timeout: time.Second}
			for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable, "/v1/rpc": http.StatusServiceUnavailable} {
				response, err := client.Get("http://" + addr.String() + path)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode != want {
					t.Fatalf("standby %s: %d, want %d", path, response.StatusCode, want)
				}
			}
			if projections.Load() != 0 {
				t.Fatal("standby reached executable store/schema projections without possession")
			}
			select {
			case code := <-returned:
				t.Fatalf("standby failed rather than waiting: %d", code)
			default:
			}
			cancel()
			if err := holder.CloseUnactivated(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-returned:
			case <-time.After(3 * time.Second):
				t.Fatal("cancelled serving waiter did not join")
			}
			if projections.Load() != 0 {
				t.Fatal("cancellation published a late executable projection")
			}
		})
	}
}
