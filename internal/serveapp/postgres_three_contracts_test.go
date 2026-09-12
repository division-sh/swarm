package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// This private, fsync-enabled cluster is the only server these outage probes
// stop. Never stop the shared SWARM_TEST_POSTGRES_DSN service.
type contractPostgresServer struct {
	t                            *testing.T
	bin, data, log, options, dsn string
	running                      bool
}

func newContractPostgresServer(t *testing.T) *contractPostgresServer {
	t.Helper()
	bin := os.Getenv("SWARM_INVESTIGATION_PG_BIN")
	if bin == "" {
		t.Skip("set SWARM_INVESTIGATION_PG_BIN for disposable PostgreSQL server-loss investigation")
	}
	root := t.TempDir()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	s := &contractPostgresServer{t: t, bin: bin, data: filepath.Join(root, "data"), log: filepath.Join(root, "postgres.log")}
	s.options = fmt.Sprintf("-h 127.0.0.1 -p %d -k %s -c max_connections=80", port, root)
	s.dsn = fmt.Sprintf("host=127.0.0.1 port=%d user=swarm_probe dbname=postgres sslmode=disable connect_timeout=2", port)
	s.run("initdb", "-D", s.data, "-U", "swarm_probe", "--auth=trust", "--no-locale")
	t.Cleanup(func() {
		if s.running {
			s.stop()
		}
	})
	s.start()
	return s
}

func (s *contractPostgresServer) run(name string, args ...string) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, filepath.Join(s.bin, name), args...).CombinedOutput()
	if err != nil {
		s.t.Fatalf("private postgres %s: %v\n%s", name, err, out)
	}
}
func (s *contractPostgresServer) start() {
	s.run("pg_ctl", "-D", s.data, "-l", s.log, "-o", s.options, "-w", "start")
	s.running = true
}
func (s *contractPostgresServer) stop() {
	s.run("pg_ctl", "-D", s.data, "-m", "immediate", "-w", "stop")
	s.running = false
}

func TestServePostgresLossAndRestartFromDurableState(t *testing.T) {
	for _, loss := range []string{"connection", "server"} {
		t.Run(loss, func(t *testing.T) {
			server := newContractPostgresServer(t)
			observer, err := sql.Open("postgres", server.dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = observer.Close() })
			stubServeRuntimeWorkspaceLifecycle(t)
			oldBuild := buildStoresForServe
			buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
				return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendPostgres}, PostgresDSN: server.dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
			}
			t.Cleanup(func() { buildStoresForServe = oldBuild })
			root := writeServedEventPublishFollowUpFixture(t)
			configPath := filepath.Join(t.TempDir(), "swarm.yaml")
			if err := os.WriteFile(configPath, []byte("runtime:\n  recovery_on_startup: true\nworkspace: {}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			opts := cliapp.ServeOptions{ConfigPath: configPath, SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, ShutdownGrace: time.Second}
			serve := startServeRuntimeTestProcess(t, opts)
			serve.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, serve.outputString()) + "/v1/rpc"
			initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, root), "payload": map[string]any{"item_id": "durable-before-loss"}, "idempotency_key": "2444-before-loss"})
			entityID := requireServedEventPublishEntityState(t, observer, "postgres", initial.RunID, "", "waiting")
			requireServedEntityReadback(t, endpoint, initial.RunID, entityID, "waiting")
			var beforeAuthority string
			if err := observer.QueryRow(`SELECT authority_id::text FROM runtime_startup_authority_facts ORDER BY authority_generation DESC, transition_ordinal DESC LIMIT 1`).Scan(&beforeAuthority); err != nil {
				t.Fatal(err)
			}
			if loss == "server" {
				server.stop()
			} else {
				var pid int
				if err := observer.QueryRow(`SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid::bigint=CASE WHEN hashtext($1)<0 THEN 4294967295::bigint ELSE 0::bigint END AND objid::bigint=(hashtext($1)::bigint & 4294967295::bigint) AND objsubid=1 LIMIT 1`, "swarm:runtime:shared-store-owner").Scan(&pid); err != nil {
					t.Fatal(err)
				}
				var terminated bool
				if err := observer.QueryRow(`SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil || !terminated {
					t.Fatalf("terminate exact authority %d: %t %v", pid, terminated, err)
				}
			}
			code, exited := serve.waitForExit(15 * time.Second)
			if !exited || code == 0 {
				t.Fatalf("%s loss did not fail closed: exit=%t code=%d\n%s", loss, exited, code, serve.outputString())
			}
			if !strings.Contains(serve.outputString(), "ownership") {
				t.Fatalf("missing authority-loss evidence: %s", serve.outputString())
			}
			response, err := (&http.Client{Timeout: time.Second}).Get(strings.TrimSuffix(endpoint, "/v1/rpc") + "/readyz")
			if err == nil {
				defer response.Body.Close()
				if response.StatusCode == http.StatusOK {
					t.Fatal("failed runtime still advertised readiness")
				}
			}
			if loss == "server" {
				refusal := startServeRuntimeTestProcess(t, opts)
				refusalCode, done := refusal.waitForExit(10 * time.Second)
				if !done || refusalCode == 0 || serveOutputIsReady(refusal.outputString()) {
					t.Fatalf("unavailable service admitted startup: done=%t code=%d\n%s", done, refusalCode, refusal.outputString())
				}
				server.start()
			}
			restarted := startServeRuntimeTestProcess(t, opts)
			restarted.waitForReadyLine()
			nextEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, restarted.outputString()) + "/v1/rpc"
			requireServedEntityReadback(t, nextEndpoint, initial.RunID, entityID, "waiting")
			var afterAuthority string
			if err := observer.QueryRow(`SELECT authority_id::text FROM runtime_startup_authority_facts ORDER BY authority_generation DESC, transition_ordinal DESC LIMIT 1`).Scan(&afterAuthority); err != nil {
				t.Fatal(err)
			}
			if afterAuthority == beforeAuthority {
				t.Fatal("restart reused predecessor process authority")
			}
			follow := requireServedEventPublishRPCResult(t, nextEndpoint, map[string]any{"event_name": "item.processed", "run_id": initial.RunID, "payload": map[string]any{"item_id": "review"}, "idempotency_key": "2444-after-loss"})
			requireServedEventPublishEntityState(t, observer, "postgres", initial.RunID, entityID, "done")
			requireServedRunStatusWithDebug(t, nextEndpoint, observer, "postgres", initial.RunID, "completed")
			if follow.RunID != initial.RunID || follow.NewRunCreated {
				t.Fatalf("successor lost durable run identity: %#v", follow)
			}
			if n := servedEventPublishScalarCount(t, observer, "postgres", "application_events_by_run", initial.RunID, ""); n != 2 {
				t.Fatalf("application events=%d want 2, no duplicate processing", n)
			}
			if code := restarted.stop(); code != 0 {
				t.Fatalf("healthy successor exit=%d\n%s", code, restarted.outputString())
			}
			t.Logf("stock pq %s loss: predecessor nonzero, no ready endpoint, exact new authority, same durable run/entity continued once", loss)
		})
	}
}
