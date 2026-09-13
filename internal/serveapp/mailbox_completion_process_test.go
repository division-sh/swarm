package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
)

type mailboxCompletionChild struct {
	Source, Config, Store string
	Barrier               bool
}

func TestMailboxCompletionServeProcessHelper(t *testing.T) {
	raw := os.Getenv("SWARM_MAILBOX_COMPLETION_CHILD")
	if raw == "" {
		t.Skip("parent-owned process proof")
	}
	var request mailboxCompletionChild
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath, opts.StoreMode = request.Source, request.Config, request.Store
	opts.StoreModeSet = true
	opts.PlatformSpecPath = defaultPlatformSpecPath
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.SelfCheck, opts.Verbose = true, true
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	if request.Barrier {
		ready, release := os.NewFile(3, "mailbox-committed"), os.NewFile(4, "mailbox-release")
		defer ready.Close()
		defer release.Close()
		opts.TestWorkflowNodeHandlerStartHook = func(_ context.Context, _ string, event events.Event) error {
			if event.Type() != "work.completed" {
				return nil
			}
			payload, err := canonicaljson.Decode(event.Payload())
			if err != nil {
				return err
			}
			value, _ := payload.Lookup("result")
			result, _ := value.String()
			if result != "approved" {
				return nil
			}
			if _, err := ready.Write([]byte{1}); err != nil {
				return err
			}
			var signal [1]byte
			_, err = io.ReadFull(release, signal[:])
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("serve exit=%d", code)
	}
}

func TestServedMailboxCompletionProcessBoundariesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"released_graceful_drain", "abrupt_process_death"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				unsetStoreSelectorEnv(t)
				root := canonicalrouting.CopyGateCompletionDiagnostic(t)
				var db *sql.DB
				var config string
				if backend == "postgres" {
					dsn, connection, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					db = connection
					config = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
				} else {
					path := filepath.Join(t.TempDir(), "mailbox.sqlite")
					config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, path, channelOnboardingHostWorkspaceFields())
					var err error
					db, err = sql.Open("sqlite", path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = db.Close() })
				}
				setServeRuntimeRecovery(t, config, false, true)
				readyR, readyW, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				releaseR, releaseW, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				for _, f := range []*os.File{readyR, readyW, releaseR, releaseW} {
					t.Cleanup(func() { _ = f.Close() })
				}
				start := func(barrier bool) (*channelOnboardingCrashServeProcess, servedControlProofRuntime) {
					raw, err := json.Marshal(mailboxCompletionChild{Source: root, Config: config, Store: backend, Barrier: barrier})
					if err != nil {
						t.Fatal(err)
					}
					p := startServedCrashProcess(t, "TestMailboxCompletionServeProcessHelper", []string{"SWARM_MAILBOX_COMPLETION_CHILD=" + string(raw)}, readyW, releaseR)
					return p, servedControlProofRuntime{Endpoint: p.endpoint(t) + "/v1/rpc", DB: db, Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, root)}
				}
				first, rt := start(true)
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "seed"})
				entityID := requireServedEventPublishEntityState(t, db, backend, seed.RunID, "", "review")
				waitServedRunDeliveryQuiescence(t, db, backend, seed.RunID)
				var id, hash string
				if err := db.QueryRow(`SELECT card_id,card_content_hash FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&id, &hash); err != nil {
					t.Fatal(err)
				}
				params := map[string]any{"card_id": id, "observed_content_hash": hash, "verdict": "approve", "idempotency_key": "process-cut"}
				domain := gateCompletionRequestDomain(t, db, params)
				response := make(chan gateCompletionHTTPResult, 1)
				go func() { response <- gateCompletionHTTP(context.Background(), rt.Endpoint, params) }()
				reached := make(chan error, 1)
				go func() { var b [1]byte; _, err := io.ReadFull(readyR, b[:]); reached <- err }()
				select {
				case err := <-reached:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(20 * time.Second):
					t.Fatalf("commit barrier not reached\n%s", first.output.String())
				}
				committed := gateCompletionRead(t, rt, "committed_before_handler_release", seed.RunID, id, domain)
				if len(committed.API) != 1 || committed.DecisionEvents != 1 || committed.OutcomeEvents != 2 {
					t.Fatalf("missing atomic commit at cut: %+v", committed)
				}
				if cut == "abrupt_process_death" {
					if err := first.kill(); err != nil {
						t.Fatal(err)
					}
					if first.waitError() == nil {
						t.Fatal("killed process reported graceful exit")
					}
					if lost := <-response; lost.Err == nil {
						t.Fatalf("expected transport loss, got %+v", lost)
					}
				} else {
					if _, err := releaseW.Write([]byte{1}); err != nil {
						t.Fatal(err)
					}
					gateCompletionAssertResponse(t, <-response, committed, false)
					requireServedEventPublishEntityState(t, db, backend, seed.RunID, entityID, "done")
					waitServedRunDeliveryQuiescence(t, db, backend, seed.RunID)
					if err := first.stop(); err != nil {
						t.Fatalf("graceful stop: %v\n%s", err, first.output.String())
					}
				}
				second, rt := start(false)
				requireServedEventPublishEntityState(t, db, backend, seed.RunID, entityID, "done")
				waitServedRunDeliveryQuiescence(t, db, backend, seed.RunID)
				replay := gateCompletionHTTP(context.Background(), rt.Endpoint, params)
				after := gateCompletionRead(t, rt, "retained_restart_exact_replay", seed.RunID, id, domain)
				gateCompletionAssertResponse(t, replay, after, true)
				if after.DecisionEventID != committed.DecisionEventID || after.DecisionEvents != 1 || after.OutcomeEvents != 2 || fmt.Sprint(after.Changes) != fmt.Sprint(committed.Changes) || fmt.Sprint(after.API) != fmt.Sprint(committed.API) {
					t.Fatal("process interruption or replay changed committed decision facts")
				}
				if err := second.stop(); err != nil {
					t.Fatalf("restart stop: %v\n%s", err, second.output.String())
				}
			})
		}
	}
}
