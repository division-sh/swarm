package serveapp

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestReleaseCompiledLifecycleJourneysBothStores(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "swarm")
	build := exec.Command("go", "build", "-race", "-o", binary, "./cmd/swarm")
	build.Dir = repoRootForTest()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build race-instrumented release executable: %v\n%s", err, output)
	}
	for _, backend := range servedparity.RequiredBackends {
		for _, gate := range []bool{true, false} {
			name := "connected_loop"
			variant := canonicalrouting.LifecycleLoopConnected
			if gate {
				name, variant = "nested_gate", canonicalrouting.LifecycleGateNested
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				root := canonicalrouting.CopyLifecycleEmitter(t, variant)
				rt := startLifecycleReleaseProcess(t, binary, backend, root)
				prefix := ""
				if gate {
					prefix = "outer/inner/"
				}
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
					"event_name": prefix + "work.requested", "bundle_hash": rt.BundleHash,
					"payload": map[string]any{"seed": true}, "idempotency_key": "release-seed",
				})
				if gate {
					entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
					params := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
					var decision map[string]any
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decision)
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "done")
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
					history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
					if len(history) != 3 {
						t.Fatalf("nested gate history: %+v", history)
					}
					cause, ok := history[1].Evidence.Compiled()
					if !ok || cause.FlowID() != "outer/inner" || cause.Edge().Source != "gate" || cause.Edge().Verdict != "approve" || history[1].From != "review" || history[1].To != "approved" {
						t.Fatalf("nested gate lost its exact compiled cause: %+v", history[1])
					}
					var entity operatorread.OperatorEntityFull
					requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
					if entity.Fields["result"] != "approved" {
						t.Fatalf("nested gate final public result: %+v", entity)
					}
					requireLifecycleEventCount(t, rt, seed.RunID, prefix+"work.completed", 1)
					before := lifecycleStoredSnapshot(t, rt, seed.RunID)
					requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decision)
					if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
						t.Fatal("duplicate acknowledged gate decision changed durable state")
					}
					return
				}

				entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "waiting")
				publish := func(event, key string, payload map[string]any) servedEventPublishRPCResult {
					return requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
						"event_name": event, "run_id": seed.RunID, "source_event_id": seed.EventID,
						"payload": payload, "idempotency_key": key,
					})
				}
				publish("loop.start", "release-start", map[string]any{"seed": true})
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
				var finalEvent string
				for attempt := 1; attempt <= 2; attempt++ {
					current := readLifecycleLoop(t, rt, seed.RunID, entityID)
					if current.Attempt != attempt || current.MaxAttempts != 2 {
						t.Fatalf("wrong public loop generation: %+v", current)
					}
					payload := map[string]any{"revision_id": current.RevisionID}
					publish("loop.admit", fmt.Sprintf("release-admit-%d", attempt), payload)
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "review")
					finalEvent = publish("loop.repeat", fmt.Sprintf("release-repeat-%d", attempt), payload).EventID
					next := "drafting"
					if attempt == 2 {
						next = "escaped"
					}
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, next)
				}
				closed := readLifecycleLoop(t, rt, seed.RunID, entityID)
				if closed.Status != loopruntime.StatusClosed || closed.CloseReason != "escaped" || closed.Attempt != 2 {
					t.Fatalf("loop cap not closed: %+v", closed)
				}
				receiver := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "done")
				if receiver == entityID {
					t.Fatal("connected receiver borrowed the producer entity")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
				if len(history) != 5 {
					t.Fatalf("loop transition history: %+v", history)
				}
				last := history[len(history)-1]
				cause, ok := last.Evidence.Compiled()
				if !ok || cause.FlowID() != "." || cause.Edge().Source != "loop.escape" || cause.Edge().LoopID != "revision" || last.TriggerEventID != finalEvent || last.From != "review" || last.To != "escaped" {
					t.Fatalf("cap escaped through the wrong compiled carrier: %+v", last)
				}
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": receiver}, &entity)
				if entity.Fields["revision_id"] != closed.RevisionID {
					t.Fatalf("connected public result lost escaped revision: %+v", entity)
				}
				requireLifecycleEventCount(t, rt, seed.RunID, "loop.escaped", 1)
			})
		}
	}
}

func startLifecycleReleaseProcess(t *testing.T, binary string, backend servedparity.Backend, root string) servedControlProofRuntime {
	t.Helper()
	unsetStoreSelectorEnv(t)
	var db *sql.DB
	var config, backendName string
	if backend == servedparity.BackendDefaultSQLite {
		path := filepath.Join(t.TempDir(), "lifecycle.sqlite")
		config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", path, channelOnboardingHostWorkspaceFields())
		var err error
		db, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		backendName = "sqlite"
	} else {
		dsn, postgres, _ := testutil.StartEmptyPostgres(t)
		db, backendName = postgres, "postgres"
		config = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
	}
	env := append(releaseProviderTriggerProcessEnv(), "PGPASSWORD="+os.Getenv("PGPASSWORD"), "ANTHROPIC_API_KEY=", "OPENAI_API_KEY=")
	verify := exec.Command(binary, "verify", root, "--config", config)
	verify.Dir, verify.Env = repoRootForTest(), env
	if output, err := verify.CombinedOutput(); err != nil {
		t.Fatalf("public verify failed: %v\n%s", err, output)
	}
	args := []string{"serve", root, "--config", config, "--workspace-backend", "host", "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0", "--self-check", "--verbose"}
	if backend == servedparity.BackendExplicitPostgres {
		args = append(args, "--store", "postgres")
	}
	cmd := exec.Command(binary, args...)
	output := &lockedBuffer{}
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = repoRootForTest(), env, output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	process := &channelOnboardingCrashServeProcess{cmd: cmd, output: output, exited: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		process.waitMu.Lock()
		process.waitErr = err
		process.waitMu.Unlock()
		close(process.exited)
	}()
	t.Cleanup(func() {
		if err := process.stop(); err != nil {
			t.Errorf("release executable shutdown: %v\n%s", err, output.String())
			_ = process.kill()
		}
		if t.Failed() {
			t.Log(output.String())
		}
	})
	return servedControlProofRuntime{Endpoint: process.endpoint(t) + "/v1/rpc", DB: db, Backend: backendName, BundleHash: servedEventPublishFixtureBundleHash(t, root)}
}
