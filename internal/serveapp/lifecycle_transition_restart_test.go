package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// Restarts the real serve owner against the same selected database, not seeded
// lifecycle rows or a replacement receiver/transition implementation.
func lifecycleRestartHarness(t *testing.T, backend, root string, readinessBudget ...time.Duration) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, servedControlProofRuntime)) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	opts := &cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	var db *sql.DB
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "lifecycle.sqlite"))
		captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })
	} else {
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		original := buildStoresForServe
		buildStoresForServe = func(_ context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				return nil, err
			}
			storetest.BootstrapPostgresRuntimeStore(t, pg)
			db = storetest.DatabaseForTest(pg)
			return openSelectedPostgresOwner(t, dsn, db, cfg), nil
		}
		t.Cleanup(func() { buildStoresForServe = original })
		opts.ConfigPath = writeServeRuntimeTestConfig(t)
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	return opts, func() (*serveRuntimeTestProcess, servedControlProofRuntime) {
		process := startServeRuntimeTestProcess(t, *opts)
		if len(readinessBudget) == 0 {
			process.waitForReadyLine()
		} else {
			deadline := time.NewTimer(readinessBudget[0])
			defer deadline.Stop()
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for !serveOutputIsReady(process.outputString()) {
				select {
				case code := <-process.done:
					process.recordStopped(code)
					t.Fatalf("serve exited before readiness: code=%d\n%s", code, process.outputString())
				case <-deadline.C:
					t.Fatalf("serve readiness exceeded %s\n%s", readinessBudget[0], process.outputString())
				case <-ticker.C:
				}
			}
		}
		endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
		return process, servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, opts.SourceRoot)}
	}
}

func lifecycleGateDecisionParams(t *testing.T, rt servedControlProofRuntime, runID, verdict string) map[string]any {
	t.Helper()
	cardID := waitLifecycleGateCard(t, rt, runID)
	return lifecycleDecisionParamsForCard(t, rt, cardID, verdict)
}

func lifecycleDecisionParamsForCard(t *testing.T, rt servedControlProofRuntime, cardID, verdict string) map[string]any {
	t.Helper()
	var detail struct {
		DecisionCard struct {
			ContentHash string `json:"card_content_hash"`
		} `json:"decision_card"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
	if detail.DecisionCard.ContentHash == "" {
		t.Fatal("missing frozen card hash")
	}
	return map[string]any{"card_id": cardID, "verdict": verdict, "observed_content_hash": detail.DecisionCard.ContentHash, "idempotency_key": "frozen-decision"}
}

func TestServedCompiledGateFrozenTransitionEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			sourceA := canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateSharedEvent)
			sourceB := canonicalrouting.CopyLifecycleChangedGate(t)
			opts, start := lifecycleRestartHarness(t, backend, sourceA)
			first, rt := start()
			seedParams := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "frozen-start"}
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, seedParams)
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
			params := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			if code := first.stop(); code != 0 {
				t.Fatalf("first stop=%d", code)
			}
			opts.SourceRoot = sourceB
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, rt := start()
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
				t.Fatal("restart mutated pending lifecycle")
			}
			missingPin := requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", params)
			if missingPin.Data["code"] != "BUNDLE_UNAVAILABLE" {
				t.Fatalf("missing pin=%#v", missingPin)
			}
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
				t.Fatal("unavailable pin mutated gate")
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("unpinned stop=%d", code)
			}
			// Both artifacts were persisted by real source-backed serve boots.
			// Use the supported pin loader, with incompatible B first, not a
			// synthetic runtime registration or source replacement.
			opts.BundleHashes = []string{rt.BundleHash, seedParams["bundle_hash"].(string)}
			second, rt = start()
			if got := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve"); got["card_id"] != params["card_id"] || got["observed_content_hash"] != params["observed_content_hash"] {
				t.Fatalf("restart reminted frozen card: %#v / %#v", params, got)
			}
			var decided map[string]any
			stale := map[string]any{"card_id": params["card_id"], "verdict": "approve", "observed_content_hash": strings.Repeat("0", len(params["observed_content_hash"].(string))), "idempotency_key": "stale-decision"}
			requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", stale)
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
				t.Fatal("stale hash mutated pending gate")
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decided)
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "done")
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			var entity operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
			if entity.Fields["result"] != "approved" {
				t.Fatalf("ambient gate replaced frozen result: %#v", entity)
			}
			history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
			if len(history) != 3 || history[1].From != "review" || history[1].To != "approved" || history[1].Evidence.FlowID() != "." {
				t.Fatalf("frozen transition history=%#v", history)
			}
			compiled, ok := history[1].Evidence.Compiled()
			if !ok || compiled.Edge().Source != "gate" || compiled.Edge().DecisionID != "review_decision" || compiled.Edge().Verdict != "approve" {
				t.Fatalf("frozen selected gate cause=%#v", compiled)
			}
			requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", 1)
			requireLifecycleEventCount(t, rt, seed.RunID, "work.changed", 0)
			settled := lifecycleStoredSnapshot(t, rt, seed.RunID)
			conflict := map[string]any{"card_id": params["card_id"], "verdict": "reject", "observed_content_hash": params["observed_content_hash"], "idempotency_key": "contradictory-decision"}
			requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", conflict)
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != settled {
				t.Fatal("contradictory verdict mutated routed gate")
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("second stop=%d", code)
			}
			third, rt := start()
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decided)
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, seedParams)
			if duplicate.EventID != seed.EventID {
				t.Fatal("restart reminted ingress")
			}
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != settled {
				t.Fatal("terminal restart/replay changed evidence")
			}
			requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", 1)
			requireLifecycleEventCount(t, rt, seed.RunID, "work.changed", 0)
			if code := third.stop(); code != 0 {
				t.Fatalf("third stop=%d", code)
			}
		})
	}
}

func TestServedCompiledTransitionRestartOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"before_cap_handler", "before_escape_consumer"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				root := canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopRepeatEmits)
				opts, start := lifecycleRestartHarness(t, backend, root)
				var armed atomic.Bool
				reached := make(chan struct{}, 1)
				opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, evt events.Event) error {
					match := "loop.repeat"
					if cut == "before_escape_consumer" {
						match = "loop.escaped"
					}
					if !armed.Load() || string(evt.Type()) != match {
						return nil
					}
					select {
					case reached <- struct{}{}:
					default:
					}
					<-ctx.Done()
					return ctx.Err()
				}
				first, rt := start()
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "restart-seed"})
				entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "waiting")
				params := func(event, key string, payload map[string]any) map[string]any {
					return map[string]any{"event_name": event, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": payload, "idempotency_key": key}
				}
				requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.start", "restart-start", map[string]any{"seed": true}))
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
				var capParams map[string]any
				var capRevision string
				for attempt := 1; attempt <= 2; attempt++ {
					loop := readLifecycleLoop(t, rt, seed.RunID, entityID)
					capRevision = loop.RevisionID
					payload := map[string]any{"revision_id": loop.RevisionID}
					requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.admit", fmt.Sprintf("restart-admit-%d", attempt), payload))
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "review")
					capParams = params("loop.repeat", fmt.Sprintf("restart-repeat-%d", attempt), payload)
					if attempt == 2 {
						break
					}
					requireServedEventPublishRPCResult(t, rt.Endpoint, capParams)
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				}
				armed.Store(true)
				cap := requireServedEventPublishRPCResult(t, rt.Endpoint, capParams)
				select {
				case <-reached:
				case <-time.After(15 * time.Second):
					t.Fatal("cap restart barrier never reached")
				}
				state := "review"
				if cut == "before_escape_consumer" {
					state = "escaped"
				}
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, state)
				if code := first.stop(); code != 0 {
					t.Fatalf("first stop=%d", code)
				}
				opts.TestWorkflowNodeHandlerStartHook = nil
				setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
				second, rt := start()
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "escaped")
				receipt := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "done")
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				closed := readLifecycleLoop(t, rt, seed.RunID, entityID)
				if closed.RevisionID != capRevision || closed.Attempt != 2 || closed.Status != loopruntime.StatusClosed || closed.CloseReason != "escaped" {
					t.Fatalf("restart loop=%#v", closed)
				}
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": receipt}, &entity)
				if entity.Fields["revision_id"] != capRevision {
					t.Fatalf("consumer=%#v", entity)
				}
				history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
				if len(history) != 5 || history[4].TriggerEventID != cap.EventID || history[4].From != "review" || history[4].To != "escaped" {
					t.Fatalf("restart history=%#v", history)
				}
				compiled, ok := history[4].Evidence.Compiled()
				if !ok || compiled.Edge().Source != "loop.escape" || compiled.Edge().LoopID != "revision" {
					t.Fatalf("restart escape cause=%#v", compiled)
				}
				settled := lifecycleStoredSnapshot(t, rt, seed.RunID)
				if code := second.stop(); code != 0 {
					t.Fatalf("second stop=%d", code)
				}
				third, rt := start()
				duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, capParams)
				if duplicate.EventID != cap.EventID || lifecycleStoredSnapshot(t, rt, seed.RunID) != settled {
					t.Fatal("terminal restart changed cap identity/history")
				}
				requireLifecycleEventCount(t, rt, seed.RunID, "ordinary.repeated", 1)
				requireLifecycleEventCount(t, rt, seed.RunID, "loop.escaped", 1)
				if code := third.stop(); code != 0 {
					t.Fatalf("third stop=%d", code)
				}
			})
		}
	}
}
