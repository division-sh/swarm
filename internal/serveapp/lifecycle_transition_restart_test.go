package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/testutil"
)

// Restarts the real serve owner against the same selected database, not seeded
// lifecycle rows or a replacement receiver/transition implementation.
func lifecycleRestartHarness(t *testing.T, backend, root string, readinessBudget ...time.Duration) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, servedControlProofRuntime)) {
	t.Helper()
	var deliveryReader servedRunDeliveryReader
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { deliveryReader = p.deps.DeliveryStore.SummarizeRun })
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	opts := &cliapp.ServeOptions{SourceRoot: root, SwarmDir: t.TempDir(), SwarmDirSet: true, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	var db *sql.DB
	var receiverReader receiverProofStateReader
	var gateReadback lifecycleGateRestartReadback
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
		db, _, _ = selectedRuntimeStoreForTest(t, p)
		receiverReader = requireReceiverProofStateReader(t, p.deps.EventStore)
		var ok bool
		gateReadback, ok = p.deps.EventStore.(lifecycleGateRestartReadback)
		if !ok {
			t.Fatal("restart proof requires the original card and timer read owners")
		}
	})
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "lifecycle.sqlite"))
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
		var runtime *runtimepkg.Runtime
		startOptions := *opts
		startOptions.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
			runtime = rt
			if opts.TestRuntimeReadyHook != nil {
				opts.TestRuntimeReadyHook(rt)
			}
		}
		process := startServeRuntimeTestProcess(t, startOptions)
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
		return process, servedControlProofRuntime{ReadRunDeliveries: deliveryReader, Endpoint: endpoint, DB: db, Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, opts.SourceRoot), ReceiverStateReader: receiverReader, GateRestartReadback: gateReadback, Runtime: runtime}
	}
}

func lifecycleGateDecisionParams(t *testing.T, rt servedControlProofRuntime, runID, verdict string) map[string]any {
	t.Helper()
	cardID := waitLifecycleGateCard(t, rt, runID)
	return lifecycleDecisionParamsForCard(t, rt, cardID, verdict)
}

func lifecycleDecisionParamsForCard(t *testing.T, rt servedControlProofRuntime, cardID, verdict string) map[string]any {
	t.Helper()
	return lifecycleDecisionParamsFromEndpoint(t, rt.Endpoint, cardID, verdict)
}

func lifecycleDecisionParamsFromEndpoint(t *testing.T, endpoint, cardID, verdict string) map[string]any {
	t.Helper()
	var detail struct {
		DecisionCard struct {
			ContentHash string `json:"card_content_hash"`
		} `json:"decision_card"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
	if detail.DecisionCard.ContentHash == "" {
		t.Fatal("missing frozen card hash")
	}
	return map[string]any{"card_id": cardID, "verdict": verdict, "observed_content_hash": detail.DecisionCard.ContentHash, "idempotency_key": "frozen-decision"}
}

func TestServedCompiledGateFrozenTransitionEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			configureStandingLifecycleCredentials(t)
			sourceA := canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateSharedEvent)
			sourceB := canonicalrouting.CopyLifecycleChangedGate(t)
			for _, root := range []string{sourceA, sourceB} {
				for _, edit := range []struct{ file, from, to string }{
					{"schema.yaml", "  review:\n", "  review:\n    timers:\n      - {after: 1h, advances_to: done}\n"},
					{"events.yaml", "work.requested:\n", "work.prepared:\n  seed: boolean\nwork.requested:\n"},
					{"nodes.yaml", "    advances_to: review\n", "    advances_to: waiting\n    emit: {event: work.prepared, fields: {seed: true}}\npreparer:\n  work.prepared:\n    advances_to: review\n"},
				} {
					path := filepath.Join(root, edit.file)
					body, err := os.ReadFile(path)
					if err != nil || strings.Count(string(body), edit.from) != 1 {
						t.Fatalf("unique admitted fixture edit %s: body=%s err=%v", path, body, err)
					}
					if err := os.WriteFile(path, []byte(strings.Replace(string(body), edit.from, edit.to, 1)), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			opts, start := lifecycleRestartHarness(t, backend, sourceA)
			tokenFile := filepath.Join(t.TempDir(), "api-token")
			if err := os.WriteFile(tokenFile, []byte(apiv1.DefaultLoopbackAPIToken), 0o600); err != nil {
				t.Fatal(err)
			}
			first, rt := start()
			seedParams := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "frozen-start"}
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, seedParams)
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
			params := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			originalEvidence := readLifecycleGateRestartEvidence(t, rt.GateRestartReadback, seed.RunID)
			prefix := originalEvidence.Instance.TransitionHistory
			if len(originalEvidence.Cards) != 1 || len(originalEvidence.Timers) != 1 || originalEvidence.Timers[0].Status != "active" || len(prefix) != 1 || prefix[0].From != "waiting" || prefix[0].To != "review" {
				t.Fatalf("positive pending card, active timer and completed transition setup: %+v", originalEvidence)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("first stop=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			var second *serveRuntimeTestProcess
			var captured <-chan *processLifecycleSupervisor
			pendingEvidence := originalEvidence
			for phase, root := range []string{sourceA, sourceB, sourceA} {
				opts.SourceRoot = root
				if phase == 1 {
					captured = captureSelectedResetSupervisor(t)
				}
				second, rt = start()
				currentSnapshot := lifecycleStoredSnapshot(t, rt, seed.RunID)
				if phase == 0 {
					if currentSnapshot != before {
						t.Fatalf("same-source pending restart changed whole snapshot: before=%s after=%s", before, currentSnapshot)
					}
					if code := second.stop(); code != 0 {
						t.Fatalf("same-source pending stop=%d", code)
					}
					continue
				}
				pendingEvidence = requireLifecycleGateRestartReplacement(t, rt.GateRestartReadback, rt.Runtime.Options.WorkflowModule.SemanticSource(), rt.BundleHash, seed.RunID, pendingEvidence)
				if phase == 1 {
					supervisor := <-captured
					supervisor.mu.Lock()
					original := supervisor.execution["mailbox.decide"]
					if original == nil {
						supervisor.mu.Unlock()
						t.Fatal("restarted supervisor has no mailbox.decide handler")
					}
					supervisor.execution["mailbox.decide"] = func(ctx context.Context, req apiv1.Request) (any, error) {
						result, err := original(ctx, req)
						text := fmt.Sprintf("%#v", result)
						truncated := len(text) > 1024
						if truncated {
							text = text[:1024]
						}
						t.Logf("MISSING_PIN_ORIGINAL result_type=%T result=%s text_truncated=%t error_type=%T", result, text, truncated, err)
						logGateUpgradeCauseTree(t, err)
						return result, err
					}
					supervisor.mu.Unlock()
					withdrawn := originalEvidence.Cards[params["card_id"].(string)]
					out, errOut := &lockedBuffer{}, &lockedBuffer{}
					if code := executeCLI(t.Context(), []string{"--config", opts.ConfigPath, "mailbox", "view", withdrawn.CardID, "--api-server", strings.TrimSuffix(rt.Endpoint, "/v1/rpc"), "--api-token-file", tokenFile}, out, errOut, nil); code != 0 || !strings.Contains(out.String(), "\nReplaced by a new card after an update. No action was taken.\n") {
						t.Fatalf("withdrawn card renderer: code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
					}
					missingPin := requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", params)
					supervisor.mu.Lock()
					supervisor.execution["mailbox.decide"] = original
					supervisor.mu.Unlock()
					if missingPin.Data["code"] != "MAILBOX_CARD_SUPERSEDED" {
						t.Fatalf("missing pin=%#v", missingPin)
					}
				} else {
					pending := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
					if pendingEvidence.Cards[pending["card_id"].(string)].CardID == "" || pending["card_id"] == params["card_id"] {
						t.Fatalf("A-B-A lost distinct third identity: original=%v pending=%v cards=%+v", params, pending, pendingEvidence.Cards)
					}
					params["idempotency_key"] = "withdrawn-after-return"
					if refused := requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", params); refused.Data["code"] != "MAILBOX_CARD_SUPERSEDED" {
						t.Fatalf("original action revived: error=%+v", refused)
					}
					requireLifecycleGateRestartMembers(t, "old A after return no-write", pendingEvidence, readLifecycleGateRestartEvidence(t, rt.GateRestartReadback, seed.RunID))
					params = pending
					params["idempotency_key"] = "replacement-decision"
					stale := map[string]any{"card_id": params["card_id"], "verdict": "approve", "observed_content_hash": strings.Repeat("0", len(params["observed_content_hash"].(string))), "idempotency_key": "stale-decision"}
					requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", stale)
				}
				if got := lifecycleStoredSnapshot(t, rt, seed.RunID); got != currentSnapshot {
					t.Fatalf("phase %d refused action mutated lifecycle: before=%s after=%s", phase, currentSnapshot, got)
				}
				requireLifecycleGateRestartMembers(t, fmt.Sprintf("phase %d refusal no-write", phase), pendingEvidence, readLifecycleGateRestartEvidence(t, rt.GateRestartReadback, seed.RunID))
				if phase == 1 {
					if code := second.stop(); code != 0 {
						t.Fatalf("unpinned stop=%d", code)
					}
				}
			}
			var decided map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decided)
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "done")
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			var entity operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
			if entity.Fields["result"] != "approved" {
				t.Fatalf("ambient gate replaced frozen result: %#v", entity)
			}
			history := readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID)
			if len(history) != 1 || history[0].From != "approved" || history[0].To != "done" {
				t.Fatalf("frozen transition history=%#v", history)
			}
			retainedPrefix := readLifecycleTransitionAtCut(t, rt.ReceiverStateReader, seed.RunID, entityID, prefix[0].TriggerEventID)
			requireLifecycleGateRestartMembers(t, "completed pre-upgrade transition at its fixed cut", prefix[0], retainedPrefix)
			gateRecord := readLifecycleTransitionAtCut(t, rt.ReceiverStateReader, seed.RunID, entityID, history[0].TriggerEventID)
			if gateRecord.From != "review" || gateRecord.To != "approved" || gateRecord.Evidence.FlowID() != "." {
				t.Fatalf("frozen historical gate transition=%#v", gateRecord)
			}
			compiled, ok := gateRecord.Evidence.Compiled()
			if !ok || compiled.Edge().Source != "gate" || compiled.Edge().DecisionID != "review_decision" || compiled.Edge().Verdict != "approve" {
				t.Fatalf("frozen selected gate cause=%#v", compiled)
			}
			requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", 1)
			requireLifecycleEventCount(t, rt, seed.RunID, "work.changed", 0)
			settled := lifecycleStoredSnapshot(t, rt, seed.RunID)
			settledEvidence := readLifecycleGateRestartEvidence(t, rt.GateRestartReadback, seed.RunID)
			conflict := map[string]any{"card_id": params["card_id"], "verdict": "reject", "observed_content_hash": params["observed_content_hash"], "idempotency_key": "contradictory-decision"}
			requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", conflict)
			if got := lifecycleStoredSnapshot(t, rt, seed.RunID); got != settled {
				t.Fatalf("contradictory verdict changed answered successor: before=%s after=%s", settled, got)
			}
			requireLifecycleGateRestartMembers(t, "answered successor contradiction no-write", settledEvidence, readLifecycleGateRestartEvidence(t, rt.GateRestartReadback, seed.RunID))
			if code := second.stop(); code != 0 {
				t.Fatalf("second stop=%d", code)
			}
			third, rt := start()
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decided)
			duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, seedParams)
			if duplicate.EventID != seed.EventID {
				t.Fatal("restart reminted ingress")
			}
			if got := lifecycleStoredSnapshot(t, rt, seed.RunID); got != settled {
				t.Fatalf("terminal restart/replay changed evidence: before=%s after=%s", settled, got)
			}
			requireLifecycleGateRestartMembers(t, "answered successor replay no-write", settledEvidence, readLifecycleGateRestartEvidence(t, rt.GateRestartReadback, seed.RunID))
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
				seed := servedEventPublishRPCResult{RunID: requireServedBootConstructedRun(t, rt.Endpoint, rt.BundleHash, rt.Runtime)}
				entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, seed.RunID, "waiting")
				params := func(event, key string, payload map[string]any) map[string]any {
					params := map[string]any{"event_name": event, "run_id": seed.RunID, "payload": payload, "idempotency_key": key}
					if seed.EventID != "" {
						params["source_event_id"] = seed.EventID
					}
					return params
				}
				started := requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.start", "restart-start", map[string]any{"seed": true}))
				seed.EventID = started.EventID
				requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
				requireLifecycleCurrentTransition(t, readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID), started.EventID, "waiting", "drafting")
				var capParams map[string]any
				var capRevision string
				for attempt := 1; attempt <= 2; attempt++ {
					loop := readLifecycleLoop(t, rt.ReceiverStateReader, seed.RunID, entityID)
					capRevision = loop.RevisionID
					payload := map[string]any{"revision_id": loop.RevisionID}
					admit := requireServedEventPublishRPCResult(t, rt.Endpoint, params("loop.admit", fmt.Sprintf("restart-admit-%d", attempt), payload))
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "review")
					requireLifecycleCurrentTransition(t, readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID), admit.EventID, "drafting", "review")
					capParams = params("loop.repeat", fmt.Sprintf("restart-repeat-%d", attempt), payload)
					if attempt == 2 {
						break
					}
					repeated := requireServedEventPublishRPCResult(t, rt.Endpoint, capParams)
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "drafting")
					requireLifecycleCurrentTransition(t, readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID), repeated.EventID, "review", "drafting")
					waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
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
				waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
				closed := readLifecycleLoop(t, rt.ReceiverStateReader, seed.RunID, entityID)
				if closed.RevisionID != capRevision || closed.Attempt != 2 || closed.Status != loopruntime.StatusClosed || closed.CloseReason != "escaped" {
					t.Fatalf("restart loop=%#v", closed)
				}
				var entity operatorread.OperatorEntityFull
				requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": receipt}, &entity)
				if entity.Fields["revision_id"] != capRevision {
					t.Fatalf("consumer=%#v", entity)
				}
				history := readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID)
				if len(history) != 1 || history[0].TriggerEventID != cap.EventID || history[0].From != "review" || history[0].To != "escaped" {
					t.Fatalf("restart history=%#v", history)
				}
				compiled, ok := history[0].Evidence.Compiled()
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
