package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedGateCompletionInterruptionDiagnosticOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/interrupted_%t", backend, cut), func(t *testing.T) {
				opts, start := gateCompletionHarness(t, backend, canonicalrouting.CopyGateCompletionDiagnostic(t))
				reached := make(chan struct{}, 1)
				released := make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(released) }) }
				defer release()
				if cut {
					opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
						if event.Type() != "work.completed" {
							return nil
						}
						var payload struct {
							Result string `json:"result"`
						}
						if err := json.Unmarshal(event.Payload(), &payload); err != nil {
							return err
						}
						if payload.Result == "ready" {
							return nil
						}
						select {
						case reached <- struct{}{}:
						default:
						}
						<-released
						return nil
					}
				}
				first, rt := start()
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "completion-seed"})
				entityID := requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, "", "review")
				var cardID, hash string
				if err := rt.DB.QueryRow(`SELECT card_id,card_content_hash FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&cardID, &hash); err != nil {
					t.Fatal(err)
				}
				params := map[string]any{"card_id": cardID, "verdict": "approve", "observed_content_hash": hash, "idempotency_key": "gate-completion"}
				domain := gateCompletionRequestDomain(t, rt.DB, params)
				initial := gateCompletionRead(t, rt, "before_domain_commit", seed.RunID, cardID, domain)
				if initial.Status != "pending" || initial.State != "review" || initial.DecisionEvents != 0 || len(initial.API) != 0 {
					t.Fatalf("invalid initial cut: %#v", initial)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan gateCompletionHTTPResult, 1)
				endpoint := rt.Endpoint
				go func() { done <- gateCompletionHTTP(ctx, endpoint, params) }()
				var firstReply gateCompletionHTTPResult
				if cut {
					select {
					case <-reached:
					case <-time.After(15 * time.Second):
						t.Fatal("durable outcome-consumer barrier not reached")
					}
					select {
					case reply := <-done:
						t.Fatalf("HTTP completed before semantic cut: %#v", reply)
					default:
					}
					committed := gateCompletionRead(t, rt, "domain_committed_before_dispatch_completion", seed.RunID, cardID, domain)
					if committed.Status != "decided" || committed.State != "approved" || committed.DecisionEventID == "" || committed.DecisionEvents != 1 || committed.OutcomeEvents != 2 {
						t.Fatalf("barrier lacks durable decision: %#v", committed)
					}
					if len(committed.API) != 1 {
						t.Errorf("atomic mailbox response absent at durable domain cut: rows=%d", len(committed.API))
					}
					t.Log("GRACEFUL DRAIN: release measured durable barrier; process-death proof is separately exercised")
					release()
				} else {
					select {
					case firstReply = <-done:
					case <-time.After(15 * time.Second):
						t.Fatal("acknowledged control did not answer")
					}
					requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "done")
					waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
					ack := gateCompletionRead(t, rt, "acknowledged_success", seed.RunID, cardID, domain)
					gateCompletionAssertResponse(t, firstReply, ack, false)
				}
				if code := first.stop(); code != 0 {
					t.Fatalf("first stop=%d", code)
				}
				if cut {
					select {
					case firstReply = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("HTTP did not finish after runtime shutdown")
					}
					t.Logf("INTERRUPTED_HTTP transport=%v application=%#v request_context=%v", firstReply.Err, firstReply.Envelope.Error, ctx.Err())
				}
				opts.TestWorkflowNodeHandlerStartHook = nil
				setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
				second, rt := start()
				requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "done")
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
				after := gateCompletionRead(t, rt, "after_restart_before_retry", seed.RunID, cardID, domain)
				retry := gateCompletionHTTP(ctx, rt.Endpoint, params)
				final := gateCompletionRead(t, rt, "after_same_key_retry", seed.RunID, cardID, domain)
				gateCompletionAssertResponse(t, retry, final, true)
				if after.Status != final.Status || after.DecisionEventID != final.DecisionEventID || after.History != final.History || after.Accumulator != final.Accumulator || !reflect.DeepEqual(after.Changes, final.Changes) || final.DecisionEvents != 1 || final.OutcomeEvents != 2 {
					t.Error("same-key retry mutated durable decision/outcome/history")
				}
				if !cut {
					conflict := map[string]any{"card_id": cardID, "verdict": "reject", "observed_content_hash": hash, "idempotency_key": "gate-completion"}
					reply := gateCompletionHTTP(ctx, rt.Endpoint, conflict)
					t.Logf("ACK_CONTROL same-key-conflict domain=%#v reply=%#v", gateCompletionRequestDomain(t, rt.DB, conflict), reply)
					if reply.Err != nil || reply.Envelope.Error == nil || reply.Envelope.Error.Data["code"] != "IDEMPOTENCY_CONFLICT" {
						t.Errorf("same-key conflict=%#v", reply)
					}
				}
				if code := second.stop(); code != 0 {
					t.Fatalf("second stop=%d", code)
				}
			})
		}
	}
}
