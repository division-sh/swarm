package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedCompiledGateOutcomeRestartOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateSharedEvent)
			opts, start := lifecycleRestartHarness(t, backend, root)
			reached := make(chan struct{}, 1)
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
				if event.Type() != "work.completed" {
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
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "gate-cut-seed"})
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
			params := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
			var decision map[string]any
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			requestDone := make(chan error, 1)
			// A committed verdict may await its downstream publication before
			// answering HTTP; issue it concurrently with the restart barrier.
			go func() {
				raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "gate-cut", "method": "mailbox.decide", "params": params})
				if err != nil {
					requestDone <- err
					return
				}
				req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, rt.Endpoint, bytes.NewReader(raw))
				if err != nil {
					requestDone <- err
					return
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
				response, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
				if err != nil {
					requestDone <- err
					return
				}
				defer response.Body.Close()
				var envelope servedJSONRPCEnvelope
				err = json.NewDecoder(response.Body).Decode(&envelope)
				if err == nil && envelope.Error != nil {
					err = fmt.Errorf("interrupted verdict response: %#v", envelope.Error)
				}
				requestDone <- err
			}()
			select {
			case <-reached:
			case <-time.After(15 * time.Second):
				t.Fatal("gate committed-output barrier not reached")
			}
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "approved")
			before := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
			if len(before) != 2 {
				t.Fatalf("gate did not commit before barrier: %#v", before)
			}
			compiled, ok := before[1].Evidence.Compiled()
			if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "gate" || compiled.Edge().Verdict != "approve" {
				t.Fatalf("gate selected cause before restart=%#v", compiled)
			}
			var outcomeID string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.completed'`, seed.RunID).Scan(&outcomeID); err != nil {
				t.Fatal(err)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("pre-consumer stop=%d", code)
			}
			cancelRequest()
			select {
			case err := <-requestDone:
				if err != nil {
					t.Logf("HTTP response interrupted after durable verdict proof: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("interrupted verdict HTTP request did not exit")
			}
			opts.TestWorkflowNodeHandlerStartHook = nil
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, rt := start()
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "done")
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			after := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
			if len(after) != 3 || !reflect.DeepEqual(after[:2], before) || after[2].TriggerEventID != outcomeID {
				t.Fatalf("restart changed gate cause or replayed route: before=%#v after=%#v", before, after)
			}
			var entity operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
			if entity.Fields["result"] != "approved" {
				t.Fatalf("recovered gate consumer=%#v", entity)
			}
			settled := lifecycleStoredSnapshot(t, rt, seed.RunID)
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decision)
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != settled {
				t.Fatal("recovered gate duplicate mutated history")
			}
			requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", 1)
			if code := second.stop(); code != 0 {
				t.Fatalf("recovered stop=%d", code)
			}
		})
	}
}
