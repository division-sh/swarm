package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestServedColdStaticAgentRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := issue2564ServeHarness(t, backend, canonicalrouting.CopySelectedFutureProviderInput(t), true)
			reached := make(chan string, 4)
			opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
				if !strings.HasSuffix(string(event.Type()), "/work.finish") {
					return nil
				}
				reached <- event.ID()
				<-ctx.Done()
				return ctx.Err()
			}
			first, served := start()
			t.Cleanup(func() {
				if code := first.stop(); code != 0 {
					t.Errorf("cold source cleanup=%d\n%s", code, first.outputString())
				}
			})
			params := map[string]any{
				"event_name": "task.assigned", "bundle_hash": served.BundleHash,
				"payload": map[string]any{"work_id": "restart"}, "idempotency_key": "cold-static-restart",
			}
			body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "cold-restart", "method": "event.publish", "params": params})
			if err != nil {
				t.Fatal(err)
			}
			requestCtx, cancelRequest := context.WithCancel(t.Context())
			request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, served.Endpoint, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
			requestDone := make(chan struct{})
			var requestErr error
			go func() {
				defer close(requestDone)
				response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
				if response != nil {
					_ = response.Body.Close()
					if response.StatusCode != http.StatusOK {
						requestErr = fmt.Errorf("initial publication HTTP status=%d", response.StatusCode)
						return
					}
				}
				requestErr = err
			}()
			t.Cleanup(func() {
				cancelRequest()
				first.stop()
				<-requestDone
			})
			held := make(map[string]operatorread.OperatorEventFull, 4)
			barrierDeadline := time.NewTimer(servedProofPollDeadline)
			defer barrierDeadline.Stop()
			for len(held) != 4 {
				select {
				case id := <-reached:
					if _, duplicate := held[id]; duplicate {
						t.Fatal("provider output reached its consumer twice before restart")
					}
					event, err := served.selected.LoadOperatorEvent(t.Context(), id)
					if err != nil || len(event.Deliveries) != 1 || event.Deliveries[0].Terminal || len(event.DeadLetters) != 0 {
						t.Fatalf("output barrier lost its exact pending carrier: %+v %v", event, err)
					}
					held[id] = event
				case <-barrierDeadline.C:
					t.Fatalf("only %d of four providers reached the durable output barrier\n%s", len(held), first.outputString())
				}
			}
			var runID string
			for _, event := range held {
				if runID != "" && runID != event.RunID {
					t.Fatal("provider outputs crossed runs")
				}
				runID = event.RunID
			}
			seeds := served.events(t, runID, "task.assigned")
			if len(seeds) != 1 {
				t.Fatalf("held provider lacks one exact committed source: %+v", seeds)
			}
			seed := seeds[0]
			before := coldStaticAgentReceipts(t, served, seed.RunID)
			if len(before) == 0 {
				t.Fatal("held provider has no admitted actor delivery")
			}
			// Lose the caller acknowledgment only after durable evidence exists.
			// Cancellation is not evidence of a failed or rolled-back COMMIT.
			cancelRequest()
			<-requestDone
			// HTTP headers may have arrived, but no response body was consumed.
			// That is a lost caller acknowledgment, not an unknown database commit.
			if requestErr != nil && !errors.Is(requestErr, context.Canceled) {
				t.Fatalf("initial publication failed before the intentional lost acknowledgment: %v", requestErr)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("pending source cleanup=%d\n%s", code, first.outputString())
			}
			opts.TestWorkflowNodeHandlerStartHook = nil
			second, recovered := start()
			t.Cleanup(func() {
				if code := second.stop(); code != 0 {
					t.Errorf("recovered source cleanup=%d\n%s", code, second.outputString())
				}
			})
			// Reissue the accepted command through public admission, not through
			// a reconstructed current descriptor or a new creation request.
			duplicate := requireServedEventPublishRPCResult(t, recovered.Endpoint, params)
			if duplicate.EventID != seed.EventID || duplicate.RunID != seed.RunID {
				t.Fatalf("restart reminted the accepted source: before=%+v after=%+v", seed, duplicate)
			}
			waitColdStaticProviderCompletion(t, recovered, seed.RunID)
			recovered.waitDeliveries(t, seed.RunID)
			after := waitColdStaticAgentReceipts(t, recovered, seed.RunID)
			for id, original := range before {
				settled, found := after[id]
				claimVersion := original.ClaimVersion
				if !original.Terminal {
					claimVersion++
				}
				if !found || !reflect.DeepEqual(original.Route, settled.Route) || settled.ClaimVersion != claimVersion {
					t.Fatalf("restart changed actor/dependency or repeated the accepted claim: before=%+v after=%+v", original, settled)
				}
			}
			for _, original := range held {
				current, err := recovered.selected.LoadOperatorEvent(t.Context(), original.EventID)
				if err != nil || current.EventID != original.EventID || current.RunID != original.RunID || current.Source != original.Source ||
					!current.CreatedAt.Equal(original.CreatedAt) || !reflect.DeepEqual(current.Payload, original.Payload) || len(current.DeadLetters) != 0 || len(current.Deliveries) != 1 {
					t.Fatalf("restart changed the provider output: original=%+v current=%+v err=%v", original, current, err)
				}
				previous, settled := original.Deliveries[0], current.Deliveries[0]
				if settled.DeliveryID != previous.DeliveryID || !reflect.DeepEqual(settled.Route, previous.Route) || settled.Status != "delivered" || !settled.Terminal || settled.ClaimVersion != previous.ClaimVersion+1 {
					t.Fatalf("pending carrier did not return and recover exactly once: before=%+v after=%+v", previous, settled)
				}
			}
		})
	}
}

func waitColdStaticAgentReceipts(t *testing.T, served issue2564ServedFixture, runID string) map[string]operatorread.OperatorEventDelivery {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	for {
		receipts := coldStaticAgentReceipts(t, served, runID)
		settled := len(receipts) == 4
		for _, delivery := range receipts {
			if delivery.Status != "delivered" || !delivery.Terminal {
				settled = false
			}
		}
		if settled {
			return receipts
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of four static actor receipts settled\n%s", len(receipts), issue2564H2CompletionReceipt(t, served, runID))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func coldStaticAgentReceipts(t *testing.T, served issue2564ServedFixture, runID string) map[string]operatorread.OperatorEventDelivery {
	t.Helper()
	receipts := make(map[string]operatorread.OperatorEventDelivery)
	for _, event := range served.events(t, runID, "") {
		for _, delivery := range event.Deliveries {
			if !delivery.Route.Recipient.IsAgent() {
				continue
			}
			if !delivery.Route.Target.EntitylessReceiver() || delivery.Route.Initialization.Empty() || delivery.Route.Initialization.ConstructionTarget().Route().EntityID == "" {
				t.Fatalf("static actor inherited or lost construction ownership: %+v", delivery)
			}
			receipts[delivery.DeliveryID] = delivery
		}
	}
	return receipts
}
