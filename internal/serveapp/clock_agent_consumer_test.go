package serveapp

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestOwnedMockClockLocalAgentConsumerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyClockDeployment(t, false)
			for path, body := range map[string]string{
				"schema.yaml":       "name: clock-agent\nstages: []\nschedules:\n  poll: {every: 250ms, emit: poll.tick}\n",
				"agents.yaml":       "observer:\n  role: observer\n  intent: {inline: 'Observe the bare poll.tick event.'}\n  model: regular\n  subscriptions: [poll.tick]\n  emit_events: []\n  mock: {kind: python, module: mocks/observer.py}\n",
				"mocks/observer.py": "def handle(input):\n    return {'text': 'Clock observed.', 'usage': {'input_tokens': 1, 'output_tokens': 1}}\n",
			} {
				if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0o700); err != nil {
					t.Fatal(err)
				}
				writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), body)
			}
			opts, _ := clockDeploymentHarness(t, backend, root)
			process := startOwnedMockLifecycleTestProcess(t, repoRootForTest(), t.TempDir(), *opts)
			process.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			runtime := servedTestProcessRuntime(t, process)
			statuses, err := runtime.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 || !statuses[0].RestartDisposition.Executable() {
				t.Fatalf("agent clock lacks a real selected generation: statuses=%+v err=%v", statuses, err)
			}
			runID := statuses[0].RunID
			deadline := time.Now().Add(10 * time.Second)
			var settled operatorread.OperatorEventFull
			for settled.EventID == "" {
				var page operatorread.OperatorEventListResult
				requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "poll.tick"}, "limit": 100}, &page)
				for _, event := range page.Events {
					if event.EventName != "poll.tick" {
						continue
					}
					if event.ProducerType != events.EventProducerInstance || event.Source != "." || len(event.Payload) != 0 || len(event.DeadLetters) != 0 || len(event.Deliveries) != 1 {
						t.Fatalf("local agent clock publication=%+v", event)
					}
					if event.Deliveries[0].Terminal && event.Deliveries[0].Status == "delivered" {
						if event.Deliveries[0].SubscriberType != "agent" || event.Deliveries[0].SubscriberID != "observer" {
							t.Fatalf("local agent acquired foreign delivery=%+v", event)
						}
						settled = event
						break
					}
				}
				if settled.EventID == "" && time.Now().After(deadline) {
					t.Fatalf("local agent did not execute and settle: %+v\n%s", page, process.outputString())
				}
				time.Sleep(10 * time.Millisecond)
			}
			var full operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": settled.EventID}, &full)
			if len(full.Deliveries) != 1 || full.Deliveries[0].Target.FlowID != "." || full.Deliveries[0].Target.FlowInstance != runID || len(full.DeadLetters) != 0 {
				t.Fatalf("local agent full delivery lost exact root ownership: %+v", full)
			}
			assertMockUsageReadback(t, endpoint, settled.RunID, "observer")
			if code := process.stop(); code != 0 {
				t.Fatalf("local agent clock shutdown=%d\n%s", code, process.outputString())
			}
		})
	}
}
