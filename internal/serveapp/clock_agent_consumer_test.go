package serveapp

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestOwnedMockClockLocalAgentConsumerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyClockDeployment(t, false)
			for path, body := range map[string]string{
				"schema.yaml":       "name: clock-agent\nstages: []\nschedules:\n  poll: {every: 250ms, emit: poll.tick}\npins:\n  outputs: [agent.observed]\n",
				"events.yaml":       "poll.tick:\nagent.observed:\n",
				"agents.yaml":       "observer:\n  role: observer\n  intent: {inline: 'Observe the bare poll.tick event and emit agent.observed.'}\n  model: regular\n  subscriptions: [poll.tick]\n  emit_events: [agent.observed]\n  mock: {kind: python, module: mocks/observer.py}\n",
				"mocks/observer.py": "def handle(input):\n    if input['tool_results']:\n        return {'text': 'Clock observed.'}\n    return {'calls': [{'name': 'emit_agent_observed', 'arguments': {}}]}\n",
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
			if full.EventID != settled.EventID || full.RunID != runID || full.Source != "." || len(full.Deliveries) != 1 || full.Deliveries[0].DeliveryID != settled.Deliveries[0].DeliveryID || len(full.DeadLetters) != 0 {
				t.Fatalf("local agent full delivery lost exact publication: %+v", full)
			}
			var frame agentframe.Inspection
			requireServedJSONRPCResult(t, endpoint, "agent.frame", map[string]any{"scope": "effective", "run_id": runID, "agent_id": "observer", "root": true}, &frame)
			identity := frame.Session.AgentIdentity.Value
			if frame.Scope != agentframe.InspectionEffective || identity == nil || identity.Validate() != nil || identity.RunID != runID || identity.AgentID() != "observer" || identity.Route.Presence != agentidentity.RouteRoot {
				t.Fatalf("local agent borrowed a different execution identity: %+v", frame)
			}
			// Root agents are stateless. Their declared output proves execution
			// independently of memory-scoped session/usage readback.
			for {
				var observations operatorread.OperatorEventListResult
				requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "agent.observed"}, "limit": 100}, &observations)
				var matched bool
				for _, event := range observations.Events {
					if event.SourceEventID != settled.EventID {
						continue
					}
					if matched || event.EventName != "agent.observed" || event.ProducerType != events.EventProducerAgent || event.Source != "observer" || len(event.Payload) != 0 || len(event.Deliveries) != 0 || len(event.DeadLetters) != 0 {
						t.Fatalf("agent execution output is not exact: %+v", observations)
					}
					matched = true
				}
				if matched {
					break
				}
				if time.Now().After(deadline) {
					var diagnosis operatorread.OperatorAgentDiagnosis
					requireServedJSONRPCResult(t, endpoint, "agent.diagnose", map[string]any{"run_id": runID, "agent_id": "observer"}, &diagnosis)
					var logs operatorread.OperatorRuntimeLogListResult
					requireServedJSONRPCResult(t, endpoint, "runtime.logs", map[string]any{"run_id": runID, "limit": 100}, &logs)
					t.Fatalf("clock agent has no exact execution output: observations=%+v diagnosis=%+v frame=%+v logs=%+v\n%s", observations, diagnosis, frame, logs, process.outputString())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("local agent clock shutdown=%d\n%s", code, process.outputString())
			}
		})
	}
}
