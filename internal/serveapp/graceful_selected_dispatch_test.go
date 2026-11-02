package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

type stopDispatchProcessObservation struct {
	startupownership.ProcessCapability
	entered chan struct{}
	release <-chan struct{}
	once    *sync.Once
}

func (p stopDispatchProcessObservation) ProveCurrent(ctx context.Context) error {
	if err := p.ProcessCapability.ProveCurrent(ctx); err != nil {
		return err
	}
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return p.ProcessCapability.ProveCurrent(ctx)
}

func TestOrdinaryDispatchStopsBeforeSelectedHandoffBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, materialized := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/materialized_%t", backend, materialized), func(t *testing.T) {
				root := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
				lookup := func(node *yaml.Node, keys ...string) *yaml.Node {
					for _, key := range keys {
						var next *yaml.Node
						for i := 0; i < len(node.Content); i += 2 {
							if node.Content[i].Value == key {
								next = node.Content[i+1]
								break
							}
						}
						if next == nil {
							t.Fatalf("fixture lacks %s", key)
						}
						node = next
					}
					return node
				}
				value := func(v any) *yaml.Node {
					node := new(yaml.Node)
					if err := node.Encode(v); err != nil {
						t.Fatal(err)
					}
					return node
				}
				for _, name := range []string{"schema.yaml", "hold/schema.yaml", "hold/nodes.yaml", "events.yaml"} {
					path := filepath.Join(root, name)
					body, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					var doc yaml.Node
					if err := yaml.Unmarshal(body, &doc); err != nil {
						t.Fatal(err)
					}
					switch name {
					case "schema.yaml", "hold/schema.yaml":
						pins := lookup(doc.Content[0], "inputs")
						pins.Content = append(pins.Content, value("item.arm"))
						if name == "schema.yaml" {
							outputs := lookup(doc.Content[0], "outputs")
							outputs.Content = append(outputs.Content, value("item.arm"))
							connect := lookup(doc.Content[0], "connect")
							connect.Content = append(connect.Content, value(map[string]any{"event": "item.arm", "from": ".", "to": "hold"}))
						} else {
							stages := lookup(doc.Content[0], "stages")
							stages.Content = append(stages.Content, value("armed"), value(map[string]any{"timers": []any{map[string]any{"after": "5s", "advances_to": "waiting"}}}))
						}
					case "hold/nodes.yaml":
						handlers := lookup(doc.Content[0], "owner")
						handlers.Content = append(handlers.Content, value("item.arm"), value(map[string]any{"advances_to": "armed"}))
					case "events.yaml":
						doc.Content[0].Content = append(doc.Content[0].Content, value("item.arm"), value(map[string]any{"note": "text"}))
					}
					body, err = yaml.Marshal(&doc)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, body, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				selectedEntered, selectedRelease := make(chan struct{}), make(chan struct{})
				var selectedOnce sync.Once
				finishSelected := sync.OnceFunc(func() { close(selectedRelease) })
				defer finishSelected()
				supervisors := make(chan *processLifecycleSupervisor, 1)
				var selected *selectedStoreOwner
				oldCaps := buildSelectedAPICapabilities
				var captured sync.Once
				buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
					captured.Do(func() { selected = owner; supervisors <- req.RuntimeSupervisor })
					caps, err := oldCaps(owner, req)
					if executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor); err == nil && ok {
						execute := executor.ExecuteSelectedContractRunFork
						executor.ExecuteSelectedContractRunFork = func(ctx context.Context, request runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
							request.AgentRuntime.ProcessCapability = stopDispatchProcessObservation{ProcessCapability: request.AgentRuntime.ProcessCapability, entered: selectedEntered, release: selectedRelease, once: &selectedOnce}
							result, err := execute(ctx, request)
							if err != nil {
								t.Logf("selected stop-window execution: %v", err)
							}
							return result, err
						}
						caps.RunFork = executor
					}
					return caps, err
				}
				t.Cleanup(func() { buildSelectedAPICapabilities = oldCaps })
				rt := startWorkspaceLiveGatewayProofRuntime(t, backend, root, "", nil, "127.0.0.1:0")
				supervisor := <-supervisors
				ordinaryEntered, ordinaryRelease := make(chan struct{}), make(chan struct{})
				finishOrdinary := sync.OnceFunc(func() { close(ordinaryRelease) })
				defer finishOrdinary()
				provider := llmapi.New(t, llmapi.Anthropic, "/v1/messages", "workspace-live-proof-key", func(req llmapi.Request) (llmapi.Reply, error) {
					if req.Number == 2 {
						close(ordinaryEntered)
						<-ordinaryRelease
					}
					return llmapi.Reply{ID: fmt.Sprint(req.Number), Model: "claude-test", Text: "Completed", InputTokens: 1, OutputTokens: 1}, nil
				})
				redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "item.received", "bundle_hash": rt.BundleHash, "payload": map[string]any{"item_id": "stop"}, "idempotency_key": "dispatch-seed"})
				waitSelectedSourceNodeDelivered(t, rt.Events.(operatorread.ObservabilityReader), seed.EventID)
				publish := func(name, key string) servedEventPublishRPCResult {
					return requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": name, "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"note": key}, "idempotency_key": key})
				}
				first := publish("item.agent_hold", "first")
				readback := rt.Events.(operatorread.ObservabilityReader)
				deadline := time.Now().Add(serveRuntimeReadyTimeout)
				for {
					event, err := readback.LoadOperatorEvent(t.Context(), first.EventID)
					if err != nil {
						t.Fatal(err)
					}
					settled := false
					for _, delivery := range event.Deliveries {
						settled = settled || (delivery.SubscriberType == "agent" && delivery.Status == "delivered")
					}
					if settled {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("source provider did not settle")
					}
					time.Sleep(10 * time.Millisecond)
				}
				targetHash := rt.BundleHash
				if materialized {
					body, err := os.ReadFile(filepath.Join(root, "hold/prompts/load-agent.md"))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(root, "hold/prompts/load-agent.md"), append(body, []byte("\nUse the selected successor prompt.\n")...), 0o600); err != nil {
						t.Fatal(err)
					}
					bundle := loadWorkflowValidationBundleAt(t, root)
					fact, err := prepareServeSourceArtifact(servedRuntimeProofAuthorActivityContext(t, rt.Runtime, rt.BundleHash), selected.SourceArtifactWriter(), bundle)
					if err != nil {
						t.Fatal(err)
					}
					targetHash = fact.BundleHash()
				}
				forkDone := make(chan error, 1)
				go func() {
					forkErr := errors.New("fork request did not return")
					defer func() { forkDone <- forkErr }()
					result := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "run.fork", map[string]any{"source_run_id": seed.RunID, "fork_event_id": first.EventID, "bundle_hash": targetHash, "idempotency_key": "stop-fork"}, serveRuntimeStopTimeout)
					if result.Error != nil {
						forkErr = fmt.Errorf("fork refusal: %+v", result.Error)
						return
					}
					body, err := json.Marshal(result.Result)
					var fork apiv1.RunForkExecutionResult
					forkErr = errors.Join(err, json.Unmarshal(body, &fork))
					if forkErr == nil && (fork.ForkRunID == "" || fork.SourceRunID != seed.RunID || fork.BundleHash != targetHash) {
						forkErr = fmt.Errorf("accepted handoff lost its exact target: %+v", fork)
					}
				}()
				select {
				case <-selectedEntered:
				case err := <-forkDone:
					t.Fatalf("fork ended before retained preparation: %v", err)
				case <-time.After(serveRuntimeReadyTimeout):
					t.Fatal("selected retained preparation not reached")
				}
				publish("item.agent_hold", "accepted")
				select {
				case <-ordinaryEntered:
				case <-time.After(serveRuntimeReadyTimeout):
					t.Fatal("ordinary accepted provider not reached")
				}
				queued := publish("item.agent_hold", "queued")
				publish("item.arm", "arm")
				timers := rt.Events.(pipeline.WorkflowTimerActivationPersistence)
				var arms []pipeline.WorkflowTimerActivation
				deadline = time.Now().Add(serveRuntimeReadyTimeout)
				for {
					var err error
					arms, err = timers.ListWorkflowTimerActivations(t.Context(), seed.RunID, "", true)
					if err != nil || time.Now().After(deadline) {
						t.Fatalf("ordinary timer arm: %+v %v", arms, err)
					}
					if len(arms) == 1 {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				stopped := make(chan error, 1)
				go func() {
					stopped <- supervisor.ShutdownProcessWithOptions(context.Background(), runtimepkg.DefaultShutdownOptions())
				}()
				for {
					supervisor.mu.RLock()
					stopping := supervisor.stopping
					supervisor.mu.RUnlock()
					if stopping {
						break
					}
					time.Sleep(time.Millisecond)
				}
				unlocked := make(chan struct{})
				go func() { supervisor.operationMu.Lock(); supervisor.operationMu.Unlock(); close(unlocked) }()
				select {
				case <-unlocked:
				case <-time.After(time.Second):
					t.Fatal("selected join holds operationMu")
				}
				finishOrdinary()
				time.Sleep(time.Until(arms[0].FireAt) + 150*time.Millisecond)
				if count := len(provider.Requests()); count != 2 {
					t.Errorf("new ordinary queued delivery started during selected stop: provider calls=%d", count)
				}
				arm, found, err := timers.LoadWorkflowTimerActivation(t.Context(), arms[0].Ref.ActivationID)
				if err != nil || !found || arm.Status != "active" || !arm.FireAt.Equal(arms[0].FireAt) {
					t.Errorf("idle ordinary timer fired during selected stop: %+v found=%t err=%v", arm, found, err)
				}
				pending, err := readback.LoadOperatorEvent(t.Context(), queued.EventID)
				if err != nil {
					t.Fatal(err)
				}
				for _, delivery := range pending.Deliveries {
					if delivery.SubscriberType == "agent" && (delivery.Status == "delivered" || delivery.Terminal) {
						t.Errorf("queued ordinary work became terminal: %+v", delivery)
					}
				}
				finishSelected()
				if err := <-forkDone; err != nil {
					t.Errorf("accepted selected handoff: %v", err)
				}
				if err := <-stopped; err != nil {
					t.Errorf("process stop: %v", err)
				}
			})
		}
	}
}

func TestSelectedForkIdleTimerDoesNotHoldProcessDrainBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyRootIngressServedSessionCleanup(t)
			path := filepath.Join(root, "hold/schema.yaml")
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			body = []byte(strings.Replace(string(body), "  active: {}", "  active:\n    timers:\n      - {after: 1h, advances_to: done}", 1))
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			supervisors := make(chan *processLifecycleSupervisor, 1)
			operations := make(chan string, 1)
			var selected *selectedStoreOwner
			var captured sync.Once
			oldCaps := buildSelectedAPICapabilities
			buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
				captured.Do(func() { selected = owner; supervisors <- req.RuntimeSupervisor })
				caps, err := oldCaps(owner, req)
				if executor, ok := caps.RunFork.(apiv1.SelectedContractRunForkExecutor); ok {
					execute := executor.ExecuteSelectedContractRunFork
					executor.ExecuteSelectedContractRunFork = func(ctx context.Context, request runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
						operations <- request.ForkOperation.OperationID
						result, err := execute(ctx, request)
						if err != nil {
							t.Logf("selected idle fork returned: %v", err)
						}
						return result, err
					}
					caps.RunFork = executor
				}
				return caps, err
			}
			t.Cleanup(func() { buildSelectedAPICapabilities = oldCaps })
			rt := startWorkspaceLiveGatewayProofRuntime(t, backend, root, "", nil, "127.0.0.1:0")
			supervisor := <-supervisors
			entered, release := make(chan struct{}), make(chan struct{})
			finish := sync.OnceFunc(func() { close(release) })
			defer finish()
			provider := llmapi.New(t, llmapi.Anthropic, "/v1/messages", "workspace-live-proof-key", func(req llmapi.Request) (llmapi.Reply, error) {
				if req.Number == 2 {
					close(entered)
					<-release
				}
				return llmapi.Reply{ID: fmt.Sprint(req.Number), Model: "claude-test", Text: "Completed", InputTokens: 1, OutputTokens: 1}, nil
			})
			redirectExternalHosts(t, map[string]string{"api.anthropic.com": provider.URL})
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "item.agent_hold", "bundle_hash": rt.BundleHash, "payload": map[string]any{"note": "selected idle"}, "idempotency_key": "selected-idle-seed"})
			waitWorkspaceProofPipelineHandoff(t, rt, seed.RunID)
			deadline := time.Now().Add(serveRuntimeReadyTimeout)
			for {
				event, err := rt.Events.(operatorread.ObservabilityReader).LoadOperatorEvent(t.Context(), seed.EventID)
				if err != nil {
					t.Fatal(err)
				}
				settled := false
				for _, delivery := range event.Deliveries {
					settled = settled || (delivery.SubscriberType == "agent" && delivery.Status == "delivered")
				}
				if settled {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("source provider did not settle")
				}
				time.Sleep(time.Millisecond)
			}
			forkDone := make(chan servedJSONRPCEnvelope, 1)
			go func() {
				result := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "run.fork", map[string]any{"source_run_id": seed.RunID, "fork_event_id": seed.EventID, "bundle_hash": rt.BundleHash, "idempotency_key": "selected-idle-fork"}, serveRuntimeStopTimeout)
				forkDone <- result
			}()
			select {
			case <-entered:
			case result := <-forkDone:
				t.Fatalf("selected provider not reached: %+v", result)
			case <-time.After(serveRuntimeReadyTimeout):
				t.Fatal("selected provider not reached")
			}
			family, ok := selected.RunFork()
			if !ok {
				t.Fatal("selected fork owner missing")
			}
			op, found, err := family.Operations().LoadForkOperationByID(t.Context(), <-operations)
			if err != nil || !found || op.ForkRunID == "" {
				t.Fatalf("selected operation: %+v found=%t err=%v", op, found, err)
			}
			timers := rt.Events.(pipeline.WorkflowTimerActivationPersistence)
			var arms []pipeline.WorkflowTimerActivation
			deadline = time.Now().Add(serveRuntimeReadyTimeout)
			for {
				arms, err = timers.ListWorkflowTimerActivations(t.Context(), op.ForkRunID, "", true)
				if err != nil || time.Now().After(deadline) {
					t.Fatalf("selected idle timer: %+v err=%v", arms, err)
				}
				if len(arms) == 1 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			stopped := make(chan error, 1)
			go func() {
				stopped <- supervisor.ShutdownProcessWithOptions(context.Background(), runtimepkg.DefaultShutdownOptions())
			}()
			for {
				supervisor.mu.RLock()
				stopping := supervisor.stopping
				supervisor.mu.RUnlock()
				if stopping {
					break
				}
				time.Sleep(time.Millisecond)
			}
			finish()
			if result := <-forkDone; result.Error != nil {
				t.Errorf("selected accepted work: %+v", result.Error)
			}
			if err := <-stopped; err != nil {
				t.Errorf("idle selected projection held process drain: %v", err)
			}
			ack, found, err := family.Operations().LoadForkOperationByID(t.Context(), op.Request.OperationID)
			if err != nil || !found || ack.Status != runfork.ForkOperationActivated || ack.Result == nil || ack.ForkRunID != op.ForkRunID {
				t.Fatalf("accepted selected work lost its immutable handoff ACK: %+v found=%t err=%v", ack, found, err)
			}
			if err := ack.Result.Validate(ack.Request, ack.ForkRunID); err != nil {
				t.Fatal(err)
			}
			arm, found, err := timers.LoadWorkflowTimerActivation(t.Context(), arms[0].Ref.ActivationID)
			if err != nil || !found || arm.Status != "active" || !arm.FireAt.Equal(arms[0].FireAt) {
				t.Errorf("selected idle withdrawal changed durable arm: %+v found=%t err=%v", arm, found, err)
			}
		})
	}
}

func waitSelectedSourceNodeDelivered(t *testing.T, reader operatorread.ObservabilityReader, eventID string) {
	t.Helper()
	deadline := time.Now().Add(serveRuntimeReadyTimeout)
	for {
		event, err := reader.LoadOperatorEvent(t.Context(), eventID)
		if err != nil {
			t.Fatal(err)
		}
		seen, pending := false, false
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType != "node" {
				continue
			}
			seen = true
			if delivery.Terminal && delivery.Status != "delivered" {
				t.Fatalf("selected source seed failed before fork: %+v", delivery)
			}
			pending = pending || delivery.Status != "delivered"
		}
		if seen && !pending {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("selected source seed did not settle before fork: %+v", event.Deliveries)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
