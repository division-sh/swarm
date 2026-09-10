package cataloge2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	flowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runcontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

var errStopAfterSelectedForkCommit = errors.New("test stop after selected-fork materialization commit")

type terminalUnwindProbe struct {
	mode   string
	status string
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (p *terminalUnwindProbe) NotifyLifecycle(_ context.Context, signal runtimelifecycleprobe.Signal) {
	if signal.Kind != runtimelifecycleprobe.WorkflowTerminalCommitted {
		return
	}
	status := p.status
	if status == "" {
		status = "committed"
	}
	if signal.Status != status {
		return
	}
	p.calls.Add(1)
	if p.mode == "panic" {
		panic("test panic after durable terminal commit before completion transfer")
	}
	p.cancel()
}

func TestTerminalCommittedUnwindBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, mode := range []string{"panic", "cancellation"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, selectedForkReadinessCatalogFixture(t, 0, "node"), backend, true)
				path := "worker-flow/worker-001"
				entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
				ctx, cancel := context.WithCancel(catalogRunContext(h, catalogRuntimeRunID))
				defer cancel()
				probe := &terminalUnwindProbe{mode: mode, cancel: cancel}
				h.rt.Pipeline.SetTestLifecycleProbe(probe)
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/worker.inspect"), "cataloge2e", "", nil, 0, catalogRuntimeRunID,
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entity), path),
					eventtest.ConcreteTemplateRoutingSource("worker-flow", path, entity), time.Now().UTC())
				publishErr := h.rt.Bus.PublishAndWait(ctx, event)
				if probe.calls.Load() != 1 {
					t.Fatalf("terminal injection was not reached once: calls=%d err=%v", probe.calls.Load(), publishErr)
				}
				readCtx := catalogRunContext(h, catalogRuntimeRunID)
				if err := h.rt.Manager.WaitForQuiescence(readCtx); err != nil {
					t.Fatalf("committed terminal completion was lost on %s: %v", mode, err)
				}
				owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(path))
				if err != nil {
					t.Fatal(err)
				}
				state, found, err := h.workflow.Load(readCtx, owner)
				if err != nil || !found || state.Status != "terminated" || state.CurrentState != "complete" {
					t.Fatalf("terminal readback after %s: state=%+v found=%t err=%v", mode, state, found, err)
				}
			})
		}
	}
}

type concurrentTerminalProbe struct {
	firstAuthor    string
	started        atomic.Int32
	committed      atomic.Int32
	bothStarted    chan struct{}
	firstCommitted chan struct{}
	release        sync.Once
}

func newConcurrentTerminalProbe() *concurrentTerminalProbe {
	return &concurrentTerminalProbe{bothStarted: make(chan struct{}), firstCommitted: make(chan struct{})}
}

func (p *concurrentTerminalProbe) NotifyLifecycle(ctx context.Context, signal runtimelifecycleprobe.Signal) {
	if !strings.HasSuffix(signal.EventType, "/worker.observed") {
		return
	}
	switch signal.Kind {
	case runtimelifecycleprobe.HandlerStarted:
		if p.firstAuthor != "" {
			if p.started.Add(1) == 2 {
				close(p.bothStarted)
			}
			event, ok := runtimecorrelation.InboundEventFromContext(ctx)
			if !ok {
				panic("terminal ordering proof requires the exact inbound author")
			}
			if strings.Contains(event.SourceAgent(), p.firstAuthor) {
				<-p.bothStarted
			} else {
				<-p.firstCommitted
			}
			return
		}
		switch p.started.Add(1) {
		case 1:
			<-p.bothStarted
		case 2:
			close(p.bothStarted)
			<-p.firstCommitted
		}
	case runtimelifecycleprobe.WorkflowTerminalCommitted:
		p.committed.Add(1)
		p.release.Do(func() { close(p.firstCommitted) })
	}
}

func (p *concurrentTerminalProbe) require(t *testing.T) {
	t.Helper()
	if p.started.Load() != 2 || p.committed.Load() == 0 {
		t.Fatalf("missing contested terminal interleaving: handlers=%d commits=%d", p.started.Load(), p.committed.Load())
	}
}

type fencedFrontierAgent struct {
	config models.AgentConfig
	calls  *atomic.Int32
}

func (a fencedFrontierAgent) ID() string   { return a.config.ID }
func (a fencedFrontierAgent) Type() string { return "test" }
func (a fencedFrontierAgent) Subscriptions() []events.EventType {
	result := make([]events.EventType, len(a.config.Subscriptions))
	for i, eventType := range a.config.Subscriptions {
		result[i] = events.EventType(eventType)
	}
	return result
}
func (a fencedFrontierAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	a.calls.Add(1)
	return nil, errors.New("terminal node must fence the agent before execution")
}

func TestRunScopedConcurrentAgentsTerminalRetirementBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, firstAuthor := range []string{"worker-agent", "second-agent"} {
			t.Run(string(backend)+"/"+firstAuthor+"_first", func(t *testing.T) {
				root := selectedForkReadinessCatalogFixture(t, 2, "agent")
				h := newRuntimeHarnessForBackend(t, root, backend, true)
				path := "worker-flow/worker-001"
				entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
				probe := newConcurrentTerminalProbe()
				probe.firstAuthor = firstAuthor
				h.rt.Pipeline.SetTestLifecycleProbe(probe)
				ctx, cancel := context.WithTimeout(catalogRunContext(h, catalogRuntimeRunID), 10*time.Second)
				defer cancel()
				event := catalogRunScopedWorkerReadyEvent(t, catalogRuntimeRunID, path, entity, uuid.NewString())
				if err := h.rt.Bus.PublishAndWait(ctx, event); err != nil {
					t.Fatalf("ordinary two-agent terminal execution: %v", err)
				}
				if err := h.rt.Manager.WaitForQuiescence(ctx); err != nil {
					t.Fatalf("terminal completion: %v", err)
				}
				probe.require(t)
				owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(path))
				if err != nil {
					t.Fatal(err)
				}
				state, found, err := h.workflow.Load(ctx, owner)
				if err != nil || !found || state.Status != "terminated" || state.CurrentState != "complete" {
					t.Fatalf("terminal canonical readback: state=%+v found=%t err=%v", state, found, err)
				}
				observed, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
				if err != nil {
					t.Fatal(err)
				}
				outputs, delivered := 0, 0
				for _, item := range observed {
					if item.EventName == path+"/worker.observed" {
						outputs++
					}
					if item.EventName != path+"/worker.ready" {
						continue
					}
					for _, delivery := range item.Deliveries {
						if delivery.Route.AgentIdentity.IsZero() {
							continue
						}
						if delivery.Status != "delivered" {
							t.Fatalf("accepted sibling delivery remained unsettled: %+v", delivery)
						}
						delivered++
					}
				}
				if outputs != 2 || delivered != 2 {
					t.Fatalf("terminal public readback: outputs=%d delivered=%d, want exactly two each", outputs, delivered)
				}
				beforeDuplicate := selectedForkReadinessSnapshot(t, ctx, h, catalogRuntimeRunID, owner.Route)
				for i := 0; i < 2; i++ {
					if err := h.rt.Manager.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
						Instance: flowidentity.Stored(nil, "worker-flow", path, "worker-001", entity, ""), FinalState: "complete",
					}); err != nil {
						t.Fatal(err)
					}
				}
				if err := h.rt.Manager.WaitForQuiescence(ctx); err != nil {
					t.Fatal(err)
				}
				if afterDuplicate := selectedForkReadinessSnapshot(t, ctx, h, catalogRuntimeRunID, owner.Route); beforeDuplicate != afterDuplicate {
					t.Fatalf("duplicate terminal request changed durable/public evidence:\nbefore=%s\nafter=%s", beforeDuplicate, afterDuplicate)
				}
				bundleHash, err := runtimecontracts.BundleHash(h.bundle)
				if err != nil {
					t.Fatal(err)
				}
				specDigest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
				if err != nil {
					t.Fatal(err)
				}
				transcript := &catalogExecutionTranscript{version: catalogReplayTranscriptVersion,
					platformSpecDigest: specDigest, bundleHash: bundleHash, runID: catalogRuntimeRunID}
				var rootPayload map[string]any
				if len(event.Payload()) != 0 {
					if err := json.Unmarshal(event.Payload(), &rootPayload); err != nil {
						t.Fatal(err)
					}
				}
				transcript.groups = []catalogTranscriptGroup{{steps: []catalogTriggerStep{{
					Event: string(event.Type()), Payload: rootPayload, inputKind: catalogReplayInputRootIngress,
					eventID: event.ID(), createdAt: event.CreatedAt(), sourceAgent: event.SourceAgent(),
				}}}}
				loadYAML(t, filepath.Join(root, "tests/fixtures.yaml"), &transcript.agentFixtures)
				reopened := h.reopenFromTranscript(transcript)
				recoveryCtx := catalogRunContext(reopened, catalogRuntimeRunID)
				if err := reopened.rt.Manager.WaitForQuiescence(recoveryCtx); err != nil {
					t.Fatal(err)
				}
				recovered, found, err := reopened.workflow.Load(recoveryCtx, owner)
				if err != nil || !found || recovered.Status != "terminated" || recovered.CurrentState != "complete" {
					t.Fatalf("terminal recovery readback: state=%+v found=%t err=%v", recovered, found, err)
				}
				for _, cfg := range reopened.rt.Manager.ListAgentConfigs() {
					if cfg.Identity.RunID == catalogRuntimeRunID && cfg.Identity.FlowInstance() == path {
						t.Fatal("startup reactivated a terminal flow agent")
					}
				}
				recoveredEvents, err := catalogRunScopedOperatorEvents(reopened, catalogRuntimeRunID)
				if err != nil {
					t.Fatal(err)
				}
				recoveredOutputs := 0
				for _, item := range recoveredEvents {
					if item.EventName == path+"/worker.observed" {
						recoveredOutputs++
					}
				}
				if recoveredOutputs != outputs {
					t.Fatalf("terminal recovery repeated output: before=%d after=%d", outputs, recoveredOutputs)
				}
			})
		}
	}
}

func TestStageTimerTerminalJoinsAgentsWithoutJoiningItsCallbackBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := selectedForkReadinessCatalogFixture(t, 2, "agent")
			schemaPath := filepath.Join(root, "worker-flow/schema.yaml")
			schema, err := os.ReadFile(filepath.Join("testdata", "terminal-retirement", "timer-schema.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(schemaPath, schema, 0600); err != nil {
				t.Fatal(err)
			}
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			completed := make(chan error, 1)
			var once sync.Once
			h.rt.Pipeline.SetTestEntityStateHook(func(_, state string) {
				if state == "complete" {
					once.Do(func() {
						completed <- nil
					})
				}
			})
			path := "worker-flow/worker-001"
			materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
			select {
			case err := <-completed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("accepted stage timer could not join terminal agents")
			}
			if err := h.rt.Manager.WaitForQuiescence(catalogRunContext(h, catalogRuntimeRunID)); err != nil {
				t.Fatal(err)
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(path))
			if err != nil {
				t.Fatal(err)
			}
			state, found, err := h.workflow.Load(catalogRunContext(h, catalogRuntimeRunID), owner)
			if err != nil || !found || state.Status != "terminated" || state.CurrentState != "complete" {
				t.Fatalf("timer terminal canonical readback: state=%+v found=%t err=%v", state, found, err)
			}
		})
	}
}

type stopAfterSelectedForkCommit struct {
	forkexecution.SelectedContractForkLifecycle
}

func (s stopAfterSelectedForkCommit) MaterializeRunForkForSelectedContractExecution(ctx context.Context, req runforkreadiness.MaterializeRequest) (runfork.RunForkMaterialization, error) {
	result, err := s.SelectedContractForkLifecycle.MaterializeRunForkForSelectedContractExecution(ctx, req)
	if err != nil {
		return result, err
	}
	return result, errStopAfterSelectedForkCommit
}

func TestSelectedForkFlowOwnedReadinessBothStoresInitial(t *testing.T) {
	runSelectedForkFlowOwnedReadinessBothStores(t, "initial")
}

func TestSelectedForkFlowOwnedReadinessBothStoresStaged(t *testing.T) {
	runSelectedForkFlowOwnedReadinessBothStores(t, "staged")
}

func runSelectedForkFlowOwnedReadinessBothStores(t *testing.T, selectedStage string) {
	t.Helper()
	if selectedStage != "initial" && selectedStage != "staged" {
		t.Fatalf("invalid readiness proof stage %q", selectedStage)
	}
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, declarations := range []int{0, 1, 2} {
			for _, frontier := range []string{"node", "activity", "activity_failure", "activity_rejected", "activity_write", "activity_write_failure", "activity_loop", "activity_loop_rule", "activity_loop_failure", "agent", "mixed", "mixed_progress"} {
				activityFrontier := strings.HasPrefix(frontier, "activity")
				if strings.HasPrefix(frontier, "activity_loop") && declarations != 0 {
					continue
				}
				if declarations == 0 && frontier != "node" && !activityFrontier {
					continue
				}
				for _, stage := range []string{"initial", "staged"} {
					if stage != selectedStage {
						continue
					}
					t.Run(fmt.Sprintf("%s/declared_%d/%s/%s", backend, declarations, frontier, stage), func(t *testing.T) {
						root := selectedForkReadinessCatalogFixture(t, declarations, frontier)
						var activityCalls atomic.Int32
						if activityFrontier {
							server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
								activityCalls.Add(1)
								w.Header().Set("Content-Type", "application/json")
								if strings.HasSuffix(frontier, "failure") {
									w.WriteHeader(http.StatusBadRequest)
								}
								_, _ = w.Write([]byte(`{}`))
							}))
							defer server.Close()
							tools, err := os.ReadFile(filepath.Join("testdata", "terminal-retirement", "activity-tools.yaml"))
							if err != nil {
								t.Fatal(err)
							}
							if strings.HasPrefix(frontier, "activity_write") {
								tools = []byte(strings.ReplaceAll(string(tools), "read_only", "non_idempotent_write"))
							}
							if err := os.WriteFile(filepath.Join(root, "tools.yaml"), []byte(strings.ReplaceAll(string(tools), "http://terminal-proof.invalid", server.URL)), 0600); err != nil {
								t.Fatal(err)
							}
						}
						h := newRuntimeHarnessForBackend(t, root, backend, true)
						selected := runScopedCatalogStore(t, h)
						path := "worker-flow/worker-001"
						entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
						ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
						if _, err := selected.PauseRunControl(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "readiness matrix", ControlledBy: "cataloge2e"}); err != nil {
							t.Fatal(err)
						}
						eventName := "worker.inspect"
						if frontier != "node" && !activityFrontier {
							eventName = "worker.ready"
						}
						event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/"+eventName), "cataloge2e", "", nil, 0, catalogRuntimeRunID,
							events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entity), path),
							eventtest.ConcreteTemplateRoutingSource("worker-flow", path, entity), time.Now().UTC())
						if err := h.rt.Bus.PublishAndWait(ctx, event); err != nil {
							t.Fatal(err)
						}
						owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(path))
						if err != nil {
							t.Fatal(err)
						}
						before, found, err := h.workflow.Load(ctx, owner)
						if err != nil || !found {
							t.Fatalf("source before: %v %t", err, found)
						}
						routesBefore, err := selected.ListFlowInstanceRouteRecords(ctx, owner)
						if err != nil {
							t.Fatal(err)
						}
						var sourceStore interface {
							storetest.DurableDataCatalogStore
							forkexecution.SourceArtifactSelectedContractSourceStore
						} = h.pg
						var forkStore forkexecution.SelectedContractForkLifecycle = h.pg
						if h.sqlite != nil {
							sourceStore, forkStore = h.sqlite, h.sqlite
						}
						loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, sourceStore)
						installCatalogSelectedSourceTopology(t, ctx, h, loaded)
						cfg := testRuntimeConfig()
						cfg.LLM.Backend = "anthropic"
						options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg)
						var terminalProbe *concurrentTerminalProbe
						if declarations == 2 && frontier == "agent" {
							terminalProbe = newConcurrentTerminalProbe()
							terminalProbe.firstAuthor = "worker-agent"
							if stage == "staged" {
								terminalProbe.firstAuthor = "second-agent"
							}
							options.AgentManagerOptions.TestLifecycleProbe = terminalProbe
						}
						var fencedAgentCalls atomic.Int32
						if frontier == "mixed" {
							options.AgentFactory = func(config models.AgentConfig) (runtimemanager.Agent, error) {
								return fencedFrontierAgent{config: config, calls: &fencedAgentCalls}, nil
							}
						}
						executionOwner := selectedContractExecutionOwnerForCatalogHarness(t, h)
						if activityFrontier && stage == "initial" {
							executionOwner = selectedContractExecutionOwnerForCatalogHarness(t, h, &activityLineageProofStore{
								SelectedContractForkLifecycle: forkStore, t: t, h: h, calls: &activityCalls,
								hostile: declarations == 0 && frontier == "activity", hostileLoop: frontier == "activity_loop" || frontier == "activity_loop_rule", rejectFinal: frontier == "activity_rejected",
							})
						}
						if stage == "staged" {
							executionOwner = selectedContractExecutionOwnerForCatalogHarness(t, h, stopAfterSelectedForkCommit{forkStore})
						}
						result, err := forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
							SourceRunID: catalogRuntimeRunID, At: event.ID(), AllowSourceFreeze: true,
							Owner: executionOwner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
						})
						forkRun := result.Materialization.ForkRunID
						refused := stage == "staged" && frontier != "agent"
						if stage == "staged" {
							if !errors.Is(err, errStopAfterSelectedForkCommit) || forkRun == "" {
								t.Fatalf("stage: %#v %v", result, err)
							}
							beforeActivation := selectedForkReadinessSnapshot(t, ctx, h, forkRun, owner.Route)
							attempts := 1
							if refused {
								attempts = 2
							}
							for attempt := 0; attempt < attempts; attempt++ {
								activated, activateErr := forkexecution.ActivateSelectedContractRunFork(ctx, forkexecution.SelectedContractActivationGateRequest{
									ForkRunID: forkRun, AllowSourceFreeze: true, Store: selected,
									ExecutionOwner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, AgentRuntime: options,
								})
								if !refused {
									if activateErr != nil || !activated.Activated || activated.ExecutedEventCount != 1 {
										logSelectedForkRecoveryFailure(t, ctx, h, forkRun, activateErr)
										t.Fatalf("admitted recovery: activated=%t count=%d fork=%s err=%v", activated.Activated, activated.ExecutedEventCount, forkRun, activateErr)
									}
									continue
								}
								if activateErr == nil || !strings.Contains(activateErr.Error(), runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported) || activated.Activated || activated.ExecutedEventCount != 0 || activated.ForkLocalRuntimeContainer != nil || len(activated.ForkEvents) != 0 {
									t.Fatalf("unsupported history must refuse before execution: %#v %v", activated, activateErr)
								}
								if afterActivation := selectedForkReadinessSnapshot(t, ctx, h, forkRun, owner.Route); beforeActivation != afterActivation {
									t.Fatalf("refused activation mutated fork on attempt %d:\nbefore=%s\nafter=%s", attempt, beforeActivation, afterActivation)
								}
							}
						} else if frontier == "activity_rejected" {
							if err == nil || !strings.Contains(err.Error(), "fork activity lineage") || result.ExecutedEventCount != 1 || activityCalls.Load() != 1 {
								t.Fatalf("failed final validation lost execution evidence: count=%d calls=%d err=%v", result.ExecutedEventCount, activityCalls.Load(), err)
							}
						} else if frontier == "mixed" {
							if err == nil || !strings.Contains(err.Error(), "authoritative_delivery_incomplete") || result.ExecutedEventCount != 0 || fencedAgentCalls.Load() != 0 {
								t.Fatalf("terminal-node fence must refuse agent execution: count=%d calls=%d err=%v", result.ExecutedEventCount, fencedAgentCalls.Load(), err)
							}
						} else if err != nil || result.ExecutedEventCount != 1 {
							t.Logf("frontier=%s activity calls=%d", frontier, activityCalls.Load())
							var logs operatorread.OperatorRuntimeLogListResult
							var logsErr error
							if h.sqlite != nil {
								logs, logsErr = h.sqlite.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Component: "agent-manager", Limit: 20})
							} else {
								logs, logsErr = h.pg.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Component: "agent-manager", Limit: 20})
							}
							t.Logf("manager diagnostics count=%d err=%v", len(logs.Logs), logsErr)
							for _, log := range logs.Logs {
								t.Logf("manager terminal diagnostic: %+v", log)
							}
							observed, readErr := catalogRunScopedOperatorEvents(h, forkRun)
							for _, event := range observed {
								t.Logf("fork evidence: event=%s payload=%v deliveries=%+v", event.EventName, event.Payload, event.Deliveries)
							}
							if readErr != nil {
								t.Logf("fork evidence readback: %v", readErr)
							}
							t.Fatalf("execute: count=%d fork=%s err=%v", result.ExecutedEventCount, forkRun, err)
						}
						forkOwner, err := flowidentity.NewRunScopedFlowInstance(forkRun, owner.Route)
						if terminalProbe != nil {
							terminalProbe.require(t)
						}
						if err != nil {
							t.Fatal(err)
						}
						forkState, found, err := h.workflow.Load(ctx, forkOwner)
						fenced := (frontier == "mixed" || frontier == "activity_rejected") && stage == "initial"
						if err != nil || (fenced && found) || (!fenced && (!found || forkState.Config["worker_id"] != "worker-001")) {
							t.Fatalf("fork flow: %#v found=%t err=%v", forkState, found, err)
						}
						readiness, found, err := h.rt.Pipeline.LoadDynamicFlowRuntimeReadiness(ctx, forkRun, owner.Route)
						if err != nil || !found || len(readiness.Plan.Agents) != declarations {
							t.Fatalf("complete readiness: %#v %t %v", readiness, found, err)
						}
						if fenced && (readiness.RunStatus != "cancelled" || readiness.InstanceStatus != "terminated" || readiness.InstanceTerminatedAt.IsZero()) {
							t.Fatalf("fenced mixed frontier left live durable authority: %+v", readiness)
						}
						observed, err := catalogRunScopedOperatorEvents(h, forkRun)
						if err != nil || (!refused && !fenced && len(observed) == 0) || ((refused || fenced) && len(observed) != 0) {
							t.Fatalf("public fork readback: %#v %v", observed, err)
						}
						if !refused && !fenced && forkState.CurrentState != "complete" {
							t.Fatalf("fork did not execute to terminal state: %#v", forkState)
						}
						if activityFrontier {
							wantCalls := int32(1)
							if refused {
								wantCalls = 0
							}
							if got := activityCalls.Load(); got != wantCalls {
								t.Fatalf("fork activity calls = %d, want %d", got, wantCalls)
							}
							if !refused && !fenced {
								assertCatalogActivityLineage(t, activityLineageEvents(t, ctx, h, forkRun), strings.HasSuffix(frontier, "failure"))
								if strings.HasPrefix(frontier, "activity_write") {
									assertCatalogActivityJournal(t, ctx, h, forkRun, strings.HasSuffix(frontier, "failure"))
								}
							}
							if !refused {
								// Activation hands completion to the enclosing runtime; join its
								// durable outcome before comparing refused reactivation attempts.
								h.waitForRunTerminalID(forkRun, catalogRuntimePublishTimeout)
								terminal, err := selected.LoadRunLifecycleSnapshot(ctx, forkRun)
								wantStatus := "completed"
								if fenced {
									wantStatus = "cancelled"
								}
								if err != nil || terminal.Status != wantStatus {
									t.Fatalf("terminal fork: %+v err=%v; want %s", terminal, err, wantStatus)
								}
								beforeRetry := activityLineageStateSnapshot(t, ctx, h, forkRun)
								for attempt := 0; attempt < 2; attempt++ {
									_, retryErr := forkexecution.ActivateSelectedContractRunFork(ctx, forkexecution.SelectedContractActivationGateRequest{
										ForkRunID: forkRun, Store: selected, AllowSourceFreeze: true, ExecutionOwner: selectedContractExecutionOwnerForCatalogHarness(t, h), SourceLoader: loader, AgentRuntime: options,
									})
									if retryErr == nil || activityCalls.Load() != wantCalls {
										t.Fatalf("repeated final verification executed: calls=%d err=%v", activityCalls.Load(), retryErr)
									}
								}
								if afterRetry := activityLineageStateSnapshot(t, ctx, h, forkRun); afterRetry != beforeRetry {
									t.Fatalf("repeated verification changed terminal fork:\nbefore=%s\nafter=%s", beforeRetry, afterRetry)
								}
							}
						}
						after, found, err := h.workflow.Load(ctx, owner)
						if err != nil || !found {
							t.Fatalf("source after: %v %t", err, found)
						}
						routesAfter, err := selected.ListFlowInstanceRouteRecords(ctx, owner)
						if err != nil || before.CurrentState != after.CurrentState || before.Revision != after.Revision || !reflect.DeepEqual(before.Config, after.Config) || !reflect.DeepEqual(routesBefore, routesAfter) {
							t.Fatalf("source changed: before=%#v after=%#v err=%v", before, after, err)
						}
					})
				}
			}
		}
	}
}

func selectedForkReadinessSnapshot(t *testing.T, ctx context.Context, h *runtimeHarness, runID string, route flowidentity.Route) string {
	t.Helper()
	selected := runScopedCatalogStore(t, h)
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, route)
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := h.workflow.Load(ctx, owner)
	if err != nil || !found {
		t.Fatalf("snapshot flow: %t %v", found, err)
	}
	readiness, found, err := h.rt.Pipeline.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
	if err != nil || !found {
		t.Fatalf("snapshot readiness: %t %v", found, err)
	}
	routes, err := selected.ListFlowInstanceRouteRecords(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := selected.LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := catalogRunScopedOperatorEvents(h, runID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal([]any{state, readiness, routes, lifecycle, events})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func selectedForkReadinessCatalogFixture(t *testing.T, declarations int, frontier string) string {
	t.Helper()
	return canonicalrouting.CopySelectedForkReadiness(t, declarations, frontier)
}
