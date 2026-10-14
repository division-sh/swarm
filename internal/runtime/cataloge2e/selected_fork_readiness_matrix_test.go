package cataloge2e

import (
	"context"
	"encoding/json"
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
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type terminalUnwindProbe struct {
	kind   runtimelifecycleprobe.Kind
	node   string
	mode   string
	status string
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (p *terminalUnwindProbe) NotifyLifecycle(_ context.Context, signal runtimelifecycleprobe.Signal) {
	if signal.Kind != p.kind || (p.node != "" && signal.SubscriberID != p.node) {
		return
	}
	status := p.status
	if status == "" {
		status = "completed"
	}
	if signal.Status != status {
		return
	}
	p.calls.Add(1)
	if p.mode == "panic" {
		if p.kind == runtimelifecycleprobe.WorkflowTerminalCommitted {
			panic("test panic after durable terminal commit before completion transfer")
		}
		panic("test panic after acknowledged final-stage handler completion")
	}
	p.cancel()
}

func TestTerminalCommittedUnwindBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, mode := range []string{"panic", "cancellation"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, selectedForkReadinessCatalogFixture(t, 0, "node"), backend, true)
				path := "worker-flow/worker-001"
				materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path, "worker.inspect.requested")
				ctx, cancel := context.WithCancel(catalogRunContext(h, catalogRuntimeRunID))
				defer cancel()
				probe := &terminalUnwindProbe{kind: runtimelifecycleprobe.HandlerCompleted, node: identitytest.FlowNode(t, "worker-flow", "inspect-node").Key(), mode: mode, cancel: cancel}
				h.rt.Pipeline.SetTestLifecycleProbe(probe)
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType("worker.inspect"), "cataloge2e", "", []byte(`{"worker_id":"worker-001"}`), 0, catalogRuntimeRunID,
					events.EventEnvelope{}, eventtest.RootRoutingSource(catalogRuntimeRunID), time.Now().UTC())
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
				if err != nil || !found || state.Status != "active" || !state.TerminatedAt.IsZero() || state.CurrentState != "complete" {
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
	case runtimelifecycleprobe.HandlerCompleted:
		if signal.Status != "completed" {
			return
		}
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

type countedFrontierAgent struct {
	config models.AgentConfig
	calls  *atomic.Int32
}

func (a countedFrontierAgent) ID() string   { return a.config.ID }
func (a countedFrontierAgent) Type() string { return "test" }
func (a countedFrontierAgent) Subscriptions() []events.EventType {
	result := make([]events.EventType, len(a.config.Subscriptions))
	for i, eventType := range a.config.Subscriptions {
		result[i] = events.EventType(eventType)
	}
	return result
}
func (a countedFrontierAgent) OnEvent(ctx context.Context, _ events.Event) ([]events.Event, error) {
	a.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("ordinary final entry canceled accepted agent work: %w", err)
	}
	return nil, nil
}

func TestRunScopedConcurrentAgentsTerminalRetirementBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, firstAuthor := range []string{"worker-agent", "second-agent"} {
			t.Run(string(backend)+"/"+firstAuthor+"_first", func(t *testing.T) {
				root := selectedForkReadinessCatalogFixture(t, 2, "agent")
				h := newRuntimeHarnessForBackend(t, root, backend, true)
				path := "worker-flow/worker-001"
				entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path, "worker.ready.requested")
				probe := newConcurrentTerminalProbe()
				probe.firstAuthor = firstAuthor
				h.rt.Pipeline.SetTestLifecycleProbe(probe)
				ctx, cancel := context.WithTimeout(catalogRunContext(h, catalogRuntimeRunID), 10*time.Second)
				defer cancel()
				event := catalogRunScopedWorkerReadyEvent(t, catalogRuntimeRunID, uuid.NewString())
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
				if err != nil || !found || state.Status != "active" || !state.TerminatedAt.IsZero() || state.CurrentState != "complete" {
					t.Fatalf("terminal canonical readback: state=%+v found=%t err=%v", state, found, err)
				}
				observed, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
				if err != nil {
					t.Fatal(err)
				}
				outputs, delivered := 0, 0
				var observedNames []string
				for _, item := range observed {
					observedNames = append(observedNames, fmt.Sprintf("%s:%d", item.EventName, len(item.Deliveries)))
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
					t.Fatalf("terminal public readback: outputs=%d delivered=%d, want exactly two each; events=%v", outputs, delivered, observedNames)
				}
				// Ordinary final entry retained both accepted agents. Explicit cleanup
				// remains a separate supported operation with idempotent retirement.
				request := runtimepipeline.FlowInstanceDeactivationRequest{Instance: flowidentity.Stored(nil, "worker-flow", path, "worker-001", entity, ""), FinalState: "complete"}
				if err := h.rt.Manager.DeactivateFlowInstanceModel(ctx, request); err != nil {
					t.Fatal(err)
				}
				if err := h.rt.Manager.WaitForQuiescence(ctx); err != nil {
					t.Fatal(err)
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

func TestStageTimerFinalEntryPreservesAgentsAndSettlesCallbackBothStores(t *testing.T) {
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
			materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path, "worker.ready.requested")
			select {
			case err := <-completed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("accepted stage timer did not acknowledge final entry")
			}
			if err := h.rt.Manager.WaitForQuiescence(catalogRunContext(h, catalogRuntimeRunID)); err != nil {
				t.Fatal(err)
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(path))
			if err != nil {
				t.Fatal(err)
			}
			state, found, err := h.workflow.Load(catalogRunContext(h, catalogRuntimeRunID), owner)
			if err != nil || !found || state.Status != "active" || !state.TerminatedAt.IsZero() || state.CurrentState != "complete" {
				t.Fatalf("timer terminal canonical readback: state=%+v found=%t err=%v", state, found, err)
			}
			agents := 0
			for _, cfg := range h.rt.Manager.ListAgentConfigs() {
				if cfg.Identity.RunID == catalogRuntimeRunID && cfg.Identity.FlowInstance() == path {
					agents++
				}
			}
			if agents != 2 {
				t.Fatalf("ordinary stage deadline retired declared agents: count=%d want 2", agents)
			}
		})
	}
}

func stageCatalogSelectedContractFork(
	t testing.TB,
	ctx context.Context,
	store forkexecution.SelectedContractForkLifecycle,
	owner forkexecution.SelectedContractExecutionOwner,
	loader forkexecution.SelectedContractSourceLoader,
	selection runfork.RunForkContractSelection,
	options forkexecution.SelectedContractAgentRuntimeOptions,
	sourceRunID, eventID string,
) runfork.RunForkMaterialization {
	t.Helper()
	prepared, err := owner.Prepare(ctx, forkexecution.SelectedContractExecutionRequest{
		SourceRunID: sourceRunID, At: eventID, AllowSourceFreeze: true,
		SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
	})
	if err != nil {
		t.Fatalf("prepare selected fork for direct staging: %v", err)
	}
	defer func() {
		if err := prepared.Close(); err != nil {
			t.Errorf("close directly staged selected fork preparation: %v", err)
		}
	}()
	request, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatalf("project directly staged selected fork request: %v", err)
	}
	commitCtx := runtimecorrelation.WithSourceArtifactFact(ctx, request.SourceArtifactFact)
	materialized, err := store.MaterializeRunForkForSelectedContractExecution(commitCtx, request)
	if err != nil {
		t.Fatalf("direct selected fork materialization: %v", err)
	}
	binding := materialized.SelectedContractBinding
	if materialized.ForkRunID == "" || materialized.SourceRunID != request.SourceRunID || materialized.ForkPoint.EventID != request.At ||
		binding == nil || binding.BindingID == "" || binding.ForkRunID != materialized.ForkRunID ||
		binding.SourceRunID != request.SourceRunID || binding.ForkEventID != request.At || binding.ContractSelection != request.ContractSelection {
		t.Fatalf("directly staged selected fork returned inexact binding: %+v", materialized)
	}
	stored, found, err := store.LoadRunForkSelectedContractBinding(commitCtx, materialized.ForkRunID)
	if err != nil || !found || !reflect.DeepEqual(stored, *binding) {
		t.Fatalf("directly staged selected binding readback = %+v found=%t err=%v; want %+v", stored, found, err, *binding)
	}
	return materialized
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
						root := localReadinessFixture(t, declarations, frontier)
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
						ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
						eventName := "worker.inspect"
						if frontier != "node" && !activityFrontier {
							eventName = "worker.ready"
						}
						frontierID := activateLocalReadinessFrontier(t, ctx, h, eventName)
						owner, err := flowidentity.NewRunScopedFlowInstance(catalogRuntimeRunID, flowidentity.RouteForInstancePath(path))
						if err != nil {
							t.Fatal(err)
						}
						before, found, err := h.workflow.Load(ctx, owner)
						if err != nil || !found {
							t.Fatalf("source before: %v %t", err, found)
						}
						routesBefore := catalogCommittedDeliveryRoutes(t, ctx, selected, frontierID)
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
						var acceptedAgentCalls atomic.Int32
						if frontier == "mixed" {
							options.AgentFactory = func(config models.AgentConfig) (runtimemanager.Agent, error) {
								return countedFrontierAgent{config: config, calls: &acceptedAgentCalls}, nil
							}
						}
						executionOwner := selectedContractExecutionOwnerForCatalogHarness(t, h)
						if activityFrontier && stage == "initial" {
							executionOwner = selectedContractExecutionOwnerForCatalogHarness(t, h, &activityLineageProofStore{
								SelectedContractForkLifecycle: forkStore, t: t, h: h, calls: &activityCalls,
								hostile: declarations == 0 && frontier == "activity", hostileLoop: frontier == "activity_loop" || frontier == "activity_loop_rule", rejectFinal: frontier == "activity_rejected",
							})
						}
						var result forkexecution.SelectedContractExecutionResult
						if stage == "staged" {
							result.Materialization = stageCatalogSelectedContractFork(t, ctx, forkStore, executionOwner, loader, selection, options, catalogRuntimeRunID, frontierID)
						} else {
							result, err = forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
								SourceRunID: catalogRuntimeRunID, At: frontierID, AllowSourceFreeze: true,
								Owner: executionOwner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
							})
						}
						forkRun := result.Materialization.ForkRunID
						refused := stage == "staged" && frontier != "agent"
						if stage == "staged" {
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
							t.Logf("rejected activity execution evidence: %v", err)
							if err == nil || !strings.Contains(err.Error(), "fork activity lineage") || result.ExecutedEventCount != 1 || activityCalls.Load() != 1 {
								t.Fatalf("failed final validation lost execution evidence: count=%d calls=%d err=%v", result.ExecutedEventCount, activityCalls.Load(), err)
							}
						} else if frontier == "mixed" {
							if err != nil || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 || !result.Activation.Activated || acceptedAgentCalls.Load() != int32(declarations) {
								t.Fatalf("ordinary final entry must retain accepted agent execution: count=%d calls=%d err=%v", result.ExecutedEventCount, acceptedAgentCalls.Load(), err)
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
						fenced := frontier == "activity_rejected" && stage == "initial"
						if err != nil || (fenced && found) || (!fenced && (!found || forkState.Fields["worker_id"] != "worker-001")) {
							logSelectedForkRecoveryFailure(t, ctx, h, forkRun, err)
							t.Fatalf("fork flow: %#v found=%t err=%v", forkState, found, err)
						}
						readiness, found, err := h.rt.Pipeline.LoadDynamicFlowRuntimeReadiness(ctx, forkRun, owner.Route)
						if err != nil || (fenced && found) || (!fenced && (!found || len(readiness.Plan.Agents) != declarations)) {
							t.Fatalf("complete readiness: %#v %t %v", readiness, found, err)
						}
						if fenced {
							terminal, err := selected.LoadRunLifecycleSnapshot(ctx, forkRun)
							if err != nil || terminal.Status != "cancelled" || terminal.EndedAt == nil {
								t.Fatalf("fenced frontier lost its terminal tombstone: %+v err=%v", terminal, err)
							}
						}
						observed, err := catalogRunScopedOperatorEvents(h, forkRun)
						if err != nil || (!refused && !fenced && len(observed) == 0) || ((refused || fenced) && len(observed) != 0) {
							t.Fatalf("public fork readback: %#v %v", observed, err)
						}
						if frontier == "mixed" && !refused {
							accepted := 0
							for _, event := range observed {
								for _, delivery := range event.Deliveries {
									if delivery.Route.AgentIdentity.IsZero() {
										continue
									}
									if delivery.Status != "delivered" || delivery.ClaimVersion != 1 || delivery.RetryCount != 0 || delivery.Route.AgentIdentity.RunID != forkRun {
										t.Fatalf("accepted mixed agent lost its exact child-run settlement: %+v", delivery)
									}
									accepted++
								}
							}
							if accepted != declarations || forkState.Status != "active" || !forkState.TerminatedAt.IsZero() {
								t.Fatalf("ordinary final entry retired or lost accepted agents: deliveries=%d state=%+v", accepted, forkState)
							}
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
						routesAfter := catalogCommittedDeliveryRoutes(t, ctx, selected, frontierID)
						if before.CurrentState != after.CurrentState || before.Revision != after.Revision || !reflect.DeepEqual(before.Fields, after.Fields) || !reflect.DeepEqual(routesBefore, routesAfter) {
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
	lifecycle, err := selected.LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := catalogRunScopedOperatorEvents(h, runID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal([]any{state, readiness, lifecycle, events})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func selectedForkReadinessCatalogFixture(t *testing.T, declarations int, frontier string) string {
	t.Helper()
	return canonicalrouting.CopySelectedForkReadiness(t, declarations, frontier)
}
