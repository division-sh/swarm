package runforkexecution

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

type issue642TimerCrashPersistence struct {
	pipeline.WorkflowPersistenceOwner
	checkpoint func(string)
}

func (p issue642TimerCrashPersistence) CommitWorkflowTimerOccurrence(ctx context.Context, command pipeline.WorkflowTimerOccurrenceCommand) (pipeline.CommittedWorkflowTimerOccurrence, error) {
	result, err := p.WorkflowPersistenceOwner.CommitWorkflowTimerOccurrence(ctx, command)
	if result.Outcome == pipeline.WorkflowTimerOccurrenceCommitted {
		// The native timer and publication transaction has acknowledged; no
		// postcommit dispatch or graceful cleanup runs before the parent kills us.
		p.checkpoint(command.Activation.RunID)
	}
	return result, err
}

func newIssue642TimerCrashPersistence(t *testing.T, selected any, checkpoint func(string)) pipeline.WorkflowPersistence {
	t.Helper()
	owner, ok := selected.(pipeline.WorkflowPersistenceOwner)
	if !ok {
		t.Fatalf("timer crash requires canonical workflow owner, got %T", selected)
	}
	return pipeline.NewWorkflowPersistence(issue642TimerCrashPersistence{owner, checkpoint})
}

func issue642TimerCrashFixture(t *testing.T, repo string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "tests/tier5-flow-lifecycle/test-timer-fire"))); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"nodes.yaml": `test-node:
  execution_type: system_node
  subscribes_to: [timer.check]
  timers:
    - id: check_timer
      event: timer.check
      delay: 1s
      start_on: state:waiting
  event_handlers:
    timer.check:
      advances_to: checked
`,
		"events.yaml": "timer.scheduled:\ntimer.check:\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// This prepares exact component construction through the normal lifecycle
// planner. It is a native crash proof, not a public source-creation acceptance.
func seedIssue642TimerCrashSource(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, loaded LoadedSelectedContractSource, runID string) string {
	t.Helper()
	ctx = effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact), executionmode.Mock)
	at := runlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-time.Minute))
	bundle, found := semanticview.Bundle(loaded.Source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("timer crash source requires its immutable artifact")
	}
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{
		RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact,
		BundleHash: loaded.SourceArtifactFact.BundleHash(), StartedAt: at,
	})
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "timer.scheduled", "source-runtime", "", []byte(`{}`), 0, runID,
		events.EventEnvelope{}, events.NoRoutingSource(), at, executionmode.Mock)
	identity := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
	command := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, identity)
	work, _ := worklifetime.OccurrenceFromContext(ctx)
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runForkTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	sourceBus, err := bus.NewEventBusWithOptions(owner.ports.events, bus.EventBusOptions{
		ExecutionPosture: executionposture.MockOnly, WorkOwner: work, PipelineObligations: owner.ports.pipelineObligations,
		ContractBundle: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact,
		RuntimeInstanceID: runForkTestRuntimeInstanceID, DeliveryAuthority: authority, ReceiverExecution: eventreceiver.NormalExecution(),
		Durable: owner.ports.busDurable,
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(sourceBus, pipeline.PipelineCoordinatorOptions{
		Module: selectedContractWorkflowModule{source: loaded.Source}, Persistence: owner.ports.workflow,
		SourceArtifactFact: loaded.SourceArtifactFact, ExecutionPosture: executionposture.MockOnly,
		ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
		RunLifecycle: selected.(runlifecycle.OperationOwner), PipelineObligations: owner.ports.pipelineObligations,
		DeliveryStore: owner.ports.busDurable.DeliveryLifecycle, DeadLetters: selected.(deadletters.AcknowledgedRecorder),
		DecisionCards: owner.ports.decisionCards, ProposedEffects: owner.ports.proposedEffects, HumanTasks: owner.ports.humanTasks,
		DecisionCardDraftExpiry: owner.ports.decisionCardDraftExpiry, HumanTaskExpiry: owner.ports.humanTaskExpiry,
		DeliveryRuntime: sourceBus,
	})
	if coordinator == nil {
		t.Fatal("source timer lifecycle planner was not admitted")
	}
	instance, lifecycle, err := coordinator.PrepareInitialEntryLifecycle(ctx, flowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()}, command.Plan.Instance, at)
	if err != nil || len(lifecycle.Timers) != 1 {
		t.Fatalf("canonical initial timer preparation: timers=%d err=%v", len(lifecycle.Timers), err)
	}
	command, err = flowactivationfixture.Command(ctx, instance, lifecycle, at)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Created || !committed.Acknowledged {
		t.Fatalf("source timer constructor: %+v err=%v", committed, err)
	}
	// A later real publication selects the timer-bearing construction, rather
	// than pretending the creating ingress already included later effects.
	marker := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(uuid.NewString(), "timer.scheduled", "source-runtime", "", []byte(`{}`), 0, runID,
		events.EventEnvelope{}, events.NoRoutingSource(), at.Add(time.Second), executionmode.Mock)
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, marker, nil, pipelineobligation.ScopeSubscribed)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
	return marker.ID()
}

func TestIssue642TimerForkCrashRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"retained_timer_after_activation", "retained_timer_occurrence_committed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				var selected startupownership.Store
				var construct func() SelectedContractExecutionOwner
				var dsn string
				if backend == "sqlite" {
					s := storetest.StartSQLiteRuntimeStore(t)
					selected, dsn = s, s.Path()
					construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
				} else {
					dsn = testutil.StartPostgresDSN(t)
					s, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
					selected = s
					construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
				}
				checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, cut)
				operations := selected.(interface {
					LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
				})
				original, found, err := operations.LoadForkOperation(t.Context(), "retained-crash", "fixed-cut", "retained-crash-transport")
				if err != nil || !found || original.Status != runfork.ForkOperationActivated || original.ForkRunID != checkpoint.ForkRun || original.Result == nil {
					t.Fatalf("timer crash lost permanent activation: %+v found=%v err=%v", original, found, err)
				}
				owner := construct()
				plan, err := owner.ports.fork.PlanRunFork(t.Context(), runfork.RunForkPlanRequest{SourceRunID: checkpoint.SourceRun, At: original.Request.ForkEventID})
				if err != nil || len(plan.WorkflowTimers) != 1 || plan.WorkflowTimers[0].Status != "active" {
					t.Fatalf("crash source cut lost its exact arm: timers=%+v err=%v", plan.WorkflowTimers, err)
				}
				if original.Request.ResolvedPoint == nil || plan.ForkPoint != *original.Request.ResolvedPoint {
					t.Fatalf("permanent timer cut differs from read-only source replan: plan=%#v recorded=%#v", plan.ForkPoint, original.Request.ResolvedPoint)
				}
				before, err := storetest.ReadSelectedForkSourceDomain(t.Context(), selected, checkpoint.SourceRun)
				if err != nil {
					t.Fatal(err)
				}
				ctx := runForkTestContext(t)
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				process, _ := worklifetime.ProcessFromContext(ctx)
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				baseline := process.ActiveCount()
				t.Cleanup(func() {
					if err := owner.RetireSelectedContexts(context.Background()); err != nil {
						t.Error(err)
					}
				})
				loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
					SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
				})
				if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("timer continuation recovery: %+v err=%v", recovered, err)
				}
				wait, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				root := flowidentity.RunScopedFlowInstance{RunID: checkpoint.ForkRun, Route: flowidentity.StoredRoute(".", checkpoint.ForkRun, checkpoint.ForkRun)}
				for {
					header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait, root)
					if err != nil {
						t.Fatal(err)
					}
					availability, err := owner.ports.fork.LoadRunBundleAvailability(wait, checkpoint.ForkRun)
					if err != nil {
						t.Fatal(err)
					}
					pending, err := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, checkpoint.ForkRun)
					if err != nil {
						t.Fatal(err)
					}
					owner.ports.contexts.mu.Lock()
					retained := len(owner.ports.contexts.entries)
					owner.ports.contexts.mu.Unlock()
					if found && header.CurrentState == "checked" && availability.Status == "completed" && pending == 0 && retained == 0 && process.ActiveCount() == baseline {
						break
					}
					select {
					case <-wait.Done():
						t.Fatalf("timer recovery did not complete: header=%+v run=%s pending=%d retained=%d leases=%d baseline=%d", header, availability.Status, pending, retained, process.ActiveCount(), baseline)
					case <-time.After(10 * time.Millisecond):
					}
				}
				reader := selected.(interface {
					LoadWorkflowTimerActivation(context.Context, string) (pipeline.WorkflowTimerActivation, bool, error)
					LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
				})
				header, err := reader.LoadRunHeader(wait, checkpoint.ForkRun)
				if err != nil || header.Status != "completed" || header.EndedAt == nil || header.Failure != nil {
					t.Fatalf("timer receiver completion is not successful run completion: %+v err=%v", header, err)
				}
				source, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(plan.WorkflowTimers[0])
				if err != nil {
					t.Fatal(err)
				}
				projection, err := runfork.ProjectWorkflowTimerRecord(source.PersistenceRecord(), source.Ref, checkpoint.ForkRun, plan.ForkPoint, nil, runlifecycle.CanonicalTimestamp(header.StartedAt))
				if err != nil {
					t.Fatal(err)
				}
				actual, found, err := reader.LoadWorkflowTimerActivation(wait, projection.ActivationID)
				if err != nil || !found || actual.Status != "fired" || actual.FiredAt.IsZero() || actual.FiredAt.Before(source.FireAt) {
					t.Fatalf("recovered timer lost its exact terminal occurrence: %+v found=%v err=%v", actual, found, err)
				}
				projection.Status, projection.FiredAt = "fired", actual.FiredAt
				expected, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(projection)
				if err != nil || !reflect.DeepEqual(expected.Canonical(), actual.Canonical()) {
					t.Fatalf("timer recovery changed immutable arm/due/lineage: expected=%+v actual=%+v err=%v", expected, actual, err)
				}
				occurrenceID := timeridentity.WorkflowTimerOccurrenceEventID(actual.Occurrence())
				event := storetest.LoadCanonicalEventRecord(t, wait, selected, occurrenceID)
				occurrence, valid := timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.TaskID())
				if !valid || occurrence != actual.Occurrence() || event.RunID() != checkpoint.ForkRun || event.Type() != "timer.check" ||
					event.SourceAgent() != "runtime.workflow_timer" || !reflect.DeepEqual(event.Payload(), actual.Payload) || event.RoutingSource() != actual.RoutingSource {
					t.Fatalf("recovered publication lost exact timer cause: event=%+v timer=%+v", event, actual)
				}
				if count, err := storetest.ReadLifecycleEventCardinality(wait, selected, checkpoint.ForkRun, "timer.check"); err != nil || count != 1 {
					t.Fatalf("timer recovery duplicated publication: count=%d err=%v", count, err)
				}
				if receipt := storetest.ObservePipelineReceipt(t, wait, selected, event.ID()); receipt.Count != 1 || receipt.Outcome != "success" {
					t.Fatalf("timer receiver did not settle exactly once: %+v", receipt)
				}
				settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, checkpoint.ForkRun)
				if err != nil || settlement.Total != 1 || settlement.Delivered != 1 {
					t.Fatalf("timer recovery did not settle exactly its receiver: %+v err=%v", settlement, err)
				}
				if storage := storetest.ObserveWorkflowTimerReplayStorage(t, wait, selected, checkpoint.ForkRun, checkpoint.ForkRun); storage.Timers != 1 || storage.ActiveTimers != 0 {
					t.Fatalf("timer recovery rearmed or retained work: %+v", storage)
				}
				unchanged, found, err := reader.LoadWorkflowTimerActivation(wait, source.Ref.ActivationID)
				if err != nil || !found || !reflect.DeepEqual(source.Canonical(), unchanged.Canonical()) {
					t.Fatalf("recovery changed the source's armed obligation: %+v found=%v err=%v", unchanged, found, err)
				}
				retry, found, err := operations.LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
				if err != nil || !found || !reflect.DeepEqual(original, retry) {
					t.Fatalf("timer recovery changed acknowledgment: %+v err=%v", retry, err)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, checkpoint.SourceRun)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("timer recovery changed source business facts: %v", err)
				}
				stateBefore, err := storetest.ReadSelectedExecutionStorage(wait, selected, checkpoint.ForkRun)
				if err != nil {
					t.Fatal(err)
				}
				terminalOwner := construct()
				if err := terminalOwner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				terminal, err := terminalOwner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{})
				if err != nil || len(terminal) != 1 || terminal[0].RunID != checkpoint.ForkRun || terminal[0].Disposition != runfork.SelectedForkRecoveryTerminal {
					t.Fatalf("completed timer child reopened execution: %+v err=%v", terminal, err)
				}
				stateAfter, err := storetest.ReadSelectedExecutionStorage(wait, selected, checkpoint.ForkRun)
				if err != nil || !reflect.DeepEqual(stateBefore, stateAfter) || process.ActiveCount() != baseline {
					t.Fatalf("terminal timer recovery reissued work or retained leases: before=%+v after=%+v err=%v", stateBefore, stateAfter, err)
				}
				t.Logf("ISSUE642_TIMER_SIGKILL_RECOVERED backend=%s cut=%s child=%s due=%s occurrence=%s completed=%s leases=%d", backend, cut, checkpoint.ForkRun, actual.FireAt.Format(time.RFC3339Nano), event.ID(), header.EndedAt, process.ActiveCount())
			})
		}
	}
}
