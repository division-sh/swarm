package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type timerDiagnosticObservationForTest struct {
	entry RuntimeLogEntry
	runID string
	err   error
}

// Observation happens only after the canonical logger has returned. A channel
// proves the recovery callback ran without replacing persistence or sleeping.
type timerDiagnosticLoggerForTest struct {
	systemNodeRuntimeLogger
	observed chan timerDiagnosticObservationForTest
}

func (l *timerDiagnosticLoggerForTest) LogRuntime(ctx context.Context, entry RuntimeLogEntry) error {
	err := l.systemNodeRuntimeLogger.LogRuntime(ctx, entry)
	l.observed <- timerDiagnosticObservationForTest{entry: entry, runID: runtimecorrelation.RunIDFromContext(ctx), err: err}
	return err
}

// This named read fault leaves all setup, list, and mutation operations with
// the exact selected owner. It fails only one persisted activation's read.
type timerDiagnosticWakeupReadFaultForTest struct {
	WorkflowTimerActivationPersistence
	mu           sync.Mutex
	activationID string
	failure      error
	enabled      bool
	failedReads  int
}

func (f *timerDiagnosticWakeupReadFaultForTest) setEnabled(enabled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enabled = enabled
}

func (f *timerDiagnosticWakeupReadFaultForTest) LoadWorkflowTimerActivation(ctx context.Context, id string) (WorkflowTimerActivation, bool, error) {
	f.mu.Lock()
	if f.enabled && id == f.activationID {
		f.failedReads++
		f.mu.Unlock()
		return WorkflowTimerActivation{}, false, f.failure
	}
	f.mu.Unlock()
	return f.WorkflowTimerActivationPersistence.LoadWorkflowTimerActivation(ctx, id)
}

func (f *timerDiagnosticWakeupReadFaultForTest) readFailures() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failedReads
}

type timerDiagnosticOccurrenceFaultForTest struct {
	WorkflowTimerOccurrenceOwner
	readFault *timerDiagnosticWakeupReadFaultForTest
	commit    bool
	calls     int
}

func (f *timerDiagnosticOccurrenceFaultForTest) CommitWorkflowTimerOccurrence(ctx context.Context, command WorkflowTimerOccurrenceCommand) (CommittedWorkflowTimerOccurrence, error) {
	if err := command.Validate(); err != nil {
		return CommittedWorkflowTimerOccurrence{}, err
	}
	f.calls++
	if !f.commit {
		// Preparation succeeded, but the occurrence did not commit. The real
		// fire owner must release preparation and attempt its existing rearm.
		f.readFault.setEnabled(true)
		return CommittedWorkflowTimerOccurrence{}, f.readFault.failure
	}
	result, err := f.WorkflowTimerOccurrenceOwner.CommitWorkflowTimerOccurrence(ctx, command)
	if result.Outcome == WorkflowTimerOccurrenceCommitted {
		// Preserve the acknowledged fire and its successor; fail only the next
		// process projection read, not the committed selected-store operation.
		f.readFault.setEnabled(true)
	}
	return result, err
}

// This bridge exposes only canonical runtime inputs and detached proof data.
// Construction and persisted logging remain in the external native-store test.
type WorkflowDiagnosticFixtureForTest struct {
	Context              context.Context
	Coordinator          *PipelineCoordinator
	VerifyFailure        func(context.Context, string, string, string, string, runtimefailures.Envelope) error
	Events               func(context.Context) []events.Event
	PublishHandler       func(context.Context, events.Event) error
	VerifyHandlerFailure func(context.Context, events.Event, string, runtimefailures.Envelope) error
}

func VerifyWorkflowHandlerFailureDiagnosticPersistsOnBothStoresForTest(t *testing.T, factory func(*testing.T, string, *runtimecontracts.WorkflowContractBundle) WorkflowDiagnosticFixtureForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: diagnostic-handler\nstages:\n  ready: {initial: true}\n  done: {terminal: true}\n",
				"entities.yaml": "test_entity: {}\n",
				"events.yaml":   "work.ready:\n",
				"nodes.yaml":    "router:\n  execution_type: system_node\n  subscribes_to: [work.ready]\n  event_handlers:\n    work.ready: {advances_to: done}\n",
			})
			f := factory(t, backend, bundle)
			ctx, pc := f.Context, f.Coordinator
			t.Cleanup(func() {
				joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := pc.StopWorkflowTimerLifecycle(joinCtx); err != nil {
					t.Errorf("join handler diagnostic lifecycle: %v", err)
				}
			})
			runID := runtimecorrelation.RunIDFromContext(ctx)
			entityID := FlowInstanceEntityID(runID)
			at := time.Now().UTC()
			instance := workflowTimerMaterializedInstance(ctx, entityID, runID, WorkflowInstance{
				WorkflowVersion: pc.SemanticSource().WorkflowVersion(), EntityType: "test_entity", CurrentState: "ready", CreatedAt: at, EnteredStageAt: at,
			})
			identity := testRunScopedWorkflowInstanceFromContext(ctx, runID)
			if result, err := pc.MaterializeInitialEntry(ctx, identity, instance, at); err != nil || result != WorkflowInitialMaterializationCreated {
				t.Fatalf("materialize canonical handler receiver: %v, %v", result, err)
			}
			before, found, err := pc.Load(ctx, identity)
			if err != nil || !found {
				t.Fatalf("load admitted handler receiver: found=%t err=%v", found, err)
			}
			node := pipelineSourceNode(t, pc.SemanticSource(), ".", "router")
			cause := errors.New("named pre-handler early-arrival cut")
			injected := fmt.Errorf("wrapped receiver refusal: %w", runtimefailures.Wrap(runtimefailures.ClassEarlyArrival,
				"diagnostic_receiver_not_ready", "receiver-proof", "prepare", nil, cause))
			calls := 0
			pc.testWorkflowNodeHandlerStartHook = func(_ context.Context, gotNode string, _ events.Event) error {
				if gotNode != node.Key() {
					return fmt.Errorf("unexpected handler node %s", gotNode)
				}
				calls++
				return injected
			}
			t.Cleanup(func() { pc.testWorkflowNodeHandlerStartHook = nil })
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "work.ready", "operator", "", []byte(`{}`), 0, runID,
				events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}), at)
			if err := f.PublishHandler(ctx, event); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("real admitted handler attempts = %d, want one", calls)
			}
			want := runtimefailures.FromError(injected, runtimeWorkflowID, "execute_handler")
			if !errors.Is(want, cause) {
				t.Fatal("canonical normalization lost the original cause")
			}
			if err := f.VerifyHandlerFailure(ctx, event, node.Key(), want.Failure); err != nil {
				t.Fatal(err)
			}
			persisted, found, err := pc.Load(ctx, identity)
			if err != nil || !found || !reflect.DeepEqual(persisted, before) {
				t.Fatalf("refused handler changed durable workflow state: %+v, found=%t err=%v", persisted, found, err)
			}
		})
	}
}

func VerifyWorkflowTimerFailureDiagnosticsPersistOnBothStoresForTest(t *testing.T, factory func(*testing.T, string, *runtimecontracts.WorkflowContractBundle) WorkflowDiagnosticFixtureForTest) {
	actions := []string{
		"workflow_timer_reconcile_failed",
		"workflow_timer_register_failed",
		"workflow_timer_restore_register_failed",
		"workflow_timer_reconcile_retry_failed",
		"workflow_timer_fire_failed",
		"workflow_timer_recurrence_reconcile_failed",
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, action := range actions {
			for _, kind := range []string{"typed_cause", "untyped"} {
				t.Run(backend+"/"+action+"/"+kind, func(t *testing.T) {
					recurring := action == "workflow_timer_recurrence_reconcile_failed"
					bundle := workflowTimerOwnerBundle(t, recurring)
					f := factory(t, backend, bundle)
					ctx, pc := f.Context, f.Coordinator
					store := pc.workflowStore
					path := runtimecorrelation.RunIDFromContext(ctx)
					entityID := FlowInstanceEntityID(path)
					lifecycle := pc.workflowTimers
					owner, ok := worklifetime.OccurrenceFromContext(ctx)
					if !ok || lifecycle == nil || f.VerifyFailure == nil {
						t.Fatal("timer proof requires selected lifecycle, diagnostic reader, and exact work owner")
					}
					lifecycle.workOwner = owner
					createdAt := canonicalWorkflowTimerTime(time.Now().UTC().Add(-2 * time.Hour))
					instance := workflowTimerMaterializedInstance(ctx, entityID, path, WorkflowInstance{
						WorkflowVersion: pc.SemanticSource().WorkflowVersion(), EntityType: "test_entity", CurrentState: "waiting",
						CreatedAt: createdAt, EnteredStageAt: createdAt,
					})
					identity := testRunScopedWorkflowInstanceFromContext(ctx, path)
					if result, err := pc.MaterializeInitialEntry(ctx, identity, instance, createdAt); err != nil || result != WorkflowInitialMaterializationCreated {
						t.Fatalf("materialize admitted timer owner: %v, %v", result, err)
					}
					activations, err := lifecycle.initialEntryTimerActivations(ctx, identity)
					if err != nil {
						t.Fatal(err)
					}
					if len(activations) != 1 {
						t.Fatalf("persisted initial activations = %+v, want one", activations)
					}
					activation := activations[0]
					wakeup, err := newWorkflowTimerWakeup(activation)
					if err != nil {
						t.Fatal(err)
					}
					// The actual scheduler admits tasks but withholds autonomous firing.
					// Each row drives the named lifecycle caller at an exact temporal cut.
					scheduler := NewSchedulerWithWorkOwner(owner)
					if err := scheduler.PrepareStartup(); err != nil {
						t.Fatal(err)
					}
					if err := lifecycle.bindScheduler(scheduler); err != nil {
						t.Fatal(err)
					}
					var stopOnce sync.Once
					stop := func() {
						t.Helper()
						stopOnce.Do(func() {
							stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							if err := pc.StopWorkflowTimerLifecycle(stopCtx); err != nil {
								t.Errorf("join diagnostic timer lifecycle: %v", err)
							}
							scheduler.Stop()
							if err := scheduler.Wait(stopCtx); err != nil {
								t.Errorf("join diagnostic timer scheduler: %v", err)
							}
						})
					}
					t.Cleanup(stop)
					if err := lifecycle.ReconcileWakeup(ctx, activation.Ref); err != nil {
						t.Fatalf("initial exact wakeup registration: %v", err)
					}
					if active, _ := workflowTimerScheduledCounts(scheduler); active != 1 {
						t.Fatalf("initial scheduled activations = %d, want one", active)
					}
					observation := &timerDiagnosticLoggerForTest{systemNodeRuntimeLogger: lifecycle.logger, observed: make(chan timerDiagnosticObservationForTest, 16)}
					lifecycle.logger = observation
					cause := errors.New("injected timer read boundary")
					injected := error(cause)
					if kind == "typed_cause" {
						injected = fmt.Errorf("wrapped timer dependency: %w", runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable,
							"timer_diagnostic_selected_dependency", "selected-timer-proof", "activation.read", map[string]any{"activation_id": activation.Ref.ActivationID}, cause))
					}
					readOwner := store.timerActivations
					fault := &timerDiagnosticWakeupReadFaultForTest{WorkflowTimerActivationPersistence: readOwner, activationID: activation.Ref.ActivationID, failure: injected}
					store.timerActivations = fault
					occurrenceOwner := store.timerOccurrences
					occurrenceFault := &timerDiagnosticOccurrenceFaultForTest{WorkflowTimerOccurrenceOwner: occurrenceOwner, readFault: fault, commit: recurring}
					switch action {
					case "workflow_timer_reconcile_failed":
						fault.setEnabled(true)
						if err := pc.ArmInitialEntryTimers(ctx, identity); err != nil {
							t.Fatalf("existing reconciliation must queue recovery, not reject arming: %v", err)
						}
					case "workflow_timer_register_failed":
						store.timerOccurrences = occurrenceFault
						outcome, next, err := lifecycle.fireWakeup(ctx, wakeup)
						if outcome != WorkflowTimerFireRetry || next || !errors.Is(err, injected) || !errors.Is(err, cause) {
							t.Fatalf("uncommitted fire/rearm outcome = %s/%v, %v", outcome, next, err)
						}
						if occurrenceFault.calls != 1 {
							t.Fatalf("occurrence attempts = %d, want one", occurrenceFault.calls)
						}
					case "workflow_timer_restore_register_failed":
						fault.setEnabled(true)
						if err := pc.RestoreWorkflowTimers(ctx); err != nil {
							t.Fatalf("existing restore must queue owned recovery: %v", err)
						}
					case "workflow_timer_reconcile_retry_failed":
						fault.setEnabled(true)
						queued, err := lifecycle.ReconcileWakeupWithRecovery(ctx, activation.Ref)
						if !queued || !errors.Is(err, injected) || !errors.Is(err, cause) {
							t.Fatalf("recovery admission = %v, %v", queued, err)
						}
					case "workflow_timer_fire_failed":
						fault.setEnabled(true)
						lifecycle.handleWakeup(ctx, wakeup)
					case "workflow_timer_recurrence_reconcile_failed":
						store.timerOccurrences = occurrenceFault
						lifecycle.handleWakeup(ctx, wakeup)
						if occurrenceFault.calls != 1 {
							t.Fatalf("recurrence occurrence attempts = %d, want one", occurrenceFault.calls)
						}
					}
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					var observed timerDiagnosticObservationForTest
				waitForCaller:
					for {
						select {
						case observed = <-observation.observed:
							if observed.err != nil {
								t.Fatalf("actual diagnostic persistence failed for %s: %v", observed.entry.Action, observed.err)
							}
							if observed.entry.Action == action {
								break waitForCaller
							}
						case <-waitCtx.Done():
							t.Fatalf("named lifecycle caller %s did not persist its diagnostic: %v", action, waitCtx.Err())
						}
					}
					lifecycle.recoveryMu.Lock()
					ownedRecovery := len(lifecycle.recovering)
					lifecycle.recoveryMu.Unlock()
					if ownedRecovery != 1 || !lifecycle.startWakeupRecovery(activation.Ref) {
						t.Fatalf("existing recovery did not coalesce one exact activation: %d", ownedRecovery)
					}
					lifecycle.recoveryMu.Lock()
					coalescedRecovery := len(lifecycle.recovering)
					lifecycle.recoveryMu.Unlock()
					if coalescedRecovery != ownedRecovery {
						t.Fatalf("duplicate admission changed recovery count: %d -> %d", ownedRecovery, coalescedRecovery)
					}
					stop()
					lifecycle.recoveryMu.Lock()
					pendingRecovery := len(lifecycle.recovering)
					lifecycle.recoveryMu.Unlock()
					if pendingRecovery != 0 {
						t.Fatalf("joined lifecycle retained %d recovery operations", pendingRecovery)
					}
					if active, draining := workflowTimerScheduledCounts(scheduler); active != 0 || draining != 0 {
						t.Fatalf("joined scheduler retained %d active/%d draining tasks", active, draining)
					}
					want := runtimefailures.FromError(injected, runtimeWorkflowID, action)
					if kind == "typed_cause" && (want.Failure.Class != runtimefailures.ClassDependencyUnavailable || want.Failure.Detail.Code != "timer_diagnostic_selected_dependency") {
						t.Fatalf("typed injection was not preserved: %+v", want.Failure)
					}
					if kind == "untyped" && (want.Failure.Class != runtimefailures.ClassInternalFailure || want.Failure.Detail.Code != "unclassified_runtime_error") {
						t.Fatalf("untyped injection did not consume canonical normalization: %+v", want.Failure)
					}
					if !errors.Is(want, cause) || observed.entry.Failure == nil || !reflect.DeepEqual(*observed.entry.Failure, want.Failure) {
						t.Fatalf("diagnostic lost failure normalization/causal chain: %+v, want %+v", observed.entry.Failure, want.Failure)
					}
					wantRunID := runtimecorrelation.RunIDFromContext(ctx)
					if action == "workflow_timer_reconcile_retry_failed" {
						// Existing owned recovery uses a process-scoped context. Do not
						// invent run correlation or replace its cancellation policy.
						wantRunID = ""
					}
					if observed.runID != wantRunID {
						t.Fatalf("diagnostic context run = %q, want existing caller scope %q", observed.runID, wantRunID)
					}
					if err := f.VerifyFailure(ctx, action, wantRunID, activation.Ref.ActivationID, activation.Ref.DeclarationKey, want.Failure); err != nil {
						t.Fatal(err)
					}
					minimumReads := 1
					if action == "workflow_timer_reconcile_failed" || action == "workflow_timer_register_failed" || action == "workflow_timer_restore_register_failed" {
						minimumReads = 3
					} else if action == "workflow_timer_reconcile_retry_failed" {
						minimumReads = 2
					}
					if fault.readFailures() < minimumReads {
						t.Fatalf("named read failures = %d, want at least %d through existing recovery", fault.readFailures(), minimumReads)
					}
					store.timerActivations, store.timerOccurrences = readOwner, occurrenceOwner
					persisted, found, err := store.loadPersistedWorkflowTimerActivation(ctx, activation.Ref.ActivationID)
					if err != nil || !found {
						t.Fatalf("selected activation readback: %v, %v", found, err)
					}
					if recurring {
						if persisted.Ref != activation.Ref || persisted.Status != workflowTimerStatusActive || persisted.FiredAt.IsZero() || !persisted.FireAt.After(activation.FireAt) || len(f.Events(ctx)) != 1 {
							t.Fatalf("reconcile failure lost acknowledged recurrence: %+v, publications=%d", persisted, len(f.Events(ctx)))
						}
						published := f.Events(ctx)[0]
						if published.ID() != timeridentity.WorkflowTimerOccurrenceEventID(activation.occurrence()) || published.RunID() != runtimecorrelation.RunIDFromContext(ctx) {
							t.Fatalf("committed occurrence identity changed: %+v", published)
						}
					} else if !reflect.DeepEqual(persisted, activation) || len(f.Events(ctx)) != 0 {
						t.Fatalf("failed operation changed durable activation/publication: before=%+v after=%+v publications=%d", activation, persisted, len(f.Events(ctx)))
					}
					before := len(observation.observed)
					lifecycle.logFailure(ctx, action, activation.Ref, nil)
					if len(observation.observed) != before {
						t.Fatal("nil failure produced a diagnostic")
					}
				})
			}
		}
	}
}
