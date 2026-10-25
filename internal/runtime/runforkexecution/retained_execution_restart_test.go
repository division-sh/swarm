package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

// Fail the first restore read after the exact ready attempt is verified for adoption.
type selectedRecoveryRestoreFailure struct {
	pipeline.WorkflowPersistenceOwner
	ready sync.Map
	armed atomic.Bool
	fired atomic.Bool
	seen  chan struct{}
	cause error
}

func (p *selectedRecoveryRestoreFailure) VerifyDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	if err := p.WorkflowPersistenceOwner.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt); err != nil {
		return err
	}
	if _, ready := p.ready.Load(attempt); ready {
		p.armed.Store(true)
	}
	return nil
}

func (p *selectedRecoveryRestoreFailure) AdvanceFlowAttachment(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt, previous pipeline.FlowAttachmentPhase, at time.Time) (pipeline.FlowAttachmentAdvanceResult, error) {
	result, err := p.WorkflowPersistenceOwner.AdvanceFlowAttachment(ctx, attempt, previous, at)
	if result.Admitted() && result.Phase == pipeline.FlowAttachmentReady {
		p.ready.Store(attempt, struct{}{})
	}
	return result, err
}

func (p *selectedRecoveryRestoreFailure) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route flowidentity.Route) (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
	if p.armed.Swap(false) && p.fired.CompareAndSwap(false, true) {
		close(p.seen)
		return pipeline.DynamicFlowRuntimeReadiness{}, false, p.cause
	}
	return p.WorkflowPersistenceOwner.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
}

func TestIssue642RetainedForkCrashRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"retained_after_activation", "retained_event_committed", "restore_failure_after_adoption"} {
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
				checkpointCut := cut
				if cut == "restore_failure_after_adoption" {
					t.Setenv(publishedJoinFlowEnv, "orders")
					checkpointCut = "retained_join_after_activation"
				}
				checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, checkpointCut)
				operations := selected.(interface {
					LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
				})
				original, found, err := operations.LoadForkOperation(context.Background(), "retained-crash", "fixed-cut", "retained-crash-transport")
				beforeACK := cut == "retained_event_committed"
				expectedState := runfork.ForkOperationActivated
				if beforeACK {
					expectedState = runfork.ForkOperationMaterialized
				}
				if err != nil || !found || original.Status != expectedState || original.ForkRunID != checkpoint.ForkRun || (original.Result == nil) != beforeACK || original.Failure != nil {
					t.Fatalf("crash lost permanent activation: %+v found=%v error=%v", original, found, err)
				}
				before, err := storetest.ReadSelectedForkSourceDomain(context.Background(), selected, checkpoint.SourceRun)
				if err != nil {
					t.Fatal(err)
				}
				ctx := runForkTestContext(t)
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				owner := construct()
				var restoreFailure *selectedRecoveryRestoreFailure
				if cut == "restore_failure_after_adoption" {
					restoreFailure = &selectedRecoveryRestoreFailure{WorkflowPersistenceOwner: selected.(pipeline.WorkflowPersistenceOwner), seen: make(chan struct{}), cause: errors.New("recovery restoration read failed after activation adoption")}
					owner.ports.workflow = pipeline.NewWorkflowPersistence(restoreFailure)
				}
				process, _ := worklifetime.ProcessFromContext(ctx)
				if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
					t.Fatal(err)
				}
				baselineLeases := process.ActiveCount()
				t.Cleanup(func() {
					if err := owner.RetireSelectedContexts(context.Background()); err != nil {
						t.Error(err)
					}
				})
				loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
				binding, err := owner.ports.fork.RequireRunForkSelectedContractBinding(ctx, checkpoint.ForkRun)
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, SelectedContractSourceLoadRequest{SourceRunID: checkpoint.ForkRun, Selection: binding.ContractSelection})
				if err != nil {
					t.Fatal(err)
				}
				var ordinary *rootruntime.Runtime
				var contexts *rootruntime.RuntimeContextManager
				if restoreFailure == nil {
					ordinary, contexts = ordinaryForkRuntimeFixture(t, ctx, selected, owner, loaded, executionposture.MockOnly)
				}
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
					SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
					LiveContexts: contexts, LiveRuns: selected.(runlifecycle.LiveRunReader),
					AcquireLiveRuntime: func(ctx context.Context, hash string) (*rootruntime.RuntimeContextUse, error) {
						if contexts == nil {
							return nil, errors.New("control-only recovery has no executable live target")
						}
						use, _, err := contexts.AcquireBundleHash(ctx, hash)
						return use, err
					},
				})
				expectedRecovery := runfork.SelectedForkRecoveryControlOnly
				if beforeACK {
					expectedRecovery = runfork.SelectedForkRecoveryResume
				}
				if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != expectedRecovery {
					t.Fatalf("retained recovery failed: %+v %v", recovered, err)
				}
				wait, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if restoreFailure != nil {
					var startErr error
					ordinary, _, startErr = nodeReplyRuntime(t, ctx, selected, owner, loaded, executionposture.MockOnly, true)
					if startErr != nil && !errors.Is(startErr, restoreFailure.cause) {
						t.Fatalf("ordinary recovery returned an independent startup error: %v", startErr)
					}
					select {
					case <-restoreFailure.seen:
					case <-wait.Done():
						t.Fatal("recovery did not reach the post-adoption restoration read")
					}
					if err := owner.RetireSelectedContexts(wait); err != nil {
						t.Fatalf("join exact recovery cleanup after adoption: %v", err)
					}
					if err := ordinary.Shutdown(); err != nil {
						t.Fatalf("join ordinary recovery after failed restoration: %v", err)
					}
					if process.ActiveCount() != baselineLeases {
						t.Fatalf("post-adoption failure retained execution leases: got=%d want=%d", process.ActiveCount(), baselineLeases)
					}
					retry, found, err := operations.LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
					if err != nil || !found || !reflect.DeepEqual(original, retry) {
						t.Fatalf("restoration failure changed the original acknowledgment: %+v %v", retry, err)
					}
					after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, checkpoint.SourceRun)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("restoration failure changed source business state: %v", err)
					}
					return
				}
				ack, found, err := operations.LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
				if err != nil || !found || ack.Status != runfork.ForkOperationActivated || ack.Result == nil || ack.ForkRunID != original.ForkRunID || ack.BindingID != original.BindingID || !reflect.DeepEqual(ack.Request, original.Request) || (!beforeACK && !reflect.DeepEqual(ack, original)) {
					t.Fatalf("recovery did not preserve its exact operation and permanent ACK: %+v err=%v", ack, err)
				}
				inputID := activityidentity.ForkLineageEventID(checkpoint.ForkRun, ack.Result.ForkEventID)
				childOwner, err := flowidentity.NewRunScopedFlowInstance(checkpoint.ForkRun, flowidentity.Route{ScopeKey: ".", InstanceID: checkpoint.ForkRun, InstancePath: checkpoint.ForkRun})
				if err != nil {
					t.Fatal(err)
				}
				for {
					cardinality, err := storetest.ReadLifecycleEventCardinality(wait, selected, checkpoint.ForkRun, "item.received")
					if err != nil {
						t.Fatal(err)
					}
					if cardinality > 1 {
						t.Fatal("recovery duplicated the committed selected input")
					}
					outputs, err := storetest.ReadLifecycleEventCardinality(wait, selected, checkpoint.ForkRun, "item.processed")
					if err != nil || outputs > 1 {
						t.Fatalf("recovered receiver output count=%d: %v", outputs, err)
					}
					pending, err := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, checkpoint.ForkRun)
					if err != nil {
						t.Fatal(err)
					}
					settlement, err := owner.ports.busDurable.DeliveryLifecycle.SummarizeRun(wait, checkpoint.ForkRun)
					if err != nil {
						t.Fatal(err)
					}
					header, found, err := owner.ports.workflow.LoadWorkflowInstance(wait, childOwner)
					if err != nil {
						t.Fatal(err)
					}
					availability, err := owner.ports.fork.LoadRunBundleAvailability(wait, checkpoint.ForkRun)
					if err != nil {
						t.Fatal(err)
					}
					owner.ports.contexts.mu.Lock()
					retained := len(owner.ports.contexts.entries)
					owner.ports.contexts.mu.Unlock()
					leases := process.ActiveCount()
					if cardinality == 1 && outputs == 1 && pending == 0 && settlement.Total == 1 && settlement.Delivered == 1 && found && header.CurrentState == "done" && availability.Status == "live" && retained == 0 {
						break
					}
					select {
					case <-wait.Done():
						t.Fatalf("recovery did not finish the original receiver: input=%d output=%d pipeline=%d deliveries=%+v header=%+v run=%s retained=%d leases=%d baseline=%d", cardinality, outputs, pending, settlement, header, availability.Status, retained, leases, baselineLeases)
					case <-time.After(10 * time.Millisecond):
					}
				}
				if err := ordinary.Shutdown(); err != nil || process.ActiveCount() != baselineLeases {
					t.Fatalf("ordinary recovery retained its resources: work=%d baseline=%d err=%v", process.ActiveCount(), baselineLeases, err)
				}
				input := storetest.LoadCanonicalEventRecord(t, wait, selected, inputID)
				lineage, present := input.SelectedForkLineage()
				if !present || input.RunID() != checkpoint.ForkRun || lineage.SourceRunID() != checkpoint.SourceRun || lineage.SourceEventID() != ack.Result.ForkEventID {
					t.Fatal("recovered input lost exact fixed-cut lineage")
				}
				retry, found, err := operations.LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
				if err != nil || !found || !reflect.DeepEqual(ack, retry) {
					t.Fatalf("recovery changed the original acknowledgment: %+v %v", retry, err)
				}
				after, err := storetest.ReadSelectedForkSourceDomain(wait, selected, checkpoint.SourceRun)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("child recovery changed source business state: %v", err)
				}
			})
		}
	}
}
