package runforkexecution

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestIssue642RetainedForkCrashRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"retained_after_activation", "retained_event_committed"} {
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
				original, found, err := operations.LoadForkOperation(context.Background(), "retained-crash", "fixed-cut", "retained-crash-transport")
				if err != nil || !found || original.Status != runfork.ForkOperationActivated || original.ForkRunID != checkpoint.ForkRun || original.Result == nil {
					t.Fatalf("crash lost permanent activation: %+v found=%v error=%v", original, found, err)
				}
				before, err := storetest.ReadSelectedForkSourceDomain(context.Background(), selected, checkpoint.SourceRun)
				if err != nil {
					t.Fatal(err)
				}
				ctx := runForkTestContext(t)
				capability := selectedContractTestProcessCapability(t, ctx, selected)
				owner := construct()
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
				recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
					SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: capability},
				})
				if err != nil || len(recovered) != 1 || recovered[0].RunID != checkpoint.ForkRun || recovered[0].Disposition != runfork.SelectedForkRecoveryResume {
					t.Fatalf("retained recovery failed: %+v %v", recovered, err)
				}
				wait, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				inputID := activityidentity.ForkLineageEventID(checkpoint.ForkRun, original.Result.ForkEventID)
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
					if cardinality == 1 && outputs == 1 && pending == 0 && settlement.Total == 1 && settlement.Delivered == 1 && found && header.CurrentState == "done" && availability.Status == "completed" && retained == 0 && leases == baselineLeases {
						break
					}
					select {
					case <-wait.Done():
						t.Fatalf("recovery did not finish the original receiver: input=%d output=%d pipeline=%d deliveries=%+v header=%+v run=%s retained=%d leases=%d baseline=%d", cardinality, outputs, pending, settlement, header, availability.Status, retained, leases, baselineLeases)
					case <-time.After(10 * time.Millisecond):
					}
				}
				input := storetest.LoadCanonicalEventRecord(t, wait, selected, inputID)
				lineage, present := input.SelectedForkLineage()
				if !present || input.RunID() != checkpoint.ForkRun || lineage.SourceRunID() != checkpoint.SourceRun || lineage.SourceEventID() != original.Result.ForkEventID {
					t.Fatal("recovered input lost exact fixed-cut lineage")
				}
				retry, found, err := operations.LoadForkOperation(wait, original.Request.Actor, original.Request.IdempotencyKey, original.Request.TransportHash)
				if err != nil || !found || !reflect.DeepEqual(original, retry) {
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
