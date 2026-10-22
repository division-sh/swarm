package runforkexecution

import (
	"context"
	"reflect"
	"testing"
	"time"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func stageContextOnlyDeadlineDescendant(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner,
	loader SelectedContractSourceLoader, selection runfork.RunForkContractSelection, options SelectedContractAgentRuntimeOptions,
	bundleHash string, parent runfork.RunForkMaterialization) (runfork.RunForkMaterialization, *runfork.ForkOperationRequest) {
	t.Helper()
	rows, err := storetest.ReadReplyContextRecords(ctx, selected, parent.ForkRunID)
	if err != nil || len(rows) != 2 {
		t.Fatalf("context-only predecessor lacks two open levels: %+v error=%v", rows, err)
	}
	for _, row := range rows {
		_, found, err := selected.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, row.RequestEventID)
		if err != nil || found || row.State != replycontext.StateOpen {
			t.Fatalf("context-only cut invented a logical request or closed a level: row=%+v found=%t error=%v", row, found, err)
		}
	}
	before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, parent.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	operation := &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "context-only-deadline", IdempotencyKey: uuid.NewString(),
		TransportHash: "context-only-deadline", SourceRunID: parent.ForkRunID, AtStart: true,
		TargetBundleHash: bundleHash, AllowSourceFreeze: true, ContractSelection: selection}
	prepared, err := owner.Prepare(ctx, SelectedContractExecutionRequest{SourceRunID: parent.ForkRunID, AtStart: true,
		AllowSourceFreeze: true, ForkOperation: operation, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	}()
	request, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	request.ForkOperation = operation
	child, err := owner.ports.fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
	if err != nil || child.SourceRunID != parent.ForkRunID || child.ForkPoint.Kind != runfork.RunForkPointRunStart {
		t.Fatalf("stage context-only original-deadline descendant: %+v error=%v", child, err)
	}
	after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, parent.ForkRunID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("context-only materialization changed its immediate source: error=%v", err)
	}
	return child, operation
}

// Reconstruct both the native store and process owner while the child is still
// materialized. Recovery, not an observation receipt, activates its execution.
func restartSelectedDeadlineChild(t *testing.T, ctx context.Context, source *rootruntime.Runtime,
	selected startupownership.Store, predecessor SelectedContractExecutionOwner, reopen func() startupownership.Store,
	loaded LoadedSelectedContractSource, staged runfork.RunForkMaterialization, rows []replycontext.Record,
	operation *runfork.ForkOperationRequest, hold *nestedDeadlineWorkerHold, contexts chan context.Context, sourceRun string,
) (startupownership.Store, SelectedContractExecutionOwner, context.Context) {
	t.Helper()
	ops := selected.(interface {
		LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
	})
	before, found, err := ops.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
	if err != nil || !found || before.Status != runfork.ForkOperationMaterialized || before.ForkRunID != staged.ForkRunID || before.Result != nil {
		t.Fatalf("staged deadline operation lost its exact cut: %+v found=%t error=%v", before, found, err)
	}
	arms := map[string]joinruntime.Activation{}
	for _, row := range rows {
		arms[row.ID] = nestedDeadlineArm(t, ctx, predecessor, row)
	}
	if err := source.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := predecessor.RetireSelectedContexts(ctx); err != nil {
		t.Fatal(err)
	}
	capability := predecessor.ports.contexts.capability
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	oldProcess, _ := worklifetime.ProcessFromContext(ctx)
	fixture, _ := runForkTestWorkFixtures.Load(t)
	oldOccurrence := fixture.(*runForkTestWorkFixture).runtime
	cleanup, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCleanup()
	if _, err := oldOccurrence.RetireAndWait(cleanup); err != nil {
		t.Fatal(err)
	}
	oldProcess.Retire()
	if _, err := oldProcess.Join(cleanup); err != nil || oldProcess.ActiveCount() != 0 {
		t.Fatalf("deadline predecessor retained process work: count=%d error=%v", oldProcess.ActiveCount(), err)
	}
	if err := selected.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	selected = reopen()
	process := worklifetime.NewProcess()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "nested-deadline-successor", BundleHash: loaded.SourceArtifactFact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := occurrence.RetireAndWait(cleanup); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(cleanup); err != nil {
			t.Error(err)
		}
	})
	ctx = worklifetime.WithOccurrence(worklifetime.WithProcess(context.Background(), process), occurrence)
	ctx = correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
	newCapability := selectedContractTestProcessCapability(t, ctx, selected)
	if capability.ProveCurrent(ctx) == nil || newCapability == capability {
		t.Fatal("deadline restart retained predecessor process authority")
	}
	var owner SelectedContractExecutionOwner
	switch native := selected.(type) {
	case *store.SQLiteRuntimeStore:
		owner = newSelectedContractSQLiteExecutionOwnerForTest(t, native)
	case *store.PostgresStore:
		owner = newSelectedContractExecutionOwnerForTest(t, native)
	default:
		t.Fatalf("unexpected native deadline store: %T", selected)
	}
	if err := owner.BindSelectedProcess(ctx, process, newCapability); err != nil {
		t.Fatal(err)
	}
	owner.ports.busDurable.RunLifecycle = nestedDeadlineServingContext{OperationOwner: owner.ports.busDurable.RunLifecycle, sourceRun: sourceRun, contexts: contexts}
	persisted, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
	if err != nil || !reflect.DeepEqual(rows, persisted) {
		t.Fatalf("native reopen changed open reply levels: %+v error=%v", persisted, err)
	}
	for _, row := range persisted {
		original, retained := arms[row.ID], nestedDeadlineArm(t, ctx, owner, row)
		if !reflect.DeepEqual(original, retained) {
			t.Fatalf("native reopen changed the original deadline arm: before=%+v after=%+v", original, retained)
		}
	}
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
	options := SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.MockOnly, ProcessCapability: newCapability}
	options.AgentManagerOptions.TestLifecycleProbe = hold
	recovery, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly), SelectedForkRecoveryEnvironment{
		SourceLoader: loader, AgentRuntime: options})
	if err != nil {
		t.Fatalf("recover original deadline child: %+v error=%v", recovery, err)
	}
	matched := 0
	for _, entry := range recovery {
		if entry.RunID == staged.ForkRunID {
			matched++
			if entry.Disposition != runfork.SelectedForkRecoveryResume || entry.Operation == nil || entry.Operation.Request.OperationID != operation.OperationID {
				t.Fatalf("successor did not resume the exact staged deadline operation: %+v", entry)
			}
		} else if entry.Disposition != runfork.SelectedForkRecoveryStaged {
			t.Fatalf("successor executed an unrelated context-only predecessor: %+v", entry)
		}
	}
	if matched != 1 {
		t.Fatalf("successor deadline child count=%d recovery=%+v", matched, recovery)
	}
	ops = selected.(interface {
		LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
	})
	ack, found, err := ops.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
	if err != nil || !found || ack.Status != runfork.ForkOperationActivated || ack.Result == nil || ack.ForkRunID != staged.ForkRunID ||
		ack.BindingID != before.BindingID || !reflect.DeepEqual(ack.Request, before.Request) || !ack.Result.ForkPoint.SameIdentity(staged.ForkPoint) {
		t.Fatalf("deadline successor changed the operation/cut acknowledgment: %+v error=%v", ack, err)
	}
	return selected, owner, ctx
}
