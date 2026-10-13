package cataloge2e

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestIssue642TwoOpenUnclaimedRepliesStagedRestartBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyNestedFanOutReply(t)
			h := newRuntimeHarnessForBackend(t, root, backend, true)
			var selected unclaimedFanOutReplyStore = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			cut, sourceRows := prepareUnclaimedFanOutReplySource(t, h, selected)
			if len(h.publishedOrder) != 1 {
				t.Fatal("unclaimed restart must capture its exact original root input")
			}
			input, found, err := selected.LoadPreparedPublishEvent(h.ctx, h.publishedOrder[0])
			if err != nil || !found {
				t.Fatalf("read actual original root input: found=%t error=%v", found, err)
			}
			event := input.Event.Event()
			var payload map[string]any
			if err := json.Unmarshal(event.Payload(), &payload); err != nil {
				t.Fatal(err)
			}
			step := catalogTriggerStep{Event: string(event.Type()), Payload: payload, eventID: event.ID(), createdAt: event.CreatedAt(),
				sourceAgent: event.SourceAgent(), inputKind: catalogReplayInputRootIngress}
			loader, selection, _ := selectedContractForkFixtureSelection(t, h.ctx, repoRootFromCatalogE2E(t), root, selected)
			owner := selectedContractExecutionOwnerForCatalogHarness(t, h)
			options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig())
			operation := runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "nested-unclaimed-restart", IdempotencyKey: uuid.NewString(),
				TransportHash: "nested-unclaimed-restart", SourceRunID: catalogRuntimeRunID, ForkEventID: cut,
				TargetBundleHash: selection.BundleHash, AllowSourceFreeze: true, ContractSelection: selection}
			staged := stageCatalogSelectedContractFork(t, h.ctx, selected, owner, loader, selection, options, catalogRuntimeRunID, cut, operation)
			childRows, inner := requireUnclaimedFanOutReplyStaging(t, h.ctx, selected, staged.ForkRunID, cut)
			h, owner = restartNestedReplyChild(t, h, staged, childRows, operation, step)
			selected = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			t.Cleanup(func() { logUnclaimedFanOutReplyFailure(t, h.ctx, selected, owner, staged.ForkRunID) })
			ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), 20*time.Second)
			defer cancel()
			waitCopiedActivityRequesterReturn(t, ctx, selected, staged.ForkRunID)
			requireUnclaimedFanOutReplyReturn(t, ctx, selected, staged.ForkRunID, cut, inner, childRows, sourceRows)
		})
	}
}

// This is an actual orderly runtime/process-owner reconstruction while both
// child replies are open. It does not claim an OS-process-death proof.
func restartNestedReplyChild(t *testing.T, h *runtimeHarness, staged runfork.RunForkMaterialization, rows []replycontext.Record, operation runfork.ForkOperationRequest, sourceStep catalogTriggerStep) (*runtimeHarness, runforkexecution.SelectedContractExecutionOwner) {
	t.Helper()
	if len(rows) != 2 || staged.SelectedContractBinding == nil || staged.ForkRunStatus != runfork.RunForkMaterializedStatus {
		t.Fatal("nested restart requires two actual open rows and a paused selected child")
	}
	var selected copiedActivityReplyStore = h.pg
	if h.sqlite != nil {
		selected = h.sqlite
	}
	ops := selected.(interface {
		LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
	})
	before, found, err := ops.LoadForkOperation(h.ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
	if err != nil || !found || before.Status != runfork.ForkOperationMaterialized || before.Result != nil || before.ForkRunID != staged.ForkRunID ||
		before.Request.ResolvedPoint == nil || *before.Request.ResolvedPoint != staged.ForkPoint {
		t.Fatalf("staged nested child lost its permanent operation/cut: %+v error=%v", before, err)
	}
	childBefore, err := storetest.ReadSelectedForkSourceDomain(h.ctx, selected, staged.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootFromCatalogE2E(t),
		PlatformSpecPath: platformSpecPathFromCatalogE2E(t), Store: selected}
	retry := stageCatalogSelectedContractFork(t, h.ctx, selected, selectedContractExecutionOwnerForCatalogHarness(t, h), loader,
		operation.ContractSelection, selectedContractAgentRuntimeOptionsForCatalogHarness(h, testRuntimeConfig()), operation.SourceRunID, operation.ForkEventID, operation)
	retryRows, rowsErr := storetest.ReadReplyContextRecords(h.ctx, selected, staged.ForkRunID)
	childAfter, childErr := storetest.ReadSelectedForkSourceDomain(h.ctx, selected, staged.ForkRunID)
	stable, found, operationErr := ops.LoadForkOperation(h.ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
	if retry.ForkRunID != staged.ForkRunID || retry.ForkRunStatus != staged.ForkRunStatus || !retry.ForkPoint.SameIdentity(staged.ForkPoint) ||
		!reflect.DeepEqual(retry.SelectedContractBinding, staged.SelectedContractBinding) || rowsErr != nil || childErr != nil || operationErr != nil || !found ||
		!reflect.DeepEqual(rows, retryRows) || !reflect.DeepEqual(childBefore, childAfter) || !reflect.DeepEqual(before, stable) {
		t.Fatalf("nested materialization retry changed child, replies or permanent acknowledgment: child=%+v errors=%v/%v/%v", retry, rowsErr, childErr, operationErr)
	}
	digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
	if err != nil {
		t.Fatal(err)
	}
	oldProcess, oldCapability := h.processOwner, h.processTopology
	reopened := h.reopenFromTranscript(&catalogExecutionTranscript{version: catalogReplayTranscriptVersion,
		platformSpecDigest: digest, bundleHash: operation.TargetBundleHash, runID: catalogRuntimeRunID,
		groups: []catalogTranscriptGroup{{steps: []catalogTriggerStep{sourceStep}}}})
	if oldProcess.ActiveCount() != 0 || reopened.processOwner == oldProcess || reopened.processTopology == oldCapability {
		t.Fatal("nested restart retained the old process work or capability")
	}
	if oldCapability.ProveCurrent(context.Background()) == nil {
		t.Fatal("released predecessor still supplies current process ownership")
	}
	selected = reopened.pg
	if reopened.sqlite != nil {
		selected = reopened.sqlite
	}
	ctx, cancel := context.WithTimeout(reopened.ctx, 20*time.Second)
	defer cancel()
	persisted, err := storetest.ReadReplyContextRecords(ctx, selected, staged.ForkRunID)
	if err != nil || !reflect.DeepEqual(rows, persisted) {
		t.Fatalf("ordinary restart executed or changed the selected open replies: %+v error=%v", persisted, err)
	}
	loader = runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repoRootFromCatalogE2E(t),
		PlatformSpecPath: platformSpecPathFromCatalogE2E(t), Store: selected}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	inspected, err := runforkexecution.InspectSelectedForkRecoveries(ctx, selected.(runforkexecution.SelectedForkRecoveryInspectionReader), loader)
	if err != nil || len(inspected) != 1 || inspected[0].Entry.Binding.ForkRunID != staged.ForkRunID ||
		inspected[0].Evidence.RunState != runfork.RunForkMaterializedStatus || inspected[0].Evidence.ExecutionState != "" ||
		inspected[0].Evidence.Plan.Disposition != runfork.SelectedForkRecoveryResume || inspected[0].Evidence.Plan.Operation == nil {
		t.Fatalf("inspect exact staged nested recovery: %+v error=%v", inspected, err)
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil || !reflect.DeepEqual(snapshot, after) {
		t.Fatalf("nested recovery inspection changed persistent state: %v", err)
	}
	owner := constructCatalogSelectedExecutionOwner(t, reopened, nil)
	if err := owner.BindSelectedProcess(ctx, reopened.processOwner, reopened.processTopology); err != nil {
		t.Fatal(err)
	}
	reopened.selectedOwners = append(reopened.selectedOwners, owner)
	options := selectedContractAgentRuntimeOptionsForCatalogHarness(reopened, testRuntimeConfig())
	recovered, err := owner.RecoverSelectedForkContexts(ctx, effects.NewRecoveryRequest(time.Now().UTC(), options.ExecutionPosture), runforkexecution.SelectedForkRecoveryEnvironment{
		SourceLoader: loader, AgentRuntime: options,
	})
	if err != nil || len(recovered) != 1 || recovered[0].RunID != staged.ForkRunID || recovered[0].ExecutionID != "" ||
		recovered[0].Disposition != runfork.SelectedForkRecoveryResume || recovered[0].Operation == nil || recovered[0].Operation.Request.OperationID != operation.OperationID {
		t.Fatalf("resume staged nested replies under the successor: %+v error=%v", recovered, err)
	}
	ops = selected.(interface {
		LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
	})
	ack, found, err := ops.LoadForkOperation(ctx, operation.Actor, operation.IdempotencyKey, operation.TransportHash)
	if err != nil || !found || ack.Status != runfork.ForkOperationActivated || ack.Result == nil || ack.ForkRunID != staged.ForkRunID ||
		ack.BindingID != before.BindingID || !reflect.DeepEqual(before.Request, ack.Request) || !ack.Result.ForkPoint.SameIdentity(staged.ForkPoint) {
		t.Fatalf("nested successor lost its exact activation acknowledgment: %+v error=%v", ack, err)
	}
	return reopened, owner
}
