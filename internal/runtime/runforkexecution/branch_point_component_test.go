package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestDeliveryAuthorityActivationKeepsAdvancedSourceLiveBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newActivityForkFixture(t, backend)
			selected := fixture.owner.ports.fork
			ctx, request, advance := q1StateOnlyGateComponentSource(t, fixture.owner)
			attachment := q1StageBranchPointComponent(t, ctx, request)
			advance()
			activation, err := attachment.activate()
			if err != nil || !activation.Activated || !activation.SourceAdvancedAfterFork || activation.SourceFrozen || activation.BranchDivergence == nil {
				t.Fatalf("expected independently live advanced source: %+v %v", activation, err)
			}
			if err := selected.(runlifecycle.OperationOwner).RequireActiveRun(ctx, request.SourceRunID); err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "freeze.delivery", "test", "", []byte(`{}`), 0, request.SourceRunID, events.EventEnvelope{}, time.Now().UTC())
			storetest.CommitSemanticEvent(t, ctx, selected, event)
			route := selectedExecutionTestAgentRoute(t, request.SourceRunID, "advanced-source", "")
			storetest.CommitDeliveryObligationsForPersistedEvent(t, ctx, selected, event, []events.DeliveryRoute{route})
			id, err := deliverylifecycle.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			store := fixture.owner.ports.busDurable.DeliveryLifecycle
			prior, err := store.Snapshot(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := deliverylifecycle.NewNormalExecutionAuthority(prior.Authority.SourceArtifact(), uuid.NewString(), prior.Authority.Generation()+1)
			if err != nil {
				t.Fatal(err)
			}
			before := q1ComponentSnapshot(t, selected)
			if err := store.ActivateDeliveryAuthority(ctx, authority); err != nil {
				t.Fatal(err)
			}
			got, err := store.Snapshot(ctx, id)
			if err != nil || !got.Authority.Equal(authority) || got.Status != prior.Status || got.ClaimVersion != prior.ClaimVersion || !reflect.DeepEqual(got.Route, prior.Route) {
				t.Fatalf("advanced source incorrectly excluded: %+v %v", got, err)
			}
			if !reflect.DeepEqual(q1ComponentRowsForRun(t, before, attachment.materialized.ForkRunID), q1ComponentRowsForRun(t, q1ComponentSnapshot(t, selected), attachment.materialized.ForkRunID)) {
				t.Fatal("normal source activation rewrote the independently selected child")
			}
		})
	}
}

// Old root: runtimepersistence.TestSelectedBranchPointActivationAtomicityBothStores.
// The native owner and every backend/point/status oracle are unchanged.
func TestSelectedBranchPointActivationAtomicityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/live_handoff", func(t *testing.T) {
			proveSelectedLiveHandoff(t, backend, false)
		})
		t.Run(backend+"/live_handoff_unacknowledged_publication", func(t *testing.T) {
			proveSelectedLiveHandoff(t, backend, true)
		})
		for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
			for _, status := range []string{"running", "cancelled"} {
				t.Run(backend+"/"+string(kind)+"/"+status, func(t *testing.T) {
					proveQ1BranchPointAtomicity(t, backend, kind, status)
				})
			}
		}
	}
}

func proveSelectedLiveHandoff(t *testing.T, backend string, pending bool) {
	t.Helper()
	fixture := newActivityForkFixture(t, backend)
	ctx, request, _ := q1GateComponentSource(t, fixture.owner, !pending, pending, true)
	attachment := q1StageBranchPointComponent(t, ctx, request)
	p := attachment.prepared
	if err := p.retainedRuntime.startExecution(attachment.container.attachment.ctx, attachment.container.attachment.pipeline, attachment.container.diagnostics, false); err != nil {
		t.Fatal("start selected synchronous handoff runtime: ", err)
	}
	deliveries := p.owner.ports.busDurable.DeliveryLifecycle
	parked := make(map[string]deliverylifecycle.Snapshot)
	var inputs []committedSelectedInput
	defer func() {
		for _, input := range inputs {
			if err := attachment.container.attachment.runtime.bus.AbandonPreparedPublish(context.WithoutCancel(input.ctx), input.prepared); err != nil {
				t.Error(err)
			}
		}
	}()
	if pending {
		var err error
		inputs, err = attachment.container.commitSelectedInputs()
		if err != nil {
			t.Fatal(err)
		}
		for _, input := range inputs {
			handoffs, err := input.prepared.CommittedDeliveryHandoffs()
			if err != nil {
				t.Fatal(err)
			}
			for _, handoff := range handoffs {
				snapshot, err := deliveries.Snapshot(ctx, handoff.DeliveryID())
				if err != nil || snapshot.Status != deliverylifecycle.StatusPending {
					t.Fatalf("selected input is not at the unacknowledged publication cut: %+v %v", snapshot, err)
				}
				receipt, err := storetest.ReadExactPipelineReceiptOutcomeReason(ctx, p.owner.ports.fork, snapshot.EventID)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					t.Fatal(err)
				}
				if receipt.Outcome == "success" {
					t.Fatal("selected input already has a pipeline ACK before synchronous dispatch")
				}
				parked[handoff.DeliveryID()] = snapshot
			}
		}
		if len(parked) == 0 {
			t.Fatal("pending handoff reach did not commit a real selected delivery")
		}
	}
	capability := request.AgentRuntime.ProcessCapability
	process, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: p.loadedSource.SourceArtifactFact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	ordinary, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: p.loadedSource.SourceArtifactFact.BundleHash(), RuntimeInstanceID: process.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ordinary.Retire(context.WithoutCancel(ctx)); err != nil {
			t.Error(err)
		}
	})
	if _, err := ordinary.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := ordinary.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	selectedGrant := p.retainedRuntime.generationGrant
	selectedEvidence, err := selectedGrant.Evidence()
	if err != nil || selectedEvidence.State != startupownership.GrantAdmitted {
		t.Fatalf("selected grant was not admitted before self-release: %+v %v", selectedEvidence, err)
	}
	activationRequest := runfork.RunForkSelectedContractExecutionActivateRequest{
		LiveHandoff: true, InheritedSchedules: p.readiness, ForkOperation: p.forkOperation,
		DataPins: attachment.materialized.DataPins, ExecutionSource: p.loadedSource.Source,
		ForkRunID: attachment.materialized.ForkRunID, AllowSourceFreeze: true,
		AllowedSourceEventIDs: selectedContractExecutionFrontierEventIDs(p.frontier.FrontierEvents),
		FrontierAdmission:     p.frontier, RouteTopology: p.routeTopology, RecipientPlanning: *p.model.RecipientPlanning,
	}
	activationContext := effects.WithAuthority(p.operation.Context(), attachment.container.authority)
	foreign := evidence
	foreign.BundleHash = "bundle-v2:sha256:" + strings.Repeat("f", 64)
	before := q1ComponentSnapshot(t, p.owner.ports.fork)
	for _, rejected := range []context.Context{
		activationContext,
		startupownership.WithGenerationGrantEvidence(activationContext, selectedEvidence),
		startupownership.WithGenerationGrantEvidence(activationContext, foreign),
	} {
		if activation, err := p.owner.ports.fork.ActivateRunForkForSelectedContractExecution(rejected, activationRequest); err == nil || activation.Activated {
			t.Fatalf("missing, selected or foreign grant authorized live handoff: %+v %v", activation, err)
		}
		if !reflect.DeepEqual(before, q1ComponentSnapshot(t, p.owner.ports.fork)) {
			t.Fatal("refused live handoff changed source, child, executor or permanent evidence")
		}
	}
	activationContext = startupownership.WithGenerationGrantEvidence(activationContext, evidence)
	acknowledged := true
	if pending {
		activation, err := p.owner.ports.fork.ActivateRunForkForSelectedContractExecution(activationContext, activationRequest)
		if err == nil || activation.Activated || !strings.Contains(err.Error(), "unparked or foreign continuations") {
			t.Fatalf("unacknowledged publication did not refuse at the transfer fence: %+v %v", activation, err)
		}
		if !reflect.DeepEqual(before, q1ComponentSnapshot(t, p.owner.ports.fork)) {
			t.Fatal("transfer refusal failed to roll back executor closure, live selection or immutable ACK")
		}
		for len(inputs) > 0 {
			input := inputs[0]
			inputs = inputs[1:]
			if err := attachment.container.dispatchCommittedInput(input); err != nil {
				t.Fatal(err)
			}
		}
		for id := range parked {
			actual, err := deliveries.Snapshot(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			parked[id] = actual
			receipt, err := storetest.ReadExactPipelineReceiptOutcomeReason(ctx, p.owner.ports.fork, actual.EventID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				t.Fatal(err)
			}
			acknowledged = acknowledged && receipt.Outcome == "success"
		}
	}
	if err := p.retainedRuntime.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	p.retainedRuntime.manager = nil
	before = q1ComponentSnapshot(t, p.owner.ports.fork)
	if !acknowledged {
		if activation, err := p.owner.ports.fork.ActivateRunForkForSelectedContractExecution(activationContext, activationRequest); err == nil || activation.Activated || !strings.Contains(err.Error(), "unparked or foreign continuations") {
			t.Fatalf("nil selected dispatch was mistaken for a pipeline ACK: %+v %v", activation, err)
		}
		if !reflect.DeepEqual(before, q1ComponentSnapshot(t, p.owner.ports.fork)) {
			t.Fatal("pending pipeline allowed a partial live handoff")
		}
		return
	}
	activation, err := p.owner.ports.fork.ActivateRunForkForSelectedContractExecution(activationContext, activationRequest)
	if err != nil || !activation.Activated || activation.ForkRunStatus != runfork.RunForkLiveStatus || activation.SourceRunStatus != string(runlifecycle.StatePaused) || activation.SourceFrozen {
		t.Fatalf("exact selected-to-live handoff failed: %+v err=%v", activation, err)
	}
	for id, prior := range parked {
		got, err := deliveries.Snapshot(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if prior.Status == deliverylifecycle.StatusPending || prior.Status == deliverylifecycle.StatusFailed {
			if got.Authority.Kind() != deliverylifecycle.ExecutionAuthorityNormalRuntime || got.Authority.ExecutionID() != evidence.GrantID || got.Authority.Generation() != evidence.RuntimeGeneration {
				t.Fatalf("parked continuation lacks its exact ordinary generation: %+v", got)
			}
			prior.Authority = got.Authority
		}
		if !reflect.DeepEqual(prior, got) {
			t.Fatalf("live handoff rewrote route, claim or terminal evidence: before=%+v after=%+v", prior, got)
		}
	}
	actual, err := ordinary.InspectRunExecutionOwnership(ctx, attachment.materialized.ForkRunID)
	if err != nil || actual != manager.RunExecutionOwned {
		t.Fatalf("fresh ordinary grant cannot adopt its committed child: %v %v", actual, err)
	}
	if actual, err := selectedGrant.InspectRunExecutionOwnership(ctx, attachment.materialized.ForkRunID); err == nil || actual == manager.RunExecutionOwned {
		t.Fatalf("closed selected authority retained executable ownership: %v %v", actual, err)
	}
	operation, found, err := p.owner.ports.fork.LoadForkOperationByID(ctx, p.forkOperation.OperationID)
	if err != nil || !found || operation.Result == nil || operation.Result.ForkRunStatus != runfork.RunForkLiveStatus || operation.Result.ForkPoint != activation.ForkPoint {
		t.Fatalf("live handoff lost its immutable exact ACK: %+v found=%t err=%v", operation, found, err)
	}
	after := q1ComponentSnapshot(t, p.owner.ports.fork)
	if repeated, err := p.owner.ports.fork.ActivateRunForkForSelectedContractExecution(activationContext, activationRequest); err == nil || repeated.Activated || !repeated.RepeatedActivationFailed {
		t.Fatalf("second live activation did not preserve the first handoff: %+v %v", repeated, err)
	}
	if !reflect.DeepEqual(after, q1ComponentSnapshot(t, p.owner.ports.fork)) {
		t.Fatal("repeated live activation changed the committed handoff")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	attachment.closed = true
	if pending {
		return
	}

	secondRequest := request
	secondOperation := *request.ForkOperation
	secondOperation.OperationID, secondOperation.IdempotencyKey = uuid.NewString(), uuid.NewString()
	secondRequest.ForkOperation = &secondOperation
	second := q1StageBranchPointComponent(t, ctx, secondRequest)
	q := second.prepared
	if err := q.retainedRuntime.manager.Shutdown(); err != nil {
		t.Fatal(err)
	}
	q.retainedRuntime.manager = nil
	secondActivation, err := q.owner.ports.fork.ActivateRunForkForSelectedContractExecution(
		startupownership.WithGenerationGrantEvidence(effects.WithAuthority(q.operation.Context(), second.container.authority), evidence),
		runfork.RunForkSelectedContractExecutionActivateRequest{
			LiveHandoff: true, InheritedSchedules: q.readiness, ForkOperation: q.forkOperation,
			DataPins: second.materialized.DataPins, ExecutionSource: q.loadedSource.Source,
			ForkRunID: second.materialized.ForkRunID, AllowSourceFreeze: true,
			AllowedSourceEventIDs: selectedContractExecutionFrontierEventIDs(q.frontier.FrontierEvents),
			FrontierAdmission:     q.frontier, RouteTopology: q.routeTopology, RecipientPlanning: *q.model.RecipientPlanning,
		})
	if err != nil || !secondActivation.Activated || secondActivation.ForkRunStatus != runfork.RunForkLiveStatus || secondActivation.SourceRunStatus != string(runlifecycle.StatePaused) || second.materialized.ForkRunID == attachment.materialized.ForkRunID {
		t.Fatalf("older source did not switch the actually live sibling: %+v %v", secondActivation, err)
	}
	if actual, err := ordinary.InspectRunExecutionOwnership(ctx, attachment.materialized.ForkRunID); err != nil || actual == manager.RunExecutionOwned {
		t.Fatalf("paused sibling retained executable ordinary ownership: %v %v", actual, err)
	}
	if actual, err := ordinary.InspectRunExecutionOwnership(ctx, second.materialized.ForkRunID); err != nil || actual != manager.RunExecutionOwned {
		t.Fatalf("second live child lacks ordinary ownership: %v %v", actual, err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	second.closed = true
}

func proveQ1BranchPointAtomicity(t *testing.T, backend string, kind runfork.RunForkPointKind, status string) {
	t.Helper()
	fixture := newActivityForkFixture(t, backend)
	owner, selected := fixture.owner, fixture.owner.ports.fork
	build := q1DeploymentComponentSource
	if kind == runfork.RunForkPointEvent {
		build = q1StateOnlyGateComponentSource
	}
	ctx, request, advance := build(t, owner)
	attachment := q1StageBranchPointComponent(t, ctx, request)
	advance()
	if status == "cancelled" {
		if _, _, err := storetest.TerminalizeRun(ctx, selected.(storetest.RunFixtureStore), runlifecycle.TerminalRequest{RunID: request.SourceRunID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	before := q1ComponentSnapshot(t, selected)
	remove := q1DivergenceInsertFault(t, ctx, selected)
	activation, err := attachment.activate()
	remove()
	if err == nil || !strings.Contains(err.Error(), "typed_receiver_owner_fault") || activation.Activated {
		t.Fatalf("divergence failure did not refuse activation: %+v err=%v", activation, err)
	}
	if !reflect.DeepEqual(before, q1ComponentSnapshot(t, selected)) {
		t.Fatal("failed divergence write partially committed source/child/author activity/operation state")
	}
	activation, err = attachment.activate()
	if err != nil || !activation.Activated || activation.SourceFrozen || activation.BranchDivergence == nil {
		t.Fatalf("branch retry did not activate: %+v err=%v", activation, err)
	}
	point := activation.BranchDivergence.ForkPoint
	if point.Kind != kind || point.Revision <= 0 || point.EventID != request.At || activation.BranchDivergence.ForkEventID != request.At || activation.SourceRunStatus != status || activation.BranchDivergence.SourceRunStatusAfterActivation != status {
		t.Fatalf("activation lost fixed typed point/source state: %+v", activation)
	}
	after := q1ComponentSnapshot(t, selected)
	if persisted := q1ComponentBranchPoint(t, after, attachment.materialized.ForkRunID); persisted != point {
		t.Fatalf("durable point differs from activation: %+v != %+v", persisted, point)
	}
	if !reflect.DeepEqual(q1ComponentRowsForRun(t, before, request.SourceRunID), q1ComponentRowsForRun(t, after, request.SourceRunID)) {
		t.Fatal("advanced source state/history was frozen, rewound or otherwise mutated")
	}
	operationRequest := attachment.prepared.forkOperation
	for repetition := 0; repetition < 2; repetition++ {
		operation, found, err := selected.LoadForkOperation(ctx, operationRequest.Actor, operationRequest.IdempotencyKey, operationRequest.TransportHash)
		if err != nil || !found || operation.Status != runfork.ForkOperationActivated || operation.ForkRunID != attachment.materialized.ForkRunID || operation.Result == nil || operation.Result.ForkPoint.Kind != point.Kind || operation.Result.ForkPoint.Revision != point.Revision || operation.Result.ForkPoint.EventID != point.EventID || operation.Result.SourceFrozen || operation.Result.SourceRunStatus != status {
			t.Fatalf("permanent completion lost exact branch point: %+v found=%t err=%v", operation, found, err)
		}
		encoded, err := json.Marshal(operation.Result)
		if err != nil {
			t.Fatal(err)
		}
		var decoded runfork.ForkOperationResult
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.ForkPoint != operation.Result.ForkPoint {
			t.Fatalf("serialized public completion point changed: %+v err=%v", decoded, err)
		}
	}
	if _, err := attachment.activate(); err == nil {
		t.Fatal("repeated activation rewrote first completion")
	}
	if !reflect.DeepEqual(after, q1ComponentSnapshot(t, selected)) {
		t.Fatal("exact readback or rejected second activation mutated first committed evidence")
	}
	attachment.close(t)
}

func q1DivergenceInsertFault(t *testing.T, ctx context.Context, selected any) func() {
	t.Helper()
	if err := storetest.SetSelectedBranchDivergenceInsertFault(ctx, selected, true); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	remove := func() {
		once.Do(func() {
			if err := storetest.SetSelectedBranchDivergenceInsertFault(context.WithoutCancel(ctx), selected, false); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(remove)
	return remove
}

func q1ComponentSnapshot(t *testing.T, selected any) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(runForkTestContext(t), selected)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func q1ComponentRowsForRun(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, runID string) map[string][]string {
	t.Helper()
	selected := make(map[string][]string, len(snapshot))
	for name, table := range snapshot {
		if !slices.Contains(table.Columns, "run_id") {
			continue
		}
		selected[name] = []string{}
		rows, err := storetest.DecodeSelectedForkSnapshotRows(table)
		if err != nil {
			t.Fatal(err)
		}
		for index, row := range rows {
			value, err := row["run_id"].Text()
			if err != nil {
				t.Fatal(err)
			}
			if value == runID {
				selected[name] = append(selected[name], table.Rows[index])
			}
		}
	}
	return selected
}

func q1ComponentBranchRows(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, child string) []map[string]storetest.SelectedForkSnapshotCell {
	t.Helper()
	table, found := snapshot["run_fork_selected_contract_branch_divergences"]
	if !found {
		t.Fatal("native branch table is absent")
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(table)
	if err != nil {
		t.Fatal(err)
	}
	var matched []map[string]storetest.SelectedForkSnapshotCell
	for _, row := range rows {
		id, err := row["fork_run_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if id == child {
			matched = append(matched, row)
		}
	}
	return matched
}

func q1ComponentRowsWithIdentity(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, table, column, identity string) []string {
	t.Helper()
	rows, err := storetest.DecodeSelectedForkSnapshotRows(snapshot[table])
	if err != nil {
		t.Fatal(err)
	}
	var matched []string
	for index, row := range rows {
		value, err := row[column].Text()
		if err != nil {
			t.Fatal(err)
		}
		if value == identity {
			matched = append(matched, snapshot[table].Rows[index])
		}
	}
	return matched
}

func q1ComponentBranchPoint(t *testing.T, snapshot map[string]storetest.SelectedForkStorageTableSnapshot, child string) runfork.RunForkPoint {
	t.Helper()
	rows := q1ComponentBranchRows(t, snapshot, child)
	if len(rows) != 1 {
		t.Fatalf("branch rows=%d, want exactly one", len(rows))
	}
	row := rows[0]
	kind, err := row["fork_point_kind"].Text()
	if err != nil {
		t.Fatal(err)
	}
	var revision int64
	if err := json.Unmarshal(row["fork_revision"].Value, &revision); err != nil {
		t.Fatal(err)
	}
	event, err := row["fork_event_id"].Text()
	if err != nil {
		t.Fatal(err)
	}
	if (string(row["fork_event_id"].Value) != "null") != (runfork.RunForkPointKind(kind) == runfork.RunForkPointEvent) {
		t.Fatal("stored event nullability differs from typed point kind")
	}
	return runfork.RunForkPoint{Kind: runfork.RunForkPointKind(kind), Revision: revision, EventID: event}
}
