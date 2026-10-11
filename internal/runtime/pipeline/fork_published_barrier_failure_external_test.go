package pipeline_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func verifyPublishedBarrierCompletionRollback(t *testing.T, ctx context.Context, selected *storeselected.Owner, family storeselected.RunFork, result runforkexecution.SelectedContractExecutionResult, plan runfork.RunForkPlan, original, observed forkEngineObservation, probe *publishedBarrierClaimProbe, sourceRun string, sourceBefore operatorread.OperatorEntityFull) {
	t.Helper()
	claim, valid := deliverylifecycle.ClaimFromContext(observed.ctx)
	if !valid {
		t.Fatal("completion rollback requires its actual claim")
	}
	snapshot, err := selected.RuntimeDeps().DeliveryStore.Snapshot(ctx, claim.DeliveryID())
	if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.EventID != observed.id {
		t.Fatalf("completion failure lacks the exact pending occurrence: %+v err=%v", snapshot, err)
	}
	entityID := snapshot.Route.Target.Route().EntityID
	before, err := selected.Entities().LoadOperatorEntity(ctx, entityID, claim.RunID())
	if err != nil {
		t.Fatal(err)
	}
	close(probe.resume)
	if _, err := probe.Wait(ctx, lifecycleprobe.Signal{Kind: lifecycleprobe.DeliveryStatusChanged, EventID: observed.id, SubscriberID: claim.SubscriberID(), SubscriberType: "node", Status: "dead_letter"}); err != nil {
		diagnostic, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		physical, readErr := selected.RuntimeDeps().DeliveryStore.Snapshot(diagnostic, claim.DeliveryID())
		t.Logf("completion failure exact delivery: %+v read_error=%v", physical, readErr)
		report, readErr := selected.Runs().LoadRunDebugReport(diagnostic, claim.RunID(), operatorread.RunDebugQueryOptions{})
		t.Logf("completion failure durable pipeline/runtime: deliveries=%+v failures=%+v fanout=%+v logs=%+v read_error=%v", report.Deliveries, report.FailedDeliveries, report.FanOut, report.RuntimeLogs, readErr)
		t.Fatal(err)
	}
	settled, err := selected.RuntimeDeps().DeliveryStore.Snapshot(ctx, claim.DeliveryID())
	if err != nil || !settled.MatchesSettlementClaim(claim) || settled.Status != deliverylifecycle.StatusDeadLetter || settled.ReasonCode != "handler_terminal_failure" ||
		settled.Failure == nil || failures.ValidateEnvelope(*settled.Failure) != nil || settled.Failure.Component != "workflow-runtime" || settled.Failure.Operation != "execute_handler" {
		t.Fatalf("completion failure lost exact canonical dead-letter evidence: %+v err=%v", settled, err)
	}
	key := plan.FanOutObligations[0].Intent.Request.Key
	key.RunID = claim.RunID()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		receipt, err := storetest.ReadExactPipelineReceiptOutcomeReason(ctx, selected.RuntimeDeps().DeliveryStore, observed.id)
		if err == nil {
			if receipt.Outcome != "dead_letter" || receipt.Reason != settled.ReasonCode {
				t.Fatalf("completion rollback was reinterpreted as successful handoff: %+v", receipt)
			}
			break
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var barrier fanoutbarrier.Barrier
	for {
		barrier, err = storetest.ReadExactFanOutBarrier(ctx, selected.RuntimeDeps().DeliveryStore, key)
		if err != nil || barrier.Status == fanoutbarrier.StatusOutcomeDeadLettered {
			break
		}
		if barrier.Status != fanoutbarrier.StatusClosedPending {
			t.Fatalf("dead-lettered completion changed to another terminal disposition: %+v", barrier)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("dead-lettered completion has no terminal barrier owner: %+v cause=%v", barrier, ctx.Err())
		}
	}
	if err != nil || barrier.Status != fanoutbarrier.StatusOutcomeDeadLettered || barrier.Summary == nil || *barrier.Summary != (fanoutbarrier.Summary{Total: 1, Succeeded: 1}) ||
		barrier.TransferredOccurrence == nil || barrier.ScheduleActivationID != "" || barrier.TransferredOccurrence.Publication.EventID != observed.id || barrier.TransferredOccurrence.Publication.SourceEventID != original.id {
		t.Fatalf("completion failure lost terminal summary/lineage or rearmed a timer: %+v err=%v", barrier, err)
	}
	after, err := selected.Entities().LoadOperatorEntity(ctx, entityID, claim.RunID())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed completion committed producer stage/state/revision: before=%+v after=%+v err=%v", before, after, err)
	}
	root, err := selected.Entities().LoadOperatorEntity(ctx, claim.RunID(), claim.RunID())
	if err != nil || root.Fields["completed_total"] != float64(0) || root.Entity.Revision != 1 {
		t.Fatalf("failed completion reached the success consumer: %+v err=%v", root, err)
	}
	if count, err := storetest.ReadLifecycleEventCardinality(ctx, selected.RuntimeDeps().DeliveryStore, claim.RunID(), "producer/batch.completed"); err != nil || count != 0 {
		t.Fatalf("failed completion published a success output: count=%d err=%v", count, err)
	}
	physical := readForkEngineExecutionProof(t, ctx, selected, forkEngineExecutionProofRequest(t, result, original.id, observed.id, claim))
	if physical.DeadLetterCount != 1 || physical.DeliveryHandoff || physical.PipelineOutcome != "dead_letter" || physical.PipelineReason != settled.ReasonCode {
		t.Fatalf("rollback lost its one DL or fabricated a success handoff: %+v", physical)
	}
	afterSource, err := selected.Entities().LoadOperatorEntity(ctx, sourceRun, sourceRun)
	if err != nil || !reflect.DeepEqual(sourceBefore, afterSource) {
		t.Fatalf("failed child completion changed source state: %v", err)
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := family.RetireSelectedContexts(cleanup); err != nil {
		t.Fatal(err)
	}
	retired, err := storetest.ReadExactFanOutBarrier(cleanup, selected.RuntimeDeps().DeliveryStore, key)
	if err != nil || !reflect.DeepEqual(barrier, retired) {
		t.Fatalf("explicit retirement rewrote dead-letter business history: %+v err=%v", retired, err)
	}
	joined := readForkEngineExecutionProof(t, cleanup, selected, forkEngineExecutionProofRequest(t, result, original.id, observed.id, claim))
	if joined.DeadLetterCount != physical.DeadLetterCount || joined.DeliveryHandoff || joined.PipelineOutcome != physical.PipelineOutcome || joined.PipelineReason != physical.PipelineReason ||
		!reflect.DeepEqual(joined.PipelineFailure, physical.PipelineFailure) || joined.ExecutionState != "running" || joined.RunStatus != "running" || len(joined.Failure) != 0 {
		t.Fatalf("retirement changed the exact DL or converted delivery terminality to run failure: %+v", joined)
	}
}
