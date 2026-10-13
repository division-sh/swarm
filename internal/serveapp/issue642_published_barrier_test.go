package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type issue642ServedBarrierClaim struct{ run, event string }

type issue642ServedBarrierProbe struct {
	*lifecycleprobe.Probe
	producer string
	claims   chan issue642ServedBarrierClaim
	resume   chan struct{}
	once     sync.Once
}

func (p *issue642ServedBarrierProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	p.Probe.NotifyLifecycle(ctx, signal)
	if signal.Kind != lifecycleprobe.DeliveryStatusChanged || signal.EventType != "platform.join_complete" || signal.SubscriberID != p.producer || signal.Status != "in_progress" {
		return
	}
	claim, valid := deliverylifecycle.ClaimFromContext(ctx)
	if !valid {
		return
	}
	p.claims <- issue642ServedBarrierClaim{run: claim.RunID(), event: signal.EventID}
	p.once.Do(func() {
		select {
		case <-p.resume:
		case <-ctx.Done():
		}
	})
}

// This is a real authenticated HTTP publication/fork/readback journey, not
// registry-v5 acceptance or future-provider/restart qualification.
func TestIssue642ServedPublishedBarrierReachesFinalConsumerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			producer, err := identity.AdmitExecutableNodeDeclaration("producer", "producer")
			if err != nil {
				t.Fatal(err)
			}
			probe := &issue642ServedBarrierProbe{Probe: lifecycleprobe.New(), producer: producer.Key(), claims: make(chan issue642ServedBarrierClaim, 4), resume: make(chan struct{})}
			opts, start := issue2564ServeHarness(t, backend, canonicalrouting.CopyForkFanOutBarrierReceiver(t), false)
			opts.TestLifecycleProbe = probe
			process, rt := start()
			var release sync.Once
			t.Cleanup(func() {
				release.Do(func() { close(probe.resume) })
				if code := process.stop(); code != 0 {
					t.Errorf("published barrier serve shutdown=%d\n%s", code, process.outputString())
				}
			})
			reader, ok := rt.selected.(interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			})
			if !ok {
				t.Fatalf("missing canonical fixed-cut reader: %T", rt.selected)
			}
			phase, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.seeded", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"token": "seed"}, "idempotency_key": "served-published-barrier-seed"})
			issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start.requested", "run_id": seed.RunID,
				"payload": map[string]any{"token": "7", "tokens": []string{"7"}}, "idempotency_key": "served-published-barrier-trigger"})
			var original issue642ServedBarrierClaim
			select {
			case original = <-probe.claims:
			case <-phase.Done():
				t.Fatal(phase.Err())
			}
			plan, err := reader.PlanRunFork(phase, runfork.RunForkPlanRequest{SourceRunID: seed.RunID, At: original.event})
			if err != nil || original.run != seed.RunID || len(plan.FanOutObligations) != 1 || plan.FanOutObligations[0].Barrier.Status != fanoutbarrier.StatusClosedPending {
				t.Fatalf("public source lost its exact unfinished published cut: %+v err=%v", plan, err)
			}
			release.Do(func() { close(probe.resume) })
			issue642WaitExactBarrierPipeline(t, phase, rt, original.event)
			completed := issue642WaitBarrierOutput(t, phase, rt, seed.RunID)
			issue642WaitExactBarrierPipeline(t, phase, rt, completed.EventID)
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(phase, rt.selected, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", map[string]any{"source_run_id": seed.RunID, "fork_event_id": original.event,
				"allow_source_freeze": true, "idempotency_key": "served-published-barrier-fork"}, &fork)
			// The permanent response is sealed at activation, before asynchronous
			// publication/execution. Actual progress is proved below, not inferred
			// from that activation-point counter.
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == seed.RunID || fork.ForkRunID == "" ||
				fork.Owner != runfork.RunForkSelectedContractExecutionOwner || fork.ForkEventID != original.event || fork.ForkRunStatus != "running" ||
				fork.ForkRevision != plan.ForkPoint.Revision || fork.ExecutedEventCount != 0 {
				t.Fatalf("public fork lost exact activation/cut acknowledgment: %+v", fork)
			}
			childID := activityidentity.ForkLineageEventID(fork.ForkRunID, original.event)
			child := issue642WaitBarrierDeliveredEvent(t, phase, rt, childID, producer.Key())
			if child.RunID != fork.ForkRunID || child.EventID == original.event {
				t.Fatal("public fork reused the source claim")
			}
			issue642WaitExactBarrierPipeline(t, phase, rt, child.EventID)
			output := issue642WaitBarrierOutput(t, phase, rt, fork.ForkRunID)
			issue642WaitExactBarrierPipeline(t, phase, rt, output.EventID)
			entity, err := rt.selected.LoadOperatorEntity(phase, fork.ForkRunID, fork.ForkRunID)
			if err != nil || entity.Fields["completed_total"] != float64(1) || entity.Entity.Revision != 2 {
				t.Fatalf("public final consumer did not commit once: %+v err=%v", entity, err)
			}
			final, err := reader.PlanRunFork(phase, runfork.RunForkPlanRequest{SourceRunID: fork.ForkRunID, At: output.EventID})
			if err != nil || len(final.FanOutObligations) != 1 {
				t.Fatalf("public child lost its committed final-consumer cut: %+v err=%v", final, err)
			}
			barrier := final.FanOutObligations[0].Barrier
			if barrier == nil || barrier.Status != fanoutbarrier.StatusFired || barrier.Summary == nil || *barrier.Summary != (fanoutbarrier.Summary{Total: 1, Succeeded: 1}) || barrier.TransferredOccurrence == nil || barrier.ScheduleActivationID != "" || barrier.TransferredOccurrence.Publication.SourceEventID != original.event || barrier.TransferredOccurrence.Publication.EventID != child.EventID {
				t.Fatalf("public barrier changed summary/lineage or invented a schedule: %+v", barrier)
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(phase, rt.selected, seed.RunID)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("public child changed source business state: %v", err)
			}
			issue642RequireServedBarrierGrandchild(t, rt, fork, child, producer.Key())
		})
	}
}

func issue642RequireServedBarrierGrandchild(t *testing.T, rt issue2564ServedFixture, parent apiv1.RunForkExecutionResult, inherited operatorread.OperatorEventFull, producer string) {
	t.Helper()
	phase, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	reader := rt.selected.(interface {
		PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	})
	plan, err := reader.PlanRunFork(phase, runfork.RunForkPlanRequest{SourceRunID: parent.ForkRunID, At: inherited.EventID})
	if err != nil || len(plan.FanOutObligations) != 1 || plan.FanOutObligations[0].Barrier == nil || plan.FanOutObligations[0].Barrier.Status != fanoutbarrier.StatusClosedPending {
		t.Fatalf("served grandchild lacks its exact unfinished transferred cut: %+v err=%v", plan, err)
	}
	before, err := storetest.ReadSelectedForkSourceDomain(phase, rt.selected, parent.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	grandchild := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, map[string]any{"source_run_id": parent.ForkRunID, "fork_event_id": inherited.EventID,
		"allow_source_freeze": true, "idempotency_key": "served-published-barrier-grandchild"})
	if grandchild.SourceRunID != parent.ForkRunID || grandchild.ForkRunID == "" || grandchild.ForkRunID == parent.ForkRunID || grandchild.ForkRunID == parent.SourceRunID ||
		grandchild.ForkEventID != inherited.EventID || grandchild.ForkRevision != plan.ForkPoint.Revision || grandchild.ForkRunStatus != "running" ||
		grandchild.Owner != runfork.RunForkSelectedContractExecutionOwner || grandchild.ExecutedEventCount != 0 {
		t.Fatalf("served grandchild changed the committed join cut or borrowed activation: %+v", grandchild)
	}
	publication := issue642WaitBarrierDeliveredEvent(t, phase, rt, activityidentity.ForkLineageEventID(grandchild.ForkRunID, inherited.EventID), producer)
	issue642WaitExactBarrierPipeline(t, phase, rt, publication.EventID)
	output := issue642WaitBarrierOutput(t, phase, rt, grandchild.ForkRunID)
	issue642WaitExactBarrierPipeline(t, phase, rt, output.EventID)
	entity, err := rt.selected.LoadOperatorEntity(phase, grandchild.ForkRunID, grandchild.ForkRunID)
	if err != nil || entity.Fields["completed_total"] != float64(1) || entity.Entity.Revision != 2 {
		t.Fatalf("served grandchild did not commit its real final consumer once: %+v err=%v", entity, err)
	}
	final, err := reader.PlanRunFork(phase, runfork.RunForkPlanRequest{SourceRunID: grandchild.ForkRunID, At: output.EventID})
	if err != nil || len(final.FanOutObligations) != 1 || final.FanOutObligations[0].Barrier == nil {
		t.Fatalf("served grandchild lost its exact settled barrier cut: %+v err=%v", final, err)
	}
	barrier := final.FanOutObligations[0].Barrier
	if barrier.Status != fanoutbarrier.StatusFired || barrier.Summary == nil || *barrier.Summary != (fanoutbarrier.Summary{Total: 1, Succeeded: 1}) ||
		barrier.ScheduleActivationID != "" || barrier.TransferredOccurrence == nil || barrier.TransferredOccurrence.Publication.SourceRunID != parent.ForkRunID ||
		barrier.TransferredOccurrence.Publication.SourceEventID != inherited.EventID || barrier.TransferredOccurrence.Publication.EventID != publication.EventID {
		t.Fatalf("served grandchild lost immediate-source lineage/summary or rearmed a timer: %+v", barrier)
	}
	after, err := storetest.ReadSelectedForkSourceDomain(phase, rt.selected, parent.ForkRunID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("served grandchild mutated its immediate source business state: %v", err)
	}
}

func issue642WaitExactBarrierPipeline(t *testing.T, ctx context.Context, rt issue2564ServedFixture, id string) {
	t.Helper()
	for {
		receipt, err := storetest.ReadExactPipelineReceiptOutcomeReason(ctx, rt.persistence.deps.DeliveryStore, id)
		if err == nil {
			if receipt.Outcome != "success" {
				t.Fatalf("exact barrier pipeline failed: event=%s receipt=%+v\n%s", id, receipt, rt.debug(t, ""))
			}
			return
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func issue642WaitBarrierOutput(t *testing.T, ctx context.Context, rt issue2564ServedFixture, runID string) operatorread.OperatorEventFull {
	t.Helper()
	for {
		page, err := rt.selected.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
			Filter: operatorread.OperatorEventListFilter{RunID: runID, EventName: "producer/batch.completed"}, Limit: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) != 0 {
			if len(page.Events) != 1 || page.NextCursor != "" {
				t.Fatalf("published barrier duplicated business output: %+v", page)
			}
			return page.Events[0]
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("run %s lacks real barrier business output: %v", runID, ctx.Err())
		}
	}
}

func issue642WaitBarrierDeliveredEvent(t *testing.T, ctx context.Context, rt issue2564ServedFixture, eventID, node string) operatorread.OperatorEventFull {
	t.Helper()
	for {
		event, err := rt.selected.LoadOperatorEvent(ctx, eventID)
		if err == nil {
			if len(event.DeadLetters) != 0 || event.NoDelivery != nil || len(event.Deliveries) > 1 {
				t.Fatalf("transferred barrier did not preserve exact executable work: %+v", event)
			}
			if len(event.Deliveries) == 1 {
				row := event.Deliveries[0]
				if row.Terminal {
					if row.Status != "delivered" || row.SubscriberType != "node" || row.SubscriberID != node || row.ClaimVersion != 1 || row.Failure != nil {
						t.Fatalf("transferred barrier lost its exact once-only fresh child claim: %+v", row)
					}
					return event
				}
			}
		} else if !errors.Is(err, operatorread.ErrEventNotFound) {
			t.Fatal(err)
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("transferred barrier event %s did not settle: %v", eventID, ctx.Err())
		}
	}
}
