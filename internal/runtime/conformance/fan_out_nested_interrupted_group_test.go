package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Keep the real coordinator as the route interceptor. This fault interrupts
// one exact event before its recipient, without creating a substitute receiver.
type nestedInterruptedGroup struct {
	*pipeline.PipelineCoordinator
	mu      sync.Mutex
	ids     [3]string
	visits  [3]int
	enabled bool
	failure error
}

func (g *nestedInterruptedGroup) InterceptDeliveryRoute(ctx context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	g.mu.Lock()
	for i, id := range g.ids {
		if delivery.Event().ID() != id {
			continue
		}
		g.visits[i]++
		if i == 1 && g.enabled {
			g.mu.Unlock()
			return false, nil, pipelineobligation.ReleaseForRetry("activity_contract_pin_unavailable", nil), g.failure
		}
	}
	g.mu.Unlock()
	return g.PipelineCoordinator.InterceptDeliveryRoute(ctx, delivery, route)
}

func (g *nestedInterruptedGroup) snapshot() [3]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.visits
}

func TestIssue2394NestedInterruptedGroupRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadCanonicalRoutingSource(t, canonicalrouting.CopyNotifyAllChildrenNestedServing(t))
			probe := newNestedServingProbe(t)
			rt, db := newNestedServingRuntime(t, backend, source, nil, probe)
			runID := uuid.NewString()
			ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
			if err := rt.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "nested-interrupted-group", rt.sourceArtifactFact)); err != nil {
				t.Fatal(err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, rt, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "portfolio-main"})
			accounts := []string{"sibling-c", "sibling-a", "sibling-b"}
			publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": "portfolio-main", "account_ids": accounts})
			probe.armPostCommitHold(t)
			notifyID := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{"portfolio_id": "portfolio-main", "command": "interrupted-group"})
			held := waitNestedServingPostCommit(t, probe)
			parents := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, db, runID, notifyID)
			assertNotifyAllChildrenItemSequence(t, parents, accounts)
			if held.ParentEvent != notifyID || held.Publications != 3 || len(parents) != 3 {
				t.Fatalf("interrupted group requires real exact three-member commit: %+v parents=%+v", held, parents)
			}
			fault := &nestedInterruptedGroup{PipelineCoordinator: rt.pipeline, enabled: true, failure: errors.New("nested proof interrupted middle group member")}
			for i, parent := range parents {
				fault.ids[i] = parent.ID
			}
			rt.bus.SetInterceptors(fault)
			probe.releaseHeld()
			var failed nestedServingReceipt
			for i := 0; i < 2; i++ {
				select {
				case receipt := <-probe.receipts:
					if receipt.ParentEvent == notifyID {
						failed = receipt
					} else if receipt.Err != nil {
						t.Fatalf("unrelated registration failed: %v", receipt.Err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("actual interrupted grouped caller did not return")
				}
			}
			if failed.Key != held.Key || !errors.Is(failed.Err, fault.failure) || failed.Err.Error() != fault.failure.Error() || failed.Publications != 3 {
				t.Fatalf("actual grouped interruption receipt=%+v", failed)
			}
			if got := fault.snapshot(); got != [3]int{1, 1, 0} {
				t.Fatalf("group did not stop at middle member: visits=%v", got)
			}
			reader := nestedPublicReader(t, rt.selected)
			assertNestedDeliveredEvent(t, ctx, reader, parents[0].ID, 1)
			assertNestedPipelineReceipt(t, ctx, db, parents[0].ID, true)
			for _, parent := range parents[1:] {
				assertNestedPipelineReceipt(t, ctx, db, parent.ID, false)
				view, err := reader.LoadOperatorEvent(ctx, parent.ID)
				if err != nil || len(view.Deliveries) != 1 || view.Deliveries[0].Terminal || view.NoDelivery != nil {
					t.Fatalf("interrupted/unvisited real recipient settled early: %+v err=%v", view, err)
				}
			}
			var childIntents int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND flow_path='account'`, runID).Scan(&childIntents); err != nil || childIntents != 1 {
				t.Fatalf("interrupted suffix executed child producer: child intents=%d err=%v", childIntents, err)
			}
			var firstChild nestedServingReceipt
			select {
			case firstChild = <-probe.receipts:
				if firstChild.ParentEvent != parents[0].ID || firstChild.Err != nil || firstChild.Publications != 2 {
					t.Fatalf("healthy prefix child did not drain before recovery owner installation: %+v", firstChild)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("healthy prefix child did not return")
			}
			quietCtx, cancelQuiet := context.WithTimeout(ctx, 5*time.Second)
			defer cancelQuiet()
			if err := rt.bus.WaitForQuiescence(quietCtx); err != nil {
				t.Fatal(err)
			}

			fault.mu.Lock()
			fault.enabled = false
			fault.mu.Unlock()
			tailView, err := reader.LoadOperatorEvent(ctx, parents[2].ID)
			if err != nil || len(tailView.Deliveries) != 1 {
				t.Fatalf("suffix recovery requires exact persisted delivery: %+v err=%v", tailView, err)
			}
			// The default fixture owner models only live publication transfers.
			// Recovery must use the existing production continuation coordinator
			// with authority loaded from the actual persisted delivery snapshot.
			startNotifyAllChildrenDeliveryContinuations(t, ctx, rt.selected, rt, parents[2].ID, tailView.Deliveries[0].Route)
			// Repeated postcommit callbacks are checked after the pipeline owner
			// transfers persisted recovery to the continuation coordinator.
			replay := func(id string) {
				view, err := reader.LoadOperatorEvent(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				event, err := view.EventSnapshot()
				if err != nil {
					t.Fatal(err)
				}
				if err := rt.bus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{{Event: event}}); err != nil {
					t.Fatalf("actual repeated postcommit callback for %s: %v", id, err)
				}
			}
			// Both unfinished members still belong to pipeline recovery, not
			// delivery continuation recovery. The real bounded scan establishes
			// the durable handoff before the coordinator dispatches recipients.
			// It also clears any stale staged callback through its actual owner.
			if _, err := rt.bus.SweepPipelineObligations(ctx, 32); err != nil {
				t.Fatalf("real pipeline recovery of interrupted member: %v", err)
			}
			assertNestedPipelineReceipt(t, ctx, db, parents[2].ID, true)
			waitNotifyAllChildrenRuntime(t, rt, runID)
			if got := fault.snapshot(); got != [3]int{1, 2, 1} {
				t.Fatalf("recovery replayed committed prefix or lost suffix: visits=%v", got)
			}
			for _, parent := range parents {
				assertNestedPipelineReceipt(t, ctx, db, parent.ID, true)
				assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 1)
				replay(parent.ID)
			}
			if got := fault.snapshot(); got != [3]int{1, 2, 1} {
				t.Fatalf("duplicate committed callback executed recipients: visits=%v", got)
			}
			assertNestedSiblingTaskEffects(t, ctx, rt.selected, db, runID, "account.task.requested", "account.task.completed", parents)
			assertNestedExactBarrier(t, ctx, db, held.Key, "fired", fanoutbarrier.Summary{Total: 3, Succeeded: 3})
			assertNestedInterruptedFinalState(t, ctx, db, rt, runID, parents)
			finalParents := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, db, runID, notifyID)
			assertNotifyAllChildrenItemSequence(t, finalParents, accounts)
			for i := range parents {
				if finalParents[i].ID != parents[i].ID {
					t.Fatal("recovery rewrote committed parent prefix")
				}
			}
			counts := probe.snapshot()
			if counts.Active != 0 || counts.PeakActive != 1 || counts.Started != 5 || counts.Returned != 5 || counts.SuccessfulCommits != 5 || counts.CommitPlans != 0 || counts.Carriers != 0 || counts.LoadedItems != 0 {
				t.Fatalf("interrupted nested actual caller accounting: %+v", counts)
			}
			assertNestedExactBarrier(t, ctx, db, firstChild.Key, "fired", fanoutbarrier.Summary{Total: 2, Succeeded: 2})
			for i := 0; i < 2; i++ {
				select {
				case receipt := <-probe.receipts:
					if receipt.Err != nil || receipt.Publications != 2 {
						t.Fatalf("recovered real child turn=%+v", receipt)
					}
					assertNestedExactBarrier(t, ctx, db, receipt.Key, "fired", fanoutbarrier.Summary{Total: 2, Succeeded: 2})
				case <-time.After(5 * time.Second):
					t.Fatal("missing recovered nested child caller")
				}
			}
			summary, err := rt.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.Committed != 12 || summary.Owed != 0 || summary.SemanticRejected != 0 || summary.BarrierTerminal != 4 || summary.BlocksCompletion() {
				t.Fatalf("interrupted group final public summary=%+v err=%v", summary, err)
			}
			t.Log("B07 actual middle RetryRelease/error interrupts group; prefix success survives, bounded pipeline scan transfers middle/suffix to production continuation recovery, duplicate callbacks do not execute recipients, six task effects/four exact barriers; not full M29 failed-handoff disposal census")
		})
	}
}

func assertNestedPipelineReceipt(t *testing.T, ctx context.Context, db *sql.DB, id string, success bool) {
	t.Helper()
	var outcome string
	err := db.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, id).Scan(&outcome)
	if success && (err != nil || outcome != "success") {
		t.Fatalf("event %s pipeline receipt=%q err=%v", id, outcome, err)
	}
	if !success && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("event %s acquired premature pipeline receipt=%q err=%v", id, outcome, err)
	}
}

func assertNestedInterruptedFinalState(t *testing.T, ctx context.Context, db *sql.DB, rt notifyAllChildrenRuntime, runID string, parents []notifyAllChildrenItemEvent) {
	t.Helper()
	reader := nestedPublicReader(t, rt.selected)
	for _, parent := range parents {
		view := assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 1)
		name := eventidentity.ExternalizeForFlow(view.Deliveries[0].Target.FlowInstance, []string{"account.tasks.completed"}, "account.tasks.completed")
		id := loadNotifyAllChildrenSingleEventID(t, ctx, rt.selected, db, runID, name)
		completion, err := reader.LoadOperatorEvent(ctx, id)
		if err != nil || completion.Payload["account_id"] != parent.AccountID || completion.NoDelivery == nil || len(completion.Deliveries) != 0 {
			t.Fatalf("recovered child public barrier=%+v err=%v", completion, err)
		}
		for field, want := range map[string]float64{"total": 2, "succeeded": 2, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0} {
			if completion.Payload[field] != want {
				t.Fatalf("recovered child barrier %s=%v want=%v", field, completion.Payload[field], want)
			}
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT current_state,fields FROM entity_state WHERE run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := make(map[string]bool)
	for rows.Next() {
		var state string
		var raw []byte
		if err := rows.Scan(&state, &raw); err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if task, ok := fields["task"].(string); ok {
			account, _ := fields["account_id"].(string)
			key := account + ":" + task
			if state != "completed" || seen[key] || fields["task_key"] != key {
				t.Fatalf("recovered task state=%s fields=%v duplicate=%v", state, fields, seen[key])
			}
			seen[key] = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 6 {
		t.Fatalf("recovered task states=%v", seen)
	}
	for _, parent := range parents {
		for _, task := range []string{"prepare", "publish"} {
			if !seen[parent.AccountID+":"+task] {
				t.Fatalf("missing recovered task %s/%s", parent.AccountID, task)
			}
		}
	}
}
