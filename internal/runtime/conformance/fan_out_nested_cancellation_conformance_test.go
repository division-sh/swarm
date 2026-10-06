package conformance

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// Observe actual handler entries without replacing routing or execution. The
// first task remains held by the existing lifecycle gate before engine mutation.
type nestedCancellationHandlers struct {
	gate *nestedChildHandlerGate
	mu   sync.Mutex
	ids  []string
}

func (p *nestedCancellationHandlers) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.HandlerStarted && eventidentity.LeafName(signal.EventType) == p.gate.eventName {
		p.mu.Lock()
		p.ids = append(p.ids, signal.EventID)
		p.mu.Unlock()
	}
	p.gate.NotifyLifecycle(ctx, signal)
}

func (p *nestedCancellationHandlers) assertOnlyHeld(t *testing.T, ids ...string) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.ids) != len(ids) {
		t.Fatalf("partially dispatched group entered unexpected task handlers: %v, want %v", p.ids, ids)
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		seen[id] = true
	}
	for _, id := range p.ids {
		if !seen[id] {
			t.Fatalf("partially dispatched group entered unexpected task handler %s, want %v", id, ids)
		}
		delete(seen, id)
	}
	if len(seen) != 0 {
		t.Fatalf("partially dispatched group missed held task handlers: %v", seen)
	}
}

func TestIssue2394NestedHeldChildCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadCanonicalRoutingSource(t, canonicalrouting.CopyNotifyAllChildrenNestedServing(t))
			probe := newNestedServingProbe(t)
			probe.pauseAccountTurn = 2
			probe.accountTurnPaused = make(chan struct{}, 1)
			gate := newNestedChildHandlerGateCount(t, "account.task.requested", 2)
			rt, db := newNestedServingRuntime(t, backend, source, nil, probe, gate)
			handlers := &nestedCancellationHandlers{gate: gate}
			rt.pipeline.SetTestLifecycleProbe(handlers)
			selectedControl, ok := rt.selected.(runcontrol.Store)
			if !ok {
				t.Fatal("nested cancellation requires the actual selected run-control owner")
			}
			controller := runcontrol.NewController(selectedControl, rt.bus, runcontrol.Options{TimerCancellations: rt.pipeline.TimerCancellationReconciler()})
			rt.bus.SetRunDispatchGate(controller)
			runID := uuid.NewString()
			ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
			if err := rt.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "nested-held-child-stop", rt.sourceArtifactFact)); err != nil {
				t.Fatal(err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, rt, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "portfolio-main"})
			accounts := []string{"sibling-c", "sibling-a", "sibling-b"}
			publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": "portfolio-main", "account_ids": accounts})
			waitNotifyAllChildrenRuntime(t, rt, runID)
			notifyID := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{"portfolio_id": "portfolio-main", "command": "cancel-nested-proof"})
			held := gate.wait(t)
			secondHeld := gate.wait(t)
			select {
			case <-probe.accountTurnPaused:
			case <-time.After(5 * time.Second):
				t.Fatal("second child turn did not reach pre-claim gate")
			}
			var parent, issued nestedServingReceipt
			for deadline := time.After(5 * time.Second); parent.ReturnedAt.IsZero() || issued.Key.RunID == ""; {
				select {
				case receipt := <-probe.receipts:
					if receipt.ParentEvent == notifyID {
						parent = receipt
					}
					if receipt.Key.ElementRef.FlowPath == "account" && receipt.Publications == 2 && receipt.Err == nil {
						issued = receipt
					}
				case <-deadline:
					t.Fatal("parent and issued child did not return before held child cancellation")
				}
			}
			if parent.ReturnedAt.IsZero() || parent.Publications != 3 || parent.Err != nil {
				t.Fatalf("actual parent prefix receipt=%+v", parent)
			}
			deadline := time.Now().Add(5 * time.Second)
			var parentStatus string
			for time.Now().Before(deadline) {
				if err := db.QueryRowContext(ctx, `SELECT status FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, parent.Key.RunID, parent.Key.TriggeringDeliveryID, parent.Key.ElementRef.FlowPath, parent.Key.ElementRef.Family, parent.Key.ElementRef.SemanticPath).Scan(&parentStatus); err != nil {
					t.Fatal(err)
				}
				if parentStatus == "fired" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			assertNestedExactBarrier(t, ctx, db, parent.Key, "fired", fanoutbarrier.Summary{Total: 3, Succeeded: 3})
			reader := nestedPublicReader(t, rt.selected)
			parents := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, runID, notifyID)
			assertNotifyAllChildrenItemSequence(t, parents, accounts)
			parentViews := make(map[string]operatorread.OperatorEventFull, len(parents))
			for _, event := range parents {
				parentViews[event.ID] = assertNestedDeliveredEvent(t, ctx, reader, event.ID, 1)
				assertNestedPipelineReceipt(t, ctx, db, event.ID, true)
			}
			heldView, err := reader.LoadOperatorEvent(ctx, held.EventID)
			if err != nil || len(heldView.Deliveries) != 1 || heldView.Deliveries[0].Terminal {
				t.Fatalf("held child before stop=%+v err=%v", heldView, err)
			}
			path := heldView.Deliveries[0].Target.FlowInstance
			var beforeState string
			var beforeFields []byte
			var beforeRevision int64
			if err := db.QueryRowContext(ctx, `SELECT current_state,fields,revision FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, path).Scan(&beforeState, &beforeFields, &beforeRevision); err != nil {
				t.Fatal(err)
			}
			if beforeState != "pending" {
				t.Fatalf("child mutated before execution gate: %s", beforeState)
			}
			loadPrefix := func() []string {
				rows, err := db.QueryContext(ctx, `SELECT event_id FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed' ORDER BY event_id`, runID)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var ids []string
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, id)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return ids
			}
			prefix := loadPrefix()
			if len(prefix) != 8 {
				t.Fatalf("expected registration3/parent3/held-child2 exact prefix, got %v", prefix)
			}
			counts := probe.snapshot()
			if counts.Active != 0 || counts.Started != counts.Returned || counts.FirstError != nil {
				t.Fatalf("pre-claim pause must leave no live serving turn with the durable eight-event prefix: %+v", counts)
			}
			handlers.assertOnlyHeld(t, held.EventID, secondHeld.EventID)
			triggerDeadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(triggerDeadline) {
				var outcome string
				err := db.QueryRowContext(ctx, `SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, notifyID).Scan(&outcome)
				if err == nil && outcome == "success" {
					break
				}
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					t.Fatal(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			assertNestedPipelineReceipt(t, ctx, db, notifyID, true)
			// The second child has not claimed an ordinal. Fence the occurrence,
			// join that paused turn, then stop without preempting the committed group.
			rt.workOwner.Retire()
			probe.releaseHeld()
			rt.fanOutServing.Close()
			quiescence, cancelQuiescence := context.WithTimeout(ctx, 5*time.Second)
			defer cancelQuiescence()
			if err := rt.bus.WaitForQuiescence(quiescence); err != nil {
				t.Fatal(err)
			}
			handlers.assertOnlyHeld(t, held.EventID, secondHeld.EventID)
			stopCtx, cancelStop := context.WithTimeout(ctx, 5*time.Second)
			defer cancelStop()
			result, err := controller.Stop(stopCtx, runcontrol.TransitionRequest{RunID: runID, Reason: "nested-held-child-proof", ControlledBy: "conformance"})
			if err != nil || result.Status != runcontrol.StatusCancelled || result.Recovery.Err != nil {
				t.Fatalf("production run-control cancellation=%+v err=%v", result, err)
			}
			gate.open()
			var afterState string
			var afterFields []byte
			var afterRevision int64
			if err := db.QueryRowContext(ctx, `SELECT current_state,fields,revision FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, path).Scan(&afterState, &afterFields, &afterRevision); err != nil {
				t.Fatal(err)
			}
			if afterState != beforeState || afterRevision != beforeRevision || string(afterFields) != string(beforeFields) || !reflect.DeepEqual(loadPrefix(), prefix) {
				t.Fatalf("canceled held child mutated entity/prefix: state=%s/%s revision=%d/%d fields=%s/%s", beforeState, afterState, beforeRevision, afterRevision, beforeFields, afterFields)
			}
			for _, id := range []string{held.EventID, secondHeld.EventID} {
				post, err := reader.LoadOperatorEvent(ctx, id)
				if err != nil || len(post.Deliveries) != 1 || !post.Deliveries[0].Terminal || post.Deliveries[0].Status != string(deliverylifecycle.StatusDeadLetter) || post.Deliveries[0].ReasonCode != "run_stopped" {
					t.Fatalf("canceled child %s public terminal disposition=%+v err=%v", id, post, err)
				}
				effectName := eventidentity.ExternalizeForFlow(post.Deliveries[0].Target.FlowInstance, []string{"account.task.completed"}, "account.task.completed")
				if ids := nestedEventIDs(t, ctx, db, runID, effectName, id); len(ids) != 0 {
					t.Fatalf("canceled held child %s emitted business effects: %v", id, ids)
				}
			}
			assertNestedExactBarrier(t, ctx, db, parent.Key, "fired", fanoutbarrier.Summary{Total: 3, Succeeded: 3})
			for _, event := range parents {
				view := assertNestedDeliveredEvent(t, ctx, reader, event.ID, 1)
				if !reflect.DeepEqual(view, parentViews[event.ID]) {
					t.Fatalf("cancellation rewrote acknowledged parent publication %s: before=%+v after=%+v", event.ID, parentViews[event.ID], view)
				}
				assertNestedPipelineReceipt(t, ctx, db, event.ID, true)
			}
			rows, err := db.QueryContext(ctx, `SELECT triggering_delivery_id,flow_path,declaration_family,semantic_path FROM fan_out_intents WHERE run_id=$1 AND flow_path='account'`, runID)
			if err != nil {
				t.Fatal(err)
			}
			var children []fanoutobligation.IntentKey
			for rows.Next() {
				key := fanoutobligation.IntentKey{RunID: runID}
				if err := rows.Scan(&key.TriggeringDeliveryID, &key.ElementRef.FlowPath, &key.ElementRef.Family, &key.ElementRef.SemanticPath); err != nil {
					t.Fatal(err)
				}
				children = append(children, key)
			}
			err = rows.Err()
			rows.Close()
			if err != nil || len(children) != 3 {
				t.Fatalf("nested cancellation exact keys=%+v err=%v", children, err)
			}
			for _, key := range children {
				want := fanoutbarrier.Summary{Total: 2, Canceled: 2}
				if key == issued.Key {
					// Issued events retain their identity and receive run_stopped
					// delivery terminalization; only unissued ordinals are canceled.
					want = fanoutbarrier.Summary{Total: 2, DeadLettered: 2}
				}
				assertNestedExactBarrier(t, ctx, db, key, string(fanoutbarrier.StatusSuppressedRunTerminal), want)
			}
			summary, err := rt.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.Committed != 8 || summary.Canceled != 4 || summary.SemanticRejected != 0 || summary.Owed != 0 || summary.BlocksCompletion() {
				t.Fatalf("canceled nested public summary=%+v err=%v", summary, err)
			}
			counts = probe.snapshot()
			if counts.Active != 0 || counts.Started != counts.Returned || counts.LoadedItems != 0 || counts.CommitPlans != 0 || counts.Carriers != 0 {
				t.Fatalf("canceled actual nested callers did not join: %+v", counts)
			}
			handlers.assertOnlyHeld(t, held.EventID, secondHeld.EventID)
			t.Logf("B16/M24 occurrence cancellation joins both held deliveries and the pre-claim turn before Stop: exact8-event prefix,4 canceled unissued ordinals,3 suppressed child barriers,parent3-success barrier unchanged,held business state unchanged")
		})
	}
}
