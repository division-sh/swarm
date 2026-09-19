package conformance

import (
	"context"
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

func (p *nestedCancellationHandlers) assertOnlyHeld(t *testing.T, id string) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if !reflect.DeepEqual(p.ids, []string{id}) {
		t.Fatalf("partially dispatched group entered unexpected task handlers: %v, want only %s", p.ids, id)
	}
}

func TestIssue2394NestedHeldChildCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadCanonicalRoutingSource(t, canonicalrouting.CopyNotifyAllChildrenNestedServing(t))
			probe := newNestedServingProbe(t)
			gate := newNestedChildHandlerGate(t, "account.task.requested")
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
			var parent nestedServingReceipt
			for i := 0; i < 2; i++ {
				select {
				case receipt := <-probe.receipts:
					if receipt.ParentEvent == notifyID {
						parent = receipt
					}
				case <-time.After(5 * time.Second):
					t.Fatal("parent did not return before held child cancellation")
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
			parents := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, db, runID, notifyID)
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
			if counts.Active != 1 || counts.Started != 3 || counts.Returned != 2 || counts.Carriers != 2 || counts.CommitPlans != 2 {
				t.Fatalf("cancellation requires a held, partially dispatched two-member child group: %+v", counts)
			}
			handlers.assertOnlyHeld(t, held.EventID)
			// Stop cannot steal a live foreground publication claim. Cancel the
			// actual runtime occurrence and join its interrupted group first;
			// then issue one parent stop, never retry busy as cancellation success.
			rt.workOwner.Retire()
			var interrupted nestedServingReceipt
			select {
			case receipt := <-probe.receipts:
				if receipt.Publications != 2 || receipt.Key.ElementRef.FlowPath != "account" || !errors.Is(receipt.Err, context.Canceled) {
					t.Fatalf("actual child group did not return cancellation: %+v", receipt)
				}
				interrupted = receipt
				t.Logf("actual partially dispatched child group joined: result=%+v err=%v", receipt.Result, receipt.Err)
			case <-time.After(5 * time.Second):
				t.Fatal("held child caller did not join after actual occurrence cancellation")
			}
			rt.fanOutServing.Close()
			handlers.assertOnlyHeld(t, held.EventID)
			stopCtx, cancelStop := context.WithTimeout(ctx, 5*time.Second)
			defer cancelStop()
			result, err := controller.Stop(stopCtx, runcontrol.TransitionRequest{RunID: runID, Reason: "nested-held-child-proof", ControlledBy: "conformance"})
			if err != nil || result.Status != runcontrol.StatusCancelled || result.Recovery.Err != nil {
				t.Fatalf("production run-control cancellation=%+v err=%v", result, err)
			}
			gate.open()
			quiescence, cancelQuiescence := context.WithTimeout(ctx, 5*time.Second)
			defer cancelQuiescence()
			if err := rt.bus.WaitForQuiescence(quiescence); err != nil {
				t.Fatal(err)
			}
			var afterState string
			var afterFields []byte
			var afterRevision int64
			if err := db.QueryRowContext(ctx, `SELECT current_state,fields,revision FROM entity_state WHERE run_id=$1 AND flow_instance=$2`, runID, path).Scan(&afterState, &afterFields, &afterRevision); err != nil {
				t.Fatal(err)
			}
			if afterState != beforeState || afterRevision != beforeRevision || string(afterFields) != string(beforeFields) || !reflect.DeepEqual(loadPrefix(), prefix) {
				t.Fatalf("canceled held child mutated entity/prefix: state=%s/%s revision=%d/%d fields=%s/%s", beforeState, afterState, beforeRevision, afterRevision, beforeFields, afterFields)
			}
			post, err := reader.LoadOperatorEvent(ctx, held.EventID)
			if err != nil || len(post.Deliveries) != 1 || !post.Deliveries[0].Terminal || post.Deliveries[0].Status != string(deliverylifecycle.StatusDeadLetter) || post.Deliveries[0].ReasonCode != "run_stopped" {
				t.Fatalf("canceled child public terminal disposition=%+v err=%v", post, err)
			}
			effectName := eventidentity.ExternalizeForFlow(path, []string{"account.task.completed"}, "account.task.completed")
			if ids := nestedEventIDs(t, ctx, db, runID, effectName, held.EventID); len(ids) != 0 {
				t.Fatalf("canceled held child emitted business effects: %v", ids)
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
				if key == interrupted.Key {
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
			handlers.assertOnlyHeld(t, held.EventID)
			t.Logf("B16/M24 actual occurrence cancellation joins partially dispatched child group, then one production Stop commits: exact8-event prefix,4 canceled unissued ordinals,3 suppressed child barriers,parent3-success barrier unchanged,held business state unchanged; not live-claim preemption or full runtime shutdown qualification")
		})
	}
}
