package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestIssue2394NestedGroupHandoffAcknowledgmentLossBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadCanonicalRoutingSource(t, canonicalrouting.CopyNotifyAllChildrenNestedServing(t))
			probe := newNestedServingProbe(t)
			rt, db := newNestedServingRuntime(t, backend, source, nil, probe)
			t.Cleanup(func() {
				if !t.Failed() {
					return
				}
				t.Logf("interrupted group counts: %+v active_work=%d", probe.snapshot(), rt.workOwner.ActiveCount())
				for _, entry := range rt.diagnostics.snapshot() {
					t.Logf("interrupted group diagnostic: %+v", entry)
				}
				dumpNotifyAllChildrenRuntimeState(t, context.Background(), rt.selected, db)
			})
			runID := uuid.NewString()
			ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
			if err := rt.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "nested-interrupted-group", rt.sourceArtifactFact)); err != nil {
				t.Fatal(err)
			}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, rt, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "portfolio-main"})
			accounts := []string{"sibling-c", "sibling-a", "sibling-b"}
			publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": "portfolio-main", "account_ids": accounts})
			waitNotifyAllChildrenRuntime(t, rt, runID)
			probe.armPostCommitHold(t)
			notifyID := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{"portfolio_id": "portfolio-main", "command": "interrupted-group"})
			held := waitNestedServingPostCommit(t, probe)
			parents := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, db, runID, notifyID)
			assertNotifyAllChildrenItemSequence(t, parents, accounts)
			if held.ParentEvent != notifyID || held.Publications != 3 || len(parents) != 3 {
				t.Fatalf("interrupted group requires real exact three-member commit: %+v parents=%+v", held, parents)
			}
			fault := errors.New("nested proof lost post-commit handoff acknowledgement")
			probe.publications.mu.Lock()
			probe.publications.dispatchFailure = fault
			probe.publications.mu.Unlock()
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
			if failed.Key != held.Key || !errors.Is(failed.Err, fault) || failed.Publications != 3 {
				t.Fatalf("committed group did not report exact lost handoff acknowledgement: %+v", failed)
			}
			reader := nestedPublicReader(t, rt.selected)
			for _, parent := range parents {
				view, err := reader.LoadOperatorEvent(ctx, parent.ID)
				if err != nil || len(view.Deliveries) != 1 || view.NoDelivery != nil {
					t.Fatalf("committed group member lost its durable delivery: %+v err=%v", view, err)
				}
			}
			rt.bus.SignalDeliveryContinuations()
			waitNotifyAllChildrenRuntimeWithin(t, rt, runID, 30*time.Second)
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
			for _, parent := range parents {
				assertNestedPipelineReceipt(t, ctx, db, parent.ID, true)
				assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 1)
				replay(parent.ID)
			}
			waitNotifyAllChildrenRuntimeWithin(t, rt, runID, 30*time.Second)
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
			if counts.Active != 0 || counts.PeakActive != 1 || counts.Started != 5 || counts.Returned != 5 || counts.SuccessfulCommits != 5 || counts.CommitPlans != 0 || counts.Carriers != 0 || counts.LoadedItems != 0 || !errors.Is(counts.FirstError, fault) {
				t.Fatalf("interrupted nested actual caller accounting: %+v", counts)
			}
			probe.publications.mu.Lock()
			life := &probe.publications
			live, acquired, returned, released := len(life.live), life.acquired, life.returned, life.released
			groupDispatched, acknowledgmentLost, lifeErr := life.groupDispatched, life.groupAcknowledgmentLost, life.err
			probe.publications.mu.Unlock()
			if lifeErr != nil || acknowledgmentLost != 3 || live != 0 || acquired != returned+released || groupDispatched != returned {
				t.Fatalf("lost acknowledgment did not preserve exact durable group transfer: live=%d acquired=%d returned=%d released=%d dispatched=%d acknowledgment_lost=%d err=%v", live, acquired, returned, released, groupDispatched, acknowledgmentLost, lifeErr)
			}
			seenChildren := make(map[string]bool, len(parents))
			for range parents {
				select {
				case receipt := <-probe.receipts:
					if receipt.Err != nil || receipt.Publications != 2 {
						t.Fatalf("recovered real child turn=%+v", receipt)
					}
					if seenChildren[receipt.ParentEvent] {
						t.Fatalf("child group executed twice: %+v", receipt)
					}
					seenChildren[receipt.ParentEvent] = true
					assertNestedExactBarrier(t, ctx, db, receipt.Key, "fired", fanoutbarrier.Summary{Total: 2, Succeeded: 2})
				case <-time.After(5 * time.Second):
					t.Fatal("missing recovered nested child caller")
				}
			}
			for _, parent := range parents {
				if !seenChildren[parent.ID] {
					t.Fatalf("missing recovered child group for %s", parent.ID)
				}
			}
			summary, err := rt.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.Committed != 12 || summary.Owed != 0 || summary.SemanticRejected != 0 || summary.BarrierTerminal != 4 || summary.BlocksCompletion() {
				t.Fatalf("interrupted group final public summary=%+v err=%v", summary, err)
			}
			t.Log("B07 post-commit acknowledgment loss leaves all three members with durable continuation ownership; repeated callbacks do not duplicate recipients, six task effects and four exact barriers survive")
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
