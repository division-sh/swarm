package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// One composed manifestation, not a recipient/topology cross-product: the
// real parent group delivers to both owners and admits child fan-out. The child
// group crosses a real node's immediate nested-publication dependency boundary.
func TestB17MixedNestedDependencyCapacityOneBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadCanonicalRoutingSource(t, b17MixedNestedSource(t))
			probe := newNestedServingProbe(t)
			childGate := newNestedChildHandlerGate(t, "account.task.requested")
			rt, db := b17MixedNestedRuntime(t, backend, source, probe, childGate)
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("B17 serving counters: %+v", probe.snapshot())
					dumpNotifyAllChildrenRuntimeState(t, context.Background(), rt.selected, db)
					for _, entry := range rt.diagnostics.snapshot() {
						t.Logf("B17 diagnostic: %+v", entry)
					}
				}
			})
			runID := uuid.NewString()
			ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
			runCtx := effects.WithExecutionMode(managedConformanceExecutionContextForBundle(t, ctx, "b17-mixed-nested", rt.sourceArtifactFact), rt.posture.RootMode())
			if err := rt.manager.Run(runCtx); err != nil {
				t.Fatal(err)
			}
			accounts := []string{"sibling-b", "sibling-a"}
			publishNotifyAllChildrenRunCreatingEvent(t, ctx, rt, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "portfolio-main"})
			publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": "portfolio-main", "account_ids": accounts})
			waitNotifyAllChildrenRuntime(t, rt, runID)
			before := probe.snapshot()
			probe.armPostCommitHold(t)
			notifyID := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{"portfolio_id": "portfolio-main", "command": "b17"})
			held := waitNestedServingPostCommit(t, probe)
			if held.ParentEvent != notifyID || held.Publications != len(accounts) {
				t.Fatalf("wrong parent turn: %+v", held)
			}
			parents := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, db, runID, notifyID)
			assertNotifyAllChildrenItemSequence(t, parents, accounts)
			probe.releaseHeld()
			signal := childGate.wait(t)
			reader := nestedPublicReader(t, rt.selected)
			deadline := time.Now().Add(5 * time.Second)
			var status string
			for time.Now().Before(deadline) {
				if err := db.QueryRowContext(ctx, `SELECT status FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, held.Key.RunID, held.Key.TriggeringDeliveryID, held.Key.ElementRef.FlowPath, held.Key.ElementRef.Family, held.Key.ElementRef.SemanticPath).Scan(&status); err != nil {
					t.Fatal(err)
				}
				if status == "fired" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			assertNestedExactBarrier(t, ctx, db, held.Key, "fired", fanoutbarrier.Summary{Total: 2, Succeeded: 2})
			for _, parent := range parents {
				view := assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 2)
				kinds := map[string]int{}
				for _, delivery := range view.Deliveries {
					kinds[delivery.SubscriberType]++
				}
				if kinds["node"] != 1 || kinds["agent"] != 1 {
					t.Fatalf("parent lacks exact real mixed recipients: %+v", view.Deliveries)
				}
				b17RequirePipelineReceipt(t, ctx, db, parent.ID, 1)
			}
			b17RequirePipelineReceipt(t, ctx, db, signal.EventID, 0)
			child, err := reader.LoadOperatorEvent(ctx, signal.EventID)
			if err != nil || len(child.Deliveries) != 1 || child.Deliveries[0].Terminal || child.NoDelivery != nil {
				t.Fatalf("held real grandchild was prematurely settled: %+v err=%v", child, err)
			}
			counts := probe.snapshot()
			if counts.Active != 1 || counts.PeakActive != 1 || counts.Started != before.Started+2 || counts.Carriers != 2 {
				t.Fatalf("nested mixed execution escaped the sole serving permit: %+v", counts)
			}
			var intents, owed int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cardinality-cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&intents, &owed); err != nil {
				t.Fatal(err)
			}
			if intents != 4 || owed != 2 {
				t.Fatalf("nested sibling backlog changed while one real handler is held: intents=%d owed=%d", intents, owed)
			}
			var nextTask string
			if err := db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND source_event_id=(SELECT source_event_id FROM events WHERE event_id=$2) AND event_name=(SELECT event_name FROM events WHERE event_id=$2) AND event_id<>$2`, runID, signal.EventID).Scan(&nextTask); err != nil {
				t.Fatal(err)
			}
			boundary := &b17NestedBoundary{db: db, predecessor: signal.EventID, current: nextTask, observed: make(chan error, 1)}
			rt.bus.SetInterceptors(boundary, rt.pipeline)
			childGate.open()
			select {
			case err := <-boundary.observed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("second real task emission never crossed the exact predecessor boundary")
			}
			waitNotifyAllChildrenRuntime(t, rt, runID)
			assertNestedServingDrained(t, probe)
			if boundary.calls.Load() != 1 {
				t.Fatalf("exact nested boundary executions=%d, want 1", boundary.calls.Load())
			}

			var parentReturned time.Time
			nested := map[string]nestedServingReceipt{}
			for i := 0; i < probe.snapshot().Returned; i++ {
				receipt := <-probe.receipts
				if receipt.Err != nil {
					t.Fatalf("actual finite turn failed: %+v", receipt)
				}
				if receipt.Key == held.Key {
					parentReturned = receipt.ReturnedAt
				}
				for _, parent := range parents {
					if receipt.ParentEvent == parent.ID {
						if _, duplicate := nested[parent.ID]; duplicate {
							t.Fatalf("duplicate nested turn for %s", parent.ID)
						}
						nested[parent.ID] = receipt
					}
				}
			}
			if parentReturned.IsZero() || len(nested) != 2 {
				t.Fatalf("missing real parent/nested return evidence: parent=%s nested=%+v", parentReturned, nested)
			}
			for _, parent := range parents {
				receipt := nested[parent.ID]
				if receipt.LoadedAt.Before(parentReturned) || receipt.Publications != 2 || receipt.Key == held.Key {
					t.Fatalf("recursive serving permit or incorrect child range: %+v", receipt)
				}
				assertNestedExactBarrier(t, ctx, db, receipt.Key, "fired", fanoutbarrier.Summary{Total: 2, Succeeded: 2})
				view := assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 2)
				flow := view.Deliveries[0].Target.FlowInstance
				name := eventidentity.ExternalizeForFlow(flow, []string{"account.notification.completed"}, "account.notification.completed")
				ids := nestedEventIDs(t, ctx, db, runID, name, parent.ID)
				if len(ids) != 1 {
					t.Fatalf("exact agent nested effect %s for %s: %v", name, parent.ID, ids)
				}
				effect, err := reader.LoadOperatorEvent(ctx, ids[0])
				if err != nil || effect.NoDelivery == nil || len(effect.Deliveries) != 0 {
					t.Fatalf("nested agent effect did not settle: %+v err=%v", effect, err)
				}
				taskName := eventidentity.ExternalizeForFlow(flow, []string{"account.task.requested"}, "account.task.requested")
				tasks := nestedEventIDs(t, ctx, db, runID, taskName, parent.ID)
				if len(tasks) != 2 {
					t.Fatalf("exact child range for %s: %v", parent.ID, tasks)
				}
				seen := map[string]bool{}
				for _, id := range tasks {
					task := assertNestedDeliveredEvent(t, ctx, reader, id, 1)
					value, _ := task.Payload["task"].(string)
					if (value != "prepare" && value != "publish") || seen[value] || task.Payload["account_id"] != parent.AccountID || task.Payload["task_key"] != parent.AccountID+":"+value {
						t.Fatalf("incorrect nested business identity: %+v", task)
					}
					seen[value] = true
					name := eventidentity.ExternalizeForFlow(task.Deliveries[0].Target.FlowInstance, []string{"account.task.completed"}, "account.task.completed")
					effects := nestedEventIDs(t, ctx, db, runID, name, id)
					if len(effects) != 1 {
						t.Fatalf("task %s effects=%v", id, effects)
					}
					effect, err := reader.LoadOperatorEvent(ctx, effects[0])
					if err != nil || effect.Payload["account_id"] != parent.AccountID || effect.Payload["task"] != value || effect.NoDelivery == nil || len(effect.Deliveries) != 0 {
						t.Fatalf("task business effect mismatch: %+v err=%v", effect, err)
					}
				}
			}
			summary, err := rt.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
			if err != nil || summary.BlocksCompletion() || summary.BarrierTerminal != 3 {
				t.Fatalf("mixed nested final obligation summary: %+v err=%v", summary, err)
			}
			t.Log("B17: exact predecessor flushed before real nested node dispatch; two mixed node/agent parents, four real task effects, three direct barriers, capacity one drained")
		})
	}
}

// The shared fixture starts serving with its Live author-activity context. This
// MockOnly fixture must supply its actual posture, without replacing the granted
// owner, coordinator, publication group, or any mutation/disposition result.
type b17MockServingExecutor struct{ *nestedServingProbe }

func (p b17MockServingExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	return p.nestedServingProbe.ServeFanOutCandidate(effects.WithExecutionMode(ctx, effects.ExecutionModeMock), owner, key)
}

func b17MixedNestedRuntime(t *testing.T, backend string, source semanticview.Source, probe *nestedServingProbe, child *nestedChildHandlerGate) (notifyAllChildrenRuntime, *sql.DB) {
	t.Helper()
	var selected notifyAllChildrenStore
	var db *sql.DB
	switch backend {
	case "postgres":
		var cleanup func()
		_, db, cleanup = testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		selected = storetest.AdmitPostgresRuntimeStore(t, db)
	case "sqlite":
		selected = storetest.StartSQLiteRuntimeStore(t)
		db = storetest.DatabaseForTest(selected)
	default:
		t.Fatalf("unsupported B17 backend %q", backend)
	}
	workers := 1
	rt := newNotifyAllChildrenRuntime(t, selected, db, source, time.Now, notifyAllChildrenRuntimeOptions{
		realMockAgents: true, enableGenericSchedules: true, fanOutWorkers: &workers,
		nestedPublications: &probe.publications,
		fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
			probe.PipelineCoordinator = pc
			return b17MockServingExecutor{probe}
		},
	})
	t.Cleanup(probe.releaseHeld)
	rt.pipeline.SetTestLifecycleProbe(child)
	t.Cleanup(child.open)
	return rt, db
}

type b17NestedBoundary struct {
	db                   *sql.DB
	predecessor, current string
	observed             chan error
	calls                atomic.Int32
}

func (p *b17NestedBoundary) Intercept(ctx context.Context, event events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	if eventidentity.LeafName(string(event.Type())) != "account.task.completed" || event.ParentEventID() != p.current {
		return true, nil, pipelineobligation.Continue(), nil
	}
	if p.calls.Add(1) != 1 {
		return false, nil, pipelineobligation.Continue(), fmt.Errorf("duplicate exact nested node dispatch")
	}
	var prior, current, handoffs int
	err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline' AND outcome='success'`, p.predecessor).Scan(&prior)
	if err == nil {
		err = p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, p.current).Scan(&current)
	}
	if err == nil {
		err = p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND continuation_handoff_at IS NOT NULL`, p.predecessor).Scan(&handoffs)
	}
	if err == nil && (prior != 1 || current != 0 || handoffs != 1) {
		err = fmt.Errorf("nested boundary: exact prior receipt=%d handoffs=%d, still-executing current receipts=%d", prior, handoffs, current)
	}
	select {
	case p.observed <- err:
	default:
		return false, nil, pipelineobligation.Continue(), fmt.Errorf("duplicate exact nested node dispatch")
	}
	return true, nil, pipelineobligation.Continue(), err
}

func b17RequirePipelineReceipt(t *testing.T, ctx context.Context, db *sql.DB, eventID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, eventID).Scan(&count); err != nil || count != want {
		t.Fatalf("exact pipeline receipt %s count=%d want=%d err=%v", eventID, count, want, err)
	}
}

func b17MixedNestedSource(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyNotifyAllChildrenNestedServing(t)
	agents, err := os.ReadFile(filepath.Join(canonicalrouting.RepoRoot(t), "examples/routing/notify-all-children/account/agents.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "account/agents.yaml"), agents, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []struct{ file, old, new string }{
		{"account/schema.yaml", "      - account.tasks.completed\n", "      - account.tasks.completed\n      - account.notification.completed\n"},
		{"account/events.yaml", "account.task.requested:\n", "account.notification.completed:\n  swarm:\n    consumer: external\naccount.task.requested:\n"},
	} {
		path := filepath.Join(root, edit.file)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(raw), edit.old) != 1 {
			t.Fatalf("B17 closed fixture replacement drift: %s", edit.file)
		}
		if err := os.WriteFile(path, []byte(strings.Replace(string(raw), edit.old, edit.new, 1)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
