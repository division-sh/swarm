package runforkpersistence

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestHistoricalPlanningResolverSeparatesExclusiveAndInclusiveCutsBothOrders(t *testing.T) {
	for _, first := range []string{"start", "inclusive"} {
		t.Run(first, func(t *testing.T) {
			tx, mock := startSnapshotTransaction(t)
			runID, eventID := eventtest.UUID("planning-variant-source"), eventtest.UUID("planning-variant-event")
			start, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointRunStart, 1, "")
			if err != nil {
				t.Fatal(err)
			}
			event, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointEvent, 1, eventID)
			if err != nil {
				t.Fatal(err)
			}
			resolver := capturedConstructionResolver{tx: tx, active: make(map[string]bool), snapshots: make(map[constructionCut]*runForkRevisionSnapshot)}
			loadStart := func() *runForkRevisionSnapshot {
				expectOriginalStart(t, mock, runID, runforkrevision.StartProjection{Version: 1,
					SourceBundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), FirstTurnEventID: eventID, Facts: []runforkrevision.StartFact{}})
				got, err := resolver.sourceCut(context.Background(), start)
				if err != nil || got == nil || got.StartProjection == nil || len(got.Events) != 0 {
					t.Fatalf("exclusive start lost exact absence: %+v/%v", got, err)
				}
				return got
			}
			loadInclusive := func() *runForkRevisionSnapshot {
				mock.ExpectQuery(`WITH bounded AS`).WithArgs(runID, int64(1), false).
					WillReturnRows(sqlmock.NewRows([]string{"run_id", "family", "fact_key", "first_revision", "revision", "fact"}).
						AddRow(runID, "events", eventID, 1, 1, `{"run_id":"`+runID+`","event_id":"`+eventID+`","event_name":"initial.requested","payload_base64":"e30="}`))
				got, err := resolver.inclusive(context.Background(), runID, 1)
				if err != nil || got == nil || got.StartProjection != nil || len(got.Events) != 1 || got.Events[0].EventID != eventID {
					t.Fatalf("inclusive cut lost its real event: %+v/%v", got, err)
				}
				return got
			}
			var exclusive, inclusive *runForkRevisionSnapshot
			if first == "start" {
				exclusive, inclusive = loadStart(), loadInclusive()
			} else {
				inclusive, exclusive = loadInclusive(), loadStart()
			}
			if exclusive == inclusive || len(resolver.snapshots) != 2 {
				t.Fatal("distinct cut variants shared a memo entry")
			}
			if got, err := resolver.sourceCut(context.Background(), start); err != nil || got != exclusive {
				t.Fatalf("start hit: %+v/%v", got, err)
			}
			if got, err := resolver.sourceCut(context.Background(), event); err != nil || got != inclusive {
				t.Fatalf("event hit: %+v/%v", got, err)
			}
			wrong, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointEvent, 1, eventtest.UUID("absent-planning-origin"))
			if err != nil {
				t.Fatal(err)
			}
			if got, err := resolver.sourceCut(context.Background(), wrong); err == nil || got != nil {
				t.Fatalf("hit skipped this origin's event boundary: %+v/%v", got, err)
			}
			if got, err := resolver.resolve(context.Background(), &runForkRevisionSnapshot{RunID: runID, Revision: 1, mutationsOnly: true}); err == nil || got != nil {
				t.Fatal("partial facts acquired complete-cut authority")
			}
			resolver.active[runID] = true
			if got, err := resolver.sourceCut(context.Background(), event); err == nil || got != nil {
				t.Fatal("memo hit hid a lineage cycle")
			}
			delete(resolver.active, runID)
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if got, err := resolver.inclusive(cancelled, runID, 1); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatal("inclusive hit ignored cancellation")
			}
			if got, err := resolver.sourceCut(cancelled, start); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatal("exclusive hit ignored cancellation")
			}
		})
	}
}

func TestHistoricalPlanningResolverKeepsNativeStartVariantsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openTimerHistoryNativeFixture(t, backend)
			runID := eventtest.UUID("native-planning-variant-" + backend)
			ctx := f.runContext(t, runID, time.Now().UTC())
			origin, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointRunStart, 1, "")
			if err != nil {
				t.Fatal(err)
			}
			for _, first := range []string{"start", "inclusive"} {
				t.Run(first, func(t *testing.T) {
					tx, err := f.db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					resolver := capturedConstructionResolver{tx: tx, active: make(map[string]bool), snapshots: make(map[constructionCut]*runForkRevisionSnapshot)}
					var start, inclusive *runForkRevisionSnapshot
					loadStart := func() {
						start, err = resolver.sourceCut(ctx, origin)
						if err != nil || start == nil || start.StartProjection == nil {
							t.Fatalf("native exclusive start: %+v/%v", start, err)
						}
					}
					loadInclusive := func() {
						inclusive, err = resolver.inclusive(ctx, runID, 1)
						if err != nil || inclusive == nil || inclusive.StartProjection != nil {
							t.Fatalf("native inclusive cut: %+v/%v", inclusive, err)
						}
					}
					if first == "start" {
						loadStart()
						loadInclusive()
					} else {
						loadInclusive()
						loadStart()
					}
					if start == inclusive || len(resolver.snapshots) != 2 {
						t.Fatal("native cut variants shared reconstruction authority")
					}
					if got, err := resolver.sourceCut(ctx, origin); err != nil || got != start {
						t.Fatalf("native exclusive hit changed: %+v/%v", got, err)
					}
					if got, err := resolver.inclusive(ctx, runID, 1); err != nil || got != inclusive {
						t.Fatalf("native inclusive hit changed: %+v/%v", got, err)
					}
					wrong, err := runlifecycle.ForkMaterializationRunOrigin(runID, runlifecycle.ForkOriginPointEvent, 1, eventtest.UUID("absent-native-planning-event"))
					if err != nil {
						t.Fatal(err)
					}
					if got, err := resolver.sourceCut(ctx, wrong); err == nil || got != nil {
						t.Fatalf("native inclusive hit bypassed origin validation: %+v/%v", got, err)
					}
				})
			}
		})
	}
}

func TestHistoricalPlanningContextRejectsConcurrentPrivateReaders(t *testing.T) {
	tx, _ := startSnapshotTransaction(t)
	scope := &historicalPlanningReconstruction{tx: tx}
	ctx := context.WithValue(context.Background(), historicalPlanningContextKey{}, scope)
	finish, err := beginHistoricalPlanningRead(ctx, tx, nil, runlifecycle.Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	refused := make(chan error, 1)
	go func() {
		end, err := beginHistoricalPlanningRead(ctx, tx, nil, runlifecycle.Snapshot{})
		if end != nil {
			_ = end()
		}
		refused <- err
	}()
	if err := <-refused; err == nil || !strings.Contains(err.Error(), "serialized planning owner") {
		t.Fatalf("concurrent private reader was not refused: %v", err)
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	end, err := beginHistoricalPlanningRead(ctx, tx, nil, runlifecycle.Snapshot{})
	if err != nil {
		t.Fatalf("refused concurrent reader poisoned its owner: %v", err)
	}
	if err := end(); err != nil {
		t.Fatal(err)
	}
}

func TestHistoricalEvidenceReconstructionCannotAdmitOrEscape(t *testing.T) {
	tx, _ := startSnapshotTransaction(t)
	run := runlifecycle.Snapshot{
		RunID: eventtest.UUID("evidence-private-lifetime"), State: runlifecycle.StateRunning,
		Origin: runlifecycle.ScenarioSetupRunOrigin(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), StartedAt: time.Now().UTC(),
	}
	base := context.Background()
	ctx, done, err := beginHistoricalEvidenceRead(base, tx, run)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := historicalPlanningForTx(ctx, tx)
	if err != nil || scope == nil || scope.source != nil || scope.resolver.tx != tx {
		t.Fatalf("evidence scope gained source admission or another transaction: %+v %v", scope, err)
	}
	source := struct{ semanticview.Source }{}
	if _, _, err := readHistoricalFlowInstanceConstructionTx(ctx, tx, source, run, pipeline.WorkflowInstance{}); err == nil || !strings.Contains(err.Error(), "admitted run or source") {
		t.Fatalf("evidence scope admitted a semantic source: %v", err)
	}
	if err := withHistoricalFlowInstancePlanningTx(ctx, tx, nil, run, func(context.Context) error { return nil }); err == nil {
		t.Fatal("evidence scope weakened the planning source requirement")
	}
	if _, _, err := beginHistoricalEvidenceRead(ctx, tx, run); err == nil || !strings.Contains(err.Error(), "serialized planning owner") {
		t.Fatalf("concurrent evidence reader acquired an active scope: %v", err)
	}
	wrong := run
	wrong.RunID = eventtest.UUID("foreign-evidence-run")
	if _, _, err := beginHistoricalEvidenceRead(ctx, tx, wrong); err == nil {
		t.Fatal("evidence scope crossed its run")
	}
	if err := done(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := beginHistoricalEvidenceRead(ctx, tx, run); err == nil || !strings.Contains(err.Error(), "lifetime has ended") {
		t.Fatalf("evidence context escaped its synchronous read: %v", err)
	}
	fresh, finish, err := beginHistoricalEvidenceRead(base, tx, run)
	if err != nil {
		t.Fatal(err)
	}
	freshScope, err := historicalPlanningForTx(fresh, tx)
	if err != nil || freshScope == scope || len(freshScope.resolver.snapshots) != 0 {
		t.Fatal("later evidence call borrowed an ended reconstruction")
	}
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(base)
	cancel()
	if _, _, err := beginHistoricalEvidenceRead(cancelled, tx, run); !errors.Is(err, context.Canceled) {
		t.Fatalf("evidence entry ignored cancellation: %v", err)
	}
}
