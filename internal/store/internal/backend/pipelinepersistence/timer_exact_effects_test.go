package pipelinepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/backend/workflowtimer"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// This exercises physical writers and their shared collector, not the finalizer.
func TestWorkflowTimerExactEffectsReplacementAndCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "timer-effects.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			schema := `CREATE TABLE timers (
				timer_id TEXT PRIMARY KEY, run_id TEXT, timer_name TEXT, entity_id TEXT,
				flow_scope_key TEXT, flow_instance_id TEXT, flow_instance TEXT,
				fire_event TEXT, fire_payload TEXT, routing_source TEXT, execution_mode TEXT,
				fire_at TIMESTAMP, recurring BOOLEAN, recurrence_interval TEXT,
				owner_node TEXT, owner_agent TEXT, owner_kind TEXT, task_type TEXT,
				status TEXT, created_at TIMESTAMP, fired_at TIMESTAMP, source_timer_id TEXT,
				forked_from_run_id TEXT, forked_from_event_id TEXT, reconstruction_owner TEXT)`
			if backend == "postgres" {
				for _, column := range []string{"timer_id", "run_id", "entity_id", "source_timer_id", "forked_from_run_id", "forked_from_event_id"} {
					schema = strings.ReplaceAll(schema, column+" TEXT", column+" UUID")
				}
			}
			if _, err := db.Exec(schema); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			runID, entityID := uuid.NewString(), uuid.NewString()
			source, err := events.NewRootRoutingSource(entityID)
			if err != nil {
				t.Fatal(err)
			}
			at := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
			old := runtimepipeline.WorkflowTimerActivation{
				Ref: timeridentity.WorkflowTimerActivationRef{
					ActivationID: uuid.NewString(), DeclarationKey: "waiting.timeout",
					DeclarationRevision: "old", Cause: timeridentity.WorkflowTimerActivationCauseInitial,
				},
				RunID: runID, EntityID: entityID, Route: flowidentity.StoredRoute(".", runID, runID),
				RoutingSource: source, OwnerAgent: "timer-owner", EventType: "timer.elapsed",
				Payload: []byte(`{}`), ExecutionMode: executionmode.Live,
				CreatedAt: at, FireAt: at.Add(time.Hour), Status: "active",
			}
			next := old
			next.Ref.ActivationID, next.Ref.DeclarationRevision = uuid.NewString(), "new"
			other := old
			other.Ref.ActivationID, other.RunID = uuid.NewString(), uuid.NewString()
			mutate := func(effects *revisionEffects, kind runtimepipeline.WorkflowTimerMutationKind, activation runtimepipeline.WorkflowTimerActivation, wantChanged bool) {
				t.Helper()
				ref, changed, err := commitWorkflowEngineTimerMutation(ctx, tx, backend == "postgres", effects,
					runtimepipeline.WorkflowTimerMutation{Kind: kind, Activation: activation})
				if err != nil || changed != wantChanged || ref != activation.Ref {
					t.Fatalf("%s %s: ref=%+v changed=%v, want %v: %v", kind, activation.Ref.ActivationID, ref, changed, wantChanged, err)
				}
			}
			assertEffects := func(got *revisionEffects, ids ...string) {
				t.Helper()
				want := runforkrevision.NewEffects()
				for _, id := range ids {
					if err := want.AddFact(runID, runforkrevision.FamilyTimers, id); err != nil {
						t.Fatal(err)
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("timer contributions are not exactly %v", ids)
				}
			}
			seed := runforkrevision.NewEffects()
			mutate(seed, runtimepipeline.WorkflowTimerMutationInsert, old, true)
			assertEffects(seed, old.Ref.ActivationID)
			mutate(runforkrevision.NewEffects(), runtimepipeline.WorkflowTimerMutationInsert, other, true)

			// A replacement retains both identities in one transaction/collector.
			replacement := runforkrevision.NewEffects()
			mutate(replacement, runtimepipeline.WorkflowTimerMutationCancel, old, true)
			mutate(replacement, runtimepipeline.WorkflowTimerMutationInsert, next, true)
			assertEffects(replacement, old.Ref.ActivationID, next.Ref.ActivationID)
			replay := runforkrevision.NewEffects()
			mutate(replay, runtimepipeline.WorkflowTimerMutationCancel, old, false)
			mutate(replay, runtimepipeline.WorkflowTimerMutationInsert, next, false)
			assertEffects(replay)

			cancel := runforkrevision.NewEffects()
			refs, err := workflowtimer.CancelRunsTx(ctx, tx, backend == "postgres", cancel, []string{runID, runID})
			if err != nil || len(refs) != 1 || refs[0].ActivationID != next.Ref.ActivationID {
				t.Fatalf("enumerated cancellation = %+v: %v", refs, err)
			}
			assertEffects(cancel, next.Ref.ActivationID)
			noop := runforkrevision.NewEffects()
			refs, err = workflowtimer.CancelRunsTx(ctx, tx, backend == "postgres", noop, []string{runID})
			if err != nil || len(refs) != 0 {
				t.Fatalf("cancellation replay = %+v: %v", refs, err)
			}
			assertEffects(noop)
			for _, spelling := range []string{
				"AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE",
				"bbbbbbbbcccc4ddd8eeeffffffffffff",
			} {
				variant := old
				variant.Ref.ActivationID = spelling
				variant.Recurring, variant.RecurrenceInterval = true, time.Hour
				storedID := spelling
				if backend == "postgres" {
					storedID = uuid.MustParse(spelling).String()
				}
				insertEffects := runforkrevision.NewEffects()
				mutate(insertEffects, runtimepipeline.WorkflowTimerMutationInsert, variant, true)
				assertEffects(insertEffects, storedID)
				advanceEffects := runforkrevision.NewEffects()
				if _, err := advanceWorkflowEngineTimerOccurrence(ctx, tx, backend == "postgres", advanceEffects, variant, variant.FireAt); err != nil {
					t.Fatal(err)
				}
				assertEffects(advanceEffects, storedID)
				cancelEffects := runforkrevision.NewEffects()
				mutate(cancelEffects, runtimepipeline.WorkflowTimerMutationCancel, variant, true)
				assertEffects(cancelEffects, storedID)
			}
			for _, item := range []struct {
				activation runtimepipeline.WorkflowTimerActivation
				status     string
			}{{old, "cancelled"}, {next, "cancelled"}, {other, "active"}} {
				persisted, found, err := loadWorkflowEngineTimerActivation(ctx, tx, backend == "postgres", item.activation.Ref)
				if err != nil || !found || persisted.Status != item.status {
					t.Fatalf("timer %s: found=%v status=%s want=%s: %v", item.activation.Ref.ActivationID, found, persisted.Status, item.status, err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
