package runtimepersistence

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// H07/H10: optional references and provenance do not become new local foreign
// keys. All variants are arranged before the canonical projection is written.
func TestRunForkHistoricalContextOptionalReferencesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, cell := range []string{"cause_free_mutation", "stateless_turn_without_session", "independent_audit", "dead_letter_without_delivery", "timer_ancestor_provenance", "same_id_different_family", "text_reply_id"} {
				t.Run(cell, func(t *testing.T) {
					f := newHistoricalContextFixtureWithSeed(t, backend, func(t *testing.T, tx *sql.Tx, f historicalContextFixture) {
						ctx, s := testAuthorActivityContext(), f.source
						switch cell {
						case "cause_free_mutation":
							mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE entity_mutations SET caused_by_event=NULL WHERE mutation_id=$1`, s.mutationID)
						case "stateless_turn_without_session":
							mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE agent_turns SET session_id=$1,memory_enabled=FALSE,memory_source='platform_default' WHERE turn_id=$2`, uuid.NewString(), s.turnID)
							var memory bool
							if err := tx.QueryRowContext(ctx, `SELECT memory_enabled FROM agent_turns WHERE turn_id=$1`, s.turnID).Scan(&memory); err != nil || memory {
								t.Fatalf("live turn must be stateless before canonical capture: memory=%v err=%v", memory, err)
							}
						case "timer_ancestor_provenance":
							parentTimer, parentEvent := uuid.NewString(), uuid.NewString()
							seedRunForkRevisionMatrixEvent(t, ctx, tx, f.foreignID, parentEvent, s.at, f.postgres)
							mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO timers (timer_id,timer_name,schedule_scope,schedule_key,immutable_hash,run_id,fire_event,fire_payload,routing_source,execution_mode,fire_at,initial_fire_at,recurring,owner_node,owner_kind,due_basis_kind,due_basis_absolute,task_type,status,created_at) VALUES ($1,'ancestor-timer','run','ancestor-key','ancestor-hash',$2,'matrix.fire','{}','{"kind":"root"}','live',$3,$3,FALSE,'matrix-node','system','absolute',$3,'timer','active',$4)`, parentTimer, f.foreignID, s.at.Add(time.Hour), s.at)
							parentEffects, err := runforkrevision.ForRun(f.foreignID, runforkrevision.AllFamilies()...)
							if err != nil {
								t.Fatal(err)
							}
							if _, err := finalizeRunForkRevisionMatrix(ctx, tx, f.postgres, parentEffects); err != nil {
								t.Fatal(err)
							}
							mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE timers SET forked_from_run_id=$1,source_timer_id=$2,forked_from_event_id=$3 WHERE timer_id=$4`, f.foreignID, parentTimer, parentEvent, s.timerID)
						case "same_id_different_family":
							mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE agent_conversation_audits SET session_id=$1 WHERE session_id=$2`, s.sessionID, s.auditID)
						}
					})
					for _, fact := range f.facts {
						fields := historicalContextFields(t, fact.raw)
						switch {
						case cell == "cause_free_mutation" && fact.family == "entity_mutations":
							if fields["caused_by_event"] != nil || len(f.initial.Entities) != 1 || f.initial.Entities[0].Fields["name"] != "Matrix Entity" {
								t.Fatalf("cause-free historical reconstruction = %#v plan=%#v", fields, f.initial.Entities)
							}
						case cell == "stateless_turn_without_session" && fact.family == "agent_turns":
							if _, exists := fields["memory_enabled"]; exists {
								t.Fatalf("historical turn unexpectedly expanded its schema: %#v", fields)
							}
							historicalContextRequireZero(t, f.db, `SELECT COUNT(*) FROM agent_sessions WHERE session_id=$1`, fields["session_id"])
						case cell == "independent_audit" && fact.family == "agent_conversation_audits":
							historicalContextRequireZero(t, f.db, `SELECT COUNT(*) FROM agent_sessions WHERE session_id=$1`, fields["session_id"])
						case cell == "dead_letter_without_delivery" && fact.family == "dead_letters":
							if fields["delivery_id"] != "" {
								t.Fatalf("optional delivery fixture gained delivery: %#v", fields)
							}
						case cell == "timer_ancestor_provenance" && fact.family == "timers":
							if fields["run_id"] != f.source.runID || fields["forked_from_run_id"] != f.foreignID {
								t.Fatalf("timer owner/provenance changed: %#v", fields)
							}
						case cell == "same_id_different_family" && fact.family == "agent_conversation_audits":
							if fact.key != f.source.sessionID {
								t.Fatalf("audit/session cross-family key control = %q", fact.key)
							}
						case cell == "text_reply_id" && fact.family == "reply_contexts":
							if _, err := uuid.Parse(fact.key); err == nil || fact.key != f.source.replyID {
								t.Fatalf("reply key must remain exact non-UUID TEXT: %q", fact.key)
							}
						}
					}
					before := historicalContextDatabaseRows(t, f.db, f.postgres)
					got, err := f.planner.PlanRunFork(testAuthorActivityContext(), runfork.RunForkPlanRequest{SourceRunID: f.source.runID, At: f.anchorID})
					if err != nil || !reflect.DeepEqual(got, f.initial) {
						t.Fatalf("lawful optional-reference fixed-R plan: %v\ngot=%#v\nwant=%#v", err, got, f.initial)
					}
					historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, f.db, f.postgres))
				})
			}
		})
	}
}

func TestRunForkHistoricalContextTombstoneFoldBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newHistoricalContextFixture(t, backend)
			ctx, checkpoint := testAuthorActivityContext(), uuid.NewString()
			tx, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			deleteRunForkRevisionMatrixFacts(t, ctx, tx, f.source)
			seedRunForkRevisionMatrixEvent(t, ctx, tx, f.source.runID, checkpoint, f.source.at.Add(2*time.Second), f.postgres)
			f.finalize(t, tx)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			var absent, malformedTombstones int
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=3 AND NOT present`, f.source.runID).Scan(&absent); err != nil || absent != 15 {
				t.Fatalf("canonical tombstone census=%d err=%v, want all 13 subjects plus selector and later event", absent, err)
			}
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=3 AND NOT present AND CAST(fact AS TEXT)<>'{}'`, f.source.runID).Scan(&malformedTombstones); err != nil || malformedTombstones != 0 {
				t.Fatalf("canonical deletion payload is not {}: count=%d err=%v", malformedTombstones, err)
			}
			// Both old present revisions are discarded at R3. They are deliberately
			// invalid live bodies, and must never be decoded around the tombstone.
			if _, err := f.db.Exec(`UPDATE run_fork_fact_revisions SET fact='{}' WHERE run_id=$1 AND revision<3`, f.source.runID); err != nil {
				t.Fatal(err)
			}
			f.requireLatestComplete(t)
			before := historicalContextDatabaseRows(t, f.db, f.postgres)
			got, err := f.planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: f.source.runID, At: checkpoint})
			if err != nil || got.ForkPoint.Revision != 3 || got.EventCountAtFork != 1 || len(got.Entities) != 0 || len(got.PendingWork) != 0 || len(got.FanOutObligations) != 0 {
				t.Fatalf("tombstone fold resurrected discarded facts or decoded dead bodies: plan=%#v err=%v", got, err)
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, f.db, f.postgres))

			// Reappearance is a new canonical present fact, not fallback around an
			// absence. A timer with the same ID may legitimately be visible later.
			tx, err = f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			s := f.source
			mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO timers (timer_id,timer_name,schedule_scope,schedule_key,immutable_hash,run_id,fire_event,fire_payload,routing_source,execution_mode,fire_at,initial_fire_at,recurring,owner_node,owner_kind,due_basis_kind,due_basis_absolute,task_type,status,created_at) VALUES ($1,'matrix-timer','run','matrix-key','matrix-hash',$2,'matrix.fire','{}','{"kind":"root"}','live',$3,$3,FALSE,'matrix-node','system','absolute',$3,'timer','active',$4)`, s.timerID, s.runID, s.at.Add(time.Hour), s.at)
			reappearedAt := uuid.NewString()
			seedRunForkRevisionMatrixEvent(t, ctx, tx, s.runID, reappearedAt, s.at.Add(3*time.Second), f.postgres)
			f.finalize(t, tx)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			f.requireLatestComplete(t)
			var present bool
			if err := f.db.QueryRow(`SELECT present FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=4 AND family='timers' AND fact_key=$2`, s.runID, s.timerID).Scan(&present); err != nil || !present {
				t.Fatalf("canonical reappearance missing: present=%v err=%v", present, err)
			}
			later, err := f.planner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: s.runID, At: reappearedAt})
			if err != nil || later.ForkPoint.Revision != 4 || later.EventCountAtFork != 2 {
				t.Fatalf("lawful reappearance rejected: %#v err=%v", later.ForkPoint, err)
			}
		})
	}
}

func TestRunForkHistoricalContextLocalEventMembershipBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, family := range []string{"event_deliveries", "event_receipts"} {
				t.Run(family, func(t *testing.T) {
					f := newHistoricalContextFixture(t, backend)
					for _, fact := range f.facts {
						if fact.family != family {
							continue
						}
						bad := fact
						fields := historicalContextFields(t, fact.raw)
						// The event exists in live/latest state, but not at selected R.
						fields["event_id"] = f.latestID
						bad.raw = historicalContextJSON(t, fields)
						f.reject(t, fact, bad, false)
					}
				})
			}
		})
	}
}

func TestRunForkHistoricalContextFamilyContradictionsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newHistoricalContextFixture(t, backend)
			for _, fact := range f.facts {
				if fact.family != "timers" {
					continue
				}
				t.Run("known_wrong_family", func(t *testing.T) {
					bad := fact
					bad.family = "entity_mutations"
					f.reject(t, fact, bad, false)
				})
			}
			// The closed registry is also a real schema CHECK. Do not disable it
			// merely to claim an unreachable historical reader integration case.
			t.Run("unknown_family_schema_refusal", func(t *testing.T) {
				before := historicalContextDatabaseRows(t, f.db, f.postgres)
				_, err := f.db.Exec(`INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,1,'unknown_historical_family',$2,'{}',TRUE)`, f.source.runID, uuid.NewString())
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "check constraint") {
					t.Fatalf("unknown family must be refused by the closed schema: %v", err)
				}
				historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, f.db, f.postgres))
				t.Logf("schema registry refused unknown family before historical planning: %v", err)
			})
		})
	}
}

// H08 separately exercises a tombstoned local event while its live row and
// latest retained fact remain correct. The receipt cell removes only competing
// delivery evidence at R so the named receipt membership owner is reached.
func TestRunForkHistoricalContextTombstonedLocalEventBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, family := range []string{"event_deliveries", "event_receipts"} {
				t.Run(family, func(t *testing.T) {
					f := newHistoricalContextFixture(t, backend)
					ctx := testAuthorActivityContext()
					tx, err := f.db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					seedRunForkRevisionMatrixEvent(t, ctx, tx, f.source.runID, uuid.NewString(), f.source.at.Add(2*time.Second), f.postgres)
					f.finalize(t, tx)
					// Exact diagnostic later retention for immutable events/families.
					mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) SELECT run_id,3,family,fact_key,fact,present FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=2`, f.source.runID)
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
					request := runfork.RunForkPlanRequest{SourceRunID: f.source.runID, At: f.latestID}
					if healthy, err := f.planner.PlanRunFork(ctx, request); err != nil || healthy.ForkPoint.Revision != 2 {
						t.Fatalf("healthy second-cut prerequisite: %#v err=%v", healthy.ForkPoint, err)
					}
					if _, err := f.db.Exec(`UPDATE run_fork_fact_revisions SET present=FALSE,fact='{}' WHERE run_id=$1 AND revision=2 AND family='events' AND fact_key=$2`, f.source.runID, f.source.eventID); err != nil {
						t.Fatal(err)
					}
					if family == "event_receipts" {
						if _, err := f.db.Exec(`UPDATE run_fork_fact_revisions SET present=FALSE,fact='{}' WHERE run_id=$1 AND revision=2 AND family='event_deliveries'`, f.source.runID); err != nil {
							t.Fatal(err)
						}
					}
					f.requireLatestComplete(t)
					before := historicalContextDatabaseRows(t, f.db, f.postgres)
					_, err = f.planner.PlanRunFork(ctx, request)
					if err == nil || !strings.Contains(err.Error(), family) || !strings.Contains(err.Error(), f.source.eventID) {
						t.Fatalf("expected exact %s local membership refusal for tombstoned %s, got %v", family, f.source.eventID, err)
					}
					historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, f.db, f.postgres))
					t.Logf("tombstoned local event refused by %s: %v; all rows unchanged", family, err)
				})
			}
		})
	}
}

func historicalContextRequireZero(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil || count != 0 {
		t.Fatalf("optional reference unexpectedly has local row: count=%d err=%v", count, err)
	}
}
