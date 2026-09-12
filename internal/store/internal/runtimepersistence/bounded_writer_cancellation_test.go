package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runquiescence"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// The server notice cancels the logical operation while its admitted SQL is
// still running. A healthy rollback must drain that SQL and retain the PID.
func TestBoundedPostgresWritersCancelBeforeCommit(t *testing.T) {
	for _, name := range []string{
		"ingress_ensure", "ingress_event", "routing_active", "routing_deactivate", "routing_insert_inactive",
		"mailbox_insert", "mailbox_notify", "mailbox_expire", "quiescence", "quiescence_preview", "reset_cleanup",
	} {
		for _, phase := range []string{"before_admission", "during_sql", "independent_error"} {
			t.Run(name+"/"+phase, func(t *testing.T) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				pg := newTestPostgresStore(t, db)
				db.SetMaxOpenConns(1)
				base := testAuthorActivityContext()
				now := time.Now().UTC()
				var table, event, observe string
				var invoke func(context.Context) error
				switch {
				case strings.HasPrefix(name, "ingress_"):
					table, event = "runtime_ingress_state", "INSERT"
					observe = `SELECT count(*)::text FROM runtime_ingress_state`
					invoke = func(ctx context.Context) error {
						state, err := pg.EnsureRuntimeIngressState(ctx, now)
						if err != nil && state.Status != "" {
							t.Errorf("uncommitted ingress state escaped: %#v", state)
						}
						return err
					}
					if name == "ingress_event" {
						event = "UPDATE"
						// A stale transition is still an admitted SQL write; the
						// statement trigger exercises it without an event fixture.
						invoke = func(ctx context.Context) error {
							changed, err := pg.SetRuntimeIngressTransitionEvent(ctx, runtimeingress.StatusPaused, uuid.NewString(), now)
							if changed {
								t.Error("stale transition reported a change")
							}
							return err
						}
					}
				case strings.HasPrefix(name, "routing_"):
					base = correlation.WithRunID(base, specEntityStateRunID)
					entityID := uuid.NewString()
					seedSpecEntityState(t, base, db, entityID, "bounded-writer-flow", "bounded", "B", "operating")
					rule := runtimemanager.PersistedRoutingRule{EntityID: entityID, EventPattern: "bounded.*", SubscriberID: "subscriber", InstalledBy: "test", Status: "active"}
					table, event = "routing_rules", "INSERT"
					observe = `SELECT COALESCE(string_agg(status, ',' ORDER BY rule_id), '') FROM routing_rules`
					if name == "routing_deactivate" {
						if err := pg.UpsertRoutingRule(base, rule); err != nil {
							t.Fatal(err)
						}
						event = "UPDATE"
					}
					if name != "routing_active" {
						rule.Status = "inactive"
					}
					invoke = func(ctx context.Context) error { return pg.UpsertRoutingRule(ctx, rule) }
				case strings.HasPrefix(name, "mailbox_"):
					table, event = "mailbox", "INSERT"
					observe = `SELECT COALESCE(string_agg(status || ':' || notified::text, ',' ORDER BY item_id), '') FROM mailbox`
					item := runtimetools.MailboxItem{ID: uuid.NewString(), Type: "spend_request", Summary: "bounded writer", TimeoutAt: now.Add(-time.Hour)}
					if name != "mailbox_insert" {
						if _, err := pg.InsertMailboxItem(base, item); err != nil {
							t.Fatal(err)
						}
						event = "UPDATE"
					}
					switch name {
					case "mailbox_insert":
						invoke = func(ctx context.Context) error {
							id, err := pg.InsertMailboxItem(ctx, item)
							if err != nil && id != "" {
								t.Errorf("uncommitted mailbox identity escaped: %s", id)
							}
							return err
						}
					case "mailbox_notify":
						invoke = func(ctx context.Context) error { return pg.MarkMailboxItemNotified(ctx, item.ID) }
					case "mailbox_expire":
						invoke = func(ctx context.Context) error {
							items, err := pg.ExpireMailboxItems(ctx, 20)
							if err != nil && len(items) != 0 {
								t.Errorf("uncommitted expiry rows escaped: %#v", items)
							}
							return err
						}
					}
				case strings.HasPrefix(name, "quiescence") || name == "reset_cleanup":
					runID := uuid.NewString()
					requireRunFixtureForTest(t, base, pg, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: now.Add(-time.Hour)})
					table, event = "runs", "UPDATE"
					if name == "quiescence_preview" {
						table, event = "author_activity_order", "INSERT"
					}
					observe = `SELECT COALESCE(string_agg(status, ',' ORDER BY run_id), '') FROM runs`
					invoke = func(ctx context.Context) error {
						out, err := pg.ApplyActiveRunQuiescence(ctx, runquiescence.Request{OperationName: "bounded-writer", DryRun: name == "quiescence_preview", RunIDs: []string{runID}, RequestedAt: now, ReasonCode: runquiescence.ServeAbandonReasonCode, ControlledBy: "test"})
						if err != nil && len(out.Runs) != 0 {
							t.Errorf("uncommitted quiescence result escaped: %#v", out)
						}
						return err
					}
					if name == "reset_cleanup" {
						event = "DELETE"
						invoke = func(ctx context.Context) error {
							out, err := pg.ApplyDestructiveResetCleanup(ctx, destructivereset.CleanupRequest{
								ActorTokenID: "operator", RequestedAt: now,
								Result:     destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: now.Add(-time.Minute), Plan: cleanupPlanForRunIDs(runID)},
								Quiescence: destructivereset.QuiescenceResult{OperationName: destructivereset.DefaultOperationName, AppliedAt: now.Add(-time.Second)},
							})
							if err != nil && len(out.RunIDs) != 0 {
								t.Errorf("uncommitted cleanup result escaped: %#v", out)
							}
							return err
						}
					}
				}
				var before string
				if err := db.QueryRow(observe).Scan(&before); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(base)
				defer cancel()
				conn, err := db.Conn(base)
				if err != nil {
					t.Fatal(err)
				}
				var beforePID int
				if err := conn.QueryRowContext(base, `SELECT pg_backend_pid()`).Scan(&beforePID); err != nil {
					t.Fatal(err)
				}
				notices := 0
				if err := conn.Raw(func(raw any) error {
					pq.SetNoticeHandler(raw.(driver.Conn), func(n *pq.Error) {
						if n.Message == "bounded-writer-stop" {
							notices++
							cancel()
						}
					})
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				if phase == "before_admission" {
					cancel()
				} else {
					failure := ""
					if phase == "independent_error" {
						failure = "RAISE EXCEPTION 'bounded-writer-independent-error';"
					}
					if _, err := db.Exec(`CREATE FUNCTION bounded_writer_stop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'bounded-writer-stop'; PERFORM pg_sleep(0.02); ` + failure + ` RETURN NULL; END $$`); err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec(`CREATE TRIGGER bounded_writer_stop BEFORE ` + event + ` ON ` + table + ` FOR EACH STATEMENT EXECUTE FUNCTION bounded_writer_stop()`); err != nil {
						t.Fatal(err)
					}
				}
				err = invoke(ctx)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("logical cancellation lost: %v", err)
				}
				if phase != "before_admission" && notices == 0 {
					t.Fatal("cancellation never reached admitted SQL")
				}
				if phase == "independent_error" && !strings.Contains(err.Error(), "bounded-writer-independent-error") {
					t.Fatalf("independent SQL error lost: %v", err)
				}
				var after string
				var afterPID int
				if err := db.QueryRow(observe).Scan(&after); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT pg_backend_pid()`).Scan(&afterPID); err != nil {
					t.Fatal(err)
				}
				if before != after || beforePID != afterPID {
					t.Fatalf("rollback state %q -> %q; healthy PID %d -> %d", before, after, beforePID, afterPID)
				}
				if phase != "before_admission" {
					if _, err := db.Exec(`DROP TRIGGER bounded_writer_stop ON ` + table); err != nil {
						t.Fatal(err)
					}
				}
				if err := invoke(base); err != nil {
					t.Fatalf("healthy successor failed: %v", err)
				}
			})
		}
	}
}

func TestPostgresQuiescencePreviewAndEmptySelectionRollBackInitialization(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	var before int
	if err := db.QueryRow(`SELECT count(*) FROM author_activity_order`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatal("fresh fixture already initialized author activity")
	}
	for _, dryRun := range []bool{true, false} {
		result, err := pg.ApplyActiveRunQuiescence(ctx, runquiescence.Request{
			OperationName: "bounded-dry-run", DryRun: dryRun, AllActiveRuns: true,
			ReasonCode: runquiescence.ServeAbandonReasonCode, ControlledBy: "test",
		})
		if err != nil || result.DryRun != dryRun || len(result.Runs) != 0 {
			t.Fatalf("locked dry-run result=%#v error=%v", result, err)
		}
		var after int
		if err := db.QueryRow(`SELECT count(*) FROM author_activity_order`).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Fatalf("selection committed author-activity initialization (dry_run=%t): %d -> %d", dryRun, before, after)
		}
	}
}
