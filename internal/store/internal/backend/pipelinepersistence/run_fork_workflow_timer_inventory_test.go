package pipelinepersistence

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type forkTimerInventoryReader interface {
	ReadRunForkWorkflowTimerInventoryTx(context.Context, *mutationprotocol.Attempt, string) ([]pipeline.WorkflowTimerActivation, error)
}

func forkTimerInventoryContext(runID string) context.Context {
	ctx := correlation.WithRunID(context.Background(), runID)
	ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
	return authoractivity.WithScope(ctx, authoractivity.BundleScope("timer-inventory-test", sourceartifactfixture.BundleHash))
}

// SQLmock supplies rows, but the real mutation constructor and lifecycle source
// loader own the frame and admission. No test installs native authority.
func withForkTimerInventoryAttempt(t *testing.T, postgres bool, caller context.Context, expectations func(sqlmock.Sqlmock), check func(context.Context, *mutationprotocol.Attempt, forkTimerInventoryReader)) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	expectations(mock)
	mock.ExpectRollback()
	rollback := errors.New("fork timer inventory unit rollback")
	var reader forkTimerInventoryReader
	write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		check(ctx, attempt, reader)
		return struct{}{}, rollback
	}
	var result mutationprotocol.Result[struct{}]
	if postgres {
		backend, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		reader = &PipelinePostgresOwner{RunLifecyclePostgresOwner: &runlifecycle.RunLifecyclePostgresOwner{}}
		result = mutationprotocol.RunPostgres(caller, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	} else {
		backend, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		reader = &PipelineSQLiteOwner{RunLifecycleSQLiteOwner: &runlifecycle.RunLifecycleSQLiteOwner{}}
		result = mutationprotocol.RunSQLite(caller, backend, "fork timer inventory unit", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	}
	if result.Acknowledged() || !errors.Is(result.Err(), rollback) {
		t.Fatalf("inventory unit outcome: acknowledged=%t err=%v", result.Acknowledged(), result.Err())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func forkTimerInventoryDialects(t *testing.T, check func(*testing.T, bool)) {
	t.Helper()
	for _, postgres := range []bool{false, true} {
		t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres], func(t *testing.T) { check(t, postgres) })
	}
}

func expectForkTimerInventorySource(mock sqlmock.Sqlmock, postgres bool, runID, status, bundle string, artifact bool) {
	query := `SELECT status, bundle_hash FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT status, bundle_hash FROM runs WHERE run_id = $1::uuid FOR UPDATE`
	}
	rows := sqlmock.NewRows([]string{"status", "bundle_hash"})
	if status != "" {
		rows.AddRow(status, bundle)
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(runID).WillReturnRows(rows)
	if status == "running" || status == "paused" {
		expectForkTimerInventoryArtifact(mock, postgres, bundle, artifact)
	}
}

func expectForkTimerInventoryArtifact(mock sqlmock.Sqlmock, postgres bool, bundle string, present bool) {
	query := `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)`
	if postgres {
		query = `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)`
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(bundle).WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(present))
}

func forkTimerInventoryQuery(postgres bool) string {
	query := `SELECT CAST(timer_id AS TEXT), timer_name FROM timers
		WHERE run_id = ? AND task_type = 'workflow_timer'
		AND (source_timer_id IS NOT NULL OR forked_from_run_id IS NOT NULL
		     OR reconstruction_owner IS NOT NULL OR forked_from_event_id IS NOT NULL
		     OR forked_from_point_kind IS NOT NULL OR forked_from_point_revision IS NOT NULL
		     OR source_armed_at IS NOT NULL)
		ORDER BY timer_id`
	if postgres {
		query = strings.Replace(query, "run_id = ?", "run_id = $1::uuid", 1)
	}
	return regexp.QuoteMeta(query)
}

func forkTimerInventoryRows(timers ...pipeline.WorkflowTimerActivation) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"timer_id", "timer_name"})
	for _, timer := range timers {
		rows.AddRow(timer.Ref.ActivationID, timer.Ref.TaskID())
	}
	return rows
}

func expectForkTimerInventoryActivation(t *testing.T, mock sqlmock.Sqlmock, postgres bool, activation pipeline.WorkflowTimerActivation) {
	t.Helper()
	query := `(?s)^SELECT timer_name, .*FROM timers WHERE timer_id = \? AND task_type = 'workflow_timer'$`
	if postgres {
		query = `(?s)^SELECT timer_name, .*FROM timers WHERE timer_id = \$1::uuid AND task_type = 'workflow_timer' FOR UPDATE$`
	}
	mock.ExpectQuery(query).WithArgs(activation.Ref.ActivationID).WillReturnRows(nativeTimerRows(t, activation, true))
}

func TestRunForkWorkflowTimerInventoryReadsAllInheritedLifecycleFactsBothDialects(t *testing.T) {
	forkTimerInventoryDialects(t, func(t *testing.T, postgres bool) {
		active := nativeInheritedWorkflowTimer(t)
		fired := active.Canonical()
		fired.Ref.ActivationID, fired.Status, fired.FiredAt = "66666666-6666-4666-8666-666666666666", "fired", active.FireAt.Add(time.Minute)
		cancelled := active.Canonical()
		cancelled.Ref.ActivationID, cancelled.Status = "77777777-7777-4777-8777-777777777777", "cancelled"
		cancelled.CancelCause, cancelled.CancelledAt = pipeline.WorkflowTimerCancelCauseRuleRemoved, cancelled.CreatedAt
		recurring := active.Canonical()
		recurring.Ref.ActivationID, recurring.Recurring, recurring.RecurrenceInterval = "88888888-8888-4888-8888-888888888888", true, time.Hour
		recurring.FireAt, recurring.FiredAt = recurring.SourceArmedAt.Add(5*time.Hour), recurring.SourceArmedAt.Add(4*time.Hour)
		expected := []pipeline.WorkflowTimerActivation{active, fired, cancelled, recurring}
		withForkTimerInventoryAttempt(t, postgres, forkTimerInventoryContext(active.RunID), func(mock sqlmock.Sqlmock) {
			expectForkTimerInventorySource(mock, postgres, active.RunID, "running", sourceartifactfixture.BundleHash, true)
			mock.ExpectQuery(forkTimerInventoryQuery(postgres)).WithArgs(active.RunID).WillReturnRows(forkTimerInventoryRows(expected...)).RowsWillBeClosed()
			for _, timer := range expected {
				expectForkTimerInventoryActivation(t, mock, postgres, timer)
			}
		}, func(ctx context.Context, attempt *mutationprotocol.Attempt, reader forkTimerInventoryReader) {
			got, err := reader.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, active.RunID)
			if err != nil || !reflect.DeepEqual(got, expected) {
				t.Fatalf("inventory lost persisted status/due/lineage: got=%+v err=%v", got, err)
			}
		})
	})
}

func TestRunForkWorkflowTimerInventoryEmptyStillRequiresCurrentSourceBothDialects(t *testing.T) {
	forkTimerInventoryDialects(t, func(t *testing.T, postgres bool) {
		runID := nativeInheritedWorkflowTimer(t).RunID
		withForkTimerInventoryAttempt(t, postgres, forkTimerInventoryContext(runID), func(mock sqlmock.Sqlmock) {
			expectForkTimerInventorySource(mock, postgres, runID, "paused", sourceartifactfixture.BundleHash, true)
			mock.ExpectQuery(forkTimerInventoryQuery(postgres)).WithArgs(runID).WillReturnRows(forkTimerInventoryRows()).RowsWillBeClosed()
			expectForkTimerInventoryArtifact(mock, postgres, sourceartifactfixture.BundleHash, false)
		}, func(ctx context.Context, attempt *mutationprotocol.Attempt, reader forkTimerInventoryReader) {
			got, err := reader.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, runID)
			if err != nil || got == nil || len(got) != 0 {
				t.Fatalf("empty admitted inventory=%+v err=%v", got, err)
			}
			got, err = reader.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, runID)
			var unavailable *runtimerunlifecycle.SourceArtifactUnavailableError
			if got != nil || !errors.As(err, &unavailable) {
				t.Fatalf("warmed source hid missing fresh artifact: got=%+v err=%v", got, err)
			}
		})
	})
}

func TestRunForkWorkflowTimerInventorySourceAndScopeRefusalsBothDialects(t *testing.T) {
	forkTimerInventoryDialects(t, func(t *testing.T, postgres bool) {
		for _, cut := range []string{"missing_run", "ended", "missing_artifact", "foreign_source", "missing_source", "foreign_scope", "missing_scope"} {
			t.Run(cut, func(t *testing.T) {
				runID := nativeInheritedWorkflowTimer(t).RunID
				caller := forkTimerInventoryContext(runID)
				foreignBundle := "bundle-v2:sha256:" + strings.Repeat("b", 64)
				status, bundle := "running", sourceartifactfixture.BundleHash
				switch cut {
				case "missing_run":
					status = ""
				case "ended":
					status = "completed"
				case "foreign_source":
					bundle = foreignBundle
				case "missing_source":
					caller = authoractivity.WithScope(correlation.WithRunID(context.Background(), runID), authoractivity.BundleScope("timer-inventory-test", bundle))
				case "foreign_scope":
					caller = authoractivity.WithScope(caller, authoractivity.BundleScope("timer-inventory-test", foreignBundle))
				case "missing_scope":
					caller = correlation.WithSourceArtifactFact(correlation.WithRunID(context.Background(), runID), sourceartifactfixture.Fact())
				}
				withForkTimerInventoryAttempt(t, postgres, caller, func(mock sqlmock.Sqlmock) {
					expectForkTimerInventorySource(mock, postgres, runID, status, bundle, cut != "missing_artifact")
				}, func(ctx context.Context, attempt *mutationprotocol.Attempt, reader forkTimerInventoryReader) {
					got, err := reader.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, runID)
					if got != nil || err == nil {
						t.Fatalf("source/scope refusal returned inventory: got=%+v err=%v", got, err)
					}
				})
			})
		}
	})
}

func TestRunForkWorkflowTimerInventoryRequiresExactExistingFrameBothDialects(t *testing.T) {
	forkTimerInventoryDialects(t, func(t *testing.T, postgres bool) {
		runID := nativeInheritedWorkflowTimer(t).RunID
		var staleCtx context.Context
		var staleAttempt *mutationprotocol.Attempt
		var staleReader forkTimerInventoryReader
		withForkTimerInventoryAttempt(t, postgres, forkTimerInventoryContext(runID), func(sqlmock.Sqlmock) {},
			func(ctx context.Context, attempt *mutationprotocol.Attempt, reader forkTimerInventoryReader) {
				staleCtx, staleAttempt, staleReader = ctx, attempt, reader
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				for _, candidate := range []context.Context{forkTimerInventoryContext(runID), cancelled, correlation.WithRunID(ctx, "foreign-child")} {
					if got, err := reader.ReadRunForkWorkflowTimerInventoryTx(candidate, attempt, runID); got != nil || err == nil {
						t.Fatal("detached, cancelled, or foreign child frame read inventory")
					}
				}
				for _, invalid := range []*mutationprotocol.Attempt{nil, new(mutationprotocol.Attempt)} {
					if got, err := reader.ReadRunForkWorkflowTimerInventoryTx(ctx, invalid, runID); got != nil || err == nil {
						t.Fatal("inactive or missing attempt read inventory")
					}
				}
				for _, invalid := range []string{"", " " + runID, "foreign-child"} {
					if got, err := reader.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, invalid); got != nil || err == nil {
						t.Fatal("inexact child argument read inventory")
					}
				}
				withForkTimerInventoryAttempt(t, postgres, forkTimerInventoryContext(runID), func(sqlmock.Sqlmock) {},
					func(_ context.Context, foreign *mutationprotocol.Attempt, _ forkTimerInventoryReader) {
						if got, err := reader.ReadRunForkWorkflowTimerInventoryTx(ctx, foreign, runID); got != nil || err == nil {
							t.Fatal("foreign attempt borrowed a native frame")
						}
					})
			})
		if got, err := staleReader.ReadRunForkWorkflowTimerInventoryTx(staleCtx, staleAttempt, runID); got != nil || err == nil {
			t.Fatal("stale completed attempt read inventory")
		}
	})
}

func TestRunForkWorkflowTimerInventoryRefusesIncompleteReadbackBothDialects(t *testing.T) {
	forkTimerInventoryDialects(t, func(t *testing.T, postgres bool) {
		for _, cut := range []string{"query_error", "cursor_error", "invalid_task", "mismatched_id", "padded_task", "duplicate_id", "missing_row", "partial_lineage", "marker_only", "foreign_row", "blank_lineage", "cancelled_read"} {
			t.Run(cut, func(t *testing.T) {
				first := nativeInheritedWorkflowTimer(t)
				second := first.Canonical()
				second.Ref.ActivationID = "66666666-6666-4666-8666-666666666666"
				withForkTimerInventoryAttempt(t, postgres, forkTimerInventoryContext(first.RunID), func(mock sqlmock.Sqlmock) {
					expectForkTimerInventorySource(mock, postgres, first.RunID, "running", sourceartifactfixture.BundleHash, true)
					query := mock.ExpectQuery(forkTimerInventoryQuery(postgres)).WithArgs(first.RunID)
					if cut == "query_error" {
						query.WillReturnError(errors.New("inventory query failed"))
						return
					}
					rows := forkTimerInventoryRows(first)
					switch cut {
					case "cursor_error", "cancelled_read":
						rows.AddRow(second.Ref.ActivationID, second.Ref.TaskID())
						err := errors.New("inventory cursor failed")
						if cut == "cancelled_read" {
							err = context.Canceled
						}
						rows.RowError(1, err)
					case "invalid_task":
						rows.AddRow(second.Ref.ActivationID, "generic-deadline")
					case "mismatched_id":
						rows.AddRow(second.Ref.ActivationID, first.Ref.TaskID())
					case "padded_task":
						rows.AddRow(second.Ref.ActivationID, " "+second.Ref.TaskID())
					case "duplicate_id":
						rows.AddRow(first.Ref.ActivationID, first.Ref.TaskID())
					default:
						rows.AddRow(second.Ref.ActivationID, second.Ref.TaskID())
					}
					query.WillReturnRows(rows).RowsWillBeClosed()
					switch cut {
					case "cursor_error", "cancelled_read", "invalid_task", "mismatched_id", "padded_task", "duplicate_id":
						return
					}
					expectForkTimerInventoryActivation(t, mock, postgres, first)
					if cut == "missing_row" {
						mock.ExpectQuery(`(?s)^SELECT timer_name, .*`).WithArgs(second.Ref.ActivationID).
							WillReturnRows(sqlmock.NewRows([]string{"timer_name"}))
						return
					}
					switch cut {
					case "partial_lineage":
						second.SourceTimerID = ""
					case "marker_only":
						second.SourceTimerID, second.ForkedFromRunID, second.ForkedFromPointKind = "", "", ""
						second.ForkedFromPointRevision, second.SourceArmedAt = 0, time.Time{}
					case "foreign_row":
						second.RunID = "99999999-9999-4999-8999-999999999999"
					case "blank_lineage":
						second.SourceTimerID, second.ForkedFromRunID, second.ForkedFromPointKind, second.ReconstructionOwner = "", "", "", ""
						second.ForkedFromPointRevision, second.SourceArmedAt = 0, time.Time{}
						second.FireAt = second.CreatedAt.Add(time.Hour)
					}
					expectForkTimerInventoryActivation(t, mock, postgres, second)
				}, func(ctx context.Context, attempt *mutationprotocol.Attempt, reader forkTimerInventoryReader) {
					got, err := reader.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, first.RunID)
					if got != nil || err == nil {
						t.Fatalf("incomplete readback leaked partial inventory: got=%+v err=%v", got, err)
					}
					if cut == "cancelled_read" && !errors.Is(err, context.Canceled) {
						t.Fatalf("cancelled read lost cancellation cause: %v", err)
					}
				})
			})
		}
	})
}
