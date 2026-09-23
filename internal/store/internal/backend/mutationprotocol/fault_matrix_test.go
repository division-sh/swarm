package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	runtimeactivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privatefork "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
)

const faultMatrixMissingRun = "00000000-0000-0000-0000-000000000241"

func faultMatrixStores(t *testing.T, test func(*testing.T, *sql.DB, privateactivity.Dialect)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
		faultMatrixSchema(t, db)
		test(t, db, privateactivity.DialectSQLite)
	})
	t.Run("postgres", func(t *testing.T) {
		if _, set, err := testpostgres.ConnectionFromEnvironmentIfSet(); err != nil {
			t.Fatal(err)
		} else if !set {
			t.Skip("SWARM_TEST_POSTGRES_DSN is not set")
		}
		_, db, _ := testutil.StartEmptyPostgres(t)
		faultMatrixSchema(t, db)
		test(t, db, privateactivity.DialectPostgres)
	})
}

func faultMatrixSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, ddl := range []string{
		`CREATE TABLE mutation_fault_probe (value TEXT PRIMARY KEY)`,
		`CREATE TABLE author_activity_order (singleton_id INTEGER PRIMARY KEY, last_sequence BIGINT NOT NULL)`,
		`CREATE TABLE run_fork_selected_contract_runtime_executions (fork_run_id UUID PRIMARY KEY)`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
}

func faultMatrixActivitySchema(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE author_activity_occurrences (
		occurrence_id UUID PRIMARY KEY, sequence BIGINT NOT NULL,
		kind TEXT NOT NULL, version INTEGER NOT NULL, transition TEXT NOT NULL,
		source_owner TEXT NOT NULL, source_identity TEXT NOT NULL, dedup_key TEXT NOT NULL UNIQUE,
		run_id UUID, entity_id UUID, agent_id TEXT, flow_id TEXT,
		scope_kind TEXT NOT NULL, runtime_instance_id UUID, bundle_hash TEXT,
		author_safe_summary TEXT, projection JSONB NOT NULL, failure JSONB,
		occurred_at TIMESTAMPTZ NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
}

func faultMatrixNative(db *sql.DB, opts *sql.TxOptions) nativeRunner {
	return func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
		tx, err := db.BeginTx(ctx, opts)
		if err != nil {
			return false, err
		}
		if err := write(ctx, tx); err != nil {
			return false, errors.Join(err, tx.Rollback())
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}
}

func faultMatrixInsert(ctx context.Context, attempt *Attempt, value string) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO mutation_fault_probe (value) VALUES ($1)`, value)
		return err
	})
}

func faultMatrixCount(t *testing.T, db *sql.DB, value string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM mutation_fault_probe WHERE value=$1`, value).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func faultMatrixNoValue(t *testing.T, result Result[string], phase Phase) {
	t.Helper()
	if result.Acknowledged() || result.Err() == nil || result.Phase() != phase {
		t.Fatalf("failed attempt = ack:%v phase:%v err:%v; want unacknowledged phase %v", result.Acknowledged(), result.Phase(), result.Err(), phase)
	}
	if value, ok := result.Value(); ok || value != "" {
		t.Fatalf("failed attempt exposed value %q, %v", value, ok)
	}
}

func faultMatrixDraft(key string) runtimeactivity.Draft {
	return runtimeactivity.Draft{
		Kind: runtimeactivity.KindEntityLifecycle, Transition: "created",
		SourceOwner: "entity_mutations", SourceIdentity: key, DedupKey: key,
		OccurredAt: time.Now().UTC(),
		Scope:      runtimeactivity.BundleScope("00000000-0000-0000-0000-000000000242", "bundle-v2:sha256:"+strings.Repeat("a", 64)),
		Projection: runtimeactivity.Projection{SubjectType: "entity", SubjectID: "00000000-0000-0000-0000-000000000243"},
	}
}

func TestFaultMatrixRollbackAndFinalizationPhases(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		native := faultMatrixNative(db, nil)
		cases := []struct {
			name     string
			evidence Evidence
			phase    Phase
			write    func(context.Context, *Attempt, string) error
			message  string
		}{
			{
				name: "domain_write", evidence: RevisionOnly, phase: DomainWrite,
				write:   func(_ context.Context, _ *Attempt, _ string) error { return errors.New("domain fault") },
				message: "domain fault",
			},
			{
				name: "revision_finalize", evidence: RevisionOnly, phase: RevisionFinalize,
				write: func(_ context.Context, attempt *Attempt, _ string) error {
					return attempt.AddWholeFamily(faultMatrixMissingRun, privatefork.FamilyEvents)
				},
				message: "runs",
			},
			{
				name: "activity_finalize", evidence: Story, phase: ActivityFinalize,
				write: func(ctx context.Context, attempt *Attempt, value string) error {
					return attempt.Record(ctx, faultMatrixDraft(value))
				},
				message: "author_activity_occurrences",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				value := string(dialect) + "-" + tc.name
				result := run(context.Background(), dialect, tc.evidence, Ordinary, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
					if err := faultMatrixInsert(ctx, attempt, value); err != nil {
						return "", err
					}
					return value, tc.write(ctx, attempt, value)
				})
				faultMatrixNoValue(t, result, tc.phase)
				if !strings.Contains(result.Err().Error(), tc.message) {
					t.Fatalf("fault error %q does not contain %q", result.Err(), tc.message)
				}
				if count := faultMatrixCount(t, db, value); count != 0 {
					t.Fatalf("failed %s persisted %d domain rows", tc.name, count)
				}
			})
		}
	})
}

func TestFaultMatrixCancellationAndCommitAcknowledgement(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		base := faultMatrixNative(db, nil)
		t.Run("cancel_after_domain_write", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			value := string(dialect) + "-canceled"
			native := func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return false, err
				}
				if err := write(ctx, tx); err != nil {
					return false, errors.Join(err, tx.Rollback())
				}
				if ctx.Err() == nil {
					return false, errors.New("writer did not cancel transaction context")
				}
				return false, errors.Join(ctx.Err(), tx.Rollback())
			}
			result := run(ctx, dialect, RevisionOnly, Ordinary, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
				if err := faultMatrixInsert(ctx, attempt, value); err != nil {
					return "", err
				}
				cancel()
				return value, nil
			})
			faultMatrixNoValue(t, result, CommitAdmission)
			if !errors.Is(result.Err(), context.Canceled) || faultMatrixCount(t, db, value) != 0 {
				t.Fatalf("canceled attempt was not rolled back: %v", result.Err())
			}
		})
		t.Run("missing_ack_after_native_commit", func(t *testing.T) {
			value := string(dialect) + "-missing-ack"
			lost := errors.New("acknowledgement lost")
			result := run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil,
				func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
					ack, err := base(ctx, write)
					if !ack || err != nil {
						return ack, err
					}
					return false, lost
				}, func(ctx context.Context, attempt *Attempt) (string, error) {
					return value, faultMatrixInsert(ctx, attempt, value)
				})
			faultMatrixNoValue(t, result, CommitAdmission)
			if !errors.Is(result.Err(), lost) || faultMatrixCount(t, db, value) != 1 {
				t.Fatalf("ambiguous committed transaction was misrepresented: %v", result.Err())
			}
		})
		t.Run("acknowledged_cleanup_error", func(t *testing.T) {
			value := string(dialect) + "-cleanup-error"
			cleanup := errors.New("post-commit cleanup failed")
			result := run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil,
				func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
					ack, err := base(ctx, write)
					if !ack || err != nil {
						return ack, err
					}
					return true, cleanup
				}, func(ctx context.Context, attempt *Attempt) (string, error) {
					return value, faultMatrixInsert(ctx, attempt, value)
				})
			if !result.Acknowledged() || result.Phase() != PostCommit || !errors.Is(result.Err(), cleanup) {
				t.Fatalf("acknowledged cleanup result = %+v", result)
			}
			if got, ok := result.Value(); !ok || got != value || faultMatrixCount(t, db, value) != 1 {
				t.Fatalf("acknowledged result lost committed value: %q, %v", got, ok)
			}
		})
	})
}

func TestFaultMatrixRetryResetsEffectsAndRetiresAttempts(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		ctx := context.Background()
		baseline := NewBaseline()
		retry := errors.New("retry first domain write")
		base := faultMatrixNative(db, nil)
		var generated []string
		var attempts []*Attempt
		native := func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return false, err
			}
			firstErr := write(ctx, tx)
			rollbackErr := tx.Rollback()
			if rollbackErr != nil || !errors.Is(firstErr, retry) {
				return false, fmt.Errorf("first retry attempt: %w", errors.Join(firstErr, rollbackErr))
			}
			return base(ctx, write)
		}
		result := run(ctx, dialect, RevisionOnly, Ordinary, baseline, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
			attempts = append(attempts, attempt)
			value := uuid.NewString()
			generated = append(generated, value)
			if err := faultMatrixInsert(ctx, attempt, value); err != nil {
				return "", err
			}
			if len(attempts) == 1 {
				if err := attempt.AddFact(faultMatrixMissingRun, privatefork.FamilyEvents, uuid.NewString()); err != nil {
					return "", err
				}
				return value, retry
			}
			if err := attempts[0].WithSQL(ctx, func(context.Context, *sql.Tx) error { return nil }); err == nil {
				return "", errors.New("rolled-back attempt retained SQL authority")
			}
			return value, nil
		})
		if !result.Acknowledged() || result.Err() != nil || result.Phase() != PostCommit || len(generated) != 2 || generated[0] == generated[1] {
			t.Fatalf("retry result = %+v, generated=%v", result, generated)
		}
		if got, ok := result.Value(); !ok || got != generated[1] {
			t.Fatalf("retry published stale generated value %q, %v", got, ok)
		}
		if faultMatrixCount(t, db, generated[0]) != 0 || faultMatrixCount(t, db, generated[1]) != 1 {
			t.Fatal("retry did not preserve only second attempt's durable value")
		}
		for _, attempt := range attempts {
			if err := attempt.WithSQL(ctx, func(context.Context, *sql.Tx) error { return nil }); err == nil {
				t.Fatal("completed attempt retained SQL authority")
			}
			if err := attempt.AddWholeFamily(faultMatrixMissingRun, privatefork.FamilyEvents); err == nil {
				t.Fatal("completed attempt retained revision authority")
			}
		}
		if err := baseline.AddWholeFamily(faultMatrixMissingRun, privatefork.FamilyEvents); err == nil {
			t.Fatal("used baseline remained mutable")
		}

		t.Run("predeclared_baseline_survives_retry", func(t *testing.T) {
			predeclared := NewBaseline()
			if err := predeclared.AddWholeFamily(faultMatrixMissingRun, privatefork.FamilyEvents); err != nil {
				t.Fatal(err)
			}
			value := string(dialect) + "-predeclared"
			calls := 0
			native := func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return false, err
				}
				firstErr := write(ctx, tx)
				rollbackErr := tx.Rollback()
				if rollbackErr != nil || !errors.Is(firstErr, retry) {
					return false, fmt.Errorf("first baseline attempt: %w", errors.Join(firstErr, rollbackErr))
				}
				return base(ctx, write)
			}
			outcome := run(ctx, dialect, RevisionOnly, Ordinary, predeclared, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
				calls++
				if err := faultMatrixInsert(ctx, attempt, value); err != nil {
					return "", err
				}
				if calls == 1 {
					return value, retry
				}
				return value, nil
			})
			faultMatrixNoValue(t, outcome, RevisionFinalize)
			if calls != 2 || !strings.Contains(outcome.Err().Error(), "runs") || faultMatrixCount(t, db, value) != 0 {
				t.Fatalf("predeclared baseline was not retained: calls=%d err=%v", calls, outcome.Err())
			}
		})
	})
}

func TestFaultMatrixDestructiveEarlyStoryCut(t *testing.T) {
	faultMatrixStores(t, func(t *testing.T, db *sql.DB, dialect privateactivity.Dialect) {
		var opts *sql.TxOptions
		if dialect == privateactivity.DialectPostgres {
			opts = &sql.TxOptions{Isolation: sql.LevelSerializable}
		}
		native := faultMatrixNative(db, opts)
		t.Run("direct_whole_parent_rejected_before_transaction", func(t *testing.T) {
			entered := false
			result := run(context.Background(), dialect, Story, WholeParentDeletion, nil, nil,
				func(context.Context, func(context.Context, *sql.Tx) error) (bool, error) {
					entered = true
					return false, nil
				}, func(context.Context, *Attempt) (string, error) {
					return "unexpected", nil
				})
			faultMatrixNoValue(t, result, BeforeAttempt)
			if entered || !strings.Contains(result.Err().Error(), "retention evidence") {
				t.Fatalf("direct whole-parent kind reached native transaction: entered=%v err=%v", entered, result.Err())
			}
		})
		retainedRunID, deletedRunID, foreignRunID := uuid.NewString(), uuid.NewString(), uuid.NewString()
		if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions (fork_run_id) VALUES ($1)`, retainedRunID); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name         string
			selectedKind Kind
			runID        string
			retained     bool
		}{
			{"retained_fork_cleanup", RetainedForkCleanup, retainedRunID, true},
			{"classified_whole_parent_deletion", WholeParentDeletion, deletedRunID, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				classify := func(ctx context.Context, attempt *Attempt) error {
					retained, err := attempt.SelectForkDiscardRetention(ctx, tc.runID)
					if err != nil {
						return err
					}
					if retained != tc.retained {
						return fmt.Errorf("canonical retention = %v, want %v", retained, tc.retained)
					}
					if _, err := attempt.SelectForkDiscardRetention(ctx, tc.runID); err == nil {
						return errors.New("retention classification was accepted twice")
					}
					return nil
				}
				seed := func(value string) {
					t.Helper()
					if _, err := db.Exec(`INSERT INTO mutation_fault_probe (value) VALUES ($1)`, value); err != nil {
						t.Fatal(err)
					}
				}
				deleteValue := func(ctx context.Context, attempt *Attempt, value string) error {
					return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						_, err := tx.ExecContext(ctx, `DELETE FROM mutation_fault_probe WHERE value=$1`, value)
						return err
					})
				}
				unclassified := string(dialect) + "-" + tc.name + "-unclassified"
				seed(unclassified)
				result := run(context.Background(), dialect, Story, RetainedForkCleanup, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
					if err := deleteValue(ctx, attempt, unclassified); err != nil {
						return "", err
					}
					return unclassified, attempt.BeginDestructiveCleanup(ctx)
				})
				faultMatrixNoValue(t, result, DomainWrite)
				if faultMatrixCount(t, db, unclassified) != 1 {
					t.Fatal("unclassified discard committed its deletion")
				}
				if !strings.Contains(result.Err().Error(), "not classified") {
					t.Fatalf("unclassified discard was not rejected: %v", result.Err())
				}
				incomplete := string(dialect) + "-" + tc.name + "-incomplete"
				seed(incomplete)
				result = run(context.Background(), dialect, Story, RetainedForkCleanup, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
					if err := classify(ctx, attempt); err != nil {
						return "", err
					}
					return incomplete, deleteValue(ctx, attempt, incomplete)
				})
				faultMatrixNoValue(t, result, DomainWrite)
				if !strings.Contains(result.Err().Error(), "did not finalize activity") || faultMatrixCount(t, db, incomplete) != 1 {
					t.Fatalf("classified discard without early story cut committed: %v", result.Err())
				}

				failedStory := string(dialect) + "-" + tc.name + "-story-failed"
				seed(failedStory)
				deletionReached := false
				result = run(context.Background(), dialect, Story, RetainedForkCleanup, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
					if err := classify(ctx, attempt); err != nil {
						return "", err
					}
					if err := attempt.Record(ctx, faultMatrixDraft(failedStory)); err != nil {
						return "", err
					}
					if err := attempt.BeginDestructiveCleanup(ctx); err != nil {
						return "", err
					}
					deletionReached = true
					return failedStory, deleteValue(ctx, attempt, failedStory)
				})
				faultMatrixNoValue(t, result, DomainWrite)
				if deletionReached || !strings.Contains(result.Err().Error(), "author_activity_occurrences") || faultMatrixCount(t, db, failedStory) != 1 {
					t.Fatalf("failed early story allowed cleanup: reached=%v err=%v", deletionReached, result.Err())
				}

				faultMatrixActivitySchema(t, db)
				t.Cleanup(func() {
					if _, err := db.Exec(`DROP TABLE author_activity_occurrences`); err != nil {
						t.Errorf("drop activity fault table: %v", err)
					}
				})
				if !tc.retained {
					foreign := string(dialect) + "-" + tc.name + "-foreign-effect"
					seed(foreign)
					foreignResult := run(context.Background(), dialect, Story, RetainedForkCleanup, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
						if err := attempt.AddWholeFamily(tc.runID, privatefork.FamilyEvents); err != nil {
							return "", err
						}
						if err := attempt.AddWholeFamily(foreignRunID, privatefork.FamilyEvents); err != nil {
							return "", err
						}
						if err := classify(ctx, attempt); err != nil {
							return "", err
						}
						if err := deleteValue(ctx, attempt, foreign); err != nil {
							return "", err
						}
						return foreign, attempt.BeginDestructiveCleanup(ctx)
					})
					faultMatrixNoValue(t, foreignResult, DomainWrite)
					if !strings.Contains(foreignResult.Err().Error(), "another run") || faultMatrixCount(t, db, foreign) != 1 {
						t.Fatalf("foreign-run effects were discarded: %v", foreignResult.Err())
					}
				}
				completed := string(dialect) + "-" + tc.name + "-completed"
				seed(completed)
				result = run(context.Background(), dialect, Story, RetainedForkCleanup, nil, nil, native, func(ctx context.Context, attempt *Attempt) (string, error) {
					if !tc.retained {
						if err := attempt.AddWholeFamily(tc.runID, privatefork.FamilyEvents); err != nil {
							return "", err
						}
					}
					if err := classify(ctx, attempt); err != nil {
						return "", err
					}
					if err := attempt.Record(ctx, faultMatrixDraft(completed)); err != nil {
						return "", err
					}
					if err := attempt.BeginDestructiveCleanup(ctx); err != nil {
						return "", err
					}
					if attempt.kind != tc.selectedKind {
						return "", fmt.Errorf("selected destructive kind = %v, want %v", attempt.kind, tc.selectedKind)
					}
					if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						var count int
						if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM author_activity_occurrences WHERE dedup_key=$1`, completed).Scan(&count); err != nil {
							return err
						}
						if count != 1 {
							return fmt.Errorf("early story is not visible before deletion: %d occurrences", count)
						}
						return nil
					}); err != nil {
						return "", err
					}
					if err := attempt.Record(ctx, faultMatrixDraft(completed)); err == nil {
						return "", errors.New("activity accepted after early story cut")
					}
					if err := attempt.BeginDestructiveCleanup(ctx); err == nil {
						return "", errors.New("second destructive story cut accepted")
					}
					if tc.selectedKind == WholeParentDeletion {
						if err := attempt.AddWholeFamily(faultMatrixMissingRun, privatefork.FamilyEvents); err == nil {
							return "", errors.New("whole-parent deletion accepted revision fact")
						}
					}
					if _, err := attempt.SelectForkDiscardRetention(ctx, tc.runID); err == nil {
						return "", errors.New("post-cleanup retention classification was accepted")
					}
					return completed, deleteValue(ctx, attempt, completed)
				})
				if !result.Acknowledged() || result.Err() != nil || result.Phase() != PostCommit {
					t.Fatalf("named destructive cut did not commit: %+v", result)
				}
				if got, ok := result.Value(); !ok || got != completed || faultMatrixCount(t, db, completed) != 0 {
					t.Fatalf("named destructive cut lost deletion or typed value: %q, %v", got, ok)
				}
				var occurrences int
				if err := db.QueryRow(`SELECT COUNT(*) FROM author_activity_occurrences WHERE dedup_key=$1`, completed).Scan(&occurrences); err != nil || occurrences != 1 {
					t.Fatalf("early story was not committed: occurrences=%d err=%v", occurrences, err)
				}
			})
		}
	})
}
