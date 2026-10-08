package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

func runAdmissionMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return db, mock
}

func runAdmissionDialects(t *testing.T, check func(*testing.T, privateactivity.Dialect)) {
	t.Helper()
	for _, dialect := range []privateactivity.Dialect{privateactivity.DialectPostgres, privateactivity.DialectSQLite} {
		t.Run(string(dialect), func(t *testing.T) { check(t, dialect) })
	}
}

func runAdmissionFact(t *testing.T, digit string) runtimecorrelation.SourceArtifactFact {
	t.Helper()
	fact, err := runtimecorrelation.DecodeSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat(digit, 64))
	if err != nil {
		t.Fatal(err)
	}
	return fact
}

func withRunAdmissionAttempt(t *testing.T, dialect privateactivity.Dialect, caller context.Context, check func(context.Context, *Attempt, *sql.Tx)) {
	t.Helper()
	db, mock := runAdmissionMockDB(t)
	mock.ExpectBegin()
	mock.ExpectRollback()
	rollback := errors.New("run admission unit probe rollback")
	// Model the native SQL drain while retaining the logical caller in run().
	native := func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
		sqlCtx := context.WithoutCancel(ctx)
		tx, err := db.BeginTx(sqlCtx, nil)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		return false, write(sqlCtx, tx)
	}
	result := run(caller, dialect, RevisionOnly, Ordinary, nil, nil, native, func(ctx context.Context, attempt *Attempt) (struct{}, error) {
		if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			check(ctx, attempt, tx)
			return nil
		}); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, rollback
	})
	if result.Acknowledged() || result.Err() != rollback {
		t.Fatalf("unit probe result: acknowledged=%v err=%v", result.Acknowledged(), result.Err())
	}
}

func requireRunAdmissionRefusal(t *testing.T, ctx context.Context, attempt *Attempt, tx *sql.Tx, runID string, fact runtimecorrelation.SourceArtifactFact, want error) {
	t.Helper()
	got, cached, err := CachedActiveRunSource(ctx, tx, runID)
	if cached || got != (runtimecorrelation.SourceArtifactFact{}) || err == nil || (want != nil && !errors.Is(err, want)) {
		t.Fatalf("refused cache read: fact=%v cached=%v err=%v, want %v", got, cached, err, want)
	}
	for name, operation := range map[string]func() error{
		"setter":       func() error { return CacheActiveRunSource(ctx, tx, runID, fact) },
		"invalidation": func() error { return InvalidateActiveRunSource(ctx, tx, runID) },
		"record check": func() error { return attempt.RequireActiveRunSourceAdmission(ctx, runID, fact) },
	} {
		if err := operation(); err == nil || (want != nil && !errors.Is(err, want)) {
			t.Fatalf("%s refusal: err=%v, want %v", name, err, want)
		}
	}
}

func TestRunAdmissionExactNativeAttemptAndTransaction(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		caller := context.WithValue(context.Background(), struct{}{}, "logical caller")
		runID, fact := uuid.NewString(), runAdmissionFact(t, "a")
		withRunAdmissionAttempt(t, dialect, caller, func(ctx context.Context, attempt *Attempt, tx *sql.Tx) {
			if ctx.Value(sqlAttemptKey{}) != attempt || attempt.tx != tx || attempt.callerCtx != caller || attempt.runAdmissions == nil {
				t.Fatal("native constructor did not bind the exact attempt, transaction, caller and fresh map")
			}
			if got, cached, err := CachedActiveRunSource(ctx, tx, runID); err != nil || cached || got != (runtimecorrelation.SourceArtifactFact{}) {
				t.Fatalf("new attempt had admission: fact=%v cached=%v err=%v", got, cached, err)
			}
			if err := attempt.RequireActiveRunSourceAdmission(ctx, runID, fact); err == nil {
				t.Fatal("unadmitted source minted record permission")
			}
			if err := CacheActiveRunSource(ctx, tx, " "+runID+" ", fact); err != nil {
				t.Fatal(err)
			}
			if err := attempt.WithSQL(context.Background(), func(lent context.Context, lentTx *sql.Tx) error {
				if lent.Value(sqlAttemptKey{}) != attempt || lentTx != tx {
					t.Fatal("WithSQL did not bind the same native attempt and transaction")
				}
				got, cached, err := CachedActiveRunSource(lent, lentTx, runID)
				if err != nil || !cached || !got.Matches(fact) {
					t.Fatalf("exact admission reuse: fact=%v cached=%v err=%v", got, cached, err)
				}
				return attempt.RequireActiveRunSourceAdmission(lent, " "+runID+" ", fact)
			}); err != nil {
				t.Fatal(err)
			}
		})
	})
}

func TestRunAdmissionSourceOnlyContextCannotMint(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		runID, fact := uuid.NewString(), runAdmissionFact(t, "a")
		withRunAdmissionAttempt(t, dialect, context.Background(), func(ctx context.Context, attempt *Attempt, tx *sql.Tx) {
			sourceOnly := runtimecorrelation.WithSourceArtifactFact(runtimecorrelation.WithRunID(context.Background(), runID), fact)
			for name, unbound := range map[string]context.Context{"no key": context.Background(), "source only": sourceOnly} {
				if err := CacheActiveRunSource(unbound, tx, runID, fact); err != nil {
					t.Fatalf("%s uncached setter: %v", name, err)
				}
				if got, cached, err := CachedActiveRunSource(unbound, tx, runID); err != nil || cached || got != (runtimecorrelation.SourceArtifactFact{}) {
					t.Fatalf("%s borrowed native admission: fact=%v cached=%v err=%v", name, got, cached, err)
				}
				if err := InvalidateActiveRunSource(unbound, tx, runID); err != nil {
					t.Fatal(err)
				}
				if err := attempt.RequireActiveRunSourceAdmission(unbound, runID, fact); err == nil {
					t.Fatalf("%s minted record permission", name)
				}
			}
			boundSource := runtimecorrelation.WithSourceArtifactFact(ctx, fact)
			if _, cached, err := CachedActiveRunSource(boundSource, tx, runID); err != nil || cached || len(attempt.runAdmissions) != 0 {
				t.Fatalf("context fact populated the native map: cached=%v err=%v map=%v", cached, err, attempt.runAdmissions)
			}
			if err := attempt.RequireActiveRunSourceAdmission(boundSource, runID, fact); err == nil {
				t.Fatal("bound context fact alone minted record permission")
			}
			if err := CacheActiveRunSource(ctx, tx, runID, fact); err != nil {
				t.Fatal(err)
			}
			if err := InvalidateActiveRunSource(sourceOnly, tx, runID); err != nil {
				t.Fatal(err)
			}
			if err := attempt.RequireActiveRunSourceAdmission(ctx, runID, fact); err != nil {
				t.Fatalf("unbound invalidation changed native admission: %v", err)
			}
		})
	})
}

func TestRunAdmissionForeignAndEndedScope(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		runID, fact := uuid.NewString(), runAdmissionFact(t, "a")
		var endedCtx context.Context
		var ended *Attempt
		var endedTx *sql.Tx
		withRunAdmissionAttempt(t, dialect, context.Background(), func(ctx context.Context, attempt *Attempt, tx *sql.Tx) {
			endedCtx, ended, endedTx = ctx, attempt, tx
			if err := CacheActiveRunSource(ctx, tx, runID, fact); err != nil {
				t.Fatal(err)
			}
			for name, invalidCtx := range map[string]context.Context{
				"nil context":    nil,
				"wrong key type": context.WithValue(ctx, sqlAttemptKey{}, true),
				"nil attempt":    context.WithValue(ctx, sqlAttemptKey{}, (*Attempt)(nil)),
			} {
				t.Run(name, func(t *testing.T) {
					requireRunAdmissionRefusal(t, invalidCtx, attempt, tx, runID, fact, nil)
				})
			}
			// Even a matching Tx and map cannot substitute another attempt identity.
			sameTxForeign := *attempt
			sameTxCtx := context.WithValue(ctx, sqlAttemptKey{}, &sameTxForeign)
			if err := attempt.RequireActiveRunSourceAdmission(sameTxCtx, runID, fact); err == nil {
				t.Fatal("another attempt with the same Tx and source map minted record permission")
			}
			withRunAdmissionAttempt(t, dialect, context.Background(), func(foreignCtx context.Context, foreign *Attempt, foreignTx *sql.Tx) {
				if foreign == attempt || foreignTx == tx {
					t.Fatal("independent native scopes share identity")
				}
				// Equal source and run values do not bridge attempt or Tx ownership.
				if err := CacheActiveRunSource(foreignCtx, foreignTx, runID, fact); err != nil {
					t.Fatal(err)
				}
				requireRunAdmissionRefusal(t, ctx, foreign, foreignTx, runID, fact, nil)
				requireRunAdmissionRefusal(t, foreignCtx, attempt, tx, runID, fact, nil)
				if err := attempt.WithSQL(foreignCtx, func(context.Context, *sql.Tx) error {
					t.Fatal("foreign context reached the SQL loan")
					return nil
				}); err == nil {
					t.Fatal("foreign context borrowed WithSQL")
				}
				if err := foreign.RequireActiveRunSourceAdmission(foreignCtx, runID, fact); err != nil {
					t.Fatalf("foreign refusal corrupted its owner's admission: %v", err)
				}
			})
			if err := attempt.RequireActiveRunSourceAdmission(ctx, runID, fact); err != nil {
				t.Fatalf("foreign refusal corrupted the original admission: %v", err)
			}
		})
		if ended.active {
			t.Fatal("native exit left the attempt active")
		}
		requireRunAdmissionRefusal(t, endedCtx, ended, endedTx, runID, fact, nil)
	})
}

func TestRunAdmissionFreshNativeRetry(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		db, mock := runAdmissionMockDB(t)
		mock.ExpectBegin()
		mock.ExpectRollback()
		mock.ExpectBegin()
		mock.ExpectCommit()
		runID := uuid.NewString()
		firstFact, retryFact := runAdmissionFact(t, "a"), runAdmissionFact(t, "b")
		var firstCtx context.Context
		var first *Attempt
		var firstTx *sql.Tx
		attempts := 0
		result := run(context.Background(), dialect, RevisionOnly, Ordinary, nil, nil, mutationTestRunner(t, db, false, true), func(ctx context.Context, attempt *Attempt) (runtimecorrelation.SourceArtifactFact, error) {
			attempts++
			fact := firstFact
			err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				if _, cached, err := CachedActiveRunSource(ctx, tx, runID); err != nil || cached || len(attempt.runAdmissions) != 0 {
					t.Fatalf("attempt %d inherited admission: cached=%v err=%v map=%v", attempts, cached, err, attempt.runAdmissions)
				}
				if attempts == 1 {
					firstCtx, first, firstTx = ctx, attempt, tx
				} else {
					fact = retryFact
					if first == attempt || firstTx == tx || first.active {
						t.Fatal("retry reused an active prior attempt or transaction")
					}
					requireRunAdmissionRefusal(t, firstCtx, first, firstTx, runID, firstFact, nil)
					if err := attempt.RequireActiveRunSourceAdmission(firstCtx, runID, firstFact); err == nil {
						t.Fatal("retry consumed the retired attempt's admission")
					}
					if err := attempt.RequireActiveRunSourceAdmission(ctx, runID, firstFact); err == nil {
						t.Fatal("retry retained the prior map entry")
					}
				}
				if err := CacheActiveRunSource(ctx, tx, runID, fact); err != nil {
					return err
				}
				if attempts == 2 {
					if err := attempt.RequireActiveRunSourceAdmission(ctx, runID, firstFact); err == nil {
						t.Fatal("retry admitted the rolled-back source")
					}
				}
				return attempt.RequireActiveRunSourceAdmission(ctx, runID, fact)
			})
			return fact, err
		})
		if result.Err() != nil || !result.Acknowledged() || attempts != 2 {
			t.Fatalf("retry outcome: acknowledged=%v attempts=%d err=%v", result.Acknowledged(), attempts, result.Err())
		}
		if got, ok := result.Value(); !ok || !got.Matches(retryFact) {
			t.Fatalf("retry returned stale admission: fact=%v acknowledged=%v", got, ok)
		}
	})
}

func TestRunAdmissionCancellation(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		for _, name := range []string{"current context", "original caller", "original caller with drained context"} {
			t.Run(name, func(t *testing.T) {
				caller, cancelCaller := context.WithCancel(context.Background())
				defer cancelCaller()
				runID := uuid.NewString()
				fact, replacement := runAdmissionFact(t, "a"), runAdmissionFact(t, "b")
				withRunAdmissionAttempt(t, dialect, caller, func(ctx context.Context, attempt *Attempt, tx *sql.Tx) {
					if err := CacheActiveRunSource(ctx, tx, runID, fact); err != nil {
						t.Fatal(err)
					}
					checkCtx := ctx
					if name == "current context" {
						var cancel context.CancelFunc
						checkCtx, cancel = context.WithCancel(ctx)
						cancel()
						if caller.Err() != nil {
							t.Fatal("current-context test also canceled the caller")
						}
					} else {
						cancelCaller()
						if name == "original caller" {
							checkCtx = context.WithValue(caller, sqlAttemptKey{}, attempt)
						} else if checkCtx.Err() != nil {
							t.Fatal("native SQL context was not drained")
						}
					}
					requireRunAdmissionRefusal(t, checkCtx, attempt, tx, runID, replacement, context.Canceled)
					if len(attempt.runAdmissions) != 1 || !attempt.runAdmissions[runID].Matches(fact) {
						t.Fatal("canceled setter or invalidation changed the cached admission")
					}
					if name == "current context" {
						if err := attempt.RequireActiveRunSourceAdmission(ctx, runID, fact); err != nil {
							t.Fatalf("derived cancellation poisoned the live caller's admission: %v", err)
						}
					}
				})
			})
		}
	})
}

func TestRunAdmissionRunAndSourceIsolation(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		runID, foreignRun := uuid.NewString(), uuid.NewString()
		fact, otherFact := runAdmissionFact(t, "a"), runAdmissionFact(t, "b")
		withRunAdmissionAttempt(t, dialect, context.Background(), func(ctx context.Context, attempt *Attempt, tx *sql.Tx) {
			ctx = runtimecorrelation.WithSourceArtifactFact(ctx, otherFact)
			if err := CacheActiveRunSource(ctx, tx, runID, fact); err != nil {
				t.Fatal(err)
			}
			if got, cached, err := CachedActiveRunSource(ctx, tx, runID); err != nil || !cached || !got.Matches(fact) {
				t.Fatalf("context fact replaced canonical admission: fact=%v cached=%v err=%v", got, cached, err)
			}
			if _, cached, err := CachedActiveRunSource(ctx, tx, foreignRun); err != nil || cached {
				t.Fatalf("foreign run borrowed admission: cached=%v err=%v", cached, err)
			}
			for name, check := range map[string]func() error{
				"foreign run":     func() error { return attempt.RequireActiveRunSourceAdmission(ctx, foreignRun, fact) },
				"mismatched fact": func() error { return attempt.RequireActiveRunSourceAdmission(ctx, runID, otherFact) },
				"invalid fact":    func() error { return CacheActiveRunSource(ctx, tx, runID, runtimecorrelation.SourceArtifactFact{}) },
				"empty run":       func() error { return CacheActiveRunSource(ctx, tx, " ", fact) },
			} {
				if err := check(); err == nil {
					t.Fatalf("%s was admitted", name)
				}
			}
			if len(attempt.runAdmissions) != 1 || !attempt.runAdmissions[runID].Matches(fact) {
				t.Fatal("refused source/run input changed admission")
			}
			if err := CacheActiveRunSource(ctx, tx, foreignRun, otherFact); err != nil {
				t.Fatal(err)
			}
			if err := attempt.RequireActiveRunSourceAdmission(ctx, foreignRun, otherFact); err != nil {
				t.Fatal(err)
			}
			if err := attempt.RequireActiveRunSourceAdmission(ctx, runID, fact); err != nil {
				t.Fatalf("second run changed the first admission: %v", err)
			}
		})
	})
}

func TestRunAdmissionUUIDAliasInvalidation(t *testing.T) {
	runAdmissionDialects(t, func(t *testing.T, dialect privateactivity.Dialect) {
		runID := "aabbccdd-eeff-0011-2233-445566778899"
		aliases := []string{runID, strings.ToUpper(runID), strings.ReplaceAll(runID, "-", ""), "{" + runID + "}"}
		unrelated, fact := uuid.NewString(), runAdmissionFact(t, "a")
		withRunAdmissionAttempt(t, dialect, context.Background(), func(ctx context.Context, attempt *Attempt, tx *sql.Tx) {
			for _, key := range append(aliases, unrelated) {
				if err := CacheActiveRunSource(ctx, tx, key, fact); err != nil {
					t.Fatal(err)
				}
			}
			if err := InvalidateActiveRunSource(ctx, tx, " "+aliases[1]+" "); err != nil {
				t.Fatal(err)
			}
			for _, key := range append(aliases, unrelated) {
				want := key == unrelated || (dialect == privateactivity.DialectSQLite && key != aliases[1])
				got, cached, err := CachedActiveRunSource(ctx, tx, key)
				if err != nil || cached != want || (want && !got.Matches(fact)) || (!want && got != (runtimecorrelation.SourceArtifactFact{})) {
					t.Fatalf("alias %s after invalidation: fact=%v cached=%v err=%v, want cached=%v", key, got, cached, err, want)
				}
				if err := attempt.RequireActiveRunSourceAdmission(ctx, key, fact); (err == nil) != want {
					t.Fatalf("alias %s record admission: err=%v, want admitted=%v", key, err, want)
				}
			}
			// Unsupported UUID spellings must never leave a potentially stale PG entry.
			if err := InvalidateActiveRunSource(ctx, tx, "aabb-ccdd-eeff-0011-2233-4455-6677-8899"); err != nil {
				t.Fatal(err)
			}
			if _, cached, err := CachedActiveRunSource(ctx, tx, unrelated); err != nil || cached != (dialect == privateactivity.DialectSQLite) {
				t.Fatalf("fallback invalidation: cached=%v err=%v", cached, err)
			}
		})
	})
}
