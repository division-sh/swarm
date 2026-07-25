package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresAdvisoryClaimRejectsUnownedBorrowedConnectionAndTransaction(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	t.Run("borrowed connection without private authority", func(t *testing.T) {
		eventID := uuid.NewString()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("open borrowed connection: %v", err)
		}
		defer conn.Close()
		borrowedCtx := runtimepipeline.WithPipelineSQLConnContext(ctx, conn)
		if _, err := selected.PipelineObligations().ClaimPublication(borrowedCtx, eventID); err == nil ||
			!strings.Contains(err.Error(), "lacks private session authority") {
			t.Fatalf("borrowed connection claim error = %v, want private-authority rejection", err)
		}
		assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
	})

	t.Run("transaction without private authority", func(t *testing.T) {
		eventID := uuid.NewString()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin foreign transaction: %v", err)
		}
		defer tx.Rollback()
		txCtx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
		if _, err := selected.PipelineObligations().ClaimPublication(txCtx, eventID); err == nil ||
			!strings.Contains(err.Error(), "lacks private session authority") {
			t.Fatalf("transaction-only claim error = %v, want private-authority rejection", err)
		}
		assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
	})
}

func TestPostgresDetachedWorkContextCannotReviveClosedSessionAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open session connection: %v", err)
	}
	session := newPostgresSessionAuthority(conn)
	detached := runtimepipeline.WithoutPipelineSQLConnContext(session.bindContext(ctx))
	if err := session.release(); err != nil {
		t.Fatalf("close parent session: %v", err)
	}
	if err := selected.runPostgresRuntimeMutation(detached, func(context.Context, *sql.Tx) error {
		return nil
	}); err != nil {
		t.Fatalf("detached work reused closed parent authority: %v", err)
	}
}

func TestPostgresAdvisoryClaimReleaseIgnoresForeignTransactionAndUnlocksExactSession(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	eventID := uuid.NewString()
	claim, err := selected.PipelineObligations().ClaimPublication(ctx, eventID)
	if err != nil {
		t.Fatalf("claim publication: %v", err)
	}

	foreignDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open foreign pool: %v", err)
	}
	defer foreignDB.Close()
	foreignTx, err := foreignDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin foreign transaction: %v", err)
	}
	defer foreignTx.Rollback()
	foreignCtx := runtimepipeline.WithPipelineSQLTxContext(ctx, foreignTx)
	if err := selected.PipelineObligations().Release(foreignCtx, claim); err != nil {
		t.Fatalf("release through exact retained session: %v", err)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
}

func TestPostgresPipelineClaimReleaseFailureIsTerminalAndReclaimable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		unlock     func(context.Context, *postgresSessionAuthority, string) (bool, error)
		wantDetail string
	}{
		{
			name: "false result",
			unlock: func(context.Context, *postgresSessionAuthority, string) (bool, error) {
				return false, nil
			},
			wantDetail: "did not own the lock",
		},
		{
			name: "query error",
			unlock: func(context.Context, *postgresSessionAuthority, string) (bool, error) {
				return false, errors.New("persistent injected unlock failure")
			},
			wantDetail: "persistent injected unlock failure",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, db, _ := testutil.StartPostgres(t)
			selected := newTestPostgresStore(t, db)
			db.SetMaxOpenConns(2)
			db.SetMaxIdleConns(2)
			ctx := testAuthorActivityContext()
			eventID := uuid.NewString()
			claim, err := selected.PipelineObligations().ClaimPublication(ctx, eventID)
			if err != nil {
				t.Fatalf("claim publication: %v", err)
			}
			state, err := selected.postgresPipelineClaimState(claim)
			if err != nil {
				t.Fatalf("load claim state: %v", err)
			}
			var attempts atomic.Int32
			state.postgresLease.testUnlock = func(ctx context.Context, authority *postgresSessionAuthority, key string) (bool, error) {
				attempts.Add(1)
				return tc.unlock(ctx, authority, key)
			}

			if err := selected.PipelineObligations().Release(ctx, claim); err == nil ||
				!strings.Contains(err.Error(), tc.wantDetail) {
				t.Fatalf("release error = %v, want %q", err, tc.wantDetail)
			}
			if attempts.Load() != 1 {
				t.Fatalf("unlock attempts = %d, want one terminal attempt", attempts.Load())
			}
			if _, err := selected.postgresPipelineClaimState(claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
				t.Fatalf("claim after terminal release = %v, want ErrStaleClaim", err)
			}
			if state.postgresLease != nil {
				t.Fatal("terminally released claim retained its lease")
			}
			if got := db.Stats().MaxOpenConnections; got != 2 {
				t.Fatalf("capacity after terminal release = %d, want 2", got)
			}
			assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))

			reclaimed, err := selected.PipelineObligations().ClaimPublication(ctx, eventID)
			if err != nil {
				t.Fatalf("fresh claim after terminal failure: %v", err)
			}
			if err := selected.PipelineObligations().Release(ctx, reclaimed); err != nil {
				t.Fatalf("release fresh claim: %v", err)
			}
		})
	}
}

func TestPostgresPipelineClaimSetupFailuresTerminallyReleaseAndReclaim(t *testing.T) {
	for _, tc := range []struct {
		name       string
		claim      func(context.Context, *PostgresStore, string) error
		wantDetail string
	}{
		{
			name: "issuer rejection",
			claim: func(ctx context.Context, selected *PostgresStore, eventID string) error {
				_, err := selected.claimPostgresPipelineEvent(ctx, eventID, runtimepipelineobligation.Purpose(""))
				return err
			},
			wantDetail: "purpose",
		},
		{
			name: "eligibility rejection",
			claim: func(ctx context.Context, selected *PostgresStore, eventID string) error {
				_, err := selected.PipelineObligations().ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposeRecovery)
				return err
			},
			wantDetail: runtimepipelineobligation.ErrIneligible.Error(),
		},
		{
			name: "hydration failure",
			claim: func(ctx context.Context, selected *PostgresStore, eventID string) error {
				_, err := selected.PipelineObligations().ClaimEvent(ctx, eventID, runtimepipelineobligation.PurposePublication)
				return err
			},
			wantDetail: "event record missing",
		},
	} {
		for _, failureMode := range []string{"fail_once", "persistent_failure"} {
			t.Run(tc.name+"/"+failureMode, func(t *testing.T) {
				dsn, db, _ := testutil.StartPostgres(t)
				selected := newTestPostgresStore(t, db)
				db.SetMaxOpenConns(2)
				db.SetMaxIdleConns(2)
				ctx := testAuthorActivityContext()
				eventID := uuid.NewString()
				registry := selected.postgresPipelineClaims()
				registry.testConfigureClaimLease = func(lease *sqlAdvisoryLockLease) {
					var attempts atomic.Int32
					lease.testUnlock = func(context.Context, *postgresSessionAuthority, string) (bool, error) {
						attempt := attempts.Add(1)
						if failureMode == "fail_once" && attempt > 1 {
							return true, nil
						}
						return false, errors.New(failureMode + " setup cleanup failure")
					}
				}
				t.Cleanup(func() { registry.testConfigureClaimLease = nil })

				err := tc.claim(ctx, selected, eventID)
				if err == nil ||
					!strings.Contains(err.Error(), tc.wantDetail) ||
					!strings.Contains(err.Error(), failureMode+" setup cleanup failure") {
					t.Fatalf("claim setup error = %v, want primary %q plus terminal cleanup evidence", err, tc.wantDetail)
				}
				registry.mu.Lock()
				for _, state := range registry.claims {
					if state != nil && state.claim.EventID() == eventID {
						registry.mu.Unlock()
						t.Fatal("failed claim setup retained a registry claim")
					}
				}
				registry.mu.Unlock()
				if got := db.Stats().MaxOpenConnections; got != 2 {
					t.Fatalf("capacity after failed claim setup = %d, want 2", got)
				}
				assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))

				registry.testConfigureClaimLease = nil
				reclaimed, err := selected.PipelineObligations().ClaimPublication(ctx, eventID)
				if err != nil {
					t.Fatalf("fresh claim after setup failure: %v", err)
				}
				if err := selected.PipelineObligations().Release(ctx, reclaimed); err != nil {
					t.Fatalf("release fresh claim: %v", err)
				}
			})
		}
	}
}

func TestPostgresPipelineClaimRetiresAfterUnlockedSessionCloseFailure(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	ctx := testAuthorActivityContext()
	eventID := uuid.NewString()
	claim, err := selected.PipelineObligations().ClaimPublication(ctx, eventID)
	if err != nil {
		t.Fatalf("claim publication: %v", err)
	}
	state, err := selected.postgresPipelineClaimState(claim)
	if err != nil {
		t.Fatalf("load claim state: %v", err)
	}
	state.postgresLease.releaseSession = func() error {
		return errors.New("injected session close failure")
	}

	if err := selected.PipelineObligations().Release(ctx, claim); err == nil ||
		!strings.Contains(err.Error(), "injected session close failure") {
		t.Fatalf("release error = %v, want session-close failure", err)
	}
	if _, err := selected.postgresPipelineClaimState(claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
		t.Fatalf("claim after successful unlock and failed session close = %v, want ErrStaleClaim", err)
	}
	if state.postgresLease != nil {
		t.Fatal("retired claim retained its PostgreSQL lease")
	}
	if got := db.Stats().MaxOpenConnections; got != 2 {
		t.Fatalf("capacity after terminal release failure = %d, want 2", got)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
}

func TestPostgresPipelineScanCloseFailureIsTerminalAndReclaimable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		unlock func(context.Context, *postgresSessionAuthority, string) (bool, error)
	}{
		{
			name: "fail once",
			unlock: func(context.Context, *postgresSessionAuthority, string) (bool, error) {
				return false, nil
			},
		},
		{
			name: "persistent failure",
			unlock: func(context.Context, *postgresSessionAuthority, string) (bool, error) {
				return false, errors.New("persistent injected scan release failure")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			selected := newTestPostgresStore(t, db)
			fixture := authorActivityReceiptFixture{
				store:   selected,
				db:      db,
				dialect: runtimeauthoractivity.DialectPostgres,
			}
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			eventID := commitPipelineParityEvent(t, ctx, selected, runID, time.Now().UTC().Add(-time.Minute))

			owner := selected.PipelineObligations()
			scan, err := owner.OpenScan(ctx, runtimepipelineobligation.GlobalScanRequest())
			if err != nil {
				t.Fatalf("open scan: %v", err)
			}
			batch, err := owner.ClaimBatch(ctx, scan, 1)
			if err != nil {
				t.Fatalf("claim batch: %v", err)
			}
			if len(batch.Work) != 1 || batch.Work[0].Claim.EventID() != eventID {
				t.Fatalf("claimed work = %#v, want event %s", batch.Work, eventID)
			}
			claim := batch.Work[0].Claim
			claimState, err := selected.postgresPipelineClaimState(claim)
			if err != nil {
				t.Fatalf("load claimed state: %v", err)
			}
			claimState.postgresLease.testUnlock = tc.unlock
			if err := owner.CloseScan(ctx, scan); err == nil {
				t.Fatal("close scan unexpectedly hid terminal cleanup evidence")
			}
			if _, err := selected.postgresPipelineScanState(scan); !errors.Is(err, runtimepipelineobligation.ErrStaleScan) {
				t.Fatalf("scan after failed close = %v, want ErrStaleScan", err)
			}
			if _, err := selected.postgresPipelineClaimState(claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
				t.Fatalf("claim after failed close = %v, want ErrStaleClaim", err)
			}

			fresh, err := owner.OpenScan(ctx, runtimepipelineobligation.GlobalScanRequest())
			if err != nil {
				t.Fatalf("open fresh scan: %v", err)
			}
			reclaimed, err := owner.ClaimBatch(ctx, fresh, 1)
			if err != nil {
				t.Fatalf("claim fresh batch: %v", err)
			}
			if len(reclaimed.Work) != 1 || reclaimed.Work[0].Claim.EventID() != eventID {
				t.Fatalf("fresh claimed work = %#v, want event %s", reclaimed.Work, eventID)
			}
			if err := owner.CloseScan(ctx, fresh); err != nil {
				t.Fatalf("close fresh scan: %v", err)
			}
		})
	}
}

func TestPostgresPipelineClaimUsesExactSessionAcrossCommitAndRollback(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	for _, tc := range []struct {
		name     string
		rollback bool
	}{
		{name: "commit"},
		{name: "rollback", rollback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventID := uuid.NewString()
			var claim runtimepipelineobligation.Claim
			wantErr := errors.New("force rollback")
			err := selected.runPostgresRuntimeMutation(ctx, func(txctx context.Context, _ *sql.Tx) error {
				var claimErr error
				claim, claimErr = selected.PipelineObligations().ClaimPublication(txctx, eventID)
				if claimErr != nil {
					return claimErr
				}
				if tc.rollback {
					return wantErr
				}
				return nil
			})
			if tc.rollback {
				if !errors.Is(err, wantErr) {
					t.Fatalf("mutation error = %v, want rollback sentinel", err)
				}
			} else if err != nil {
				t.Fatalf("mutation: %v", err)
			}
			if err := selected.PipelineObligations().Release(ctx, claim); err != nil {
				t.Fatalf("release after %s: %v", tc.name, err)
			}
			assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
		})
	}
}

func TestPostgresPipelineClaimReleasesRetainedReferenceBeforeTransactionCompletes(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	eventID := uuid.NewString()

	if err := selected.runPostgresRuntimeMutation(ctx, func(txctx context.Context, _ *sql.Tx) error {
		claim, err := selected.PipelineObligations().ClaimPublication(txctx, eventID)
		if err != nil {
			return err
		}
		return selected.PipelineObligations().Release(txctx, claim)
	}); err != nil {
		t.Fatalf("release retained claim before transaction completion: %v", err)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
}

func TestPostgresStartupOwnershipUsesRetainedSessionWithPoolSizeOne(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := admitTestPostgresStore(t, db)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 5*time.Second)
	defer cancel()

	lease, err := selected.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("pool-one-owner"))
	if err != nil {
		t.Fatalf("acquire startup ownership with pool size one: %v", err)
	}
	if _, err := lease.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatalf("persist startup transition on retained session: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release startup ownership: %v", err)
	}
	if got := db.Stats().InUse; got != 0 {
		t.Fatalf("startup ownership pool in-use after release = %d, want 0", got)
	}
}

func TestPostgresDestructiveResetLockReleasesExactSession(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	lockKey := "test:destructive-reset:" + uuid.NewString()
	lease, acquired, err := selected.TryAcquire(ctx, lockKey)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire destructive reset lock lease=%#v acquired=%v err=%v", lease, acquired, err)
	}

	foreignDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open foreign pool: %v", err)
	}
	defer foreignDB.Close()
	foreignTx, err := foreignDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin foreign transaction: %v", err)
	}
	defer foreignTx.Rollback()
	if err := lease.Release(runtimepipeline.WithPipelineSQLTxContext(ctx, foreignTx)); err != nil {
		t.Fatalf("release destructive reset lease: %v", err)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, lockKey)
}

func TestPostgresTerminalAdvisoryReleaseFailureDiscardsExactSession(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	lockKey := "test:terminal-advisory:" + uuid.NewString()
	lease, acquired, err := acquireAdvisoryLockLease(ctx, db, lockKey)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire terminal advisory lease=%#v acquired=%v err=%v", lease, acquired, err)
	}
	lease.testUnlock = func(context.Context, *postgresSessionAuthority, string) (bool, error) {
		return false, nil
	}
	if err := lease.Release(ctx); err == nil ||
		!strings.Contains(err.Error(), "did not own the lock") {
		t.Fatalf("terminal release error = %v, want checked false-unlock evidence", err)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, lockKey)
}

func TestPostgresAmbiguousAdvisoryAcquireDiscardsBorrowedSessionAfterTransaction(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	lockKey := "test:ambiguous-advisory-acquire:" + uuid.NewString()
	injected := errors.New("injected result scan failure after server acquisition")

	err := selected.runPostgresRuntimeMutation(ctx, func(txctx context.Context, _ *sql.Tx) error {
		_, _, acquireErr := acquireAdvisoryLockLeaseWith(
			txctx,
			db,
			lockKey,
			func(ctx context.Context, authority *postgresSessionAuthority, key string) (bool, error) {
				var acquired bool
				if err := authority.queryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&acquired); err != nil {
					return false, err
				}
				if !acquired {
					return false, errors.New("test advisory lock was unexpectedly busy")
				}
				return false, injected
			},
		)
		return acquireErr
	})
	if !errors.Is(err, injected) {
		t.Fatalf("ambiguous acquire error = %v, want injected scan failure", err)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, lockKey)
}

func assertIndependentAdvisoryLockAvailable(t *testing.T, dsn, lockKey string) {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open independent PostgreSQL pool: %v", err)
	}
	defer db.Close()
	var acquired bool
	if err := db.QueryRow(`SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired); err != nil {
		t.Fatalf("acquire independent advisory lock: %v", err)
	}
	if !acquired {
		t.Fatalf("independent advisory lock %q remained busy", lockKey)
	}
	var released bool
	if err := db.QueryRow(`SELECT pg_advisory_unlock(hashtext($1))`, lockKey).Scan(&released); err != nil {
		t.Fatalf("release independent advisory lock: %v", err)
	}
	if !released {
		t.Fatalf("independent advisory lock %q reported false unlock", lockKey)
	}
}

var _ runtimedestructivereset.LockLease = (*sqlAdvisoryLockLease)(nil)
