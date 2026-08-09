package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	runtimepipelinefixture "github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
	"github.com/google/uuid"
)

func TestPostgresAdvisoryClaimIgnoresBorrowedConnectionAndTransaction(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open borrowed connection: %v", err)
	}
	defer conn.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin foreign transaction: %v", err)
	}
	defer tx.Rollback()

	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "borrowed connection", ctx: runtimepipelinefixture.WithSQLConn(ctx, conn)},
		{name: "foreign transaction", ctx: runtimepipelinefixture.WithSQLTx(ctx, tx)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventID := uuid.NewString()
			claim, err := selected.PipelineObligations().ClaimPublication(tc.ctx, eventID)
			if err != nil {
				t.Fatalf("claim through context carrying unrelated SQL authority: %v", err)
			}
			if err := selected.PipelineObligations().Release(tc.ctx, claim); err != nil {
				t.Fatalf("release privately owned claim: %v", err)
			}
			assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
		})
	}
}
func TestPostgresRuntimeMutationCannotReviveClosedSessionAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("open session connection: %v", err)
	}
	session, err := postgresbackend.NewSessionAuthority(conn)
	if err != nil {
		t.Fatalf("create parent session: %v", err)
	}
	if err := session.Release(); err != nil {
		t.Fatalf("close parent session: %v", err)
	}
	if err := selected.runPostgresRuntimeMutation(ctx, func(context.Context, *sql.Tx) error {
		return nil
	}); err != nil {
		t.Fatalf("runtime mutation failed to acquire independent authority: %v", err)
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
	foreignCtx := runtimepipelinefixture.WithSQLTx(ctx, foreignTx)
	if err := selected.PipelineObligations().Release(foreignCtx, claim); err != nil {
		t.Fatalf("release through exact retained session: %v", err)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))
}

func TestPostgresPipelineClaimReleaseFailureIsTerminalAndReclaimable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		unlock     func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error)
		wantDetail string
	}{
		{
			name: "false result",
			unlock: func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
				return false, nil
			},
			wantDetail: "did not own the lock",
		},
		{
			name: "query error",
			unlock: func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
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
			state, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim)
			if err != nil {
				t.Fatalf("load claim state: %v", err)
			}
			var attempts atomic.Int32
			state.LeaseForTest().SetUnlockForTest(func(ctx context.Context, authority *postgresbackend.SessionAuthority, key string) (bool, error) {
				attempts.Add(1)
				return tc.unlock(ctx, authority, key)
			})

			if err := selected.PipelineObligations().Release(ctx, claim); err == nil ||
				!strings.Contains(err.Error(), tc.wantDetail) {
				t.Fatalf("release error = %v, want %q", err, tc.wantDetail)
			}
			if attempts.Load() != 1 {
				t.Fatalf("unlock attempts = %d, want one terminal attempt", attempts.Load())
			}
			if _, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
				t.Fatalf("claim after terminal release = %v, want ErrStaleClaim", err)
			}
			if state.LeaseForTest() != nil {
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
				_, err := selected.pipelinePostgresOwner.ClaimPostgresPipelineEventForTest(ctx, eventID, runtimepipelineobligation.Purpose(""))
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
				registry := selected.pipelinePostgresOwner.PostgresPipelineClaimsForTest()
				configureLease := func(lease *postgresbackend.AdvisoryLockLease) {
					var attempts atomic.Int32
					lease.SetUnlockForTest(func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
						attempt := attempts.Add(1)
						if failureMode == "fail_once" && attempt > 1 {
							return true, nil
						}
						return false, errors.New(failureMode + " setup cleanup failure")
					})
				}
				registry.SetHooksForTest(nil, nil, configureLease)
				t.Cleanup(func() { registry.SetHooksForTest(nil, nil, nil) })

				err := tc.claim(ctx, selected, eventID)
				if err == nil ||
					!strings.Contains(err.Error(), tc.wantDetail) ||
					!strings.Contains(err.Error(), failureMode+" setup cleanup failure") {
					t.Fatalf("claim setup error = %v, want primary %q plus terminal cleanup evidence", err, tc.wantDetail)
				}
				if registry.ContainsEventForTest(eventID) {
					t.Fatal("failed claim setup retained a registry claim")
				}
				if got := db.Stats().MaxOpenConnections; got != 2 {
					t.Fatalf("capacity after failed claim setup = %d, want 2", got)
				}
				assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))

				registry.SetHooksForTest(nil, nil, nil)
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
	state, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim)
	if err != nil {
		t.Fatalf("load claim state: %v", err)
	}
	state.LeaseForTest().SetReleaseSessionForTest(func() error {
		return errors.New("injected session close failure")
	})

	if err := selected.PipelineObligations().Release(ctx, claim); err == nil ||
		!strings.Contains(err.Error(), "injected session close failure") {
		t.Fatalf("release error = %v, want session-close failure", err)
	}
	if _, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
		t.Fatalf("claim after successful unlock and failed session close = %v, want ErrStaleClaim", err)
	}
	if state.LeaseForTest() != nil {
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
		unlock func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error)
	}{
		{
			name: "fail once",
			unlock: func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
				return false, nil
			},
		},
		{
			name: "persistent failure",
			unlock: func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
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
				dialect: authoractivityfixture.DialectPostgres,
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
			claimState, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim)
			if err != nil {
				t.Fatalf("load claimed state: %v", err)
			}
			claimState.LeaseForTest().SetUnlockForTest(tc.unlock)
			if err := owner.CloseScan(ctx, scan); err == nil {
				t.Fatal("close scan unexpectedly hid terminal cleanup evidence")
			}
			if _, err := selected.pipelinePostgresOwner.PostgresPipelineScanOpenClaimCountForTest(scan); !errors.Is(err, runtimepipelineobligation.ErrStaleScan) {
				t.Fatalf("scan after failed close = %v, want ErrStaleScan", err)
			}
			if _, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
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

func TestPostgresPipelineClaimsOwnIndependentPrivateSessions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	claims := make([]runtimepipelineobligation.Claim, 0, 2)
	for range 2 {
		claim, err := selected.PipelineObligations().ClaimPublication(ctx, uuid.NewString())
		if err != nil {
			t.Fatalf("claim publication: %v", err)
		}
		claims = append(claims, claim)
	}
	first, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claims[0])
	if err != nil {
		t.Fatalf("load first claim: %v", err)
	}
	second, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claims[1])
	if err != nil {
		t.Fatalf("load second claim: %v", err)
	}
	firstSession := first.LeaseForTest().Session()
	secondSession := second.LeaseForTest().Session()
	if firstSession == nil || secondSession == nil || firstSession == secondSession {
		t.Fatal("each claim must retain its own private session")
	}
	for _, claim := range claims {
		if err := selected.PipelineObligations().Release(ctx, claim); err != nil {
			t.Fatalf("release private claim: %v", err)
		}
	}
}
func TestPostgresPipelineClaimPoisonBetweenLeaseAttachAndRegistryPublicationIsTerminal(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	ctx := testAuthorActivityContext()
	eventID := uuid.NewString()
	registry := selected.pipelinePostgresOwner.PostgresPipelineClaimsForTest()
	scan, err := selected.PipelineObligations().OpenScan(ctx, runtimepipelineobligation.GlobalScanRequest())
	if err != nil {
		t.Fatalf("open scan: %v", err)
	}
	var (
		hookCalled bool
		hookErr    error
	)
	configureLease := func(lease *postgresbackend.AdvisoryLockLease) {
		hookCalled = true
		session := lease.Session()
		if !session.AttachedForTest(lease) {
			hookErr = errors.New("lease was not attached before the publication boundary")
			return
		}
		hookErr = session.ForceDiscard()
	}
	registry.SetHooksForTest(nil, nil, configureLease)
	t.Cleanup(func() { registry.SetHooksForTest(nil, nil, nil) })

	if _, err := selected.PipelineObligations().ClaimPublication(ctx, eventID); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
		t.Fatalf("claim across poisoned attach/publication boundary = %v, want ErrStaleClaim", err)
	}
	if !hookCalled {
		t.Fatal("attach/publication poison hook was not called")
	}
	if hookErr != nil {
		t.Fatalf("poison attached lease: %v", hookErr)
	}
	acquiring := registry.AcquiringForTest(eventID)
	retainedClaim := registry.ContainsEventForTest(eventID)
	reservations := selected.backend.CapacityReservationsForTest()
	if acquiring {
		t.Fatal("poisoned attach/publication boundary retained its acquiring reservation")
	}
	if retainedClaim {
		t.Fatal("poisoned attach/publication boundary retained a registry claim")
	}
	if reservations != 0 {
		t.Fatalf("poisoned attach/publication boundary retained %d capacity reservation(s)", reservations)
	}
	openClaims, err := selected.pipelinePostgresOwner.PostgresPipelineScanOpenClaimCountForTest(scan)
	if err != nil {
		t.Fatalf("load scan after poisoned publication: %v", err)
	}
	if got := openClaims; got != 0 {
		t.Fatalf("poisoned attach/publication boundary retained %d scan membership(s)", got)
	}
	if got := db.Stats().MaxOpenConnections; got != 2 {
		t.Fatalf("capacity after poisoned attach/publication boundary = %d, want 2", got)
	}
	assertIndependentAdvisoryLockAvailable(t, dsn, replayClaimLockKey(eventID))

	registry.SetHooksForTest(nil, nil, nil)
	reclaimed, err := selected.PipelineObligations().ClaimPublication(ctx, eventID)
	if err != nil {
		t.Fatalf("local reclaim after poisoned attach/publication boundary: %v", err)
	}
	if err := selected.PipelineObligations().Release(ctx, reclaimed); err != nil {
		t.Fatalf("release locally reclaimed publication: %v", err)
	}
	if err := selected.PipelineObligations().CloseScan(ctx, scan); err != nil {
		t.Fatalf("close scan after poisoned publication: %v", err)
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
	_, db, _ := testutil.StartPostgres(t)
	selected := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	lease, acquired, err := selected.AcquireDestructiveReset(ctx)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire destructive reset lock lease=%#v acquired=%v err=%v", lease, acquired, err)
	}

	foreignTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin foreign transaction: %v", err)
	}
	defer foreignTx.Rollback()
	if err := lease.Release(runtimepipelinefixture.WithSQLTx(ctx, foreignTx)); err != nil {
		t.Fatalf("release destructive reset lease: %v", err)
	}
	reacquired, acquired, err := selected.AcquireDestructiveReset(ctx)
	if err != nil || !acquired || reacquired == nil {
		t.Fatalf("reacquire destructive reset lease=%#v acquired=%v err=%v", reacquired, acquired, err)
	}
	if err := reacquired.Release(ctx); err != nil {
		t.Fatalf("release reacquired destructive reset lease: %v", err)
	}
}

func TestPostgresTerminalAdvisoryReleaseFailureDiscardsExactSession(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	lockKey := "test:terminal-advisory:" + uuid.NewString()
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(ctx, db, lockKey)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire terminal advisory lease=%#v acquired=%v err=%v", lease, acquired, err)
	}
	lease.SetUnlockForTest(func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
		return false, nil
	})
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
		_, _, acquireErr := postgresbackend.AcquireAdvisoryLockLeaseWith(
			txctx,
			db,
			lockKey,
			func(ctx context.Context, authority *postgresbackend.SessionAuthority, key string) (bool, error) {
				var acquired bool
				if err := authority.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&acquired); err != nil {
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

var _ runtimedestructivereset.LockLease = (*postgresbackend.AdvisoryLockLease)(nil)
