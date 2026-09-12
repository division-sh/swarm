package genericschedule

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestGenericClaimGracefulCancellationPreservesExactSession(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewPostgres(backend, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer owner.ReleaseGenericScheduleClaims(context.Background())
	conn, err := owner.ensureClaimConn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const id = "00000000-0000-4000-8000-000000002444"
	due := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wakeup, err := runtimegenericschedule.NewWakeup(id, due)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(context.Background(), `
		CREATE FUNCTION pg_temp.claim_type() RETURNS text LANGUAGE plpgsql AS $$
		BEGIN RAISE NOTICE 'claim-active'; PERFORM pg_sleep(0.02); RETURN 'timer'; END $$;
		CREATE TEMP TABLE runs(run_id uuid, status text);
		CREATE TEMP VIEW timers AS SELECT '00000000-0000-4000-8000-000000002444'::uuid AS timer_id,
		NULL::uuid AS run_id, pg_temp.claim_type() AS task_type, 'active'::text AS status,
		'2026-09-10 12:00:00+00'::timestamptz AS fire_at`)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notices := 0
	if err := conn.Raw(func(raw any) error {
		pq.SetNoticeHandler(raw.(driver.Conn), func(*pq.Error) { notices++; cancel() })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var before, after int
	if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	claimed, err := owner.ClaimGenericScheduleWakeup(ctx, wakeup)
	if err != nil || !claimed || notices == 0 {
		t.Fatalf("admitted claim=%v error=%v notices=%d", claimed, err, notices)
	}
	if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&after); err != nil || before != after {
		t.Fatalf("session changed: %v", err)
	}
	if claimed, err := owner.ClaimGenericScheduleWakeup(ctx, wakeup); claimed || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission=%v %v", claimed, err)
	}
	if claimed, err := owner.ClaimGenericScheduleWakeup(context.Background(), wakeup); err != nil || !claimed {
		t.Fatalf("healthy successor=%v %v", claimed, err)
	}
	var competing bool
	if err := db.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock(hashtext($1))", claimKey(wakeup)).Scan(&competing); err != nil || competing {
		t.Fatalf("possession lost: competing=%v %v", competing, err)
	}
	if err := owner.ReleaseGenericScheduleWakeup(ctx, wakeup); err != nil {
		t.Fatal(err)
	}
	observer, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	if err := observer.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock(hashtext($1))", claimKey(wakeup)).Scan(&competing); err != nil || !competing {
		t.Fatalf("claim not released: %v %v", competing, err)
	}
	if _, err := observer.ExecContext(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", claimKey(wakeup)); err != nil {
		t.Fatal(err)
	}
}

func TestGenericClaimUnsafeSessionFencesCachedKeys(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewPostgres(backend, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer owner.ReleaseGenericScheduleClaims(context.Background())
	conn, err := owner.ensureClaimConn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wakeup, err := runtimegenericschedule.NewWakeup("00000000-0000-4000-8000-000000002445", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_lock(hashtext($1))", claimKey(wakeup)); err != nil {
		t.Fatal(err)
	}
	owner.claims.keys = map[string]struct{}{claimKey(wakeup): {}}
	var pid int
	if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	if claimed, err := owner.ClaimGenericScheduleWakeup(context.Background(), wakeup); err == nil || claimed {
		t.Fatalf("unsafe cached claim=%v %v", claimed, err)
	}
	if owner.claims.conn != nil || len(owner.claims.keys) != 0 {
		t.Fatal("unsafe cached authority survived")
	}
	if _, err := conn.ExecContext(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("unsafe successor admitted")
	}
}
