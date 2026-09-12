package apiidempotency

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	apiidempotencycontract "github.com/division-sh/swarm/internal/apiidempotency"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/lib/pq"
)

func TestAPIIdempotencyGracefulSQLBoundaries(t *testing.T) {
	for _, phase := range []string{"before_admission", "purge", "completion", "external_callback"} {
		t.Run(phase, func(t *testing.T) {
			_, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			db.SetMaxOpenConns(1)
			if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS api_idempotency(method TEXT NOT NULL, actor_token_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, request_hash TEXT NOT NULL, resource_id TEXT NOT NULL, response JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, PRIMARY KEY(method,actor_token_id,idempotency_key))`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var beforePID int
			if err := conn.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&beforePID); err != nil {
				t.Fatal(err)
			}
			if err := conn.Raw(func(raw any) error {
				pq.SetNoticeHandler(raw.(driver.Conn), func(n *pq.Error) {
					if n.Message == "graceful-stop" {
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
			if phase == "purge" || phase == "completion" {
				if _, err := db.Exec(`CREATE FUNCTION api_stop_probe() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'graceful-stop'; PERFORM pg_sleep(0.02); RETURN NULL; END $$`); err != nil {
					t.Fatal(err)
				}
				event := "DELETE"
				if phase == "completion" {
					event = "INSERT"
				}
				if _, err := db.Exec(`CREATE TRIGGER api_stop BEFORE ` + event + ` ON api_idempotency FOR EACH STATEMENT EXECUTE FUNCTION api_stop_probe()`); err != nil {
					t.Fatal(err)
				}
			}
			backend, err := postgresbackend.New(db)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := NewPostgres(backend, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if phase == "before_admission" {
				cancel()
			}
			req := apiidempotencycontract.Request{Method: "test.stop", ActorTokenID: "actor", IdempotencyKey: "key", RequestHash: "hash", ResourceID: "resource", Now: time.Now().UTC(), TTL: time.Hour}
			calls := 0
			_, _, err = owner.WithAPIIdempotency(ctx, req, func(callbackCtx context.Context) (apiidempotencycontract.Completion, error) {
				calls++
				if callbackCtx != ctx || callbackCtx.Done() == nil {
					t.Error("external API callback gained detached SQL authority")
				}
				if phase == "external_callback" {
					cancel()
				}
				return apiidempotencycontract.Completion{ResourceID: "resource", Response: json.RawMessage(`{"ok":true}`)}, nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("stop lost: %v", err)
			}
			wantCalls := 0
			if phase == "completion" || phase == "external_callback" {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatalf("callbacks=%d want=%d", calls, wantCalls)
			}
			var afterPID, rows int
			if err := db.QueryRow("SELECT pg_backend_pid(), (SELECT count(*) FROM api_idempotency)").Scan(&afterPID, &rows); err != nil {
				t.Fatal(err)
			}
			if afterPID != beforePID || rows != 0 {
				t.Fatalf("healthy PID %d -> %d; completion rows=%d", beforePID, afterPID, rows)
			}
			if backend.CapacityReservationsForTest() != 0 {
				t.Fatal("retained capacity leaked")
			}
			if phase == "purge" || phase == "completion" {
				if _, err := db.Exec("DROP TRIGGER api_stop ON api_idempotency"); err != nil {
					t.Fatal(err)
				}
			}
			lease, err := AcquirePostgresRequest(context.Background(), owner, req)
			if err != nil {
				t.Fatalf("successor acquisition: %v", err)
			}
			if err := lease.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAPIIdempotencyContendedAdmissionCancellation(t *testing.T) {
	dsn, observer, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	if _, err := observer.Exec(`CREATE TABLE IF NOT EXISTS api_idempotency(method TEXT NOT NULL, actor_token_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, request_hash TEXT NOT NULL, resource_id TEXT NOT NULL, response JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, PRIMARY KEY(method,actor_token_id,idempotency_key))`); err != nil {
		t.Fatal(err)
	}
	req := apiidempotencycontract.Request{Method: "test.wait", ActorTokenID: "actor", IdempotencyKey: "key", RequestHash: "hash", ResourceID: "resource", Now: time.Now().UTC(), TTL: time.Hour}
	key := apiIdempotencyLockKey(req.Method, req.ActorTokenID, req.IdempotencyKey)
	holder, err := observer.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(context.Background(), "SELECT pg_advisory_lock(hashtext($1))", key); err != nil {
		t.Fatal(err)
	}
	defer holder.ExecContext(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", key)
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	var waiterPID int
	if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&waiterPID); err != nil {
		t.Fatal(err)
	}
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewPostgres(backend, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	executed := false
	go func() {
		_, _, err := owner.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotencycontract.Completion, error) {
			executed = true
			return apiidempotencycontract.Completion{}, errors.New("executor admitted during lock wait")
		})
		done <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		if err := observer.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='advisory')", waiterPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real advisory wait never reached")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.Canceled) || executed {
			t.Fatalf("canceled admission err=%v executed=%t", err, executed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled admission did not join")
	}
	var held bool
	if err := holder.QueryRowContext(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted)").Scan(&held); err != nil || !held {
		t.Fatalf("holder possession changed: %t %v", held, err)
	}
	if _, err := holder.ExecContext(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", key); err != nil {
		t.Fatal(err)
	}
	var nextPID int
	if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&nextPID); err != nil {
		t.Fatal(err)
	}
	if nextPID == waiterPID {
		t.Fatal("interrupted ungranted waiter was returned to pool")
	}
	lease, err := AcquirePostgresRequest(context.Background(), owner, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.CapacityReservationsForTest() != 0 {
		t.Fatal("waiter capacity leaked")
	}
}
