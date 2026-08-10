package apiidempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apiidempotencycontract "github.com/division-sh/swarm/internal/apiidempotency"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresAPIIdempotencyTerminalReleaseFailuresFailClosedAndRestoreCapacity(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS api_idempotency (
			method TEXT NOT NULL,
			actor_token_id TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			resource_id TEXT NOT NULL,
			response JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (method, actor_token_id, idempotency_key)
		)
	`); err != nil {
		t.Fatalf("create api idempotency table: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM api_idempotency`); err != nil {
		t.Fatalf("clear api idempotency table: %v", err)
	}
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatalf("postgres backend: %v", err)
	}

	cases := []struct {
		name       string
		wantError  string
		corruptEnd func(*postgresbackend.AdvisoryLockLease)
	}{
		{
			name:      "unlock_false",
			wantError: "did not own the lock",
			corruptEnd: func(lease *postgresbackend.AdvisoryLockLease) {
				lease.SetUnlockForTest(func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
					return false, nil
				})
			},
		},
		{
			name:      "unlock_error",
			wantError: "unlock failed",
			corruptEnd: func(lease *postgresbackend.AdvisoryLockLease) {
				lease.SetUnlockForTest(func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) {
					return false, errors.New("unlock failed")
				})
			},
		},
		{
			name:      "session_close_error",
			wantError: "close failed",
			corruptEnd: func(lease *postgresbackend.AdvisoryLockLease) {
				lease.SetReleaseSessionForTest(func() error { return errors.New("close failed") })
			},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			owner, err := NewPostgres(backend, func() error { return nil })
			if err != nil {
				t.Fatalf("new owner: %v", err)
			}
			owner.acquire = func(ctx context.Context, backend *postgresbackend.Backend, key string) (*postgresbackend.AdvisoryLockLease, error) {
				lease, err := acquireAPIIdempotencyLease(ctx, backend, key)
				if err == nil {
					tc.corruptEnd(lease)
				}
				return lease, err
			}
			req := apiidempotencycontract.Request{
				Method:         "test.release",
				ActorTokenID:   "actor",
				IdempotencyKey: fmt.Sprintf("terminal-%d", i),
				RequestHash:    fmt.Sprintf("hash-%d", i),
				ResourceID:     fmt.Sprintf("resource-%d", i),
				Now:            time.Now().UTC(),
				TTL:            time.Hour,
			}
			completion, replay, err := owner.WithAPIIdempotency(context.Background(), req, func(context.Context) (apiidempotencycontract.Completion, error) {
				return apiidempotencycontract.Completion{ResourceID: req.ResourceID, Response: json.RawMessage(`{"ok":true}`)}, nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("terminal release error = %v, want %q", err, tc.wantError)
			}
			if replay || completion.ResourceID != "" || len(completion.Response) != 0 {
				t.Fatalf("terminal release fabricated success: completion=%+v replay=%v", completion, replay)
			}
			if got := backend.CapacityReservationsForTest(); got != 0 {
				t.Fatalf("capacity reservations = %d, want 0", got)
			}
			if got := db.Stats().MaxOpenConnections; got != 1 {
				t.Fatalf("max open connections = %d, want restored 1", got)
			}

			clean, err := NewPostgres(backend, func() error { return nil })
			if err != nil {
				t.Fatalf("new clean owner: %v", err)
			}
			reacquireCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			executed := false
			completion, replay, err = clean.WithAPIIdempotency(reacquireCtx, req, func(context.Context) (apiidempotencycontract.Completion, error) {
				executed = true
				return apiidempotencycontract.Completion{}, errors.New("persisted completion was not replayed")
			})
			if err != nil {
				t.Fatalf("reacquire/replay: %v", err)
			}
			if !replay || executed || completion.ResourceID != req.ResourceID || string(completion.Response) != `{"ok": true}` {
				t.Fatalf("reacquire result = completion=%+v replay=%v executed=%v", completion, replay, executed)
			}
			if got := backend.CapacityReservationsForTest(); got != 0 {
				t.Fatalf("capacity reservations after replay = %d, want 0", got)
			}
		})
	}
}
