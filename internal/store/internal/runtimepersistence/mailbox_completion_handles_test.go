package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	privateidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// The served matrix exercises all thirteen domain cells; this complementary
// proof gives each competing caller an independently constructed store/SQL pool.
func TestMailboxCompletionAcrossSelectedStoreHandlesBothStores(t *testing.T) {
	type result struct {
		completion apiidempotency.Completion
		replayed   bool
		err        error
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			var execute [2]func(apiidempotency.Request) result
			var principal string
			if backend == "sqlite" {
				path := filepath.Join(t.TempDir(), "two-handles.sqlite")
				for i := range 2 {
					s := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
					p, err := s.EnsureOperatorPrincipal(ctx, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					if i != 0 && principal != p.ID {
						t.Fatal("second handle invented another selected-store principal")
					}
					principal = p.ID
					execute[i] = func(req apiidempotency.Request) result {
						lease, err := privateidempotency.AcquireSQLiteRequest(ctx, s.sQLiteOwner, req)
						if err != nil {
							return result{err: err}
						}
						defer lease.Release()
						if c, ok := lease.Replay(); ok {
							return result{completion: c, replayed: true}
						}
						completion := apiidempotency.Completion{ResourceID: req.ResourceID, Response: json.RawMessage(`{"ok":true,"original":1}`)}
						err = s.backend.RunTransaction(ctx, "mailbox independent handle completion", func(txctx context.Context, tx *sql.Tx) error {
							return privateidempotency.StoreSQLiteCompletionTx(txctx, lease, tx, completion)
						})
						return result{completion: completion, err: err}
					}
				}
			} else {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				for i := range 2 {
					db, err := sql.Open("postgres", dsn)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = db.Close() })
					s := admitTestPostgresStore(t, db)
					p, err := s.EnsureOperatorPrincipal(ctx, time.Now())
					if err != nil {
						t.Fatal(err)
					}
					if i != 0 && principal != p.ID {
						t.Fatal("second handle invented another selected-store principal")
					}
					principal = p.ID
					execute[i] = func(req apiidempotency.Request) (r result) {
						lease, err := privateidempotency.AcquirePostgresRequest(ctx, s.postgresOwner, req)
						if err != nil {
							return result{err: err}
						}
						defer func() { r.err = errors.Join(r.err, lease.Release(ctx)) }()
						if c, ok := lease.Replay(); ok {
							return result{completion: c, replayed: true}
						}
						completion := apiidempotency.Completion{ResourceID: req.ResourceID, Response: json.RawMessage(`{"ok":true,"original":1}`)}
						err = s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
							return privateidempotency.StorePostgresCompletionTx(txctx, lease, tx, completion)
						})
						return result{completion: completion, err: err}
					}
				}
			}
			for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input", "mailbox.acknowledge"} {
				t.Run(method, func(t *testing.T) {
					req := apiidempotency.Request{Method: method, Actor: apiidempotency.PrincipalActor(principal), ResourceID: uuid.NewString(), RequestHash: "same-body", IdempotencyKey: uuid.NewString(), Now: time.Now(), TTL: time.Hour}
					start, done := make(chan struct{}), make(chan result, 4)
					for i := range 4 {
						go func() { <-start; done <- execute[i%2](req) }()
					}
					close(start)
					fresh := 0
					for range 4 {
						got := <-done
						if got.err != nil || got.completion.ResourceID != req.ResourceID || !sameJSON(got.completion.Response, json.RawMessage(`{"ok":true,"original":1}`)) {
							t.Errorf("independent store result: %+v", got)
						}
						if !got.replayed {
							fresh++
						}
					}
					if fresh != 1 {
						t.Fatalf("independent handles executed %d times", fresh)
					}
				})
			}
		})
	}
}
