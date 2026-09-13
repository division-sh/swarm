package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/operatorchannel"
	privateidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/google/uuid"
)

type mailboxCompletionLeaseFixture struct {
	replay  func() (apiidempotency.Completion, bool)
	commit  func(context.Context, *sql.Tx, apiidempotency.Completion) error
	release func()
}

// This proves the shared completion contract, separately from the served
// operation/anchor matrix that proves actual domain mutation atomicity.
func TestMailboxCompletionAdmissionAndTTLBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _ := decisionCardTestStore(t, backend)
			ctx := testAuthorActivityContext()
			principal, err := selected.(interface {
				EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error)
			}).EnsureOperatorPrincipal(ctx, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			db := DatabaseForTest(selected)
			acquire := func(req apiidempotency.Request) (mailboxCompletionLeaseFixture, error) {
				switch s := selected.(type) {
				case *PostgresStore:
					lease, err := privateidempotency.AcquirePostgresRequest(ctx, s.postgresOwner, req)
					if err != nil {
						return mailboxCompletionLeaseFixture{}, err
					}
					return mailboxCompletionLeaseFixture{replay: lease.Replay, commit: func(ctx context.Context, tx *sql.Tx, c apiidempotency.Completion) error {
						return privateidempotency.StorePostgresCompletionTx(ctx, lease, tx, c)
					}, release: func() {
						if err := lease.Release(ctx); err != nil {
							t.Error(err)
						}
					}}, nil
				case *SQLiteRuntimeStore:
					lease, err := privateidempotency.AcquireSQLiteRequest(ctx, s.sQLiteOwner, req)
					if err != nil {
						return mailboxCompletionLeaseFixture{}, err
					}
					return mailboxCompletionLeaseFixture{replay: lease.Replay, commit: func(ctx context.Context, tx *sql.Tx, c apiidempotency.Completion) error {
						return privateidempotency.StoreSQLiteCompletionTx(ctx, lease, tx, c)
					}, release: lease.Release}, nil
				default:
					panic("unexpected selected store")
				}
			}
			transaction := func(fn func(context.Context, *sql.Tx) error) error {
				switch s := selected.(type) {
				case *PostgresStore:
					return s.backend.RunTransaction(ctx, fn)
				case *SQLiteRuntimeStore:
					return s.backend.RunTransaction(ctx, "mailbox completion contract", fn)
				default:
					panic("unexpected selected store")
				}
			}
			for _, method := range []string{"mailbox.decide", "mailbox.defer", "mailbox.begin_input", "mailbox.cancel_input", "mailbox.acknowledge"} {
				t.Run(method, func(t *testing.T) {
					req := apiidempotency.Request{Method: method, Actor: apiidempotency.PrincipalActor(principal.ID), ResourceID: uuid.NewString(), RequestHash: "request", IdempotencyKey: method, Now: time.Now().UTC(), TTL: time.Hour}
					lease, err := acquire(req)
					if err != nil {
						t.Fatal(err)
					}
					completion := apiidempotency.Completion{ResourceID: req.ResourceID, Response: json.RawMessage(`{"ok":true}`)}
					for name, bad := range map[string]apiidempotency.Completion{"wrong_resource": {ResourceID: uuid.NewString(), Response: completion.Response}, "invalid_response": {ResourceID: req.ResourceID, Response: json.RawMessage(`{`)}} {
						if err := transaction(func(ctx context.Context, tx *sql.Tx) error { return lease.commit(ctx, tx, bad) }); err == nil {
							t.Fatalf("accepted %s", name)
						}
					}
					changed := uuid.NewString()
					if _, err := db.Exec(`UPDATE operator_principals SET principal_id=$1`, changed); err != nil {
						t.Fatal(err)
					}
					if err := transaction(func(ctx context.Context, tx *sql.Tx) error { return lease.commit(ctx, tx, completion) }); err == nil {
						t.Fatal("principal changed after acquisition but commit succeeded")
					}
					if _, err := db.Exec(`UPDATE operator_principals SET principal_id=$1`, principal.ID); err != nil {
						t.Fatal(err)
					}
					time.Sleep(25 * time.Millisecond)
					if err := transaction(func(ctx context.Context, tx *sql.Tx) error { return lease.commit(ctx, tx, completion) }); err != nil {
						t.Fatal(err)
					}
					lease.release()
					readTimes := func() (time.Time, time.Time) {
						var created, expires time.Time
						if backend == "postgres" {
							if err := db.QueryRow(`SELECT created_at,expires_at FROM api_idempotency WHERE method=$1`, method).Scan(&created, &expires); err != nil {
								t.Fatal(err)
							}
						} else {
							var c, e string
							if err := db.QueryRow(`SELECT created_at,expires_at FROM api_idempotency WHERE method=$1`, method).Scan(&c, &e); err != nil {
								t.Fatal(err)
							}
							var err error
							created, _, err = parseSQLiteTimeString(c)
							if err != nil {
								t.Fatal(err)
							}
							expires, _, err = parseSQLiteTimeString(e)
							if err != nil {
								t.Fatal(err)
							}
						}
						return created, expires
					}
					created, expires := readTimes()
					if created.Before(req.Now.Add(25*time.Millisecond)) || expires.Sub(created) != req.TTL {
						t.Fatalf("TTL shortened by preparation: request=%v created=%v expires=%v", req.Now, created, expires)
					}
					for name, change := range map[string]func(*apiidempotency.Request){
						"foreign":  func(r *apiidempotency.Request) { r.Actor = apiidempotency.PrincipalActor(changed) },
						"missing":  func(r *apiidempotency.Request) { r.Actor = apiidempotency.PrincipalActor("") },
						"bearer":   func(r *apiidempotency.Request) { r.Actor = apiidempotency.BearerActor(principal.ID) },
						"hash":     func(r *apiidempotency.Request) { r.RequestHash = "changed" },
						"resource": func(r *apiidempotency.Request) { r.ResourceID = uuid.NewString() },
					} {
						bad := req
						change(&bad)
						got, err := acquire(bad)
						if err == nil {
							got.release()
							t.Fatalf("replay admitted %s", name)
						}
						if (name == "hash" || name == "resource") && !errors.Is(err, apiidempotency.ErrConflict) {
							t.Fatalf("%s wrong error: %v", name, err)
						}
					}
					req.Now = expires.Add(-time.Microsecond)
					lease, err = acquire(req)
					if err != nil {
						t.Fatal(err)
					}
					stored, replayed := lease.replay()
					lease.release()
					if !replayed || !sameJSON(stored.Response, completion.Response) {
						t.Fatal("unexpired exact completion not replayed")
					}
					c, e := readTimes()
					if !c.Equal(created) || !e.Equal(expires) {
						t.Fatal("replay extended retention")
					}
					req.Now = expires
					lease, err = acquire(req)
					if err != nil {
						t.Fatal(err)
					}
					_, replayed = lease.replay()
					lease.release()
					if replayed {
						t.Fatal("expired completion replayed")
					}
				})
			}
		})
	}
}
