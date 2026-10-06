package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/google/uuid"
)

func TestResetTransportCacheStoragePreservesAllKeysActorsAndExpiryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := selectedForkDiscardTestStore(t, backend)
			ctx := testAuthorActivityContext()
			owner := selected.(apiv1.APIIdempotencyStore)
			now := time.Now().UTC()
			for _, row := range []struct{ method, actor, key string }{
				{"runtime.nuke", "first", "one"},
				{"runtime.nuke", "first", "two"},
				{"runtime.nuke", "other", "one"},
				{"runtime.nuke ", "first", "one"},
				{"runtime.pause", "first", "one"},
			} {
				_, _, err := owner.WithAPIIdempotency(ctx, apiidempotency.Request{
					Method: row.method, Actor: apiidempotency.BearerActor(row.actor),
					IdempotencyKey: row.key, RequestHash: row.method, Now: now, TTL: time.Minute,
				}, func(context.Context) (apiidempotency.Completion, error) {
					return apiidempotency.Completion{Response: json.RawMessage(`{"ok":true}`)}, nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil {
				t.Fatal(err)
			}
			if count, err := ReadResetTransportCacheEntryCountForTest(ctx, selected); err != nil || count != 3 {
				t.Fatalf("all reset keys/actors: count=%d err=%v", count, err)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("cache observation mutated original owner: %v", err)
			}
			_, _, err = owner.WithAPIIdempotency(ctx, apiidempotency.Request{
				Method: "test.expire-transport-cache", Actor: apiidempotency.BearerActor("expiry-proof"),
				IdempotencyKey: uuid.NewString(), RequestHash: "expiry-proof", Now: now.Add(48 * time.Hour), TTL: time.Minute,
			}, func(context.Context) (apiidempotency.Completion, error) {
				return apiidempotency.Completion{Response: json.RawMessage(`{"ok":true}`)}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if count, err := ReadResetTransportCacheEntryCountForTest(ctx, selected); err != nil || count != 0 {
				t.Fatalf("original expiry did not clear every reset cache row: count=%d err=%v", count, err)
			}
		})
	}
}

func TestResetTransportCacheStorageRefusesUnavailableOriginalOwnersBothStores(t *testing.T) {
	ctx := context.Background()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if count, err := ReadResetTransportCacheEntryCountForTest(ctx, selected); err == nil || count != 0 {
			t.Fatalf("unowned cache evidence: count=%d err=%v", count, err)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"cancelled", "closed", "read-failed"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				selected, db, _ := selectedForkDiscardTestStore(t, backend)
				readCtx := ctx
				switch cut {
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					readCtx = cancelled
				case "closed":
					if err := selected.(interface{ Close() error }).Close(); err != nil {
						t.Fatal(err)
					}
				case "read-failed":
					if _, err := db.ExecContext(ctx, `ALTER TABLE api_idempotency RENAME TO unavailable_reset_transport_cache`); err != nil {
						t.Fatal(err)
					}
				}
				count, err := ReadResetTransportCacheEntryCountForTest(readCtx, selected)
				if err == nil || count != 0 || (cut == "cancelled" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("unavailable cache read returned evidence: count=%d err=%v", count, err)
				}
			})
		}
	}
}
