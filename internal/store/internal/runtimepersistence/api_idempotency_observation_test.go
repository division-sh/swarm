package runtimepersistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type apiCardinalityReceiptWriter interface {
	WithAPIIdempotency(context.Context, apiidempotency.Request, func(context.Context) (apiidempotency.Completion, error)) (apiidempotency.Completion, bool, error)
}

func TestAPIIdempotencyCardinalityPreservesExactMethodKeyAndAllActorsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			writer := f.store.(apiCardinalityReceiptWriter)
			ctx := testAuthorActivityContext()
			for _, receipt := range []struct{ method, actor, key string }{
				{"event.publish", "actor-one", "exact-key"},
				{"event.publish", "actor-two", "exact-key"},
				{"event.replay", "actor-one", "exact-key"},
				{"event.publish", "actor-one", "other-key"},
			} {
				commitAPIObservationReceipt(t, ctx, writer, receipt.method, receipt.actor, receipt.key)
			}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, query := range []struct {
				method, key string
				want        int
			}{
				{"event.publish", "exact-key", 2}, {"event.replay", "exact-key", 1},
				{"event.publish", "other-key", 1}, {"missing", "exact-key", 0},
				{"event.publish", "missing", 0}, {" event.publish", "exact-key", 0},
				{"event.publish", " exact-key ", 0},
			} {
				var original int
				if err := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM api_idempotency WHERE method = $1 AND idempotency_key = $2`, query.method, query.key).Scan(&original); err != nil {
					t.Fatal(err)
				}
				got, err := ReadAPIIdempotencyCardinalityForTest(ctx, f.store, query.method, query.key)
				if err != nil || got != original || got != query.want {
					t.Fatalf("method/key physical count %q/%q=%d old=%d want=%d err=%v", query.method, query.key, got, original, query.want, err)
				}
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 7 || counts.Total.ReadCommits != 7 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("API cardinality bypassed selected read owner or mutated receipts: %+v", counts)
			}
		})
	}
}

func commitAPIObservationReceipt(t *testing.T, ctx context.Context, writer apiCardinalityReceiptWriter, method, actor, key string) {
	t.Helper()
	params, err := json.Marshal(map[string]string{"method": method, "actor": actor, "key": key})
	if err != nil {
		t.Fatal(err)
	}
	params, err = canonicaljson.Canonicalize(params)
	if err != nil {
		t.Fatal(err)
	}
	req := apiidempotency.Request{Method: method, Actor: apiidempotency.BearerActor(actor), IdempotencyKey: key,
		RequestHash: fmt.Sprintf("sha256:%x", sha256.Sum256(params)), ResourceID: "observation-receipt", Now: time.Now().UTC()}
	if _, replay, err := writer.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) {
		return apiidempotency.Completion{ResourceID: req.ResourceID, Response: json.RawMessage(`{"ok":true}`)}, nil
	}); err != nil || replay {
		t.Fatal("native API observation receipt did not commit:", replay, err)
	}
}

func TestAPIIdempotencyCardinalityRefusesInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if count, err := ReadAPIIdempotencyCardinalityForTest(context.Background(), selected, "event.publish", "key"); err == nil || count != 0 {
			t.Fatal("invalid API observer became successful absence:", count, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := backend.open(t)
			for _, query := range [][2]string{{"", "key"}, {"method", ""}} {
				if count, err := ReadAPIIdempotencyCardinalityForTest(context.Background(), f.store, query[0], query[1]); err == nil || count != 0 {
					t.Fatal("incomplete API identity became successful absence:", count, err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if count, err := ReadAPIIdempotencyCardinalityForTest(ctx, f.store, "event.publish", "key"); !errors.Is(err, context.Canceled) || count != 0 {
				t.Fatal("cancelled API observation lost refusal:", count, err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if count, err := ReadAPIIdempotencyCardinalityForTest(context.Background(), f.store, "event.publish", "key"); err == nil || count != 0 {
				t.Fatal("closed API observer became successful absence:", count, err)
			}
		})
	}
}
