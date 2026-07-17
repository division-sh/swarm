package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresStore_APIIdempotencyReplaysAndConflicts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	req := APIIdempotencyRequest{
		Method: "mailbox.decide", ActorTokenID: "actor-1", IdempotencyKey: "idem-1",
		RequestHash: "sha256:first", ResourceID: "card-1", Now: now,
	}
	calls := 0
	first, replay, err := s.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		calls++
		return APIIdempotencyCompletion{ResourceID: "card-1", Response: json.RawMessage(`{"ok":true,"n":1}`)}, nil
	})
	if err != nil || replay || calls != 1 || string(first.Response) == "" {
		t.Fatalf("first idempotency completion=%#v replay=%v calls=%d err=%v", first, replay, calls, err)
	}
	second, replay, err := s.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		calls++
		return APIIdempotencyCompletion{ResourceID: "card-1", Response: json.RawMessage(`{"ok":true,"n":2}`)}, nil
	})
	if err != nil || !replay || calls != 1 || !sameJSON(first.Response, second.Response) {
		t.Fatalf("replay completion=%s replay=%v calls=%d err=%v", second.Response, replay, calls, err)
	}
	req.RequestHash = "sha256:second"
	if _, _, err := s.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		calls++
		return APIIdempotencyCompletion{}, nil
	}); err == nil {
		t.Fatal("conflicting idempotency request error = nil")
	} else {
		var conflict *APIIdempotencyConflictError
		if !errors.As(err, &conflict) || conflict.OriginalRequestHash != "sha256:first" || conflict.ConflictingRequestHash != "sha256:second" {
			t.Fatalf("conflict error = %#v", err)
		}
	}
}

func sameJSON(left, right json.RawMessage) bool {
	var l, r any
	if json.Unmarshal(left, &l) != nil || json.Unmarshal(right, &r) != nil {
		return false
	}
	return reflect.DeepEqual(l, r)
}
