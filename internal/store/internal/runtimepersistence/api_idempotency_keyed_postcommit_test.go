package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	privateidempotency "github.com/division-sh/swarm/internal/store/internal/apiidempotency"
)

func TestSQLiteAPIIdempotencyHeldPostCommitElectsOnlySameRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "keyed.sqlite")
	first := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	other := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	req := apiidempotency.Request{Method: "event.publish", Actor: apiidempotency.BearerActor("actor"), IdempotencyKey: "same", RequestHash: "original", ResourceID: "resource", Now: time.Now().UTC(), TTL: time.Hour}
	lease, err := privateidempotency.AcquireSQLiteRequest(ctx, first.sQLiteOwner, req)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	want := apiidempotency.Completion{ResourceID: req.ResourceID, Response: json.RawMessage(`{"z":[2,1], "large":9007199254740993, "u":"\u003c"}`)}
	if err := first.backend.RunTransaction(ctx, "held postcommit API completion proof", func(ctx context.Context, tx *sql.Tx) error {
		return privateidempotency.StoreSQLiteCompletionTx(ctx, lease, tx, want)
	}); err != nil {
		t.Fatal(err)
	}
	// The durable completion exists, but postcommit work still owns this lease.
	type result struct {
		completion apiidempotency.Completion
		replay     bool
		err        error
	}
	var calls atomic.Int32
	same := make(chan result, 2)
	started := make(chan struct{}, 2)
	for _, hash := range []string{"original", "conflict"} {
		go func() {
			candidate := req
			candidate.RequestHash = hash
			if hash == "conflict" {
				candidate.ResourceID = "changed-resource"
			}
			started <- struct{}{}
			completion, replay, err := other.WithAPIIdempotency(ctx, candidate, func(context.Context) (apiidempotency.Completion, error) {
				calls.Add(1)
				return apiidempotency.Completion{}, errors.New("same-key request reexecuted")
			})
			same <- result{completion, replay, err}
		}()
	}
	<-started
	<-started
	for _, variant := range []string{"key", "actor", "method"} {
		candidate := req
		switch variant {
		case "key":
			candidate.IdempotencyKey = "different"
		case "actor":
			candidate.Actor = apiidempotency.BearerActor("other-actor")
		case "method":
			candidate.Method = "run.start"
		}
		done := make(chan result, 1)
		go func() {
			completion, replay, err := other.WithAPIIdempotency(ctx, candidate, func(context.Context) (apiidempotency.Completion, error) {
				return want, nil
			})
			done <- result{completion, replay, err}
		}()
		select {
		case got := <-done:
			if got.err != nil || got.replay || !bytes.Equal(got.completion.Response, want.Response) {
				t.Errorf("independent %s result=%+v", variant, got)
			}
		case <-ctx.Done():
			lease.Release()
			<-done
			for range 2 {
				<-same
			}
			t.Fatalf("independent %s serialized behind held postcommit lease: %v", variant, ctx.Err())
		}
	}
	select {
	case got := <-same:
		lease.Release()
		<-same
		t.Fatalf("same-key completion escaped its held lease: %+v", got)
	default:
	}
	lease.Release()
	lease.Release()
	for range 2 {
		got := <-same
		if got.err != nil {
			var conflict *apiidempotency.ConflictError
			if !errors.As(got.err, &conflict) || conflict.OriginalRequestHash != "original" || conflict.ConflictingRequestHash != "conflict" {
				t.Errorf("changed same-key request=%+v", got)
			}
		} else if !got.replay || got.completion.ResourceID != want.ResourceID || !bytes.Equal(got.completion.Response, want.Response) {
			t.Errorf("same-key durable replay=%+v", got)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("same-key duplicate/conflict executed callback")
	}
	// No response was delivered by the original lease; a fresh pool must replay it.
	reopened := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	completion, replay, err := reopened.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) {
		t.Error("lost-ACK/reopen reexecuted callback")
		return apiidempotency.Completion{}, nil
	})
	if err != nil || !replay || completion.ResourceID != want.ResourceID || !bytes.Equal(completion.Response, want.Response) {
		t.Fatalf("lost-ACK/reopen completion=%+v replay=%v err=%v", completion, replay, err)
	}
}

func TestSQLiteAPIIdempotencyErrorsCancellationAndUnkeyedRemainExact(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	ctx := testAuthorActivityContext()
	req := apiidempotency.Request{Method: "event.publish", Actor: apiidempotency.BearerActor("actor"), IdempotencyKey: "error", RequestHash: "body", Now: time.Now().UTC()}
	want := apiidempotency.Completion{ResourceID: "resource", Response: json.RawMessage(`{"ok":true}`)}
	stop := errors.New("callback stopped")
	if _, _, err := store.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) { return want, stop }); !errors.Is(err, stop) {
		t.Fatalf("callback error=%v", err)
	}
	if got, replay, err := store.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) { return want, nil }); err != nil || replay || !bytes.Equal(got.Response, want.Response) {
		t.Fatalf("error cleanup retained completion/lock: %+v %v %v", got, replay, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	req.IdempotencyKey = "canceled"
	if _, _, err := store.WithAPIIdempotency(canceled, req, func(context.Context) (apiidempotency.Completion, error) {
		t.Error("canceled admission executed callback")
		return want, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission=%v", err)
	}
	if _, replay, err := store.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) { return want, nil }); err != nil || replay {
		t.Fatalf("cancellation left a completion/lock: %v %v", replay, err)
	}
	req.IdempotencyKey = ""
	calls := 0
	for range 2 {
		if _, replay, err := store.WithAPIIdempotency(ctx, req, func(context.Context) (apiidempotency.Completion, error) { calls++; return want, nil }); err != nil || replay {
			t.Fatalf("unkeyed result=%v %v", replay, err)
		}
	}
	if calls != 2 {
		t.Fatalf("unkeyed calls=%d", calls)
	}
}
