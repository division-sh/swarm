package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	swruntime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestFeedOnlyRunStartReplayPrecedenceBothStores(t *testing.T) {
	forEachDataRunLifecycleStore(t, func(t *testing.T, fixture dataRunLifecycleFixture) {
		source, catalog, ref, _, _ := deploymentRunStartTestSource(t)
		hash := runStartTestBundleHashForSource(source)
		options := runStartTestEventBusOptions(source)
		owner := newAPITestRuntimeWorkOccurrence(t, authorActivityTestRuntimeInstanceID, hash)
		options.WorkOwner = owner
		bus, err := newScopedAPITestEventBusWithDataCatalog(t, fixture.primary, &catalog, options)
		if err != nil {
			t.Fatal(err)
		}
		handler := eventPublishTestHandlerWithStores(t, fixture.primary, fixture.primary, fixture.primary, bus, source)
		data := map[string]any{"imports": []any{dataRunFusedImport(uuid.NewString(), ref, durabledata.AbsentHead(), []byte("{\"topic\":\"one\"}\n"))}, "pins": []any{}}
		body := deploymentRunStartBody(uuid.NewString(), uuid.NewString(), hash, data)
		first := rpcCall(t, handler, body)
		if first.Error != nil {
			t.Fatalf("initial eventless start: %+v", first.Error)
		}
		if err := owner.WaitForQuiescence(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := owner.RetireAndWait(t.Context()); err != nil {
			t.Fatal(err)
		}
		contexts, err := swruntime.NewRuntimeContextManager(nil)
		if err != nil {
			t.Fatal(err)
		}
		// The original exact runtime is retired and no source is selectable. The
		// retained selected-store lookup remains non-executable authority.
		now := time.Now().UTC()
		unavailable := testHandler(t, Options{AuthTokens: []string{testToken, "foreign-test-token"}, Handlers: OperatorRunStartHandlers(RunStartHandlerOptions{
			Publication: EventPublicationOptions{Idempotency: fixture.primary, Acknowledged: bus, RuntimeContexts: contexts, Now: func() time.Time { return now }},
		})})
		before, err := storetest.ReadFiniteRunStartStorageCounts(t.Context(), fixture.primary)
		if err != nil {
			t.Fatal(err)
		}
		replay := rpcCall(t, unavailable, body)
		if replay.Error != nil || !bytes.Equal(marshalTestResult(t, first.Result), marshalTestResult(t, replay.Result)) {
			t.Fatalf("source-unavailable replay: error=%+v equal_result=%t", replay.Error, bytes.Equal(marshalTestResult(t, first.Result), marshalTestResult(t, replay.Result)))
		}
		var changed map[string]any
		if err := json.Unmarshal([]byte(body), &changed); err != nil {
			t.Fatal(err)
		}
		changed["params"].(map[string]any)["bundle_hash"] = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		raw, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		conflict := rpcCall(t, unavailable, string(raw))
		if conflict.Error == nil || asMap(t, conflict.Error.Data)["code"] != IdempotencyConflictCode {
			t.Fatalf("source-unavailable conflict: %+v", conflict)
		}
		if foreign := publicCompletionRPCWithToken(t, unavailable, body, "foreign-test-token"); foreign.Error == nil {
			t.Fatal("another actor replayed the original completion")
		}
		changed["params"].(map[string]any)["bundle_hash"] = hash
		changed["params"].(map[string]any)["idempotency_key"] = uuid.NewString()
		raw, err = json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if missed := rpcCall(t, unavailable, string(raw)); missed.Error == nil {
			t.Fatal("source-unavailable new key created executable work")
		}
		now = now.Add(runStartIDempotencyTTL + time.Second)
		if expired := rpcCall(t, unavailable, body); expired.Error == nil {
			t.Fatal("expired completion resurrected unavailable source")
		}
		after, err := storetest.ReadFiniteRunStartStorageCounts(context.Background(), fixture.primary)
		// Expiration may remove only the expired transport cache. It may not
		// reconstruct the source or repeat domain creation/attachment.
		before.APIIdempotency = after.APIIdempotency
		if err != nil || after != before || after.APIIdempotency != 0 {
			t.Fatalf("replay/conflict mutated native rows: before=%+v after=%+v err=%v", before, after, err)
		}
	})
}

func marshalTestResult(t *testing.T, result any) []byte {
	t.Helper()
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPublicCompletionResultRejectsNoncanonicalBytes(t *testing.T) {
	for _, raw := range []string{`{"b":2,"a":1}`, `{"a": 1}`, `{"a":1} `, `{"a":1,"a":2}`, `{`} {
		t.Run(raw, func(t *testing.T) {
			if _, err := recordedPublicCompletionResult(apiidempotency.Completion{Response: json.RawMessage(raw)}); err == nil {
				t.Fatalf("accepted noncanonical recorded bytes %q", raw)
			}
		})
	}
	raw := json.RawMessage(`{"a":1,"b":2}`)
	if _, err := recordedPublicCompletionResult(apiidempotency.Completion{Response: raw}); err != nil {
		t.Fatalf("rejected canonical recorded bytes: %v", err)
	}
}
