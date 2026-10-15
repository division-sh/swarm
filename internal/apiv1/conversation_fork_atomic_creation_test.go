package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func conversationForkCommitCounts(t *testing.T, selected forkCommitOmissionProbeStore) (int, int) {
	t.Helper()
	forks, err := storetest.CountConversationForkStorage(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	completions, err := storetest.CountConversationForkCompletionStorage(context.Background(), selected)
	if err != nil {
		t.Fatal(err)
	}
	return forks, completions
}

func requireForkCommitCounts(t *testing.T, selected forkCommitOmissionProbeStore, forks, completions int) {
	t.Helper()
	actualForks, actualCompletions := conversationForkCommitCounts(t, selected)
	for table, want := range map[string]int{"conversation_forks": forks, "api_idempotency": completions} {
		got := actualForks
		if table == "api_idempotency" {
			got = actualCompletions
		}
		if got != want {
			t.Fatalf("%s rows=%d want=%d", table, got, want)
		}
	}
}

func TestConversationForkAtomicCreationControls(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, selected, _, now, sessionID, turnID := newConversationForkCommitFixture(t, backend)
			req := runfork.APIConversationForkCreateRequest{
				Creation:    runfork.ConversationForkCreateRequest{SourceSessionID: sessionID, ForkPoint: runfork.ConversationForkPointSelector{Kind: "turn", TurnID: turnID}, CreatedBy: "actor", Now: now},
				Idempotency: apiidempotency.Request{Method: "conversation.fork", Actor: apiidempotency.BearerActor("actor"), IdempotencyKey: "same-key", RequestHash: "same-hash", ResourceID: sessionID, Now: now, TTL: 24 * time.Hour},
			}
			t.Run("precommit_refusal", func(t *testing.T) {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := selected.CreateAPIConversationFork(canceled, req); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled admission: %v", err)
				}
				invalid := req
				invalid.Creation.ForkPoint.TurnID = uuid.NewString()
				if _, err := selected.CreateAPIConversationFork(ctx, invalid); err == nil {
					t.Fatal("invalid source turn admitted")
				}
				invalid = req
				invalid.Idempotency.Method = "conversation.fork_chat"
				if _, err := selected.CreateAPIConversationFork(ctx, invalid); err == nil {
					t.Fatal("wrong named method admitted")
				}
				requireForkCommitCounts(t, selected, 0, 0)
			})
			t.Run("completion_insert_rollback", func(t *testing.T) {
				// Fault the actual completion INSERT, after the fork INSERT in the
				// same transaction. No production callback or result is forged.
				if err := storetest.InstallConversationForkCompletionInsertFailure(context.Background(), selected); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := storetest.RemoveConversationForkCompletionInsertFailure(context.Background(), selected); err != nil {
						t.Error(err)
					}
				}()
				if _, err := selected.CreateAPIConversationFork(ctx, req); err == nil || !strings.Contains(err.Error(), "fork completion fault") {
					t.Fatalf("completion fault: %v", err)
				}
				requireForkCommitCounts(t, selected, 0, 0)
			})
			var firstID string
			t.Run("same_key_concurrency", func(t *testing.T) {
				const workers = 8
				results := make([]runfork.ConversationForkCreateResult, workers)
				errs := make([]error, workers)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range workers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						results[i], errs[i] = selected.CreateAPIConversationFork(ctx, req)
					}()
				}
				close(start)
				wg.Wait()
				fresh := 0
				firstID = results[0].Fork.ForkID
				for i, result := range results {
					if errs[i] != nil || result.Fork.ForkID == "" || result.Fork.ForkID != firstID {
						t.Fatalf("worker %d: %+v %v", i, result, errs[i])
					}
					if !result.IdempotencyReplayed {
						fresh++
					}
				}
				if fresh != 1 {
					t.Fatalf("fresh creations=%d want=1", fresh)
				}
				requireForkCommitCounts(t, selected, 1, 1)
			})
			t.Run("hash_conflict", func(t *testing.T) {
				conflict := req
				conflict.Idempotency.RequestHash = "different-hash"
				_, err := selected.CreateAPIConversationFork(ctx, conflict)
				var typed *apiidempotency.ConflictError
				if !errors.As(err, &typed) || typed.ResourceID != firstID {
					t.Fatalf("conflict=%v", err)
				}
				requireForkCommitCounts(t, selected, 1, 1)
			})
			t.Run("actor_key_and_unkeyed_distinct", func(t *testing.T) {
				actor := req
				actor.Creation.CreatedBy, actor.Idempotency.Actor = "other-actor", apiidempotency.BearerActor("other-actor")
				key := req
				key.Idempotency.IdempotencyKey = "other-key"
				unkeyed := req
				unkeyed.Idempotency.IdempotencyKey = ""
				ids := map[string]bool{firstID: true}
				for _, distinct := range []runfork.APIConversationForkCreateRequest{actor, key, unkeyed, unkeyed} {
					result, err := selected.CreateAPIConversationFork(ctx, distinct)
					if err != nil || result.IdempotencyReplayed || ids[result.Fork.ForkID] {
						t.Fatalf("distinct=%+v err=%v", result, err)
					}
					ids[result.Fork.ForkID] = true
				}
				requireForkCommitCounts(t, selected, 5, 3)
			})
			t.Run("ttl_expiry", func(t *testing.T) {
				// Retention starts at successful completion, not request preparation.
				// Reach the last persisted boundary without a wall-clock sleep, so
				// all earlier completions are expired when the new fork commits.
				var expiresAt time.Time
				expiry, err := storetest.ReadConversationForkCompletionExpiryStorage(context.Background(), selected)
				if err != nil {
					t.Fatal(err)
				}
				if backend == "sqlite" {
					raw := expiry.SQLiteText
					var err error
					expiresAt, err = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", raw)
					if err != nil {
						t.Fatalf("decode persisted completion expiry %q: %v", raw, err)
					}
				} else {
					expiresAt = expiry.PostgresTime
				}
				expired := req
				expired.Idempotency.Now, expired.Creation.Now = expiresAt, expiresAt
				result, err := selected.CreateAPIConversationFork(ctx, expired)
				if err != nil || result.IdempotencyReplayed || result.Fork.ForkID == firstID {
					t.Fatalf("expired=%+v err=%v", result, err)
				}
				if !result.Fork.ExpiresAt.Equal(expired.Creation.Now.Add(24 * time.Hour)) {
					t.Fatalf("fork TTL=%v", result.Fork.ExpiresAt)
				}
				requireForkCommitCounts(t, selected, 6, 1)
			})
		})
	}
}

// This closes a real HTTP connection after successful handler execution but
// before sending any response bytes. It is response loss, not SIGKILL or lost
// PostgreSQL COMMIT acknowledgement. Retry uses an independent native peer at
// the same location, not a reconstructed coordinator over the original pool.
func TestConversationForkLostHTTPResponseReplays(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, selected, reopen, now, sessionID, turnID := newConversationForkCommitFixture(t, backend)
			makeHandler := func(s forkCommitOmissionProbeStore) *Handler {
				return testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorConversationForkHandlers(testOperatorCapabilities{
					ConversationForkLifecycle: s, ConversationForks: s, Idempotency: s, Now: func() time.Time { return now },
				})})
			}
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"lost-response","method":"conversation.fork","params":{"source_session_id":%q,"fork_point":{"kind":"turn","turn_id":%q},"idempotency_key":"lost-response-key"}}`, sessionID, turnID)
			handler := makeHandler(selected)
			settled := make(chan rpcResponse, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, testAuthorActivityRequest(r))
				var response rpcResponse
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
					t.Error(err)
				}
				settled <- response
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/rpc", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+testToken)
			client := &http.Client{Timeout: 10 * time.Second}
			response, err := client.Do(request)
			if response != nil {
				response.Body.Close()
			}
			if !errors.Is(err, io.EOF) {
				t.Fatalf("lost HTTP response error=%v want EOF", err)
			}
			first := <-settled
			if first.Error != nil {
				t.Fatalf("server did not commit successfully: %+v", first.Error)
			}
			firstID := asMap(t, asMap(t, first.Result)["fork"])["fork_id"]
			server.Close()
			requireForkCommitCounts(t, selected, 1, 1)
			var reconstructed forkCommitOmissionProbeStore
			if backend == "sqlite" {
				reconstructed = reopen.sqlite()
			} else {
				reconstructed = reopen.postgres()
			}
			if reconstructed == selected {
				t.Fatal("replay reused the predecessor native owner")
			}
			requireForkCommitCounts(t, reconstructed, 1, 1)
			retryHandler := makeHandler(reconstructed)
			retryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { retryHandler.ServeHTTP(w, testAuthorActivityRequest(r)) }))
			defer retryServer.Close()
			request, err = http.NewRequest(http.MethodPost, retryServer.URL+"/v1/rpc", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+testToken)
			response, err = client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var retry rpcResponse
			if err := json.NewDecoder(response.Body).Decode(&retry); err != nil {
				t.Fatal(err)
			}
			if retry.Error != nil {
				t.Fatalf("retry error=%+v", retry.Error)
			}
			result := asMap(t, retry.Result)
			if result["idempotency_replayed"] != true || asMap(t, result["fork"])["fork_id"] != firstID {
				t.Fatalf("retry=%+v first=%v", result, firstID)
			}
			requireForkCommitCounts(t, selected, 1, 1)
			conflict := rpcCall(t, retryHandler, strings.Replace(body, turnID, uuid.NewString(), 1))
			if conflict.Error == nil || asMap(t, conflict.Error.Data)["code"] != IdempotencyConflictCode {
				t.Fatalf("same public key with changed request must conflict: %+v", conflict.Error)
			}
			requireForkCommitCounts(t, selected, 1, 1)
			t.Logf("%s actual HTTP EOF after commit; reconstructed owner and new listener replay fork=%v; durable forks=1 completions=1", backend, firstID)
		})
	}
}
