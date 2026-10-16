package tools_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type agentHTTPClassObservedStore struct {
	httpSettlementSelectedStore
	attempts []effects.Attempt
}

func (s *agentHTTPClassObservedStore) AuthorizeExternalAttempt(ctx context.Context, authority effects.Authority, request effects.AuthorizeRequest) (effects.Attempt, error) {
	attempt, err := s.httpSettlementSelectedStore.AuthorizeExternalAttempt(ctx, authority, request)
	if err == nil {
		s.attempts = append(s.attempts, attempt)
	}
	return attempt, err
}

func TestAgentHTTPAuthoredClassesKeepWriteUnknownBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, class := range []string{"read_only", "idempotent_write", "non_idempotent_write", "long_running"} {
			t.Run(backend+"/"+class, func(t *testing.T) {
				var selected httpSettlementSelectedStore
				var reopen func() httpSettlementSelectedStore
				if backend == "sqlite" {
					native, open := storetest.StartSQLiteRuntimeStoreWithReopen(t, t.Context())
					selected, reopen = native, func() httpSettlementSelectedStore { return open() }
				} else {
					native, open := storetest.StartPostgresRuntimeStoreWithReopen(t)
					selected, reopen = native, func() httpSettlementSelectedStore { return open() }
				}
				ctx, _ := selectedHTTPSettlementContext(t, selected, "agent-http-class-"+class)
				observer := &agentHTTPClassObservedStore{httpSettlementSelectedStore: selected}
				ctx = effects.WithController(ctx, effects.NewController(observer).WithExecutionPosture(executionposture.Live))
				for _, stage := range []string{"transport", "read_response"} {
					t.Run(stage, func(t *testing.T) {
						proveAgentHTTPClassUncertainty(t, ctx, observer, reopen, class, stage)
					})
				}
			})
		}
	}
}

func proveAgentHTTPClassUncertainty(t *testing.T, ctx context.Context, observer *agentHTTPClassObservedStore, reopen func() httpSettlementSelectedStore, class, stage string) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if stage == "read_response" {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"incomplete":`))
			w.(http.Flusher).Flush()
		}
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	value, err := yamlsource.Load([]byte(fmt.Sprintf("settlement-proof:\n  handler_type: http\n  effect_class: %s\n  http: {method: POST, url: %q, timeout_seconds: 10}\n", class, server.URL)))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := contracts.AdmitToolDeclarationsValue(value.Document("tools.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	executor := tools.NewExecutorWithOptions(nil, tools.ExecutorOptions{WorkflowSource: semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: entries})})
	ctx = effects.WithLogicalOperationIdentity(ctx, "agent-http-"+class+"-"+stage)
	out, err := executor.Execute(ctx, "settlement-proof", map[string]any{})
	failure, ok := failures.As(err)
	if out != nil || !ok || failure.Failure.Class != failures.ClassOutcomeUncertain || failure.Failure.Detail.Code != "http_tool_attempt_outcome_unconfirmed" || failure.Failure.Detail.Attributes["stage"] != stage || failure.Failure.Retryable || calls.Load() != 1 {
		t.Fatalf("agent class=%s stage=%s calls=%d output=%#v failure=%v", class, stage, calls.Load(), out, err)
	}
	if len(observer.attempts) == 0 {
		t.Fatal("native authorization did not return an attempt")
	}
	attempt := observer.attempts[len(observer.attempts)-1]
	if attempt.Class != effects.EffectWriteOrUnknown || attempt.Adapter != "authored_http_tool" || !attempt.AuthorizationAcknowledged {
		t.Fatalf("authored class changed native agent authority: %+v", attempt)
	}
	before, found, err := observer.GetExternalEffectOutcome(context.Background(), attempt.OperationID)
	if err != nil || !found || before.State != effects.StateOutcomeUncertain || before.AttemptState != effects.StateOutcomeUncertain {
		t.Fatalf("durable interruption missing: %+v found=%t err=%v", before, found, err)
	}
	count := len(observer.attempts)
	if replay, err := executor.Execute(ctx, "settlement-proof", map[string]any{}); replay != nil || err == nil || calls.Load() != 1 || len(observer.attempts) != count {
		t.Fatalf("same-owner replay dispatched: result=%#v err=%v calls=%d attempts=%d", replay, err, calls.Load(), len(observer.attempts))
	}
	peer := reopen()
	peerCtx := effects.WithController(ctx, effects.NewController(peer).WithExecutionPosture(executionposture.Live))
	if replay, err := executor.Execute(peerCtx, "settlement-proof", map[string]any{}); replay != nil || err == nil || calls.Load() != 1 {
		t.Fatalf("reopened native owner redispatched: result=%#v err=%v calls=%d", replay, err, calls.Load())
	}
	after, found, err := peer.GetExternalEffectOutcome(context.Background(), attempt.OperationID)
	if err != nil || !found || !reflect.DeepEqual(before, after) {
		t.Fatalf("reopened outcome changed: before=%+v after=%+v found=%t err=%v", before, after, found, err)
	}
	rows, err := storetest.ReadExternalAttemptStorage(context.Background(), peer)
	if err != nil || len(rows) != count {
		t.Fatalf("replay changed native physical-attempt count: rows=%+v want=%d err=%v", rows, count, err)
	}
	t.Logf("authored=%s admitted=%s stage=%s operation=%s attempt=%s requests=%d state=%s authorized_at=%s", class, attempt.Class, stage, attempt.OperationID, attempt.AttemptID, calls.Load(), after.State, attempt.AuthorizedAt.Format(time.RFC3339Nano))
}
