package tools

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const nativeTimeoutResponse = `{"items":[{"title":"Swarm","link":"https://swarm.test/","summary":"A result"}]}`

func nativeTimeoutExecutor(url string, seconds int) *Executor {
	return NewExecutorWithOptions(nil, ExecutorOptions{
		WorkflowSource: nativeTimeoutSource(url, seconds, "", ""),
		ModelRuntimes:  staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
	})
}

func nativeTimeoutSource(url string, seconds int, rate, maxWait string) semanticview.Source {
	httpValue := map[string]any{
		"method": http.MethodPost, "url": url, "body": map[string]any{"query": "{{input.query}}"},
		"headers": map[string]any{"X-Search-Budget": "preserved"},
	}
	if seconds != 0 {
		httpValue["timeout_seconds"] = seconds
	}
	return nativeTimeoutPolicySource(httpValue, rate, maxWait)
}

func nativeTimeoutPolicySource(httpValue map[string]any, rate, maxWait string) semanticview.Source {
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{
		Values: map[string]runtimecontracts.PolicyValue{"web_search_provider": {Value: map[string]any{
			"provider": "custom", "http": httpValue, "response_path": "items", "rate_limit": rate, "rate_limit_max_wait": maxWait,
			"field_mapping": map[string]any{"title": "title", "url": "link", "snippet": "summary"},
		}}},
	}})
}

func nativeTimeoutContext(harness *effecttest.Harness, identity string) context.Context {
	return WithActor(managedEffectCommittedContext(harness, identity), models.AgentConfig{
		ID: harness.Token.AgentID, Identity: harness.Token.Identity, ExecutionMode: "live",
		NativeTools: models.NativeToolConfig{WebSearch: true},
	})
}

func requireNativeTimeoutResult(t *testing.T, out any, err error) {
	t.Helper()
	want := map[string]any{"results": []map[string]any{{"title": "Swarm", "url": "https://swarm.test/", "snippet": "A result"}}}
	if err != nil || !reflect.DeepEqual(out, want) {
		t.Fatalf("normalized result=%#v err=%v, want %#v", out, err, want)
	}
}

func requireNativeTimeoutFailure(t *testing.T, err error, stage string, cause error) {
	t.Helper()
	failure, ok := failures.As(err)
	if !ok || failure.Failure.Class != failures.ClassOutcomeUncertain ||
		failure.Failure.Detail.Code != "native_web_search_outcome_unconfirmed" ||
		failure.Failure.Component != "tool-executor" || failure.Failure.Operation != "web_search" ||
		failure.Failure.Detail.Attributes["stage"] != stage || !errors.Is(err, cause) {
		t.Fatalf("native %s failure=%v, want canonical uncertainty with cause %v", stage, err, cause)
	}
}

func requireNativeTimeoutAttempt(t *testing.T, executor *Executor, ctx context.Context, harness *effecttest.Harness, calls *atomic.Int32, state runtimeeffects.State) {
	t.Helper()
	if err := harness.RequireState("native_web_search", state); err != nil {
		t.Fatal(err)
	}
	if len(harness.Attempts) != 1 || len(harness.Settlements) != 1 || calls.Load() != 1 {
		t.Fatalf("attempts=%d settlements=%d calls=%d, want one each", len(harness.Attempts), len(harness.Settlements), calls.Load())
	}
	if out, err := executor.Execute(context.WithoutCancel(ctx), "web_search", map[string]any{"query": "swarm"}); out != nil || err == nil || calls.Load() != 1 {
		t.Fatalf("same-operation replay=%#v err=%v calls=%d", out, err, calls.Load())
	}
}

func TestNativeWebSearchCustomTimeoutDeadlineIdentity(t *testing.T) {
	for _, seconds := range []int{1, 10, 120, 9223372036, 0} {
		t.Run(fmt.Sprintf("declared_%d", seconds), func(t *testing.T) {
			executor := nativeTimeoutExecutor("https://search.test/", seconds)
			harness := effecttest.New()
			ctx := nativeTimeoutContext(harness, "native-deadline")
			budget := time.Duration(seconds) * time.Second
			if seconds == 0 {
				budget = 30 * time.Second
			}
			var calls atomic.Int32
			before := time.Now()
			executor.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				observed := time.Now()
				deadline, ok := req.Context().Deadline()
				if !ok || deadline.Before(before.Add(budget)) || deadline.After(observed.Add(budget)) {
					t.Errorf("outgoing deadline=%s present=%v, want within [%s,%s] for %s", deadline, ok, before.Add(budget), observed.Add(budget), budget)
				}
				if req.Header.Get("X-Search-Budget") != "preserved" || req.GetBody == nil {
					t.Error("request context binding lost headers or GetBody")
				}
				if req.GetBody != nil {
					copy, err := req.GetBody()
					if err != nil {
						return nil, err
					}
					raw, err := io.ReadAll(copy)
					_ = copy.Close()
					if err != nil || string(raw) != `{"query":"swarm"}` {
						t.Errorf("prepared body=%s err=%v", raw, err)
					}
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(nativeTimeoutResponse))}, nil
			})
			out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
			requireNativeTimeoutResult(t, out, err)
			requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateSettled)
		})
	}
}

func TestNativeWebSearchCustomTimeoutShorterThanSharedCap(t *testing.T) {
	proveNativeTimeoutDelayedResponse(t, 1, 2*time.Second, false)
}

func TestNativeWebSearchCustomTimeoutInvalidBudgetCannotDispatch(t *testing.T) {
	executor := nativeTimeoutExecutor("https://search.test/", -1)
	harness := effecttest.New()
	ctx := nativeTimeoutContext(harness, "native-invalid-budget")
	actor, ok := ActorFromContext(ctx)
	if !ok {
		t.Fatal("missing native actor")
	}
	if _, err := executor.resolveWebSearchProviderConfig(actor); err == nil || !strings.Contains(err.Error(), "http.timeout_seconds must be positive") {
		t.Fatalf("invalid budget lost its canonical admission refusal: %v", err)
	}
	var calls atomic.Int32
	executor.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("invalid budget reached transport")
	})
	out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
	if out != nil || err == nil || err.Error() != "unsupported runtime tool: web_search" || calls.Load() != 0 || len(harness.Attempts) != 0 || len(harness.Settlements) != 0 {
		t.Fatalf("invalid authored budget=%#v/%v requests=%d attempts=%d settlements=%d", out, err, calls.Load(), len(harness.Attempts), len(harness.Settlements))
	}
}

func TestNativeWebSearchCustomTimeoutBeyondSharedCap(t *testing.T) {
	proveNativeTimeoutDelayedResponse(t, 120, 45*time.Second, true)
}

func proveNativeTimeoutDelayedResponse(t *testing.T, seconds int, delay time.Duration, success bool) {
	t.Helper()
	var calls atomic.Int32
	joined := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		defer close(joined)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-req.Context().Done():
		case <-timer.C:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(nativeTimeoutResponse))
		}
	}))
	defer server.Close()
	executor := nativeTimeoutExecutor(server.URL, seconds)
	harness := effecttest.New()
	ctx := nativeTimeoutContext(harness, "native-delayed-response")
	start := time.Now()
	out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
	t.Logf("declared=%ds upstream=%s elapsed=%s requests=%d result=%#v err=%v", seconds, delay, time.Since(start), calls.Load(), out, err)
	<-joined
	if success {
		requireNativeTimeoutResult(t, out, err)
		requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateSettled)
	} else {
		if out != nil {
			t.Fatalf("declared timeout returned output: %#v", out)
		}
		requireNativeTimeoutFailure(t, err, "transport", context.DeadlineExceeded)
		requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateOutcomeUncertain)
	}
	if executor.httpClient.Timeout != 30*time.Second {
		t.Fatal("changed original shared client timeout")
	}
}
