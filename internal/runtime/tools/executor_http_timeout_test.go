package tools

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func authoredHTTPTimeoutExecutor(t *testing.T, url string, seconds int) *Executor {
	t.Helper()
	var timeout string
	if seconds != 0 {
		timeout = fmt.Sprintf("    timeout_seconds: %d\n", seconds)
	}
	source, err := yamlsource.Load([]byte(fmt.Sprintf(`budget_probe:
  description: Observe the authored HTTP request budget.
  handler_type: http
  effect_class: read_only
  http:
    method: GET
    url: %q
%s`, url, timeout)))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := runtimecontracts.AdmitToolDeclarationsValue(source.Document("tools.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	return NewExecutorWithOptions(nil, ExecutorOptions{
		WorkflowSource: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: entries}),
	})
}

func httpTimeoutEffectContext(harness *effecttest.Harness) context.Context {
	return WithActor(managedEffectCommittedContext(harness, "authored-http-timeout"), models.AgentConfig{
		ID: harness.Token.AgentID, Identity: harness.Token.Identity, ExecutionMode: "live", Tools: []string{"budget_probe"},
	})
}

func requireHTTPTimeoutUncertainty(t *testing.T, err error, stage string) {
	t.Helper()
	failure, ok := failures.As(err)
	if !ok || failure.Failure.Class != failures.ClassOutcomeUncertain ||
		failure.Failure.Detail.Code != "http_tool_attempt_outcome_unconfirmed" ||
		failure.Failure.Component != "tool-executor" || failure.Failure.Operation != "execute_http_tool" ||
		failure.Failure.Detail.Attributes["stage"] != stage {
		t.Fatalf("HTTP %s failure = %v, want canonical outcome uncertainty", stage, err)
	}
}

func requireHTTPTimeoutReceipt(t *testing.T, out any, err error) {
	t.Helper()
	response, ok := out.(map[string]any)
	if err != nil || !ok || len(response) != 1 || response["receipt"] != "provider-42" {
		t.Fatalf("HTTP response = %#v, err=%v, want exact provider receipt", out, err)
	}
}

func requireHTTPTimeoutSingleAttempt(t *testing.T, executor *Executor, ctx context.Context, harness *effecttest.Harness, calls *atomic.Int32, state runtimeeffects.State) {
	t.Helper()
	if err := harness.RequireState("authored_http_tool", state); err != nil {
		t.Fatal(err)
	}
	if len(harness.Attempts) != 1 || len(harness.Settlements) != 1 || calls.Load() != 1 {
		t.Fatalf("attempts=%d settlements=%d requests=%d, want one of each", len(harness.Attempts), len(harness.Settlements), calls.Load())
	}
	if replay, err := executor.Execute(ctx, "budget_probe", map[string]any{}); err == nil || replay != nil || calls.Load() != 1 {
		t.Fatalf("logical replay=%#v err=%v requests=%d, want refusal without redispatch", replay, err, calls.Load())
	}
}

func TestHTTPToolDeclaredTimeoutBeyondSharedClientCap(t *testing.T) {
	for _, seconds := range []int{120, 10} {
		t.Run(fmt.Sprintf("declared_%ds", seconds), func(t *testing.T) {
			var calls atomic.Int32
			joined := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				defer close(joined)
				timer := time.NewTimer(45 * time.Second)
				defer timer.Stop()
				select {
				case <-req.Context().Done():
					return
				case <-timer.C:
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"receipt":"provider-42"}`))
				}
			}))
			defer server.Close()
			executor := authoredHTTPTimeoutExecutor(t, server.URL, seconds)
			harness := effecttest.New()
			ctx := httpTimeoutEffectContext(harness)
			start := time.Now()
			out, err := executor.Execute(ctx, "budget_probe", map[string]any{})
			t.Logf("declared=%ds upstream=45s elapsed=%s requests=%d result=%#v err=%v", seconds, time.Since(start), calls.Load(), out, err)
			<-joined
			if seconds == 120 {
				requireHTTPTimeoutReceipt(t, out, err)
				requireHTTPTimeoutSingleAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateSettled)
			} else {
				if out != nil {
					t.Fatalf("timed out request returned %#v", out)
				}
				requireHTTPTimeoutUncertainty(t, err, "transport")
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("declared timeout lost its deadline cause: %v", err)
				}
				requireHTTPTimeoutSingleAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateOutcomeUncertain)
			}
			if executor.httpClient.Timeout != 30*time.Second {
				t.Fatalf("shared client timeout changed to %s", executor.httpClient.Timeout)
			}
		})
	}
}
