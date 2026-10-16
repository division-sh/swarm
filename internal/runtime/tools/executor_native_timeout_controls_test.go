package tools

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestNativeWebSearchCustomTimeoutResponseBody(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		name := "body_outlasts_client_cap"
		if stalled {
			name = "body_exceeds_declaration"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			joined := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				defer close(joined)
				if _, err := io.Copy(io.Discard, req.Body); err != nil {
					t.Errorf("consume request body: %v", err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				if stalled {
					<-req.Context().Done()
					return
				}
				timer := time.NewTimer(100 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-req.Context().Done():
				case <-timer.C:
					_, _ = w.Write([]byte(nativeTimeoutResponse))
				}
			}))
			defer server.Close()
			executor := nativeTimeoutExecutor(server.URL, 1)
			if !stalled {
				executor.httpClient.Timeout = 10 * time.Millisecond
			}
			originalTimeout := executor.httpClient.Timeout
			harness := effecttest.New()
			ctx := nativeTimeoutContext(harness, "native-body")
			out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
			<-joined
			state := runtimeeffects.StateSettled
			if stalled {
				state = runtimeeffects.StateOutcomeUncertain
				if out != nil {
					t.Fatalf("expired body returned output: %#v", out)
				}
				requireNativeTimeoutFailure(t, err, "read", context.DeadlineExceeded)
			} else {
				requireNativeTimeoutResult(t, out, err)
			}
			requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, state)
			if executor.httpClient.Timeout != originalTimeout {
				t.Fatal("response body execution changed original client timeout")
			}
		})
	}
}

func TestNativeWebSearchCustomTimeoutCallerCancellation(t *testing.T) {
	for _, stage := range []string{"transport", "read"} {
		t.Run(stage, func(t *testing.T) {
			var calls atomic.Int32
			entered, reading := make(chan struct{}), make(chan struct{})
			joined := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				defer close(joined)
				if _, err := io.Copy(io.Discard, req.Body); err != nil {
					t.Errorf("consume request body: %v", err)
					return
				}
				close(entered)
				if stage == "read" {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				}
				<-req.Context().Done()
			}))
			defer server.Close()
			transport := &http.Transport{}
			defer transport.CloseIdleConnections()
			executor := nativeTimeoutExecutor(server.URL, 120)
			executor.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				resp, err := transport.RoundTrip(req)
				if err == nil {
					resp.Body = &httpTimeoutObservedBody{ReadCloser: resp.Body, reading: reading}
				}
				return resp, err
			})
			harness := effecttest.New()
			ctx, cancel := context.WithCancel(nativeTimeoutContext(harness, "native-cancel"))
			defer cancel()
			finished := make(chan httpTimeoutResult, 1)
			go func() {
				out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
				finished <- httpTimeoutResult{out, err}
			}()
			if stage == "read" {
				<-reading
			} else {
				<-entered
			}
			cancel()
			result := <-finished
			<-joined
			if result.out != nil {
				t.Fatalf("canceled request returned output: %#v", result.out)
			}
			requireNativeTimeoutFailure(t, result.err, stage, context.Canceled)
			requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateOutcomeUncertain)
		})
	}
}

func TestNativeWebSearchCustomTimeoutOverlappingCallsPreserveClient(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/start" {
			http.SetCookie(w, &http.Cookie{Name: "native-budget", Value: "preserved", Path: "/"})
			http.Redirect(w, req, "/result", http.StatusTemporaryRedirect)
			return
		}
		cookie, err := req.Cookie("native-budget")
		if err != nil || cookie.Value != "preserved" {
			t.Error("original cookie jar was not retained across redirect")
		}
		raw, err := io.ReadAll(req.Body)
		if err != nil || string(raw) != `{"query":"swarm"}` {
			t.Errorf("307 redirect lost request GetBody: body=%s err=%v", raw, err)
		}
		entered <- req.Header.Get("X-Timeout-Call")
		select {
		case <-req.Context().Done():
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(nativeTimeoutResponse))
		}
	}))
	defer server.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var redirects atomic.Int32
	policy := func(req *http.Request, via []*http.Request) error {
		redirects.Add(1)
		if len(via) != 1 || req.URL.Path != "/result" {
			return errors.New("unexpected redirect")
		}
		return nil
	}
	roundTripper := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		req.Header.Set("X-Timeout-Call", req.Context().Value(httpTimeoutCallKey{}).(string))
		return transport.RoundTrip(req)
	})
	client := &http.Client{Transport: roundTripper, CheckRedirect: policy, Jar: jar, Timeout: time.Nanosecond}
	short, long := nativeTimeoutExecutor(server.URL+"/start", 10), nativeTimeoutExecutor(server.URL+"/start", 120)
	short.httpClient, long.httpClient = client, client
	requireOriginal := func() {
		t.Helper()
		if short.httpClient != client || long.httpClient != client || client.Timeout != time.Nanosecond || client.Jar != jar ||
			reflect.ValueOf(client.Transport).Pointer() != reflect.ValueOf(roundTripper).Pointer() ||
			reflect.ValueOf(client.CheckRedirect).Pointer() != reflect.ValueOf(policy).Pointer() {
			t.Fatal("overlapping calls changed the original client settings")
		}
	}
	shortHarness, longHarness := effecttest.New(), effecttest.New()
	shortCtx, cancel := context.WithCancel(context.WithValue(nativeTimeoutContext(shortHarness, "native-short"), httpTimeoutCallKey{}, "short"))
	defer cancel()
	longCtx := context.WithValue(nativeTimeoutContext(longHarness, "native-long"), httpTimeoutCallKey{}, "long")
	shortDone, longDone := make(chan httpTimeoutResult, 1), make(chan httpTimeoutResult, 1)
	go func() {
		out, err := short.Execute(shortCtx, "web_search", map[string]any{"query": "swarm"})
		shortDone <- httpTimeoutResult{out, err}
	}()
	go func() {
		out, err := long.Execute(longCtx, "web_search", map[string]any{"query": "swarm"})
		longDone <- httpTimeoutResult{out, err}
	}()
	first, second := <-entered, <-entered
	if first == second || (first != "short" && first != "long") || (second != "short" && second != "long") {
		t.Fatalf("overlapping arrivals=%q/%q", first, second)
	}
	requireOriginal()
	cancel()
	shortResult := <-shortDone
	requireNativeTimeoutFailure(t, shortResult.err, "transport", context.Canceled)
	if shortResult.out != nil {
		t.Fatalf("canceled call returned output: %#v", shortResult.out)
	}
	select {
	case result := <-longDone:
		t.Fatalf("canceling the short call ended the other: %#v/%v", result.out, result.err)
	default:
	}
	requireOriginal()
	close(release)
	longResult := <-longDone
	requireNativeTimeoutResult(t, longResult.out, longResult.err)
	requireOriginal()
	if redirects.Load() != 2 || len(shortHarness.Attempts) != 1 || len(longHarness.Attempts) != 1 ||
		len(shortHarness.Settlements) != 1 || len(longHarness.Settlements) != 1 {
		t.Fatalf("redirects=%d attempts=%d/%d settlements=%d/%d", redirects.Load(), len(shortHarness.Attempts), len(longHarness.Attempts), len(shortHarness.Settlements), len(longHarness.Settlements))
	}
	if err := shortHarness.RequireState("native_web_search", runtimeeffects.StateOutcomeUncertain); err != nil {
		t.Fatal(err)
	}
	if err := longHarness.RequireState("native_web_search", runtimeeffects.StateSettled); err != nil {
		t.Fatal(err)
	}
}

func TestNativeWebSearchCustomTimeoutAdmissionBoundary(t *testing.T) {
	for _, maxWait := range []string{"500ms", "0s"} {
		t.Run(maxWait, func(t *testing.T) {
			executor := nativeTimeoutExecutor("https://search.test/", 1)
			executor.workflowSource = nativeTimeoutSource("https://search.test/", 1, "1/100ms", maxWait)
			var calls atomic.Int32
			var deadline time.Time
			executor.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				observed := time.Now()
				var ok bool
				deadline, ok = req.Context().Deadline()
				if !ok || deadline.After(observed.Add(time.Second)) {
					t.Errorf("post-admission deadline=%s present=%v, want at most observed+1s", deadline, ok)
				}
				return jsonHTTPResponse(http.StatusOK, nativeTimeoutResponse), nil
			})
			first := nativeTimeoutContext(effecttest.New(), "native-admission-first")
			out, err := executor.Execute(first, "web_search", map[string]any{"query": "swarm"})
			requireNativeTimeoutResult(t, out, err)
			harness := effecttest.New()
			second := withExternalDispatchAdmissionCollector(nativeTimeoutContext(harness, "native-admission-second"))
			before := time.Now()
			out, err = executor.Execute(second, "web_search", map[string]any{"query": "swarm"})
			if maxWait == "0s" {
				failure, ok := failures.As(err)
				if out != nil || !ok || failure.Failure.Detail.Code != externalDispatchRateLimitedCode || calls.Load() != 1 || len(harness.Attempts) != 0 {
					t.Fatalf("admission refusal=%#v/%v calls=%d attempts=%d", out, err, calls.Load(), len(harness.Attempts))
				}
			} else {
				requireNativeTimeoutResult(t, out, err)
				detail, ok := externalDispatchAdmissionDiagnosticDetail(second)
				if !ok || detail["rate_limit_outcome"] != externalDispatchOutcomeWaited || calls.Load() != 2 {
					t.Fatalf("admission wait evidence=%v present=%v calls=%d", detail, ok, calls.Load())
				}
				wait := time.Duration(int64FromAny(detail["rate_limit_wait_ms"])) * time.Millisecond
				if deadline.Before(before.Add(wait + time.Second)) {
					t.Fatalf("request budget began before admission: deadline=%s call=%s waited=%s", deadline, before, wait)
				}
			}
		})
	}
}

func TestNativeWebSearchCustomTimeoutEarlierCallerDeadline(t *testing.T) {
	executor := nativeTimeoutExecutor("https://search.test/", 120)
	harness := effecttest.New()
	ctx, cancel := context.WithTimeout(nativeTimeoutContext(harness, "native-earlier-caller"), 50*time.Millisecond)
	defer cancel()
	want, _ := ctx.Deadline()
	var calls atomic.Int32
	executor.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		if actual, ok := req.Context().Deadline(); !ok || !actual.Equal(want) {
			t.Errorf("earlier caller deadline=%s present=%v, want %s", actual, ok, want)
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
	if out != nil {
		t.Fatalf("expired caller returned output: %#v", out)
	}
	requireNativeTimeoutFailure(t, err, "transport", context.DeadlineExceeded)
	requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateOutcomeUncertain)
}

func TestNativeWebSearchCustomTimeoutPreservesBuiltins(t *testing.T) {
	for _, provider := range []struct {
		name, host, response string
	}{
		{"brave", "api.search.brave.com", `{"web":{"results":[{"title":"Swarm","url":"https://swarm.test/","description":"A result"}]}}`},
		{"serper", "google.serper.dev", `{"organic":[{"title":"Swarm","link":"https://swarm.test/","snippet":"A result"}]}`},
		{"tavily", "api.tavily.com", `{"results":[{"title":"Swarm","url":"https://swarm.test/","content":"A result"}]}`},
	} {
		t.Run(provider.name, func(t *testing.T) {
			executor := NewExecutorWithOptions(nil, ExecutorOptions{
				WorkflowSource: rateLimitedNativeWebSearchSource(provider.name, "", "", nil),
				Credentials:    nativeWebSearchCredentialStore{"brave_search_api_key": "secret"},
				ModelRuntimes:  staticAgentRuntimeResolver{runtime: nativeCapabilityRuntimeStub{}},
			})
			client := executor.httpClient
			var calls atomic.Int32
			before := time.Now()
			client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				observed := time.Now()
				deadline, ok := req.Context().Deadline()
				if !ok || deadline.Before(before.Add(30*time.Second)) || deadline.After(observed.Add(30*time.Second)) || req.URL.Host != provider.host {
					t.Errorf("builtin %s deadline=%s present=%v host=%s", provider.name, deadline, ok, req.URL.Host)
				}
				return jsonHTTPResponse(http.StatusOK, provider.response), nil
			})
			harness := effecttest.New()
			ctx := nativeTimeoutContext(harness, "native-builtin-"+provider.name)
			out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
			requireNativeTimeoutResult(t, out, err)
			requireNativeTimeoutAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateSettled)
			if executor.httpClient != client || client.Timeout != 30*time.Second || executor.managedTokenSource().HTTPClient != client {
				t.Fatal("native custom repair changed builtin or credential client")
			}
		})
	}
}
