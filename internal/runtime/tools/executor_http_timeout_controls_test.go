package tools

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

func TestHTTPToolTimeoutIncludesResponseBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-req.Context().Done():
		case <-timer.C:
			_, _ = w.Write([]byte(`{"receipt":"provider-42"}`))
		}
	}))
	defer server.Close()
	executor := authoredHTTPTimeoutExecutor(t, server.URL, 1)
	executor.httpClient.Timeout = 10 * time.Millisecond
	harness := effecttest.New()
	ctx := httpTimeoutEffectContext(harness)
	out, err := executor.Execute(ctx, "budget_probe", map[string]any{})
	requireHTTPTimeoutReceipt(t, out, err)
	requireHTTPTimeoutSingleAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateSettled)
	if executor.httpClient.Timeout != 10*time.Millisecond {
		t.Fatal("body execution changed the original client timeout")
	}
}

type httpTimeoutObservedBody struct {
	io.ReadCloser
	reading chan struct{}
	once    sync.Once
}

func (b *httpTimeoutObservedBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	return b.ReadCloser.Read(p)
}

func TestHTTPToolTimeoutCallerCancellation(t *testing.T) {
	for _, stage := range []string{"transport", "read_response"} {
		t.Run(stage, func(t *testing.T) {
			var calls atomic.Int32
			entered := make(chan struct{})
			reading := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				close(entered)
				if stage == "read_response" {
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				}
				<-req.Context().Done()
			}))
			defer server.Close()
			transport := &http.Transport{}
			defer transport.CloseIdleConnections()
			executor := authoredHTTPTimeoutExecutor(t, server.URL, 120)
			executor.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(req)
				if err == nil {
					response.Body = &httpTimeoutObservedBody{ReadCloser: response.Body, reading: reading}
				}
				return response, err
			})
			harness := effecttest.New()
			ctx, cancel := context.WithCancel(httpTimeoutEffectContext(harness))
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				out, err := executor.Execute(ctx, "budget_probe", map[string]any{})
				if out != nil {
					err = errors.Join(err, errors.New("canceled request returned output"))
				}
				finished <- err
			}()
			if stage == "transport" {
				<-entered
			} else {
				<-reading
			}
			cancel()
			err := <-finished
			requireHTTPTimeoutUncertainty(t, err, stage)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation cause lost: %v", err)
			}
			requireHTTPTimeoutSingleAttempt(t, executor, context.WithoutCancel(ctx), harness, &calls, runtimeeffects.StateOutcomeUncertain)
		})
	}
}

func TestHTTPToolTimeoutStalledBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	}))
	defer server.Close()
	executor := authoredHTTPTimeoutExecutor(t, server.URL, 1)
	harness := effecttest.New()
	ctx := httpTimeoutEffectContext(harness)
	out, err := executor.Execute(ctx, "budget_probe", map[string]any{})
	if out != nil {
		t.Fatalf("stalled body returned %#v", out)
	}
	requireHTTPTimeoutUncertainty(t, err, "read_response")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("request deadline cause lost: %v", err)
	}
	requireHTTPTimeoutSingleAttempt(t, executor, ctx, harness, &calls, runtimeeffects.StateOutcomeUncertain)
}

func TestHTTPToolTimeoutDefaultAndEarlierCallerDeadline(t *testing.T) {
	for _, seconds := range []int{0, 120} {
		t.Run(time.Duration(seconds*int(time.Second)).String(), func(t *testing.T) {
			executor := authoredHTTPTimeoutExecutor(t, "https://timeout.test", seconds)
			deadline := time.Now().Add(5 * time.Second)
			ctx, cancel := context.WithDeadline(httpTimeoutEffectContext(effecttest.New()), deadline)
			defer cancel()
			if seconds == 0 {
				ctx = httpTimeoutEffectContext(effecttest.New())
			}
			executor.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				actual, ok := req.Context().Deadline()
				if !ok {
					t.Error("request has no deadline")
				} else if seconds == 0 {
					remaining := time.Until(actual)
					if remaining <= 29*time.Second || remaining > 30*time.Second {
						t.Errorf("default request budget=%s, want 30s", remaining)
					}
				} else if !actual.Equal(deadline) {
					t.Errorf("request deadline=%s, want earlier caller=%s", actual, deadline)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"receipt":"provider-42"}`))}, nil
			})
			out, err := executor.Execute(ctx, "budget_probe", map[string]any{})
			requireHTTPTimeoutReceipt(t, out, err)
			if executor.httpClient.Timeout != 30*time.Second {
				t.Fatal("execution changed the original 30s client cap")
			}
		})
	}
}

type httpTimeoutCallKey struct{}

type httpTimeoutResult struct {
	out any
	err error
}

func TestHTTPToolTimeoutOverlappingCallsPreserveClient(t *testing.T) {
	entered := make(chan string, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/start" {
			http.SetCookie(w, &http.Cookie{Name: "budget", Value: "retained", Path: "/"})
			http.Redirect(w, req, "/result", http.StatusFound)
			return
		}
		cookie, err := req.Cookie("budget")
		if err != nil || cookie.Value != "retained" {
			t.Error("original cookie jar was not used across redirect")
		}
		entered <- req.Header.Get("X-Timeout-Call")
		select {
		case <-req.Context().Done():
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"receipt":"provider-42"}`))
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
	executor := authoredHTTPTimeoutExecutor(t, server.URL+"/start", 120)
	client := &http.Client{Transport: roundTripper, CheckRedirect: policy, Jar: jar, Timeout: time.Nanosecond}
	executor.httpClient = client
	requireOriginal := func() {
		t.Helper()
		if executor.httpClient != client || client.Timeout != time.Nanosecond || client.Jar != jar ||
			reflect.ValueOf(client.Transport).Pointer() != reflect.ValueOf(roundTripper).Pointer() ||
			reflect.ValueOf(client.CheckRedirect).Pointer() != reflect.ValueOf(policy).Pointer() {
			t.Fatal("overlapping calls mutated the original client or its settings")
		}
	}
	shortCtx, cancelShort := context.WithTimeout(context.WithValue(httpTimeoutEffectContext(effecttest.New()), httpTimeoutCallKey{}, "short"), 5*time.Second)
	defer cancelShort()
	longCtx, cancelLong := context.WithCancel(context.WithValue(httpTimeoutEffectContext(effecttest.New()), httpTimeoutCallKey{}, "long"))
	defer cancelLong()
	shortResult := make(chan httpTimeoutResult, 1)
	longResult := make(chan httpTimeoutResult, 1)
	for _, call := range []struct {
		ctx    context.Context
		result chan httpTimeoutResult
	}{{shortCtx, shortResult}, {longCtx, longResult}} {
		go func() {
			out, err := executor.Execute(call.ctx, "budget_probe", map[string]any{})
			call.result <- httpTimeoutResult{out, err}
		}()
	}
	first, second := <-entered, <-entered
	if first == second || (first != "short" && first != "long") || (second != "short" && second != "long") {
		t.Fatalf("overlapping arrivals=%q/%q", first, second)
	}
	requireOriginal()
	cancelShort()
	short := <-shortResult
	requireHTTPTimeoutUncertainty(t, short.err, "transport")
	if short.out != nil || !errors.Is(short.err, context.Canceled) {
		t.Fatalf("short call=%#v/%v", short.out, short.err)
	}
	select {
	case result := <-longResult:
		t.Fatalf("canceling one call ended the other: %#v/%v", result.out, result.err)
	default:
	}
	requireOriginal()
	close(release)
	long := <-longResult
	requireHTTPTimeoutReceipt(t, long.out, long.err)
	requireOriginal()
	if redirects.Load() != 2 {
		t.Fatalf("redirect policy calls=%d, want two independent redirects", redirects.Load())
	}
}

func TestHTTPToolTimeoutPreservesOtherClientConsumers(t *testing.T) {
	executor := authoredHTTPTimeoutExecutor(t, "https://timeout.test", 120)
	client := executor.httpClient
	var calls atomic.Int32
	client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		deadline, ok := req.Context().Deadline()
		remaining := time.Until(deadline)
		if !ok || remaining <= 29*time.Second || remaining > 30*time.Second {
			t.Errorf("native search shared-client deadline=%s present=%v, want original 30s", remaining, ok)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"results":[]}`))}, nil
	})
	if executor.managedTokenSource().HTTPClient != client {
		t.Fatal("managed-token exchange lost its original client")
	}
	req, err := http.NewRequest(http.MethodGet, "https://timeout.test/search", nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.doNormalizedSearch(managedEffectCommittedContext(effecttest.New(), "native-search-shared-client"), req, "results", nil, externalDispatchAdmissionPolicy{})
	if err != nil || len(result) != 0 || calls.Load() != 1 || executor.httpClient != client || client.Timeout != 30*time.Second {
		t.Fatalf("native search=%#v err=%v calls=%d timeout=%s", result, err, calls.Load(), client.Timeout)
	}
}
