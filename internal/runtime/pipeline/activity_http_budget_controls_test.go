package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestPreparedActivityHTTPUsesAuthoredBudget(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, seconds := range []int{0, 1, 10, 120} {
			t.Run(fmt.Sprintf("%s/%ds", method, seconds), func(t *testing.T) {
				budget := time.Duration(seconds) * time.Second
				if seconds == 0 {
					budget = 30 * time.Second
				}
				var before time.Time
				var calls atomic.Int32
				transport := activityRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					calls.Add(1)
					observed := time.Now()
					deadline, present := request.Context().Deadline()
					if !present || deadline.Before(before.Add(budget)) || deadline.After(observed.Add(budget)) {
						t.Errorf("outgoing budget=%s deadline=%s, expected in [%s,%s]", budget, deadline, before.Add(budget), observed.Add(budget))
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
				})
				client := &http.Client{Timeout: time.Millisecond, Transport: transport}
				tool := runtimecontracts.MustToolSchemaEntry(
					runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
					runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: method, URL: "https://proof.invalid", TimeoutSeconds: seconds}),
					runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.ToolInputSchema{}),
				)
				intent := runtimeengine.ActivityIntent{Tool: "budget", Input: ActivityInputForTest(map[string]any{})}
				prepared, err := (pipelineActivityDispatcher{}).prepareActivityHTTPTool(context.Background(), client, intent, tool)
				if err != nil {
					t.Fatal(err)
				}
				before = time.Now()
				if _, err := executePreparedActivityHTTPTool(context.Background(), prepared); err != nil {
					t.Fatal(err)
				}
				if client.Timeout != time.Millisecond || calls.Load() != 1 || prepared.client != client {
					t.Fatalf("shared client or physical call count changed: timeout=%s calls=%d", client.Timeout, calls.Load())
				}
			})
		}
	}
}

func TestPreparedActivityHTTPInterruptionBoundary(t *testing.T) {
	for _, phase := range []string{"before_launch", "transport", "caller_after_launch", "deadline_after_launch", "read_response", "complete_forbidden"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := errors.New("provider-secret response interrupted")
			var calls atomic.Int32
			var bodyClosed atomic.Bool
			prepared := preparedActivityHTTPTool{
				toolName: "interruption", method: http.MethodPost, url: "https://proof.invalid", timeout: time.Second,
				secrets: []string{"provider-secret"},
				client: &http.Client{Transport: activityRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					calls.Add(1)
					switch phase {
					case "transport":
						return nil, cause
					case "caller_after_launch":
						cancel()
						return nil, request.Context().Err()
					case "deadline_after_launch":
						<-request.Context().Done()
						return nil, request.Context().Err()
					case "read_response":
						return &http.Response{StatusCode: http.StatusOK, Body: &activityInterruptedResponseBody{cause: cause, closed: &bodyClosed}}, nil
					default:
						return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
					}
				})},
			}
			if phase == "before_launch" {
				cancel()
			}
			if phase == "deadline_after_launch" {
				prepared.timeout = time.Millisecond
			}
			_, err := executePreparedActivityHTTPTool(ctx, prepared)
			if err == nil {
				t.Fatal("interrupted or refused request succeeded")
			}
			wantCalls := int32(1)
			if phase == "before_launch" {
				wantCalls = 0
			}
			if calls.Load() != wantCalls {
				t.Fatalf("physical HTTP calls=%d, want %d", calls.Load(), wantCalls)
			}
			uncertain := phase != "before_launch" && phase != "complete_forbidden"
			failure, typed := runtimefailures.EnvelopeFromError(err)
			if activityHTTPOutcomeUncertain(err) != uncertain || !typed || uncertain && failure.Class != runtimefailures.ClassOutcomeUncertain {
				t.Fatalf("wrong interruption classification: uncertain=%t failure=%+v err=%v", uncertain, failure, err)
			}
			switch phase {
			case "before_launch", "caller_after_launch":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation cause was lost: %v", err)
				}
			case "deadline_after_launch":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("request deadline cause was lost: %v", err)
				}
			case "transport", "read_response":
				if !errors.Is(err, cause) || strings.Contains(err.Error(), "provider-secret") {
					t.Fatalf("original cause or redaction changed: %v", err)
				}
			case "complete_forbidden":
				if failure.Class != runtimefailures.ClassAuthorizationDenied || failure.Detail.Code != "provider_forbidden" {
					t.Fatalf("complete response lost its known refusal: %+v", failure)
				}
			}
			if phase == "read_response" && !bodyClosed.Load() {
				t.Fatal("interrupted response body was not closed")
			}
		})
	}
}

func TestPreparedActivityHTTPOverlappingCallsKeepClientAndCancellation(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	entered := make(chan struct{}, 2)
	var calls atomic.Int32
	before := time.Now()
	redirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client := &http.Client{Timeout: time.Millisecond, Jar: jar, CheckRedirect: redirect}
	client.Transport = activityRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		budget := 10 * time.Second
		if request.URL.Path == "/second" {
			budget = 120 * time.Second
		}
		observed := time.Now()
		deadline, present := request.Context().Deadline()
		if !present || deadline.Before(before.Add(budget)) || deadline.After(observed.Add(budget)) {
			t.Errorf("overlapping request borrowed another deadline: path=%s deadline=%s budget=%s", request.URL.Path, deadline, budget)
		}
		entered <- struct{}{}
		select {
		case <-request.Context().Done():
			return nil, request.Context().Err()
		case <-release:
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
		}
	})
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	for _, call := range []struct {
		ctx     context.Context
		path    string
		seconds int
		done    chan<- error
	}{{firstCtx, "/first", 10, firstDone}, {context.Background(), "/second", 120, secondDone}} {
		tool := runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: "https://proof.invalid" + call.path, TimeoutSeconds: call.seconds}),
			runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.ToolInputSchema{}),
		)
		prepared, err := (pipelineActivityDispatcher{}).prepareActivityHTTPTool(call.ctx, client, runtimeengine.ActivityIntent{Tool: "overlap", Input: ActivityInputForTest(map[string]any{})}, tool)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			_, err := executePreparedActivityHTTPTool(call.ctx, prepared)
			call.done <- err
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("both calls did not enter the shared transport")
		}
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) || !activityHTTPOutcomeUncertain(err) {
		t.Fatalf("first call cancellation was lost or not uncertain: %v", err)
	}
	select {
	case err := <-secondDone:
		t.Fatalf("first call cancellation or client cap ended the second call: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-secondDone; err != nil {
		t.Fatalf("independent second call did not finish: %v", err)
	}
	if calls.Load() != 2 || client.Timeout != time.Millisecond || client.Jar != jar || reflect.ValueOf(client.CheckRedirect).Pointer() != reflect.ValueOf(redirect).Pointer() {
		t.Fatalf("shared client or physical call count changed: calls=%d client=%+v", calls.Load(), client)
	}
}

type activityInterruptedResponseBody struct {
	cause  error
	closed *atomic.Bool
}

func (r *activityInterruptedResponseBody) Read([]byte) (int, error) { return 0, r.cause }
func (r *activityInterruptedResponseBody) Close() error {
	r.closed.Store(true)
	return nil
}
