package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/gorilla/websocket"
)

func TestRunCommandPostStartObserverFailuresDetachAndUseTerminalTruth(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	tests := []struct {
		name   string
		reason runTraceDetachReason
		opts   runCommandServerOptions
	}{
		{name: "attach failure", reason: runTraceDetachAttachFailed, opts: runCommandServerOptions{wsHTTPStatus: http.StatusServiceUnavailable}},
		{name: "subscription rejected", reason: runTraceDetachAttachFailed, opts: runCommandServerOptions{wsSubscriptionError: &jsonRPCError{Code: -32000, Message: "subscription rejected"}}},
		{name: "invalid subscription response", reason: runTraceDetachSubscriptionResponseInvalid, opts: runCommandServerOptions{wsSubscriptionResult: map[string]any{}}},
		{name: "transport loss", reason: runTraceDetachTransportLost, opts: runCommandServerOptions{wsAbruptClose: true}},
		{name: "invalid notification", reason: runTraceDetachNotificationInvalid, opts: runCommandServerOptions{wsNotificationMethod: "wrong.method", wsRows: []map[string]any{validRunCommandTraceRow("evt-invalid-notification")}}},
		{name: "subscription mismatch", reason: runTraceDetachSubscriptionMismatch, opts: runCommandServerOptions{wsNotificationID: "wrong-subscription", wsRows: []map[string]any{validRunCommandTraceRow("evt-mismatch")}}},
		{name: "invalid trace row", reason: runTraceDetachTraceRowInvalid, opts: runCommandServerOptions{wsRows: []map[string]any{{"event_name": "scan.requested", "event_created_at": "2026-05-13T10:00:01Z"}}}},
		{name: "clean stream close", reason: runTraceDetachStreamClosed, opts: runCommandServerOptions{wsCloseAfterRows: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			warning := &notifyingBuffer{needle: string(test.reason), notify: make(chan struct{})}
			serverOpts := test.opts
			serverOpts.rpcResponder = observerTerminalAfterWarningResponder(t, "6d271a82-e74c-5368-a367-38367ccbd1b2", warning.notify)
			server, calls, _ := newRunCommandServer(t, serverOpts)
			defer server.Close()

			var stdout bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			code := executeRootCommandWithOptions(
				ctx,
				t.TempDir(),
				[]string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath},
				&stdout,
				warning,
				testRunCommandOptions(server),
			)
			if code != 0 {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
			}
			fact := requireSingleRunTraceDetachFact(t, warning.String())
			assertRunTraceDetachFact(t, fact, test.reason, "6d271a82-e74c-5368-a367-38367ccbd1b2", "swarm run start --connect "+server.URL+" --reattach 6d271a82-e74c-5368-a367-38367ccbd1b2")
			methods := runCommandMethodNames(*calls)
			if len(methods) < 3 || methods[0] != "health.check" || methods[1] != "run.start" || methods[len(methods)-1] != "run.diagnose" {
				t.Fatalf("methods = %v, want health.check, run.start, then run.get polling", methods)
			}
			if !strings.Contains(stdout.String(), "run terminal: run_id=6d271a82-e74c-5368-a367-38367ccbd1b2 status=completed") {
				t.Fatalf("stdout = %q", stdout.String())
			}
		})
	}
}

func TestRunCommandTraceObserverOverBudgetNotificationShedsAndContinues(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	warning := &notifyingBuffer{needle: "response_over_budget", notify: make(chan struct{})}
	overBudgetRow := validRunCommandTraceRow("evt-over-budget")
	overBudgetRow["payload"] = map[string]any{"blob": strings.Repeat("a", cliAPIResponseBudget)}
	serverOpts := runCommandServerOptions{
		wsRows:       []map[string]any{overBudgetRow},
		rpcResponder: observerTerminalAfterWarningResponder(t, "8e510ef1-861c-5a1e-a690-fc4be757e0ce", warning.notify),
	}
	server, calls, _ := newRunCommandServer(t, serverOpts)
	defer server.Close()

	var stdout bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	code := executeRootCommandWithOptions(
		ctx,
		t.TempDir(),
		[]string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath},
		&stdout,
		warning,
		testRunCommandOptions(server),
	)
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
	}
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	if fact.ReasonCode != runTraceDetachResponseOverBudget {
		t.Fatalf("detach fact = %#v, want response_over_budget", fact)
	}
	for _, want := range []string{"run.subscribe_trace", "1 MiB"} {
		if !strings.Contains(fact.Message, want) {
			t.Fatalf("detach message missing %q: %s", want, fact.Message)
		}
	}
	methods := runCommandMethodNames(*calls)
	if len(methods) < 3 || methods[0] != "health.check" || methods[1] != "run.start" || methods[len(methods)-1] != "run.diagnose" {
		t.Fatalf("methods = %v, want health.check, run.start, then run.get polling to continue", methods)
	}
	if !strings.Contains(stdout.String(), "run terminal: run_id=8e510ef1-861c-5a1e-a690-fc4be757e0ce status=completed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunCommandTraceAttachTimeoutDetachesAndContinues(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	requestRead := make(chan struct{})
	wsClosed := make(chan struct{})
	warning := &notifyingBuffer{needle: string(runTraceDetachAttachTimedOut), notify: make(chan struct{})}
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder:   observerTerminalAfterWarningResponder(t, "7e3709ac-9ed5-5d2d-afb2-da8ed843f2ab", warning.notify),
		wsRequestRead:  requestRead,
		wsClosed:       wsClosed,
		wsHoldResponse: true,
	})
	defer server.Close()

	opts := testRunCommandOptions(server)
	opts.runTraceAttachTimeout = 25 * time.Millisecond
	var stdout bytes.Buffer
	code := executeRootCommandWithOptions(
		context.Background(),
		t.TempDir(),
		[]string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath},
		&stdout,
		warning,
		opts,
	)
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
	}
	requireSignal(t, requestRead, "subscription request read")
	requireSignal(t, wsClosed, "timed-out WebSocket close")
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachAttachTimedOut, "7e3709ac-9ed5-5d2d-afb2-da8ed843f2ab", "swarm run start --connect "+server.URL+" --reattach 7e3709ac-9ed5-5d2d-afb2-da8ed843f2ab")
}

func TestRunCommandObserverDetachPreservesTerminalFailureExit(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	warning := &notifyingBuffer{needle: string(runTraceDetachAttachFailed), notify: make(chan struct{})}
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("b3f5e62f-94a7-5c2d-b9c2-a3969fc91c40", "running")
			case "run.get", "run.diagnose":
				run := validDiagnosticRunHeader("b3f5e62f-94a7-5c2d-b9c2-a3969fc91c40")
				select {
				case <-warning.notify:
					run["status"] = "failed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
					run["failure"] = testRuntimeFailureClass(runtimefailures.ClassInternalFailure, "workflow_failed")
				default:
				}
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsHTTPStatus: http.StatusServiceUnavailable,
	})
	defer server.Close()

	var stdout bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, warning, testRunCommandOptions(server))
	if code != 7 {
		t.Fatalf("code = %d, want terminal workflow exit 7 stdout=%s stderr=%s", code, stdout.String(), warning.String())
	}
	if !strings.Contains(stdout.String(), "run terminal: run_id=b3f5e62f-94a7-5c2d-b9c2-a3969fc91c40 status=failed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachAttachFailed, "b3f5e62f-94a7-5c2d-b9c2-a3969fc91c40", "swarm run start --connect "+server.URL+" --reattach b3f5e62f-94a7-5c2d-b9c2-a3969fc91c40")
}

func TestRunCommandTerminalStatusCancelsPendingTraceObserver(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	requestRead := make(chan struct{})
	wsClosed := make(chan struct{})
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("3a92a694-7f51-5f37-83b9-6ea21b69d5f0", "running")
			case "run.get", "run.diagnose":
				awaitRunCommandTraceRequest(t, requestRead)
				run := validDiagnosticRunHeader("3a92a694-7f51-5f37-83b9-6ea21b69d5f0")
				run["status"] = "completed"
				run["ended_at"] = "2026-05-13T10:01:00Z"
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsRequestRead:  requestRead,
		wsClosed:       wsClosed,
		wsHoldResponse: true,
	})
	defer server.Close()

	opts := testRunCommandOptions(server)
	opts.runTraceAttachTimeout = time.Hour
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	requireSignal(t, requestRead, "subscription request read")
	requireSignal(t, wsClosed, "terminal observer close")
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, terminal cancellation is not observer degradation", stderr.String())
	}
}

func TestRunCommandTerminalFailureCancelsPendingTraceObserver(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	requestRead := make(chan struct{})
	wsClosed := make(chan struct{})
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("cab86c63-7aa1-58c5-87bb-a95e4dd1e56f", "running")
			case "run.get", "run.diagnose":
				awaitRunCommandTraceRequest(t, requestRead)
				run := validDiagnosticRunHeader("cab86c63-7aa1-58c5-87bb-a95e4dd1e56f")
				run["status"] = "failed"
				run["ended_at"] = "2026-05-13T10:01:00Z"
				run["failure"] = testRuntimeFailureClass(runtimefailures.ClassInternalFailure, "workflow_failed")
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsRequestRead:  requestRead,
		wsClosed:       wsClosed,
		wsHoldResponse: true,
	})
	defer server.Close()

	opts := testRunCommandOptions(server)
	opts.runTraceAttachTimeout = time.Hour
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, &stdout, &stderr, opts)
	if code != 7 {
		t.Fatalf("code = %d, want terminal workflow exit 7 stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	requireSignal(t, requestRead, "subscription request read")
	requireSignal(t, wsClosed, "terminal observer close")
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("stderr = %q, terminal cancellation is not observer degradation", stderr.String())
	}
}

func TestRunCommandActiveReattachObserverFailureDetachesAndPolls(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	warning := &notifyingBuffer{needle: string(runTraceDetachAttachFailed), notify: make(chan struct{})}
	server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, call int) map[string]any {
			if req.Method != "run.get" {
				t.Fatalf("unexpected method = %q", req.Method)
			}
			run := validDiagnosticRunHeader("60b0c147-5008-57f3-97d7-94e475f78cfc")
			if call > 1 {
				select {
				case <-warning.notify:
					run["status"] = "completed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
				default:
				}
			}
			return runCommandReadResult(req.Method, run)
		},
		wsHTTPStatus: http.StatusServiceUnavailable,
	})
	defer server.Close()

	var stdout bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--reattach", "60b0c147-5008-57f3-97d7-94e475f78cfc"}, &stdout, warning, testRunCommandOptions(server))
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
	}
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachAttachFailed, "60b0c147-5008-57f3-97d7-94e475f78cfc", "swarm run start --connect "+server.URL+" --reattach 60b0c147-5008-57f3-97d7-94e475f78cfc")
	methods := runCommandMethodNames(*calls)
	if len(methods) < 2 {
		t.Fatalf("methods = %v, want initial and followed run.get", methods)
	}
	for _, method := range methods {
		if method != "run.get" {
			t.Fatalf("methods = %v, active reattach must only poll run.get", methods)
		}
	}
}

func TestRunCommandInterruptCancelsPendingTraceObserverByMode(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, test := range []struct {
		name            string
		args            func(string) []string
		stopOnInterrupt bool
		wantMethods     []string
	}{
		{
			name: "connected start calls run.stop",
			args: func(serverURL string) []string {
				return []string{"run", "start", "--connect", serverURL, "--event", "scan.requested", "--payload", payloadPath}
			},
			stopOnInterrupt: true,
			wantMethods:     []string{"health.check", "run.start", "run.stop"},
		},
		{
			name: "active reattach only detaches",
			args: func(serverURL string) []string {
				return []string{"run", "start", "--connect", serverURL, "--reattach", "e0b6a02d-8ba3-5523-bd46-5eda1b1d40a1"}
			},
			wantMethods: []string{"run.get"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			requestRead := make(chan struct{})
			wsClosed := make(chan struct{})
			server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
				rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
					switch req.Method {
					case "health.check":
						return runCommandHealthResult()
					case "run.start":
						return runStartCommandResult("e0b6a02d-8ba3-5523-bd46-5eda1b1d40a1", "running")
					case "run.get", "run.diagnose":
						return runCommandReadResult(req.Method, validDiagnosticRunHeader("e0b6a02d-8ba3-5523-bd46-5eda1b1d40a1"))
					case "run.stop":
						if !test.stopOnInterrupt {
							t.Fatal("reattach interrupt called run.stop")
						}
						return map[string]any{"ok": true}
					default:
						t.Fatalf("unexpected method = %q", req.Method)
					}
					return nil
				},
				wsRequestRead:  requestRead,
				wsClosed:       wsClosed,
				wsHoldResponse: true,
			})
			defer server.Close()

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				<-requestRead
				cancel()
			}()
			opts := testRunCommandOptions(server)
			opts.runStatusPoll = time.Hour
			opts.runTraceAttachTimeout = time.Hour
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(ctx, t.TempDir(), test.args(server.URL), &stdout, &stderr, opts)
			if code != 130 {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
			}
			requireSignal(t, wsClosed, "interrupt observer close")
			if test.stopOnInterrupt && len(*calls) == len(test.wantMethods)+1 {
				test.wantMethods = []string{"health.check", "run.start", "run.diagnose", "run.stop"}
			}
			assertRunCommandMethods(t, calls, test.wantMethods)
			if strings.Contains(stderr.String(), runTraceObserverDetachedType) {
				t.Fatalf("stderr emitted degradation during explicit interrupt: %s", stderr.String())
			}
		})
	}
}

func TestRunCommandInterruptAfterObserverDetachPreservesModeControl(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, test := range []struct {
		name            string
		args            func(string) []string
		stopOnInterrupt bool
		wantMethods     []string
	}{
		{
			name: "connected start calls run.stop",
			args: func(serverURL string) []string {
				return []string{"run", "start", "--connect", serverURL, "--event", "scan.requested", "--payload", payloadPath}
			},
			stopOnInterrupt: true,
			wantMethods:     []string{"health.check", "run.start", "run.stop"},
		},
		{
			name: "active reattach only detaches",
			args: func(serverURL string) []string {
				return []string{"run", "start", "--connect", serverURL, "--reattach", "8be65499-5a47-5254-ba95-c98d788df5d4"}
			},
			wantMethods: []string{"run.get"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			warningSeen := make(chan struct{})
			warning := &notifyingBuffer{needle: string(runTraceDetachAttachFailed), notify: warningSeen}
			server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
				rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
					switch req.Method {
					case "health.check":
						return runCommandHealthResult()
					case "run.start":
						return runStartCommandResult("8be65499-5a47-5254-ba95-c98d788df5d4", "running")
					case "run.get", "run.diagnose":
						return runCommandReadResult(req.Method, validDiagnosticRunHeader("8be65499-5a47-5254-ba95-c98d788df5d4"))
					case "run.stop":
						if !test.stopOnInterrupt {
							t.Fatal("reattach interrupt called run.stop")
						}
						return map[string]any{"ok": true}
					default:
						t.Fatalf("unexpected method = %q", req.Method)
					}
					return nil
				},
				wsHTTPStatus: http.StatusServiceUnavailable,
			})
			defer server.Close()

			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				<-warningSeen
				cancel()
			}()
			opts := testRunCommandOptions(server)
			opts.runStatusPoll = time.Hour
			var stdout bytes.Buffer
			code := executeRootCommandWithOptions(ctx, t.TempDir(), test.args(server.URL), &stdout, warning, opts)
			if code != 130 {
				t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
			}
			if test.stopOnInterrupt && len(*calls) == len(test.wantMethods)+1 {
				test.wantMethods = []string{"health.check", "run.start", "run.diagnose", "run.stop"}
			}
			assertRunCommandMethods(t, calls, test.wantMethods)
			if strings.Count(warning.String(), runTraceObserverDetachedType) != 1 {
				t.Fatalf("detach fact count != 1: %s", warning.String())
			}
		})
	}
}

func TestRunCommandBlockedTraceRenderingDoesNotDelayInterruptControl(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	for _, test := range []struct {
		name            string
		args            func(string) []string
		stopOnInterrupt bool
		wantMethods     []string
	}{
		{
			name: "connected start stops the run",
			args: func(serverURL string) []string {
				return []string{"run", "start", "--connect", serverURL, "--event", "scan.requested", "--payload", payloadPath}
			},
			stopOnInterrupt: true,
			wantMethods:     []string{"health.check", "run.start", "run.stop"},
		},
		{
			name: "active reattach leaves the run active",
			args: func(serverURL string) []string {
				return []string{"run", "start", "--connect", serverURL, "--reattach", "46ed30c1-eebe-5006-963b-6ee774bc687a"}
			},
			wantMethods: []string{"run.get"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			wsClosed := make(chan struct{})
			stopCalled := make(chan struct{})
			server, calls, _ := newRunCommandServer(t, runCommandServerOptions{
				rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
					switch req.Method {
					case "health.check":
						return runCommandHealthResult()
					case "run.start":
						return runStartCommandResult("46ed30c1-eebe-5006-963b-6ee774bc687a", "running")
					case "run.get", "run.diagnose":
						return runCommandReadResult(req.Method, validDiagnosticRunHeader("46ed30c1-eebe-5006-963b-6ee774bc687a"))
					case "run.stop":
						if !test.stopOnInterrupt {
							t.Fatal("reattach interrupt called run.stop")
						}
						close(stopCalled)
						return map[string]any{"ok": true}
					default:
						t.Fatalf("unexpected method = %q", req.Method)
					}
					return nil
				},
				wsRows:   []map[string]any{validRunCommandTraceRow("evt-blocked-trace")},
				wsClosed: wsClosed,
			})
			defer server.Close()

			stdout := newBlockingRunWriter("trace +")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan int, 1)
			opts := testRunCommandOptions(server)
			opts.runStatusPoll = time.Hour
			go func() {
				var stderr bytes.Buffer
				done <- executeRootCommandWithOptions(ctx, t.TempDir(), test.args(server.URL), stdout, &stderr, opts)
			}()
			requireSignal(t, stdout.blocked, "blocked trace output")
			cancel()
			if test.stopOnInterrupt {
				requireSignal(t, stopCalled, "run.stop while trace output is blocked")
			}
			requireSignal(t, wsClosed, "observer settlement while trace output is blocked")
			select {
			case code := <-done:
				t.Fatalf("command returned before trace output release with code %d", code)
			default:
			}
			close(stdout.release)
			select {
			case code := <-done:
				if code != 130 {
					t.Fatalf("code = %d, want 130; stdout=%s", code, stdout.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("run command did not finish after trace output release")
			}
			if test.stopOnInterrupt && len(*calls) == len(test.wantMethods)+1 {
				test.wantMethods = []string{"health.check", "run.start", "run.diagnose", "run.stop"}
			}
			assertRunCommandMethods(t, calls, test.wantMethods)
		})
	}
}

func TestRunCommandBlockedDetachRenderingDoesNotDelayTerminalSelection(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	stderr := newBlockingRunWriter(runTraceObserverDetachedType)
	terminalSelected := make(chan struct{})
	wsClosed := make(chan struct{})
	var terminalOnce sync.Once
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("c9888279-eecc-540d-a91f-1c9bc35ecf1f", "running")
			case "run.get", "run.diagnose":
				run := validDiagnosticRunHeader("c9888279-eecc-540d-a91f-1c9bc35ecf1f")
				select {
				case <-stderr.blocked:
					run["status"] = "completed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
					terminalOnce.Do(func() { close(terminalSelected) })
				default:
				}
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsAbruptClose: true,
		wsClosed:      wsClosed,
	})
	defer server.Close()

	var stdout bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- executeRootCommandWithOptions(
			context.Background(),
			t.TempDir(),
			[]string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath},
			&stdout,
			stderr,
			testRunCommandOptions(server),
		)
	}()
	requireSignal(t, stderr.blocked, "blocked detach output")
	requireSignal(t, terminalSelected, "terminal status selection while detach output is blocked")
	requireSignal(t, wsClosed, "observer transport settlement while detach output is blocked")
	select {
	case code := <-done:
		t.Fatalf("command returned before detach output release with code %d", code)
	default:
	}
	close(stderr.release)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run command did not finish after detach output release")
	}
	fact := requireSingleRunTraceDetachFact(t, stderr.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachTransportLost, "c9888279-eecc-540d-a91f-1c9bc35ecf1f", "swarm run start --connect "+server.URL+" --reattach c9888279-eecc-540d-a91f-1c9bc35ecf1f")
	if !strings.Contains(stdout.String(), "run terminal: run_id=c9888279-eecc-540d-a91f-1c9bc35ecf1f status=completed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestForegroundRunTraceObserverTimeoutInterruptsBlockedRequestWrite(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upgraded := make(chan struct{})
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		close(upgraded)
		time.Sleep(100 * time.Millisecond)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
		close(closed)
	}))
	defer server.Close()

	wsEndpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	observer := startForegroundRunTraceObserver(context.Background(), wsEndpoint, "test-token", strings.Repeat("r", 8<<20), nil, 20*time.Millisecond)
	defer observer.stop()
	requireSignal(t, upgraded, "WebSocket upgrade")
	select {
	case det := <-observer.detached:
		if det.reason != runTraceDetachAttachTimedOut {
			t.Fatalf("reason = %q, want %q", det.reason, runTraceDetachAttachTimedOut)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked subscription write did not time out")
	}
	requireSignal(t, closed, "write-stalled WebSocket close")
}

func TestRunCommandHealthyTraceBurstWithinTransportBoundDoesNotDetach(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	lastRendered := make(chan struct{})
	stdout := &notifyingBuffer{needle: "evt-healthy-016", notify: lastRendered}
	rows := make([]map[string]any, 16)
	for i := range rows {
		rows[i] = validRunCommandTraceRow(fmt.Sprintf("evt-healthy-%03d", i+1))
	}
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("0ab3acf0-22e7-5e71-9fd8-733d351436e7", "running")
			case "run.get", "run.diagnose":
				run := validDiagnosticRunHeader("0ab3acf0-22e7-5e71-9fd8-733d351436e7")
				select {
				case <-lastRendered:
					run["status"] = "completed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
				default:
				}
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
				return nil
			}
		},
		wsRows: rows,
	})
	defer server.Close()

	var stderr bytes.Buffer
	code := executeRootCommandWithOptions(
		context.Background(),
		t.TempDir(),
		[]string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath},
		stdout,
		&stderr,
		testRunCommandOptions(server),
	)
	if code != 0 {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), runTraceObserverDetachedType) {
		t.Fatalf("healthy burst detached observer: %s", stderr.String())
	}
	for i := range rows {
		want := fmt.Sprintf("evt-healthy-%03d", i+1)
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q: %s", want, stdout.String())
		}
	}
}

func TestRunCommandQueueOverflowDetachesExactlyOnce(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	warning := &notifyingBuffer{needle: string(runTraceDetachQueueOverflow), notify: make(chan struct{})}
	rows := make([]map[string]any, 64)
	for i := range rows {
		rows[i] = validRunCommandTraceRow(fmt.Sprintf("evt-overflow-%03d", i))
	}
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: observerTerminalAfterWarningResponder(t, "7063c4a3-0896-54c7-89a5-df80077aa6c0", warning.notify),
		wsRows:       rows,
	})
	defer server.Close()

	stdout := newBlockingTraceWriter()
	done := make(chan int, 1)
	go func() {
		done <- executeRootCommandWithOptions(
			context.Background(),
			t.TempDir(),
			[]string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath},
			stdout,
			warning,
			testRunCommandOptions(server),
		)
	}()
	select {
	case <-stdout.blocked:
	case <-warning.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("trace observer neither blocked output nor reported overflow")
	}
	time.Sleep(100 * time.Millisecond)
	close(stdout.release)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run command did not finish after observer overflow")
	}
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachQueueOverflow, "7063c4a3-0896-54c7-89a5-df80077aa6c0", "swarm run start --connect "+server.URL+" --reattach 7063c4a3-0896-54c7-89a5-df80077aa6c0")
}

func TestRunCommandTerminalJoinPublishesEstablishedObserverOverflow(t *testing.T) {
	setCLIAPITestToken(t, "test-token")
	payloadPath := writeRunCommandPayloadFile(t, map[string]any{"ok": true})
	warning := newBlockingRunWriter(string(runTraceDetachQueueOverflow))
	stdout := newBlockingTraceWriter()
	defer func() {
		for _, writer := range []*blockingRunWriter{stdout, warning} {
			select {
			case <-writer.release:
			default:
				close(writer.release)
			}
		}
	}()
	rows := make([]map[string]any, 64)
	for i := range rows {
		rows[i] = validRunCommandTraceRow(fmt.Sprintf("evt-terminal-overflow-%03d", i))
	}
	subscribed := make(chan struct{})
	terminalObserved := make(chan struct{})
	var terminalObservedOnce sync.Once
	server, _, _ := newRunCommandServer(t, runCommandServerOptions{
		rpcResponder: func(req jsonRPCRequest, _ int) map[string]any {
			switch req.Method {
			case "health.check":
				return runCommandHealthResult()
			case "run.start":
				return runStartCommandResult("a310030f-840b-59b7-b13c-e7d56e8c355c", "running")
			case "run.get", "run.diagnose":
				run := validDiagnosticRunHeader("a310030f-840b-59b7-b13c-e7d56e8c355c")
				select {
				case <-warning.blocked:
					run["status"] = "completed"
					run["ended_at"] = "2026-05-13T10:01:00Z"
					terminalObservedOnce.Do(func() { close(terminalObserved) })
				default:
				}
				return runCommandReadResult(req.Method, run)
			default:
				t.Fatalf("unexpected method = %q", req.Method)
			}
			return nil
		},
		wsRows:       rows,
		wsSubscribed: subscribed,
	})
	defer server.Close()

	opts := testRunCommandOptions(server)
	opts.runTraceAttachTimeout = 30 * time.Second
	done := make(chan int, 1)
	go func() {
		done <- executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"run", "start", "--connect", server.URL, "--event", "scan.requested", "--payload", payloadPath}, stdout, warning, opts)
	}()
	requireSignalWithin(t, subscribed, "terminal trace subscription", 30*time.Second)
	requireSignalWithin(t, warning.blocked, "established terminal trace queue-overflow fact", 30*time.Second)
	requireSignalWithin(t, terminalObserved, "terminal status after established queue overflow", 30*time.Second)
	close(stdout.release)
	close(warning.release)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), warning.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run command did not finish after terminal observer join")
	}
	fact := requireSingleRunTraceDetachFact(t, warning.String())
	assertRunTraceDetachFact(t, fact, runTraceDetachQueueOverflow, "a310030f-840b-59b7-b13c-e7d56e8c355c", "swarm run start --connect "+server.URL+" --reattach a310030f-840b-59b7-b13c-e7d56e8c355c")
	if !strings.Contains(stdout.String(), "run terminal: run_id=a310030f-840b-59b7-b13c-e7d56e8c355c status=completed") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunTraceObserverDetachedFactTTYAndNonTTYParity(t *testing.T) {
	fact := runTraceObserverDetachedFact{
		Type:            runTraceObserverDetachedType,
		Severity:        runTraceObserverWarning,
		ReasonCode:      runTraceDetachTransportLost,
		RunID:           "950dc6b1-91f8-587b-bbad-cd6561bef9a8",
		RunContinues:    true,
		ReattachCommand: "swarm run start --connect http://127.0.0.1:8080 --reattach 950dc6b1-91f8-587b-bbad-cd6561bef9a8",
	}
	var machine, human bytes.Buffer
	writeRunTraceObserverDetachedWithMode(&machine, false, fact)
	writeRunTraceObserverDetachedWithMode(&human, true, fact)
	if got := requireSingleRunTraceDetachFact(t, machine.String()); got != fact {
		t.Fatalf("machine fact = %#v, want %#v", got, fact)
	}
	for _, want := range []string{"WARNING:", "950dc6b1-91f8-587b-bbad-cd6561bef9a8", string(runTraceDetachTransportLost), "the run continues", fact.ReattachCommand} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human warning = %q, want %q", human.String(), want)
		}
	}
}

func observerTerminalAfterWarningResponder(t *testing.T, runID string, warning <-chan struct{}) func(jsonRPCRequest, int) map[string]any {
	t.Helper()
	return func(req jsonRPCRequest, _ int) map[string]any {
		switch req.Method {
		case "health.check":
			return runCommandHealthResult()
		case "run.start":
			return runStartCommandResult(runID, "running")
		case "run.get", "run.diagnose":
			run := validDiagnosticRunHeader(runID)
			select {
			case <-warning:
				run["status"] = "completed"
				run["ended_at"] = "2026-05-13T10:01:00Z"
			default:
			}
			return runCommandReadResult(req.Method, run)
		default:
			t.Fatalf("unexpected method = %q", req.Method)
		}
		return nil
	}
}

func requireSingleRunTraceDetachFact(t *testing.T, raw string) runTraceObserverDetachedFact {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	if len(lines) != 1 {
		t.Fatalf("detach output lines = %d, want 1: %q", len(lines), raw)
	}
	var fact runTraceObserverDetachedFact
	if err := json.Unmarshal([]byte(lines[0]), &fact); err != nil {
		t.Fatalf("decode detach fact: %v; raw=%q", err, raw)
	}
	return fact
}

func assertRunTraceDetachFact(t *testing.T, fact runTraceObserverDetachedFact, reason runTraceDetachReason, runID, reattach string) {
	t.Helper()
	want := runTraceObserverDetachedFact{
		Type:            runTraceObserverDetachedType,
		Severity:        runTraceObserverWarning,
		ReasonCode:      reason,
		RunID:           runID,
		RunContinues:    true,
		ReattachCommand: reattach,
	}
	if fact != want {
		t.Fatalf("detach fact = %#v, want %#v", fact, want)
	}
}

func requireSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	requireSignalWithin(t, signal, name, 2*time.Second)
}

func requireSignalWithin(t *testing.T, signal <-chan struct{}, name string, timeout time.Duration) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", name)
	}
}

type blockingRunWriter struct {
	needle  string
	mu      sync.Mutex
	buf     bytes.Buffer
	blocked chan struct{}
	release chan struct{}
	started bool
}

func newBlockingRunWriter(needle string) *blockingRunWriter {
	return &blockingRunWriter{needle: needle, blocked: make(chan struct{}), release: make(chan struct{})}
}

func newBlockingTraceWriter() *blockingRunWriter {
	return newBlockingRunWriter("trace ")
}

func (w *blockingRunWriter) Write(p []byte) (int, error) {
	shouldBlock := false
	if strings.Contains(string(p), w.needle) {
		w.mu.Lock()
		if !w.started {
			w.started = true
			close(w.blocked)
			shouldBlock = true
		}
		w.mu.Unlock()
	}
	if shouldBlock {
		<-w.release
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *blockingRunWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestSettleRunTraceSubscriptionFindsPendingErrorAfterJoin(t *testing.T) {
	sub := &runTraceSubscription{
		errs: make(chan error, 1),
		done: make(chan struct{}),
	}
	sub.errs <- wrapRunTraceObserverError(runTraceDetachQueueOverflow, fmt.Errorf("run.subscribe_trace notification queue overflow"))
	close(sub.done)
	detached := make(chan runTraceDetach, 1)
	settleRunTraceSubscription(sub, detached)
	select {
	case det := <-detached:
		if det.reason != runTraceDetachQueueOverflow {
			t.Fatalf("reason = %q, want %q", det.reason, runTraceDetachQueueOverflow)
		}
	default:
		t.Fatal("pending subscription error lost after settle; no detach published")
	}
}

func TestSettleRunTraceSubscriptionHealthyCloseStaysSilent(t *testing.T) {
	sub := &runTraceSubscription{
		errs: make(chan error, 1),
		done: make(chan struct{}),
	}
	close(sub.done)
	detached := make(chan runTraceDetach, 1)
	settleRunTraceSubscription(sub, detached)
	select {
	case det := <-detached:
		t.Fatalf("healthy stream close published a detach: %#v", det)
	default:
	}
}
