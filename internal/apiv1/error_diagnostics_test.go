package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/gorilla/websocket"
)

type diagnosticRedactedError struct{ cause error }

func (e diagnosticRedactedError) Error() string { return "redacted outer message" }
func (e diagnosticRedactedError) Unwrap() error { return e.cause }

type diagnosticPanicError struct{}

func (diagnosticPanicError) Error() string { panic("panic-value-must-not-print") }

type diagnosticFormattingError struct{ calls *atomic.Int32 }

func (e diagnosticFormattingError) Error() string {
	e.calls.Add(1)
	return "message-as-returned"
}
func (diagnosticFormattingError) Format(fmt.State, rune)       { panic("optional-formatter") }
func (diagnosticFormattingError) String() string               { panic("optional-stringer") }
func (diagnosticFormattingError) GoString() string             { panic("optional-gostringer") }
func (diagnosticFormattingError) MarshalJSON() ([]byte, error) { panic("optional-marshaler") }

type diagnosticSpoofError struct{}

func (diagnosticSpoofError) Error() string        { return context.Canceled.Error() }
func (diagnosticSpoofError) Is(target error) bool { return target == context.Canceled }

func proveVerboseInternalFailureDiagnostics(t *testing.T) {
	const secret = "synthetic-error-secret"
	for _, transport := range []string{"http", "ws"} {
		var messages atomic.Int32
		for _, tc := range []struct {
			name     string
			cause    error
			records  int
			eligible bool
			secret   bool
		}{
			{"joined", errors.Join(fmt.Errorf("wrapped: %w", errors.New(secret)), errors.New("second branch")), 1, true, true},
			{"redacted", diagnosticRedactedError{errors.New(secret)}, 1, true, true},
			{"typed_unknown", fmt.Errorf("caller: %w", failures.Wrap(failures.ClassInternalFailure, "unclassified_runtime_error", "api", "event.publish", nil, errors.New(secret))), 1, true, true},
			{"known", fmt.Errorf(secret+": %w", failures.New(failures.ClassConnectorFailure, "provider_rate_limited", "provider", "call", nil)), 1, false, false},
			{"malformed", &failures.Error{Failure: failures.Envelope{Class: failures.ClassConnectorFailure}}, 1, false, false},
			{"application", NewApplicationError(RunNotFoundCode, false, nil), 0, false, false},
			{"params", NewInvalidParamsError(map[string]any{"field": "safe"}), 0, false, false},
			{"formatter", diagnosticFormattingError{&messages}, 1, true, false},
			{"panic", diagnosticPanicError{}, 1, true, false},
			{"sentinel", context.Canceled, 1, true, false},
			{"spoof", diagnosticSpoofError{}, 1, true, false},
			{"full_message", errors.New(strings.Repeat("full-message", 500)), 1, true, false},
		} {
			t.Run(transport+"/"+tc.name, func(t *testing.T) {
				var ordinary string
				for _, verbose := range []bool{false, true} {
					handler := testHandler(t, Options{AuthTokens: []string{testToken}, VerboseErrors: verbose, Handlers: map[string]MethodHandler{
						"event.publish": func(context.Context, Request) (any, error) { return nil, tc.cause },
					}})
					raw, output := diagnosticRPC(t, handler, transport)
					if verbose && string(raw) != ordinary {
						t.Fatalf("verbose changed caller bytes: %s != %s", raw, ordinary)
					}
					ordinary = string(raw)
					if strings.Contains(string(raw), secret) || strings.Contains(output, "payload-only-secret") || strings.Contains(output, testToken) || strings.Contains(output, "panic-value-must-not-print") {
						t.Fatal("diagnostics widened the caller/request/panic-value boundary")
					}
					var records []struct {
						Detail *internalFailureDiagnostics `json:"error_diagnostics"`
					}
					for _, line := range strings.Split(output, "\n") {
						if _, detail, found := strings.Cut(line, "component=api detail="); found {
							var record struct {
								Detail *internalFailureDiagnostics `json:"error_diagnostics"`
							}
							if err := json.Unmarshal([]byte(detail), &record); err != nil {
								t.Fatal(err)
							}
							records = append(records, record)
						}
					}
					if len(records) != tc.records {
						t.Fatalf("process records=%d, want %d", len(records), tc.records)
					}
					var detail *internalFailureDiagnostics
					if len(records) == 1 {
						detail = records[0].Detail
					}
					if (detail != nil) != (verbose && tc.eligible) || strings.Contains(output, secret) != (verbose && tc.eligible && tc.secret) {
						t.Fatalf("incorrect diagnostic gate verbose=%t eligible=%t: %s", verbose, tc.eligible, output)
					}
					if tc.name == "formatter" && ((!verbose && messages.Load() != 0) || (verbose && messages.Load() != 1)) {
						t.Fatalf("extra Error calls in verbose=%t: %d", verbose, messages.Load())
					}
					if detail == nil {
						continue
					}
					if len(detail.Stack) == 0 || detail.ChainIncomplete || detail.StackIncomplete || !strings.Contains(output, "(*Handler).logInternalFallback") {
						t.Fatal("missing complete reporting stack or chain")
					}
					if tc.name == "panic" {
						if !detail.Chain[0].MessageUnavailable || detail.Chain[0].Message != "" {
							t.Fatal("Error panic did not produce the fixed unavailable marker")
						}
					} else {
						want := "message-as-returned"
						if tc.name != "formatter" {
							want = tc.cause.Error()
						}
						if detail.Chain[0].Message != want {
							t.Fatal("returned error message was formatted, redacted or truncated")
						}
					}
					if tc.name == "joined" && (len(detail.Chain) != 4 || detail.Chain[2].Message != secret || detail.Chain[3].Parent != 0 || detail.Chain[3].Branch != 1) {
						t.Fatal("wrapped/joined branch order or messages were lost")
					}
					if tc.name == "sentinel" && detail.Chain[0].Kind != "context.Canceled" || tc.name == "spoof" && detail.Chain[0].Kind != "opaque" {
						t.Fatal("sentinel naming used text or custom Is instead of exact identity")
					}
				}
			})
		}
	}
}

func diagnosticRPC(t *testing.T, handler *Handler, transport string) (raw []byte, output string) {
	t.Helper()
	const body = `{"jsonrpc":"2.0","id":"diagnostic","method":"event.publish","params":{"event_name":"scan.requested","run_id":"run-log","payload":{"entity_id":"entity-log","secret":"payload-only-secret"}}}`
	output = captureProcessLog(t, func() {
		if transport == "http" {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("X-Correlation-ID", "diagnostic-correlation")
			handler.ServeHTTP(rec, testAuthorActivityRequest(req))
			if rec.Code != http.StatusOK {
				t.Fatalf("HTTP status=%d", rec.Code)
			}
			raw = append([]byte(nil), rec.Body.Bytes()...)
			return
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { handler.ServeHTTP(w, testAuthorActivityRequest(req)) }))
		defer server.Close()
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/ws", http.Header{"Authorization": {"Bearer " + testToken}, "X-Correlation-ID": {"diagnostic-correlation"}})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(body)); err != nil {
			t.Fatal(err)
		}
		_, raw, err = conn.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
	})
	return
}

func proveInternalFailureDiagnosticBounds(t *testing.T) {
	children := make([]error, diagnosticNodeLimit+1)
	for i := range children {
		children[i] = errors.New("repeated child")
	}
	detail := renderInternalFailureDiagnostics(errors.Join(children...))
	if !detail.ChainIncomplete || len(detail.Chain) != diagnosticNodeLimit || detail.Chain[diagnosticNodeLimit-1].Branch != diagnosticNodeLimit-2 {
		t.Fatal("counter walk failed to mark the omitted branches incomplete")
	}
	deep := deepReportingDiagnostic(diagnosticFrameLimit + 1)
	if !deep.StackIncomplete || len(deep.Stack) != diagnosticFrameLimit {
		t.Fatal("full frame buffer was claimed as a complete stack")
	}
}

func deepReportingDiagnostic(depth int) internalFailureDiagnostics {
	if depth == 0 {
		return renderInternalFailureDiagnostics(errors.New("stack check"))
	}
	return deepReportingDiagnostic(depth - 1)
}
