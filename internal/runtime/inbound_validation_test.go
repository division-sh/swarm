package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Owns the HTTP display/error-composition boundary; the served both-store root
// separately owns authentication, real persistence conservation and settlement.
func TestInboundPublicationValidationResponseBoundary(t *testing.T) {
	target := testInboundTarget("entity-1", "")
	source := inboundTransportSource(t, []InboundTarget{target}, false)
	bundle, _ := semanticview.Bundle(source)
	fact := testSourceArtifactFact(t, bundle.SourceArtifact.BundleHash())
	canonical := NewRuntimePayloadAdmitter(nil, source, fact)
	var failure error
	bus, err := newInboundTestEventBusWithOptions(t, nil, runtimebus.EventBusOptions{
		ContractBundle: source,
		PayloadAdmitter: func(ctx context.Context, event events.Event, flow string) (events.PayloadAdmission, error) {
			if strings.HasPrefix(string(event.Type()), "inbound.") {
				return events.PayloadAdmission{}, failure
			}
			return canonical(ctx, event, flow)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	logs := &runtimeLogPersistenceCapture{}
	logger := NewRuntimeLogger(logs, executionposture.Live, canonical)
	store := &recordingInboundStore{}
	gateway := newTestInboundGateway(t, bus, logger, nil, store)
	cleanup := errors.New("independent cleanup/store failure")
	for _, cell := range []struct {
		name, path string
		wrap       func(error) error
		status     int
	}{
		{"ordinary", "$.data.user_id", nil, 400},
		{"wrapped", "$.data.user_id", func(e error) error { return fmt.Errorf("outer: %w", e) }, 400},
		{"validation_join", "$.data.user_id", func(e error) error { return errors.Join(e, fmt.Errorf("nested: %w", e)) }, 400},
		{"cleanup_last", "$.data.user_id", func(e error) error { return errors.Join(e, cleanup) }, 503},
		{"cleanup_first", "$.data.user_id", func(e error) error { return errors.Join(cleanup, e) }, 503},
		{"nested_cleanup", "$.data.user_id", func(e error) error { return errors.Join(fmt.Errorf("wrapped: %w", errors.Join(e, cleanup)), e) }, 503},
		{"control_key", "$.data.user\n\r\t\x00_id", nil, 400},
		{"quoted_key", "$.data.\"injected\\key\"", nil, 400},
		{"unicode_key", "$.data.\u2028user", nil, 400},
		{"long_key", "$.data." + strings.Repeat("private-key", 100), nil, 400},
		{"bare_sentinel", "", func(error) error { return runtimebus.ErrPayloadValidation }, 503},
		{"unresolved_schema", "", func(error) error { return errors.New("unresolved schema type") }, 503},
	} {
		t.Run(cell.name, func(t *testing.T) {
			failure = &eventschema.Violation{Path: cell.path, Constraint: "required", Actual: "private body", Detail: "INTERNAL secret-body-value"}
			if cell.wrap != nil {
				failure = cell.wrap(failure)
			}
			beforeLogs := len(logs.records)
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"`+cell.name+`","type":"push"}`))
			gateway.Handler().ServeHTTP(response, request)
			body := response.Body.String()
			if response.Code != cell.status {
				t.Fatalf("status=%d want=%d body=%q", response.Code, cell.status, body)
			}
			if cell.status == 400 {
				if !strings.HasPrefix(body, "invalid webhook payload at ") || len(body) > 165 || strings.Count(body, "\n") != 1 || strings.ContainsAny(body, "\r\t\x00") {
					t.Fatalf("unsafe/unbounded validation response: %q", body)
				}
				if cell.path == "$.data.user_id" && !strings.Contains(body, cell.path) {
					t.Fatalf("ordinary exact path was changed: %q", body)
				}
			} else if body != "publish inbound failed\n" {
				t.Fatalf("internal error was disclosed: %q", body)
			}
			for _, private := range []string{"secret-body-value", "INTERNAL", "private body", "bus:", "independent cleanup"} {
				if strings.Contains(body, private) {
					t.Errorf("response exposed %q: %q", private, body)
				}
			}
			if store.recorded {
				t.Fatal("failed preparation reached the publication commit")
			}
			if len(logs.records) != beforeLogs+1 {
				t.Fatalf("internal diagnostics missing: before=%d after=%d", beforeLogs, len(logs.records))
			}
			if strings.Contains(cell.name, "cleanup") && !strings.Contains(string(logs.records[beforeLogs].Payload), cleanup.Error()) {
				t.Fatalf("independent failure lost from diagnostics: %s", logs.records[beforeLogs].Payload)
			}
		})
	}
	generated := fmt.Errorf("%w: %w", runtimebus.ErrPayloadValidation, &eventschema.Violation{Path: "$.generated", Detail: "internal generated payload"})
	for _, cell := range []struct {
		name   string
		err    error
		status int
	}{
		{"generated_payload", generated, 503},
		{"provider", providertriggers.Error{Status: 400, Message: "invalid provider request"}, 400},
		{"provider_cleanup", errors.Join(providertriggers.Error{Status: 400, Message: "invalid provider request"}, cleanup), 503},
		{"cleanup_provider", errors.Join(cleanup, providertriggers.Error{Status: 400, Message: "invalid provider request"}), 503},
		{"lookalike_text", errors.New("schema validation failed: $.data.user_id is required"), 503},
	} {
		t.Run(cell.name, func(t *testing.T) {
			status, message := inboundPublicationErrorResponse(cell.err)
			if status != cell.status || status == 503 && message != "publish inbound failed" {
				t.Fatalf("internal/provider response=%d %q", status, message)
			}
		})
	}
}
