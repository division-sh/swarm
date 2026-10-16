package effects

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
)

func TestProviderRequestJournalBindsPhysicalRequest(t *testing.T) {
	payload := []byte(`{"model":"frozen","input":[{"count":1.0}]}`)
	input := providerRequestJournalTestInput()
	input.Lineage = map[string]string{"provider_call": "original-coordinate"}
	journal, err := NewProviderRequestJournal(input, payload)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeProviderRequestJournal(journal)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeProviderRequestJournal(encoded)
	if err != nil || decoded.ValidateRequest(payload, Fingerprint(payload)) != nil {
		t.Fatalf("journal readback failed: %v", err)
	}
	if !bytes.Equal(decoded.Rendering, input.Rendering) {
		t.Fatal("journal changed frozen rendering bytes")
	}
	input.SemanticHeaders["content-type"] = "changed"
	input.Rendering[0] = '['
	input.Memory.Enabled = true
	input.Lineage["provider_call"] = "changed"
	if decoded.SemanticHeaders["content-type"] != "application/json" || journal.Rendering[0] != '{' || journal.Memory.Enabled ||
		journal.Lineage["provider_call"] != "original-coordinate" || decoded.Lineage["provider_call"] != "original-coordinate" {
		t.Fatal("journal retained caller-owned mutable inputs")
	}
	for _, scenario := range []string{"body", "operation", "destination", "method", "version_header", "body_pin", "request_pin", "missing_rendering", "unknown_version"} {
		t.Run(scenario, func(t *testing.T) {
			changed, err := DecodeProviderRequestJournal(encoded)
			if err != nil {
				t.Fatal(err)
			}
			body, operation := payload, Fingerprint(payload)
			switch scenario {
			case "body":
				body = []byte(`{"model":"changed"}`)
			case "operation":
				operation = Fingerprint([]byte("foreign operation"))
			case "destination":
				changed.Destination = "https://different.example/v1/messages"
			case "method":
				changed.Method = http.MethodGet
			case "version_header":
				changed.SemanticHeaders["anthropic-version"] = "changed"
			case "body_pin":
				changed.BodyFingerprint = operation
				body = []byte(`{ "model":"frozen","input":[{"count":1.0}]}`)
			case "request_pin":
				changed.RequestFingerprint = Fingerprint([]byte("foreign request"))
			case "missing_rendering":
				changed.Rendering = nil
			case "unknown_version":
				changed.Version = "unknown"
			}
			if changed.ValidateRequest(body, operation) == nil {
				t.Fatal("changed physical request was accepted")
			}
		})
	}
}

func TestProviderRequestJournalRefusesCredentialCarriersAndMalformedEvidence(t *testing.T) {
	for _, scenario := range []string{"userinfo", "query", "fragment", "non_http", "relative", "authorization", "cookie", "api_key", "unknown_header", "missing_version_header", "foreign_adapter", "caller_hash", "duplicate_rendering_key", "missing_session", "missing_frame", "missing_surface", "missing_memory", "missing_logical_operation", "zero_turn"} {
		t.Run(scenario, func(t *testing.T) {
			input := providerRequestJournalTestInput()
			switch scenario {
			case "userinfo":
				input.Destination = "https://user:private-credential@example.test/messages"
			case "query":
				input.Destination += "?api_key=private-credential"
			case "fragment":
				input.Destination += "#private-credential"
			case "non_http":
				input.Destination = "file:///tmp/provider"
			case "relative":
				input.Destination = "/messages"
			case "authorization", "cookie", "api_key":
				input.SemanticHeaders[scenario] = "private-credential"
			case "unknown_header":
				input.SemanticHeaders["x-unknown"] = "private-credential"
			case "missing_version_header":
				delete(input.SemanticHeaders, "anthropic-version")
			case "foreign_adapter":
				input.Adapter = "authored_http_tool"
			case "caller_hash":
				input.RequestFingerprint = Fingerprint([]byte("caller supplied"))
			case "duplicate_rendering_key":
				input.Rendering = []byte(`{"a":1,"a":2}`)
			case "missing_session":
				input.SessionID = ""
			case "missing_frame":
				input.FrameID = ""
			case "missing_surface":
				input.SurfaceID = ""
			case "missing_memory":
				input.Memory = nil
			case "missing_logical_operation":
				input.LogicalOperationIdentity = ""
			case "zero_turn":
				input.TurnOrdinal = 0
			}
			_, err := NewProviderRequestJournal(input, []byte(`{"model":"frozen"}`))
			if err == nil || strings.Contains(err.Error(), "private-credential") {
				t.Fatalf("invalid journal refusal missing or disclosed credentials: %v", err)
			}
		})
	}
	journal, err := NewProviderRequestJournal(providerRequestJournalTestInput(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{nil, []byte(`null`), append(append([]byte(nil), raw...), raw...), append([]byte(`{"version":"duplicate",`), raw[1:]...), append([]byte(`{"extra":true,`), raw[1:]...)} {
		if _, err := DecodeProviderRequestJournal(malformed); err == nil {
			t.Fatal("malformed or unknown journal fields were accepted")
		}
	}
}

func providerRequestJournalTestInput() ProviderRequestJournal {
	return ProviderRequestJournal{
		Version: ProviderRequestJournalVersion, Adapter: "anthropic_api",
		FrameID: "agent-frame:fixed", FrameContentHash: "frame-content-hash",
		SurfaceID: "frozen-surface", SurfaceFingerprint: "frozen-plan",
		SessionID: "frozen-session", TurnOrdinal: 2, LogicalOperationIdentity: "exact-provider-call",
		Memory: &agentmemory.Plan{},
		Method: http.MethodPost, Destination: "https://provider.example/v1/messages",
		SemanticHeaders: map[string]string{"content-type": "application/json", "anthropic-version": "2023-06-01"},
		Rendering:       []byte(`{"ordered_input":[1.0,1e0,{"byte_exact":"retained"}]}`),
	}
}
