package agentcontrol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestNewDirectiveEventPayloadPreservesDirectiveMode(t *testing.T) {
	evt, err := NewDirectiveEvent(SendDirectiveRequest{
		AgentID:   "agent-1",
		Directive: "run corpus",
		Source:    DirectiveSourceV1RPC,
	}, RunTargetResolution{
		RunID: "00000000-0000-0000-0000-000000000701",
		Mode:  RunResolutionSpecified,
	}, "00000000-0000-0000-0000-000000000702", "00000000-0000-0000-0000-000000000703", time.Date(2026, 5, 14, 3, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewDirectiveEvent: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["mode"] != DirectiveEventMode {
		t.Fatalf("mode = %#v, want %q", payload["mode"], DirectiveEventMode)
	}
	if payload["directive_text"] != "run corpus" || payload["run_id"] != evt.RunID() {
		t.Fatalf("payload = %#v", payload)
	}
	if payload["operation_id"] != "00000000-0000-0000-0000-000000000702" {
		t.Fatalf("operation_id = %#v", payload["operation_id"])
	}
	if evt.AdmissionClass() != events.EventAdmissionDiagnosticDirect {
		t.Fatalf("admission class = %q, want %q", evt.AdmissionClass(), events.EventAdmissionDiagnosticDirect)
	}
	if _, ok := payload["kill_previous"]; ok {
		t.Fatalf("payload = %#v, want no kill_previous field", payload)
	}
	validateCurrentPlatformEventPayloadForAgentControlTest(t, string(evt.Type()), evt.Payload())
}

func TestNewDirectiveEventEncodesExactRunResolutionAuthority(t *testing.T) {
	for _, test := range []struct {
		name            string
		resolution      string
		wantDisposition events.AdmittedRunDisposition
	}{
		{name: "specified", resolution: RunResolutionSpecified, wantDisposition: events.AdmittedRunRequireActive},
		{name: "active_session", resolution: RunResolutionActiveSession, wantDisposition: events.AdmittedRunRequireActive},
		{name: "new_run", resolution: RunResolutionNewRunAllocated, wantDisposition: events.AdmittedRunCreateAuthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			event, err := NewDirectiveEvent(
				SendDirectiveRequest{AgentID: "agent-1", Directive: "continue", Source: DirectiveSourceV1RPC},
				RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000711", Mode: test.resolution},
				"00000000-0000-0000-0000-000000000712",
				"00000000-0000-0000-0000-000000000713",
				time.Date(2026, 7, 19, 19, 45, 0, 0, time.UTC),
			)
			if err != nil {
				t.Fatalf("NewDirectiveEvent: %v", err)
			}
			admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatalf("AdmitForPersistence: %v", err)
			}
			if admitted.RunDisposition() != test.wantDisposition {
				t.Fatalf("run disposition = %q, want %q", admitted.RunDisposition(), test.wantDisposition)
			}
		})
	}

	if _, err := NewDirectiveEvent(
		SendDirectiveRequest{AgentID: "agent-1", Directive: "continue", Source: DirectiveSourceV1RPC},
		RunTargetResolution{RunID: "00000000-0000-0000-0000-000000000721", Mode: "unknown"},
		"00000000-0000-0000-0000-000000000722",
		"00000000-0000-0000-0000-000000000723",
		time.Date(2026, 7, 19, 19, 45, 0, 0, time.UTC),
	); err == nil {
		t.Fatal("NewDirectiveEvent unknown resolution error = nil")
	}
}

func validateCurrentPlatformEventPayloadForAgentControlTest(t testing.TB, eventType string, payload []byte) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root with go.mod not found")
		}
		dir = parent
	}
	source, err := yamlsource.LoadFile(runtimecontracts.DefaultPlatformSpecFile(dir))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
		t.Fatalf("unmarshal platform spec: %v", err)
	}
	registry := runtimecontracts.EventSchemaRegistryFromBundle(&runtimecontracts.WorkflowContractBundle{Platform: spec})
	schema, ok := registry[eventType]
	if !ok {
		t.Fatalf("missing generated platform schema for %s", eventType)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal %s payload: %v", eventType, err)
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema.Schema, decoded); err != nil {
		t.Fatalf("generated %s schema rejected producer payload %#v: %v", eventType, decoded, err)
	}
}
