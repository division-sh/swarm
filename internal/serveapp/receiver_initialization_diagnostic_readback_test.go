package serveapp

import (
	"encoding/json"
	"testing"
)

func TestReleaseReceiverRejectionDiagnosticPhysicalReadback(t *testing.T) {
	columns := []string{"event_class", "event_name", "run_id", "entity_id", "flow_instance", "source_event_id", "payload"}
	diagnostic := func(level, component, action, event string) string {
		payload, err := json.Marshal(map[string]any{
			"log_level": level,
			"message":   "Runtime warning recorded by event-bus",
			"details": map[string]any{
				"component": component, "action": action, "event_type": event,
				"failure": map[string]any{"detail": map[string]any{"code": "unclassified_runtime_error"}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return string(payload)
	}
	valid := diagnostic("warn", "event-bus", "payload_validation_rejected", "work.requested")
	values := []any{"diagnostic_direct", "platform.runtime_log", nil, nil, nil, nil, valid}
	for _, payload := range []any{valid, []byte(valid)} {
		row := append([]any(nil), values...)
		row[6] = payload
		if err := validateReceiverIngressRejectionDiagnosticRow(columns, workspaceProofPhysicalRow(t, columns, row...), "work.requested"); err != nil {
			t.Fatalf("canonical typed diagnostic rejected (%T): %v", payload, err)
		}
	}
	for _, tc := range []struct {
		name  string
		index int
		value any
	}{
		{"business_class", 0, "root_ingress"},
		{"business_name", 1, "work.requested"},
		{"run_scope", 2, "run"},
		{"empty_run_is_not_null", 2, ""},
		{"entity_scope", 3, "entity"},
		{"instance_scope", 4, "account/instance"},
		{"parent_event", 5, "parent"},
		{"wrong_payload_type", 6, int64(7)},
		{"malformed_payload", 6, "{"},
		{"wrong_level", 6, diagnostic("info", "event-bus", "payload_validation_rejected", "work.requested")},
		{"wrong_component", 6, diagnostic("warn", "other", "payload_validation_rejected", "work.requested")},
		{"wrong_action", 6, diagnostic("warn", "event-bus", "other", "work.requested")},
		{"wrong_event", 6, diagnostic("warn", "event-bus", "payload_validation_rejected", "other")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := append([]any(nil), values...)
			row[tc.index] = tc.value
			if err := validateReceiverIngressRejectionDiagnosticRow(columns, workspaceProofPhysicalRow(t, columns, row...), "work.requested"); err == nil {
				t.Fatal("noncanonical diagnostic accepted")
			}
		})
	}
	t.Run("missing_scope_column", func(t *testing.T) {
		missingColumns := append(append([]string{}, columns[:5]...), columns[6:]...)
		missingValues := append(append([]any{}, values[:5]...), values[6:]...)
		if err := validateReceiverIngressRejectionDiagnosticRow(missingColumns, workspaceProofPhysicalRow(t, missingColumns, missingValues...), "work.requested"); err == nil {
			t.Fatal("incomplete diagnostic accepted")
		}
	})
	t.Run("plain_value_array", func(t *testing.T) {
		encoded, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateReceiverIngressRejectionDiagnosticRow(columns, string(encoded), "work.requested"); err == nil {
			t.Fatal("untyped physical evidence accepted")
		}
	})
}
