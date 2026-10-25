package serveapp

import (
	"encoding/json"
	"fmt"
	"github.com/division-sh/swarm/internal/store/storetest"
	"testing"
)

func receiverIngressApplicationSnapshot(t *testing.T, reader receiverProofStateReader) map[string][]string {
	t.Helper()
	all, out := requireReceiverApplicationSnapshot(t, reader), map[string][]string{}
	for _, table := range []string{
		"runs", "events", "flow_instances", "flow_instance_runtime_readiness", "entity_state", "entity_mutations",
		"event_deliveries", "event_delivery_attempts", "event_receipts", "committed_replay_scopes", "api_idempotency",
		"author_activity_order", "author_activity_occurrences", "run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions",
	} {
		if _, exists := all[table]; !exists {
			t.Fatalf("missing native application table %s", table)
		}
		out[table] = all[table]
	}
	out["events/columns"] = all["events/columns"]
	return out
}

// Payload rejection appends one runtime diagnostic, not an admitted publication.
// Account for that exact append; retain every preexisting row and all other facts.
func requireReceiverIngressRejectionDiagnostic(t *testing.T, before, after map[string][]string, event string) {
	t.Helper()
	var columns []string
	if err := json.Unmarshal([]byte(before["events/columns"][0]), &columns); err != nil {
		t.Fatal(err)
	}
	previous := make(map[string]bool, len(before["events"]))
	for _, row := range before["events"] {
		previous[row] = true
	}
	retained := []string{}
	added := 0
	for _, row := range after["events"] {
		if previous[row] {
			retained = append(retained, row)
			continue
		}
		added++
		if err := validateReceiverIngressRejectionDiagnosticRow(columns, row, event); err != nil {
			t.Fatalf("invalid rejection diagnostic: %s err=%v", row, err)
		}
	}
	if added != 1 {
		t.Fatalf("rejection diagnostics=%d, want exactly one", added)
	}
	after["events"] = retained
}

func validateReceiverIngressRejectionDiagnosticRow(columns []string, encoded, event string) error {
	rows, err := storetest.DecodeSelectedForkSnapshotRows(storetest.SelectedForkStorageTableSnapshot{Columns: columns, Rows: []string{encoded}})
	if err != nil {
		return err
	}
	row := rows[0]
	for column, want := range map[string]string{"event_class": "diagnostic_direct", "event_name": "platform.runtime_log"} {
		value, err := workspaceProofPhysicalText(row, column)
		if err != nil || value != want {
			return fmt.Errorf("diagnostic %s=%q, want %q: %v", column, value, want, err)
		}
	}
	for _, column := range []string{"run_id", "entity_id", "flow_instance", "source_event_id"} {
		cell, found := row[column]
		if !found || cell.Type != "<nil>" || string(cell.Value) != "null" {
			return fmt.Errorf("rejection diagnostic requires physical NULL %s", column)
		}
	}
	raw, err := workspaceProofPhysicalText(row, "payload")
	if err != nil {
		return err
	}
	var payload struct {
		Level   string `json:"log_level"`
		Details struct {
			Action, Component string
			Event             string `json:"event_type"`
		}
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return err
	}
	if payload.Level != "warn" || payload.Details.Action != "payload_validation_rejected" || payload.Details.Component != "event-bus" || payload.Details.Event != event {
		return fmt.Errorf("unexpected rejection diagnostic: %s", raw)
	}
	return nil
}
