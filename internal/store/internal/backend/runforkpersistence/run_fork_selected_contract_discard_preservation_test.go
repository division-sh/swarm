package runforkpersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestDeleteSelectedContractForkStatePreservesCompletionTombstones(t *testing.T) {
	for _, tc := range []struct {
		name     string
		preserve bool
	}{
		{name: "retained completion", preserve: true},
		{name: "whole parent", preserve: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "discard.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			runID := uuid.NewString()
			for _, table := range []string{
				"agent_lifecycle_diagnostic_outbox", "fan_out_obligation_barriers", "fan_out_outcomes", "fan_out_intents",
				"dead_letters", "run_fork_delivery_event_replays", "event_delivery_handler_rule_selections",
				"event_delivery_outcomes", "event_delivery_attempts", "event_deliveries", "agent_sessions",
				"run_fork_selected_contract_branch_divergences", "run_fork_selected_contract_route_recoveries",
				"run_fork_selected_contract_executions", "event_receipts", "committed_replay_scopes", "timers",
				"activity_attempts", "entity_mutations", "agent_turns", "agent_conversation_audits",
				"author_activity_occurrences", "agents", "events",
			} {
				if _, err := db.Exec("CREATE TABLE " + table + " (run_id TEXT, fork_run_id TEXT, original_event_id TEXT, event_id TEXT, delivery_id TEXT)"); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("INSERT INTO "+table+" (run_id, fork_run_id) VALUES ($1, $1)", runID); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := deleteSelectedContractForkState(context.Background(), tx, runID, tc.preserve); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"agent_turns", "agent_conversation_audits", "author_activity_occurrences", "agents", "activity_attempts", "entity_mutations"} {
				var count int
				if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				want := 0
				if tc.preserve && table != "activity_attempts" && table != "entity_mutations" {
					want = 1
				}
				if count != want {
					t.Errorf("%s rows = %d, want %d", table, count, want)
				}
			}
		})
	}
}
