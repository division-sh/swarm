package pipelinepersistence

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the actual fresh-schema CHECK's SQL NULL truth table, not a copied
// domain predicate. Complete inherited lineage has its separate native CHECK.
func TestWorkflowTimerCancellationDDLRejectsNullPairBypasses(t *testing.T) {
	spec, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var check string
	for _, line := range strings.Split(string(spec), "\n") {
		if strings.Contains(line, "CHECK (task_type <> 'workflow_timer' OR ((status IN ('active', 'fired') AND cancel_cause") {
			if check != "" {
				t.Fatal("multiple workflow cancellation interpretations in fresh DDL")
			}
			check = strings.TrimSuffix(strings.TrimSpace(line), ",")
		}
	}
	if check == "" {
		t.Fatal("fresh timer cancellation CHECK is missing")
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE timer_cancel_check (
		task_type TEXT NOT NULL, status TEXT NOT NULL, cancel_cause TEXT,
		cancelled_at INTEGER, created_at INTEGER NOT NULL, fired_at INTEGER,
		source_timer_id TEXT, %s)`, check)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		status      string
		cause       any
		cancelledAt any
		firedAt     any
		sourceID    any
		valid       bool
	}{
		{"active", "active", nil, nil, nil, nil, true},
		{"ordinary_cancelled", "cancelled", nil, nil, nil, nil, true},
		{"rule_removed", "cancelled", "rule_removed", 100, nil, "source", true},
		{"missing_time", "cancelled", "rule_removed", nil, nil, "source", false},
		{"missing_cause", "cancelled", nil, 100, nil, "source", false},
		{"unknown_cause", "cancelled", "unknown", 100, nil, "source", false},
		{"empty_cause", "cancelled", "", nil, nil, "source", false},
		{"before_birth", "cancelled", "rule_removed", 99, nil, "source", false},
		{"before_fire", "cancelled", "rule_removed", 100, 101, "source", false},
		{"no_source_obligation", "cancelled", "rule_removed", 100, nil, nil, false},
		{"active_with_removal", "active", "rule_removed", 100, nil, "source", false},
		{"fired_with_removal", "fired", "rule_removed", 100, 100, "source", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := db.Exec(`INSERT INTO timer_cancel_check (task_type, status, cancel_cause, cancelled_at, created_at, fired_at, source_timer_id) VALUES ('workflow_timer', ?, ?, ?, 100, ?, ?)`, test.status, test.cause, test.cancelledAt, test.firedAt, test.sourceID)
			if (err == nil) != test.valid {
				t.Fatalf("DDL valid=%v want=%v: %v", err == nil, test.valid, err)
			}
		})
	}
}
