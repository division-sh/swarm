package runtimepersistence

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func forkContentionFailureDiagnostics(before, after map[string][]string, winner forkContentionResult, loser <-chan forkContentionResult) []string {
	lines := []string{fmt.Sprintf("fork_contention_cut winner_error=%v winner_activation=%+v loser_result_available=%t", winner.err, winner.activation, len(loser) > 0)}
	tables := map[string]bool{}
	for _, snapshot := range []map[string][]string{before, after} {
		for key := range snapshot {
			tables[strings.TrimSuffix(key, "/columns")] = true
		}
	}
	names := make([]string, 0, len(tables))
	for table := range tables {
		names = append(names, table)
	}
	sort.Strings(names)
	for _, table := range names {
		old, oldPresent := before[table]
		current, currentPresent := after[table]
		oldColumns, currentColumns := before[table+"/columns"], after[table+"/columns"]
		if oldPresent == currentPresent && reflect.DeepEqual(old, current) && reflect.DeepEqual(oldColumns, currentColumns) {
			continue
		}
		lines = append(lines, fmt.Sprintf("table=%q before_present=%t after_present=%t before_rows=%d after_rows=%d before_nil=%t after_nil=%t before_columns=%q after_columns=%q", table, oldPresent, currentPresent, len(old), len(current), old == nil, current == nil, oldColumns, currentColumns))
		for _, side := range []struct {
			name                 string
			rows, other, columns []string
		}{{"before", old, current, oldColumns}, {"after", current, old, currentColumns}} {
			remaining := map[string]int{}
			for _, row := range side.other {
				remaining[row]++
			}
			rows := append([]string(nil), side.rows...)
			sort.Strings(rows)
			changed, sampled := 0, 0
			for _, row := range rows {
				if remaining[row] > 0 {
					remaining[row]--
					continue
				}
				changed++
				// Count every occurrence, but never dump arbitrary application rows.
				if sampled == 8 {
					continue
				}
				sampled++
				var values []json.RawMessage
				if err := json.Unmarshal([]byte(row), &values); err != nil || len(values) != len(side.columns) {
					lines = append(lines, fmt.Sprintf("table=%q side=%s row_fp=%x invalid_snapshot_row=true", table, side.name, sha256.Sum256([]byte(row))))
					continue
				}
				cells := make([]string, len(values))
				for index, value := range values {
					cells[index] = fmt.Sprintf("%q:%x", side.columns[index], sha256.Sum256(value))
				}
				lines = append(lines, fmt.Sprintf("table=%q side=%s row_fp=%x cells=[%s]", table, side.name, sha256.Sum256([]byte(row)), strings.Join(cells, ",")))
			}
			lines = append(lines, fmt.Sprintf("table=%q side=%s changed_rows=%d sampled_rows=%d", table, side.name, changed, sampled))
		}
	}
	return lines
}

func TestSelectedForkContentionFailureDiagnostics(t *testing.T) {
	const payload = "private-bundled-source-not-for-logs"
	oldRow, newRow := `["run","paused","`+payload+`"]`, `["run","running","`+payload+`"]`
	before := map[string][]string{"runs/columns": {"run_id", "status", "payload"}, "runs": {oldRow}, "same/columns": {"id"}, "same": {`[1]`}}
	after := map[string][]string{"runs/columns": {"run_id", "status", "payload"}, "runs": {newRow}, "same/columns": {"id"}, "same": {`[1]`}}
	loser := make(chan forkContentionResult, 1)
	loser <- forkContentionResult{err: context.DeadlineExceeded}
	winner := forkContentionResult{err: context.Canceled, activation: runfork.RunForkActivation{Activated: true, SourceFrozen: true}}
	lines := forkContentionFailureDiagnostics(before, after, winner, loser)
	text := strings.Join(lines, "\n")
	for _, want := range []string{"winner_error=context canceled", "Activated:true", "SourceFrozen:true", "loser_result_available=true", `table="runs"`, fmt.Sprintf(`"status":%x`, sha256.Sum256([]byte(`"paused"`))), fmt.Sprintf(`"status":%x`, sha256.Sum256([]byte(`"running"`))), "side=before changed_rows=1 sampled_rows=1", "side=after changed_rows=1 sampled_rows=1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing diagnostic %q: %s", want, text)
		}
	}
	if strings.Contains(text, payload) || strings.Contains(text, `table="same"`) || len(loser) != 1 || (<-loser).err != context.DeadlineExceeded {
		t.Fatal("diagnostic leaked payload, logged unchanged rows or consumed the losing result")
	}
	if before["runs"][0] != oldRow || after["runs"][0] != newRow {
		t.Fatal("diagnostic mutated the captured evidence")
	}

	rows := make([]string, 10)
	for index := range rows {
		rows[index] = `["run","paused","` + payload + `"]`
	}
	bounded := strings.Join(forkContentionFailureDiagnostics(map[string][]string{"runs": rows, "runs/columns": before["runs/columns"]}, map[string][]string{"runs": {oldRow}, "runs/columns": before["runs/columns"]}, winner, nil), "\n")
	if strings.Count(bounded, "side=before row_fp=") != 8 || !strings.Contains(bounded, "side=before changed_rows=9 sampled_rows=8") || strings.Contains(bounded, payload) {
		t.Fatalf("duplicate counts, bounds or payload redaction changed: %s", bounded)
	}

	invalid := strings.Join(forkContentionFailureDiagnostics(map[string][]string{"bad": {payload}, "bad/columns": {"value"}}, nil, winner, nil), "\n")
	if !strings.Contains(invalid, "invalid_snapshot_row=true") || strings.Contains(invalid, payload) {
		t.Fatal("malformed diagnostic aborted or exposed raw data")
	}
	unchanged := forkContentionFailureDiagnostics(before, before, winner, nil)
	if len(unchanged) != 1 || !strings.Contains(unchanged[0], "loser_result_available=false") {
		t.Fatalf("unchanged snapshots acquired a table delta: %v", unchanged)
	}
	schema := strings.Join(forkContentionFailureDiagnostics(map[string][]string{"runs": {`[1]`}, "runs/columns": {"old"}}, map[string][]string{"runs": {`[1]`}, "runs/columns": {"new"}}, winner, nil), "\n")
	if !strings.Contains(schema, `before_columns=["old"] after_columns=["new"]`) {
		t.Fatal("column-only change was hidden")
	}
	ordered := []string{`[2]`, `[1]`}
	added := strings.Join(forkContentionFailureDiagnostics(nil, map[string][]string{"added": ordered, "added/columns": {"id"}}, winner, nil), "\n")
	if !strings.Contains(added, "before_present=false after_present=true") || !reflect.DeepEqual(ordered, []string{`[2]`, `[1]`}) {
		t.Fatal("table presence was hidden or row ordering was mutated")
	}
}
