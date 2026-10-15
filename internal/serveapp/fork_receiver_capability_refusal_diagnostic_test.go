package serveapp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func forkReceiverCapabilityRefusalSelectedSnapshotDelta(before, after map[string]storetest.SelectedForkStorageTableSnapshot) string {
	old, err := capabilitySelectedSnapshotDiagnosticRows(before)
	if err != nil {
		return fmt.Sprintf("before physical snapshot decode: %v", err)
	}
	current, err := capabilitySelectedSnapshotDiagnosticRows(after)
	if err != nil {
		return fmt.Sprintf("after physical snapshot decode: %v", err)
	}
	return forkReceiverCapabilityRefusalSnapshotDelta(old, current)
}

func capabilitySelectedSnapshotDiagnosticRows(snapshot map[string]storetest.SelectedForkStorageTableSnapshot) (map[string][]string, error) {
	if snapshot == nil {
		return nil, nil
	}
	result := make(map[string][]string, 2*len(snapshot))
	for table, physical := range snapshot {
		columns, err := json.Marshal(physical.Columns)
		if err != nil {
			return nil, err
		}
		result[table+"/columns"] = []string{string(columns)}
		result[table] = nil
		if physical.Rows != nil {
			result[table] = []string{}
		}
		rows, err := storetest.DecodeSelectedForkSnapshotRows(physical)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			encoded, err := capabilitySelectedDiagnosticRow(physical.Columns, row)
			if err != nil {
				return nil, err
			}
			result[table] = append(result[table], encoded)
		}
	}
	return result, nil
}

func capabilitySelectedDiagnosticRow(columns []string, row map[string]storetest.SelectedForkSnapshotCell) (string, error) {
	values := make([]json.RawMessage, len(columns))
	for index, column := range columns {
		cell := row[column]
		values[index] = cell.Value
		if cell.Type == "[]uint8" {
			text, err := cell.Text()
			if err != nil {
				return "", err
			}
			values[index], err = json.Marshal(text)
			if err != nil {
				return "", err
			}
		}
	}
	encoded, err := json.Marshal(values)
	return string(encoded), err
}

func TestForkReceiverCapabilityRefusalSelectedDiagnosticKeepsExactTransport(t *testing.T) {
	columns := []string{"payload", "revision", "timestamp"}
	before, err := storetest.EncodeSelectedForkSnapshotValues(columns, []any{[]byte("old"), int64(9007199254740993), "2026-10-14T12:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	after, err := storetest.EncodeSelectedForkSnapshotValues(columns, []any{[]byte("new"), int64(9007199254740994), "2026-10-14T14:00:00+02:00"})
	if err != nil {
		t.Fatal(err)
	}
	old := map[string]storetest.SelectedForkStorageTableSnapshot{"rows": {Columns: columns, Rows: []string{before}}}
	current := map[string]storetest.SelectedForkStorageTableSnapshot{"rows": {Columns: columns, Rows: []string{after}}}
	got := forkReceiverCapabilityRefusalSelectedSnapshotDelta(old, current)
	legacyBefore := map[string][]string{"rows/columns": {`["payload","revision","timestamp"]`}, "rows": {`["old",9007199254740993,"2026-10-14T12:00:00Z"]`}}
	legacyAfter := map[string][]string{"rows/columns": legacyBefore["rows/columns"], "rows": {`["new",9007199254740994,"2026-10-14T14:00:00+02:00"]`}}
	if want := forkReceiverCapabilityRefusalSnapshotDelta(legacyBefore, legacyAfter); got != want {
		t.Fatalf("typed snapshot changed raw bytes/number/time/column diagnostics:\ngot%s\nwant%s", got, want)
	}
	if same := forkReceiverCapabilityRefusalSelectedSnapshotDelta(old, old); same != "" {
		t.Fatal("unchanged typed snapshot gained a delta:", same)
	}
}

// Diagnose the captured cut only. Rows remain exact strings and multiset counts;
// removed/added pairs are not guessed to be updates of a shared primary key.
func forkReceiverCapabilityRefusalSnapshotDelta(before, after map[string][]string) string {
	if reflect.DeepEqual(before, after) {
		return ""
	}
	var out strings.Builder
	fmt.Fprintf(&out, "snapshot before_nil=%t after_nil=%t\n", before == nil, after == nil)
	tables := map[string]bool{}
	for key := range before {
		tables[strings.TrimSuffix(key, "/columns")] = true
	}
	for key := range after {
		tables[strings.TrimSuffix(key, "/columns")] = true
	}
	var names []string
	for table := range tables {
		names = append(names, table)
	}
	sort.Strings(names)
	for _, table := range names {
		old, oldPresent := before[table]
		current, currentPresent := after[table]
		oldColumns, oldSchemaPresent := before[table+"/columns"]
		currentColumns, currentSchemaPresent := after[table+"/columns"]
		sameSchema := oldSchemaPresent == currentSchemaPresent && reflect.DeepEqual(oldColumns, currentColumns)
		if oldPresent == currentPresent && reflect.DeepEqual(old, current) && sameSchema {
			continue
		}
		fmt.Fprintf(&out, "table=%q before_present=%t after_present=%t before_rows=%d after_rows=%d\n", table, oldPresent, currentPresent, len(old), len(current))
		fmt.Fprintf(&out, "  schema before_present=%t after_present=%t before=%s after=%s\n", oldSchemaPresent, currentSchemaPresent, capabilitySnapshotJSON(oldColumns), capabilitySnapshotJSON(currentColumns))
		fmt.Fprintf(&out, "  rows before=%s after=%s\n", capabilitySnapshotJSON(old), capabilitySnapshotJSON(current))
		oldCounts, currentCounts := capabilitySnapshotRowCounts(old), capabilitySnapshotRowCounts(current)
		rows := map[string]bool{}
		for row := range oldCounts {
			rows[row] = true
		}
		for row := range currentCounts {
			rows[row] = true
		}
		var ordered []string
		for row := range rows {
			ordered = append(ordered, row)
		}
		sort.Strings(ordered)
		for _, row := range ordered {
			if removed := oldCounts[row] - currentCounts[row]; removed > 0 {
				fmt.Fprintf(&out, "  removed count=%d row=%s named=%s\n", removed, row, capabilitySnapshotNamedRow(oldColumns, row))
			}
			if added := currentCounts[row] - oldCounts[row]; added > 0 {
				fmt.Fprintf(&out, "  added count=%d row=%s named=%s\n", added, row, capabilitySnapshotNamedRow(currentColumns, row))
			}
			if !sameSchema && oldCounts[row] == currentCounts[row] && oldCounts[row] != 0 {
				fmt.Fprintf(&out, "  retained count=%d row=%s before_named=%s after_named=%s\n", oldCounts[row], row, capabilitySnapshotNamedRow(oldColumns, row), capabilitySnapshotNamedRow(currentColumns, row))
			}
		}
	}
	return out.String()
}

func capabilitySnapshotJSON(rows []string) string {
	raw, _ := json.Marshal(rows)
	return string(raw)
}

func capabilitySnapshotRowCounts(rows []string) map[string]int {
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[row]++
	}
	return counts
}

func capabilitySnapshotNamedRow(metadata []string, row string) string {
	var columns []string
	if len(metadata) != 1 {
		return fmt.Sprintf("unavailable: column metadata entries=%d", len(metadata))
	}
	if err := json.Unmarshal([]byte(metadata[0]), &columns); err != nil {
		return fmt.Sprintf("unavailable: column metadata decode: %v", err)
	}
	var values []json.RawMessage
	if err := json.Unmarshal([]byte(row), &values); err != nil {
		return fmt.Sprintf("unavailable: row decode: %v", err)
	}
	var out strings.Builder
	for index := 0; index < max(len(columns), len(values)); index++ {
		if index != 0 {
			out.WriteString(", ")
		}
		if index < len(columns) {
			fmt.Fprintf(&out, "column[%d]=%q", index, columns[index])
		} else {
			fmt.Fprintf(&out, "column[%d]=<missing-name>", index)
		}
		if index < len(values) {
			fmt.Fprintf(&out, " value=%s", values[index])
		} else {
			out.WriteString(" value=<missing-value>")
		}
	}
	return out.String()
}

func TestForkReceiverCapabilityRefusalSnapshotDiagnostic(t *testing.T) {
	base := map[string][]string{"runs/columns": {`["run_id","status","revision"]`}, "runs": {`["r","running",9007199254740993]`}}
	tests := []struct {
		name          string
		before, after map[string][]string
		want          []string
	}{
		{"unchanged", base, base, nil},
		{"row_added", map[string][]string{"runs/columns": base["runs/columns"], "runs": {}}, base, []string{`added count=1 row=["r","running",9007199254740993]`, `column[2]="revision" value=9007199254740993`}},
		{"row_removed", base, map[string][]string{"runs/columns": base["runs/columns"], "runs": {}}, []string{`removed count=1 row=["r","running",9007199254740993]`, `column[1]="status" value="running"`}},
		{"row_changed", base, map[string][]string{"runs/columns": base["runs/columns"], "runs": {`["r","completed",9007199254740994]`}}, []string{`removed count=1`, `added count=1`, `column[1]="status" value="completed"`, `column[2]="revision" value=9007199254740994`}},
		{"duplicate_removed", map[string][]string{"runs/columns": base["runs/columns"], "runs": {base["runs"][0], base["runs"][0], base["runs"][0]}}, base, []string{`removed count=2`}},
		{"duplicate_added", base, map[string][]string{"runs/columns": base["runs/columns"], "runs": {base["runs"][0], base["runs"][0], base["runs"][0]}}, []string{`added count=2`}},
		{"schema_changed", base, map[string][]string{"runs/columns": {`["run_id","state","revision"]`}, "runs": base["runs"]}, []string{`retained count=1`, `column[1]="status" value="running"`, `column[1]="state" value="running"`}},
		{"table_added", map[string][]string{}, base, []string{`table="runs" before_present=false after_present=true`, `schema before_present=false after_present=true`, `added count=1`}},
		{"table_removed", base, map[string][]string{}, []string{`table="runs" before_present=true after_present=false`, `schema before_present=true after_present=false`, `removed count=1`}},
		{"empty_table_added", map[string][]string{}, map[string][]string{"runs": {}}, []string{`table="runs" before_present=false after_present=true`, `rows before=null after=[]`}},
		{"nil_map_is_not_empty", nil, map[string][]string{}, []string{`snapshot before_nil=true after_nil=false`}},
		{"nil_rows_are_not_empty", map[string][]string{"runs": nil}, map[string][]string{"runs": {}}, []string{`rows before=null after=[]`}},
		{"row_order_only", map[string][]string{"runs": {`[1]`, `[2]`}}, map[string][]string{"runs": {`[2]`, `[1]`}}, []string{`rows before=["[1]","[2]"] after=["[2]","[1]"]`}},
		{"column_order", base, map[string][]string{"runs/columns": {`["status","run_id","revision"]`}, "runs": base["runs"]}, []string{`before_named=column[0]="run_id" value="r"`, `after_named=column[0]="status" value="r"`}},
		{"malformed_row", base, map[string][]string{"runs/columns": base["runs/columns"], "runs": {`{`}}, []string{`added count=1 row={`, `unavailable: row decode:`}},
		{"malformed_metadata", base, map[string][]string{"runs/columns": {`{`}, "runs": base["runs"]}, []string{`retained count=1`, `unavailable: column metadata decode:`}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			beforeCopy, afterCopy := cloneCapabilitySnapshot(tc.before), cloneCapabilitySnapshot(tc.after)
			got := forkReceiverCapabilityRefusalSnapshotDelta(tc.before, tc.after)
			if len(tc.want) == 0 && got != "" {
				t.Fatalf("unchanged snapshot reported %s", got)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("diagnostic missing %q:\n%s", want, got)
				}
			}
			if !reflect.DeepEqual(beforeCopy, tc.before) || !reflect.DeepEqual(afterCopy, tc.after) {
				t.Fatal("reporter mutated captured snapshots")
			}
		})
	}
}

func TestForkReceiverCapabilityRefusalSnapshotDiagnosticDeterministic(t *testing.T) {
	before := map[string][]string{"z/columns": {`["timestamp"]`}, "z": {`["2026-10-14T12:00:00Z"]`}, "a/columns": {`["value"]`}, "a": {`[1]`}}
	after := map[string][]string{"a": {`[1.0]`}, "a/columns": before["a/columns"], "z": {`["2026-10-14T14:00:00+02:00"]`}, "z/columns": before["z/columns"]}
	var reversedBefore, reversedAfter = map[string][]string{}, map[string][]string{}
	for _, key := range []string{"z/columns", "z", "a/columns", "a"} {
		reversedBefore[key], reversedAfter[key] = before[key], after[key]
	}
	got := forkReceiverCapabilityRefusalSnapshotDelta(before, after)
	if got != forkReceiverCapabilityRefusalSnapshotDelta(reversedBefore, reversedAfter) {
		t.Fatal("map iteration changed diagnostics")
	}
	if strings.Index(got, `table="a"`) >= strings.Index(got, `table="z"`) {
		t.Fatalf("tables not sorted:\n%s", got)
	}
	for _, exact := range []string{`column[0]="value" value=1.0`, `column[0]="timestamp" value="2026-10-14T12:00:00Z"`, `column[0]="timestamp" value="2026-10-14T14:00:00+02:00"`} {
		if !strings.Contains(got, exact) {
			t.Fatalf("raw number/time changed: missing %s", exact)
		}
	}
}

func TestForkReceiverCapabilityRefusalSnapshotDiagnosticIncompleteColumns(t *testing.T) {
	for _, tc := range []struct {
		columns []string
		row     string
		want    string
	}{
		{nil, `[1]`, `column metadata entries=0`},
		{[]string{`["one"]`, `["two"]`}, `[1]`, `column metadata entries=2`},
		{[]string{`["one","two"]`}, `[1]`, `column[1]="two" value=<missing-value>`},
		{[]string{`["one"]`}, `[1,2]`, `column[1]=<missing-name> value=2`},
		{[]string{`["same","same"]`}, `[1,2]`, `column[1]="same" value=2`},
	} {
		if got := capabilitySnapshotNamedRow(tc.columns, tc.row); !strings.Contains(got, tc.want) {
			t.Fatalf("got=%s want=%s", got, tc.want)
		}
	}
}

func cloneCapabilitySnapshot(snapshot map[string][]string) map[string][]string {
	if snapshot == nil {
		return nil
	}
	copy := make(map[string][]string, len(snapshot))
	for key, values := range snapshot {
		if values == nil {
			copy[key] = nil
			continue
		}
		copy[key] = append([]string{}, values...)
	}
	return copy
}
