package runtimepersistence

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSelectedForkSnapshotEncodingRetainsPhysicalColumnValueTypes(t *testing.T) {
	var encoded []string
	for _, value := range []any{[]byte("a"), "YQ==", "a", nil, []byte(nil), int64(1), float64(1), true} {
		row, err := encodeSelectedForkSnapshotValues([]string{"value"}, []any{value})
		if err != nil {
			t.Fatal(err)
		}
		for _, prior := range encoded {
			if row == prior {
				t.Fatalf("physical types conflated for %T: %s", value, row)
			}
		}
		encoded = append(encoded, row)
	}
	columns := []string{"nullable", "bytes", "text"}
	values := []any{nil, []byte("a"), "YQ=="}
	row, err := encodeSelectedForkSnapshotValues(columns, values)
	if err != nil {
		t.Fatal(err)
	}
	var witness []struct {
		Column, Type string
		Value        any
	}
	if err := json.Unmarshal([]byte(row), &witness); err != nil || len(witness) != 3 {
		t.Fatalf("physical witness: %s err=%v", row, err)
	}
	for i, want := range []string{"<nil>", "[]uint8", "string"} {
		if witness[i].Column != columns[i] || witness[i].Type != want {
			t.Fatalf("column/type lost: %+v", witness)
		}
	}
	if values[1].([]byte)[0] != 'a' || values[2] != "YQ==" || !strings.Contains(row, `"Type":"[]uint8","Value":"YQ=="`) {
		t.Fatal("encoding changed its physical input or byte representation")
	}
}

func TestSelectedForkSnapshotEncodingNormalizesOnlyTimestampLocation(t *testing.T) {
	at := time.Date(2026, 10, 7, 12, 0, 0, 123456000, time.FixedZone("fixture", 2*60*60))
	first, err := encodeSelectedForkSnapshotValues([]string{"created_at"}, []any{at})
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeSelectedForkSnapshotValues([]string{"created_at"}, []any{at.UTC()})
	if err != nil || first != second || !strings.Contains(first, `"Type":"time.Time"`) {
		t.Fatalf("same physical instant/location normalization differs: %s / %s err=%v", first, second, err)
	}
	changed, err := encodeSelectedForkSnapshotValues([]string{"created_at"}, []any{at.Add(time.Nanosecond)})
	if err != nil || changed == first {
		t.Fatalf("different physical instant conflated: %s err=%v", changed, err)
	}
	text, err := encodeSelectedForkSnapshotValues([]string{"created_at"}, []any{at.UTC().Format(time.RFC3339Nano)})
	if err != nil || text == first {
		t.Fatal("timestamp and text conflated")
	}
}

func TestSelectedForkSnapshotEncodingRefusesIncompleteOrUnencodableRows(t *testing.T) {
	for _, probe := range []struct {
		columns []string
		values  []any
	}{
		{nil, nil}, {[]string{"one"}, nil}, {nil, []any{1}},
		{[]string{"one"}, []any{make(chan int)}},
	} {
		if row, err := encodeSelectedForkSnapshotValues(probe.columns, probe.values); err == nil || row != "" {
			t.Fatalf("incomplete physical witness escaped: %q err=%v", row, err)
		}
	}
}

func TestSelectedForkSnapshotDecodingKeepsPhysicalTypesAndExactText(t *testing.T) {
	columns := []string{"null", "bytes", "text", "integer", "decimal", "bool", "at"}
	values := []any{nil, []byte(`{"name":"original"}`), "YQ==", int64(1), float64(1), true, time.Now().UTC()}
	encoded, err := encodeSelectedForkSnapshotValues(columns, values)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := DecodeSelectedForkSnapshotRowsForTest(SelectedForkStorageTableSnapshot{Columns: columns, Rows: []string{encoded}})
	if err != nil || len(rows) != 1 {
		t.Fatalf("decode original physical row: %v/%v", rows, err)
	}
	for index, typ := range []string{"<nil>", "[]uint8", "string", "int64", "float64", "bool", "time.Time"} {
		cell := rows[0][columns[index]]
		if cell.Column != columns[index] || cell.Type != typ {
			t.Fatalf("physical column/type changed: %+v", cell)
		}
	}
	for column, want := range map[string]string{"null": "", "bytes": `{"name":"original"}`, "text": "YQ=="} {
		got, err := rows[0][column].Text()
		if err != nil || got != want {
			t.Fatalf("physical text %s=%q, want %q: %v", column, got, want, err)
		}
	}
	if _, err := rows[0]["integer"].Text(); err == nil {
		t.Fatal("integer evidence was interpreted as text")
	}
}

func TestSelectedForkSnapshotDecodingRefusesMalformedRowsWithoutPartialEvidence(t *testing.T) {
	valid, err := encodeSelectedForkSnapshotValues([]string{"value"}, []any{"original"})
	if err != nil {
		t.Fatal(err)
	}
	for name, encoded := range map[string]string{
		"plain_values":     `["original"]`,
		"missing_cell":     `[]`,
		"missing_type":     `[{"Column":"value","Value":"original"}]`,
		"missing_value":    `[{"Column":"value","Type":"string"}]`,
		"foreign_column":   strings.Replace(valid, `"Column":"value"`, `"Column":"foreign"`, 1),
		"unknown_field":    strings.Replace(valid, `"Column":`, `"Unknown":true,"Column":`, 1),
		"unsupported_type": strings.Replace(valid, `"Type":"string"`, `"Type":"custom"`, 1),
		"wrong_value_type": strings.Replace(valid, `"Type":"string"`, `"Type":"int64"`, 1),
		"untyped_null":     strings.Replace(valid, `"Value":"original"`, `"Value":null`, 1),
		"false_null":       strings.Replace(valid, `"Type":"string"`, `"Type":"<nil>"`, 1),
		"malformed_bytes":  `[{"Column":"value","Type":"[]uint8","Value":"!"}]`,
		"trailing_row":     valid + valid,
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := DecodeSelectedForkSnapshotRowsForTest(SelectedForkStorageTableSnapshot{Columns: []string{"value"}, Rows: []string{valid, encoded}})
			if err == nil || rows != nil {
				t.Fatalf("malformed row exposed partial evidence: %+v/%v", rows, err)
			}
		})
	}
	for _, columns := range [][]string{nil, {""}, {"value", "value"}} {
		if rows, err := DecodeSelectedForkSnapshotRowsForTest(SelectedForkStorageTableSnapshot{Columns: columns}); err == nil || rows != nil {
			t.Fatalf("incomplete column inventory accepted: %+v/%v", rows, err)
		}
	}
}
