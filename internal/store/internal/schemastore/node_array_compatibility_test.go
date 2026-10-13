package schemastore

import "testing"

func TestNodeStateTimestampArrayInspectionUsesExactAlias(t *testing.T) {
	for _, pair := range [][2]string{
		{"timestamp with time zone", "timestamptz"},
		{"timestamp with time zone[]", "timestamptz[]"},
	} {
		if got, want := normalizeSchemaType(pair[0]), normalizeSchemaType(pair[1]); got != want {
			t.Fatalf("exact PostgreSQL type alias differs: %q != %q", got, want)
		}
	}
	for _, pair := range [][2]string{
		{"timestamp with time zone", "timestamptz[]"},
		{"timestamp without time zone[]", "timestamptz[]"},
		{"timestamp[]", "timestamptz[]"},
		{"timestamp with time zone[][]", "timestamptz[][]"},
		{"timestamptz[][]", "timestamptz[]"},
		{"timestamp with time zone[2]", "timestamptz[]"},
		{"timestamp with time zone []", "timestamptz[]"},
		{"text[]", "timestamptz[]"},
		{"uuid[]", "timestamptz[]"},
	} {
		got, want := normalizeSchemaType(pair[0]), normalizeSchemaType(pair[1])
		if got == want {
			t.Fatalf("unrelated or unsupported type evidence was equated: %q and %q", pair[0], pair[1])
		}
		expected := schemaShape{Tables: map[string]schemaTableShape{"state": {
			Columns: map[string]schemaColumnShape{"values": {Type: want}},
		}}}
		actual := schemaShape{Tables: map[string]schemaTableShape{"state": {
			Columns: map[string]schemaColumnShape{"values": {Type: got}},
		}}}
		if drift := compareSchemaShapes(expected, actual); len(drift) == 0 {
			t.Fatalf("type mismatch escaped the unchanged schema comparator: %q and %q", pair[0], pair[1])
		}
	}
}
