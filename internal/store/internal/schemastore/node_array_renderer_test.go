package schemastore

import (
	"strings"
	"testing"
)

func TestSQLiteNodeStateScalarArrayRendererCompleteDomain(t *testing.T) {
	for _, tc := range []struct{ authored, physical string }{
		{"text", "TEXT"}, {"string", "TEXT"},
		{"integer", "BIGINT"}, {"int", "BIGINT"}, {"bigint", "BIGINT"},
		{"float", "DOUBLE PRECISION"}, {"double", "DOUBLE PRECISION"}, {"real", "DOUBLE PRECISION"},
		{"numeric", "NUMERIC"}, {"numeric(5,2)", "NUMERIC(5,2)"},
		{"boolean", "BOOLEAN"}, {"bool", "BOOLEAN"},
		{"json", "JSONB"}, {"jsonb", "JSONB"},
		{"timestamp", "TIMESTAMPTZ"}, {"timestamptz", "TIMESTAMPTZ"}, {"uuid", "UUID"},
	} {
		t.Run(tc.authored, func(t *testing.T) {
			physical, err := NodeStateFieldTypeToDDL("[" + tc.authored + "]")
			if err != nil || physical != tc.physical+"[]" {
				t.Fatalf("canonical list changed physical element type: %q %v", physical, err)
			}
			for _, spelling := range []string{physical, strings.ToLower(physical)} {
				if got, err := sqliteRenderType(spelling); err != nil || got != "TEXT" {
					t.Fatalf("sqliteRenderType(%q) = %q, %v; want TEXT", spelling, got, err)
				}
			}
		})
	}
	for _, invalid := range []string{
		"[]", "TEXT[][]", "NUMERIC(5,2)[][]", "TEXT[ ]", "TEXT[2]", "TEXT[] extra",
		"MISSING[]", "BYTEA[]", "XID8[]", "BIGSERIAL[]", "DimensionScore[]",
		"NUMERIC(5,x)[]", "NUMERIC(5)[]", "NUMERIC(5,2,1)[]", "NUMERIC(5,2[]",
		"DOUBLE extra[]", "TEXT garbage[]", "TEXT [] []",
	} {
		t.Run(invalid, func(t *testing.T) {
			if got, err := sqliteRenderType(invalid); err == nil || got != "" {
				t.Fatalf("unsupported array acquired a SQLite type: %q -> %q, %v", invalid, got, err)
			}
		})
	}
	for _, tc := range []struct{ physical, want string }{
		{"TEXT", "TEXT"}, {"NUMERIC(5,2)", "NUMERIC(5,2)"},
		{"JSONB", "TEXT"}, {"DOUBLE PRECISION", "REAL"},
	} {
		if got, err := sqliteRenderType(tc.physical); err != nil || got != tc.want {
			t.Fatalf("scalar rendering changed: %q -> %q, %v", tc.physical, got, err)
		}
	}
	if physical, err := NodeStateFieldTypeToDDL("[DimensionScore]"); err != nil || physical != "JSONB" {
		t.Fatalf("named list lost JSONB storage: %q %v", physical, err)
	}
	if got, err := sqliteRenderDefault("TEXT[]", "'{}'"); err != nil || got != "'[]'" {
		t.Fatalf("existing empty text-array encoding changed: %q %v", got, err)
	}
}
