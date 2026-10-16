package schemastore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSchemaCatalogGuardClassifiesOnlyExactReadOnlyFaultInspection(t *testing.T) {
	rel := "internal/store/internal/schemastore/test_fault_object_observation.go"
	source, err := os.ReadFile(filepath.Join(repoRootForRuntimeWriterGuard(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, source string
		wantViolation      bool
	}{
		{"exact_five", rel, string(source), false},
		{"foreign_prefix", rel, strings.Replace(string(source), "publication_completion_", "foreign_schema_probe_", 1), true},
		{"foreign_fixed_name", rel, strings.Replace(string(source), "fail_delivery_dead_letter_insert", "foreign_schema_probe", 1), true},
		{"extra_query", rel, strings.Replace(string(source), "var count int", "_, _ = tx.ExecContext(ctx, `DROP TABLE fault_target`)\nvar count int", 1), true},
		{"other_declaration", rel, string(source) + "\nconst anotherQuery = `SELECT * FROM sqlite_master`\n", true},
		{"other_file", "internal/other.go", string(source), true},
		{"retired_symbol", rel, string(source) + "\nconst ResolveSchemaCapabilities = 1\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, tc.path)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(tc.source), 0o644); err != nil {
				t.Fatal(err)
			}
			violations, err := legacySchemaInterpreterViolations(root)
			if err != nil || (len(violations) != 0) != tc.wantViolation {
				t.Fatalf("exact read-only classification changed: %v %v", violations, err)
			}
		})
	}
}
