package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSelectedStoreLegacySchemaInterpretersAreAbsent(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	for _, name := range []string{
		"internal/store/agent_lifecycle_schema.go",
		"internal/store/failure_schema_migration.go",
		"internal/store/obsolete_schema_cutoff.go",
		"internal/store/schema_capabilities.go",
		"internal/store/schema_drift.go",
	} {
		if _, err := os.Stat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Errorf("retired schema owner still exists: %s", name)
		}
	}

	forbiddenSymbols := []string{
		"BindSchemaCapabilities",
		"CanonicalEventReceiptsCapability",
		"CanonicalRuntimeLogCapability",
		"EnsureEntitySchema",
		"EnsureSchemaTables",
		"EntitySchemaPersistence",
		"GenerateEntityTableDDLs",
		"OutdatedSchemaError",
		"ResolveSchemaCapabilities",
		"SchemaFlavor",
		"StoreSchemaCapabilities",
		"availableCurrentTransactionChangeQueries",
		"operatorConversationRunIDProjectionError",
		"schemaColumnCatalog",
		"schemaContainsRequirements",
		"stripDeprecatedEntitySubjectDDL",
	}
	allowedCatalogOwners := map[string]struct{}{
		"internal/store/postgres_schema_bootstrap.go": {},
		"internal/store/sqlite_schema_bootstrap.go":   {},
	}
	catalogEvidence := []string{
		"information_schema.columns",
		"pg_catalog.pg_class",
		"pragma index_info",
		"pragma index_list",
		"pragma table_info",
		"sqlite_master",
	}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(source)
		for _, symbol := range forbiddenSymbols {
			if strings.Contains(text, symbol) {
				t.Errorf("retired schema symbol %s remains in %s", symbol, relative)
			}
		}
		if _, allowed := allowedCatalogOwners[relative]; allowed {
			return nil
		}
		lower := strings.ToLower(text)
		for _, evidence := range catalogEvidence {
			if strings.Contains(lower, evidence) {
				t.Errorf("post-admission catalog interpreter %q remains in %s", evidence, relative)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
