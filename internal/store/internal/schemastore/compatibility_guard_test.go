package schemastore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSelectedStoreLegacySchemaInterpretersAreAbsent(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
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
		"internal/store/internal/schemastore/postgres_bootstrap.go": {},
		"internal/store/internal/schemastore/sqlite_bootstrap.go":   {},
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

func TestNormalizeConstraintTreatsPostgresBooleanDeparseAsEquivalent(t *testing.T) {
	authored := `CHECK (
		task_type <> 'workflow_timer' OR (
			(source_timer_id IS NULL AND forked_from_run_id IS NULL) OR
			(source_timer_id IS NOT NULL AND forked_from_run_id IS NOT NULL)
		)
	)`
	deparsed := `CHECK (
		task_type <> 'workflow_timer' OR
		source_timer_id IS NULL AND forked_from_run_id IS NULL OR
		source_timer_id IS NOT NULL AND forked_from_run_id IS NOT NULL
	)`
	if got, want := normalizeConstraint(deparsed), normalizeConstraint(authored); got != want {
		t.Fatalf("normalized deparsed constraint = %q, want %q", got, want)
	}

	authoredPayload := `CHECK (
		task_type <> 'workflow_timer' OR
		(SUBSTR(CAST(fire_payload AS TEXT), 1, 1) = '{' AND SUBSTR(CAST(fire_payload AS TEXT), LENGTH(CAST(fire_payload AS TEXT)), 1) = '}')
	)`
	deparsedPayload := `CHECK (
		task_type <> 'workflow_timer' OR
		(SUBSTR(fire_payload, 1, 1) = '{' AND SUBSTR(fire_payload, LENGTH(fire_payload), 1) = '}')
	)`
	if got, want := normalizeConstraint(deparsedPayload), normalizeConstraint(authoredPayload); got != want {
		t.Fatalf("normalized deparsed payload constraint = %q, want %q", got, want)
	}

	authoredAtomicOperands := `CHECK (
		COALESCE(cache_read_input_tokens, 0) + COALESCE(cache_creation_input_tokens, 0) <= input_tokens AND
		provider_reported_cost_usd >= 0
	)`
	deparsedAtomicOperands := `CHECK (
		COALESCE(cache_read_input_tokens, (0)) + COALESCE(cache_creation_input_tokens, (0)) <= input_tokens AND
		provider_reported_cost_usd >= (0)
	)`
	if got, want := normalizeConstraint(deparsedAtomicOperands), normalizeConstraint(authoredAtomicOperands); got != want {
		t.Fatalf("normalized deparsed atomic operands = %q, want %q", got, want)
	}

	authoredStatus := `CHECK (
		task_type <> 'workflow_timer' OR (
			(recurring = FALSE AND (
				(status IN ('active', 'cancelled') AND fired_at IS NULL) OR
				(status = 'fired' AND fired_at IS NOT NULL AND fired_at >= fire_at)
			)) OR
			(recurring = TRUE AND status IN ('active', 'cancelled') AND (fired_at IS NULL OR fired_at >= created_at))
		)
	)`
	deparsedStatus := `CHECK (((task_type <> 'workflow_timer'::text) OR (((recurring = false) AND (((status = ANY (ARRAY['active'::text, 'cancelled'::text])) AND (fired_at IS NULL)) OR ((status = 'fired'::text) AND (fired_at IS NOT NULL) AND (fired_at >= fire_at)))) OR ((recurring = true) AND (status = ANY (ARRAY['active'::text, 'cancelled'::text])) AND ((fired_at IS NULL) OR (fired_at >= created_at))))))`
	if got, want := normalizeConstraint(deparsedStatus), normalizeConstraint(authoredStatus); got != want {
		t.Fatalf("normalized deparsed status constraint = %q, want %q", got, want)
	}
}

func TestNormalizeConstraintTreatsPostgresReferenceSpacingAsEquivalent(t *testing.T) {
	authored := `FOREIGN KEY (agent_id, flow_instance) REFERENCES agents(agent_id, flow_instance)`
	deparsed := `FOREIGN KEY (agent_id, flow_instance) REFERENCES agents (agent_id, flow_instance)`
	if got, want := normalizeConstraint(deparsed), normalizeConstraint(authored); got != want {
		t.Fatalf("normalized deparsed constraint = %q, want %q", got, want)
	}
}
