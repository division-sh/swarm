package schemastore

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
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
	violations, err := legacySchemaInterpreterViolations(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}

func legacySchemaInterpreterViolations(root string) ([]string, error) {
	var violations []string
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
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, walkErr error) error {
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
				violations = append(violations, fmt.Sprintf("retired schema symbol %s remains in %s", symbol, relative))
			}
		}
		if _, allowed := allowedCatalogOwners[relative]; allowed {
			return nil
		}
		if relative == "internal/store/internal/schemastore/test_fault_object_observation.go" {
			text, err = withoutExactTestFaultInspection(source)
			if err != nil {
				return err
			}
		}
		lower := strings.ToLower(text)
		for _, evidence := range catalogEvidence {
			if strings.Contains(lower, evidence) {
				violations = append(violations, fmt.Sprintf("post-admission catalog interpreter %q remains in %s", evidence, relative))
			}
		}
		return nil
	})
	return violations, err
}

func withoutExactTestFaultInspection(source []byte) (string, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "test_fault_object_observation.go", source, parser.AllErrors)
	if err != nil {
		return "", err
	}
	declarations := map[string]string{
		"CountPublicationCompletionFaultObjectsTx":      "3f2e8d43c38c823843f6373ba35fe10b66b314fb3055dc12be9a77a5a9eb95a5",
		"CountPublicationMaterializationFaultObjectsTx": "e9d475d738a55dfef728c524bef5801e674abbc485cf482e97cd17037e762dfa",
		"CountPublicationReplayFaultObjectsTx":          "e0cb62cd5acf3cefd723db2d2e40b46bcba29def3f52da9234963a4a71b10eeb",
		"ReadConformanceDiagnosticFaultObjectsTx":       "5c2c955cf6dfdd4db9cb384896ed4c48f0c5025c57f2a8e1461812190011c1af",
		"CountNodeRecoveryPoisonFaultObjectsTx":         "6b4c7edc252094446d5b10a98712d20a22c1af822c7bb931f23a0b4f9a0d80a8",
	}
	remaining := append([]byte(nil), source...)
	for _, node := range file.Decls {
		fn, ok := node.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || declarations[fn.Name.Name] == "" {
			continue
		}
		var formatted bytes.Buffer
		if err := format.Node(&formatted, token.NewFileSet(), fn); err != nil {
			return "", err
		}
		// #2698/6205327685 classifies precisely this read-only five-fault
		// operation, never another query, caller name or schema interpreter.
		if fmt.Sprintf("%x", sha256.Sum256(formatted.Bytes())) != declarations[fn.Name.Name] {
			continue
		}
		for offset := set.Position(fn.Pos()).Offset; offset < set.Position(fn.End()).Offset; offset++ {
			remaining[offset] = ' '
		}
	}
	return string(remaining), nil
}

func TestLegacySchemaGuardIgnoresNestedCheckoutAndRejectsCurrentSource(t *testing.T) {
	root := t.TempDir()
	foreign := filepath.Join(root, "internal", "review-nested")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(foreign, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := []byte("package probe\nconst query = `information_schema.columns`\n")
	if err := os.WriteFile(filepath.Join(foreign, "stale.go"), stale, 0o644); err != nil {
		t.Fatal(err)
	}
	check := func(want bool) {
		t.Helper()
		violations, err := legacySchemaInterpreterViolations(root)
		if err != nil || (len(violations) > 0) != want {
			t.Fatalf("schema guard = %v, %v; want violation %v", violations, err, want)
		}
	}
	check(false)
	current := filepath.Join(root, "internal", "current.go")
	if err := os.WriteFile(current, stale, 0o644); err != nil {
		t.Fatal(err)
	}
	check(true)
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
