package selected

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestProductionSelectedStoreBoundaryIsClosed(t *testing.T) {
	root := selectedStoreRepoRoot(t)
	var failures []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		failures = append(failures, selectedStoreBoundaryViolations(filepath.ToSlash(rel), string(body))...)
		return nil
	})
	if err != nil {
		t.Fatalf("scan production selected-store boundary: %v", err)
	}
	sort.Strings(failures)
	if len(failures) != 0 {
		t.Fatalf("production selected-store boundary violations:\n%s", strings.Join(failures, "\n"))
	}
}

func TestProductionSelectedStoreBoundaryGuardRejectsOldInterpreters(t *testing.T) {
	fixtures := map[string]string{
		"internal/runtime/direct_constructor.go": `package runtime
import "github.com/division-sh/swarm/internal/store/construction"
var _ = construction.OpenPostgres
`,
		"internal/serveapp/concrete_probe.go": `package serveapp
import "github.com/division-sh/swarm/internal/store"
func probe(value *store.PostgresStore) bool { return value != nil }
`,
		"internal/serveapp/reflection_ledger.go": `package serveapp
import "reflect"
func roles(value any) int { return reflect.TypeOf(value).NumField() }
`,
		"internal/serveapp/legacy_bundle.go": `package serveapp
type storeBundle struct { SQLDB any; Postgres any }
`,
		"internal/store/selected/generic_writer.go": `package selected
func (o *Owner) BundleWriter() any { return nil }
`,
	}
	for path, body := range fixtures {
		if failures := selectedStoreBoundaryViolations(path, body); len(failures) == 0 {
			t.Errorf("fixture %s unexpectedly passed the selected-store boundary guard", path)
		}
	}
}

func selectedStoreBoundaryViolations(path, body string) []string {
	allowedSelectedImports := map[string]bool{
		"internal/serveapp/main.go":               true,
		"internal/serveapp/store_capabilities.go": true,
		"internal/serveapp/store_runtime.go":      true,
		"internal/cliapp/store_authority.go":      true,
	}
	insideSelected := strings.HasPrefix(path, "internal/store/selected/")
	insideStore := strings.HasPrefix(path, "internal/store/")
	insideTestSupport := strings.HasPrefix(path, "internal/testutil/") || strings.HasPrefix(path, "internal/testpostgres/")

	var tokens []string
	if strings.HasPrefix(path, "internal/serveapp/") {
		tokens = append(tokens,
			"storeBundle", "selectedConcreteRuntimeStore", "selectedRuntimeStoreFacade",
			"selectedStoreBundleRoleLedger", "validateSelectedStoreBundleRoles",
			"APIOptionalCapabilityBuilder", "configuredRunFork", "stores.SQLDB", "stores.Postgres",
		)
	}
	if strings.HasPrefix(path, "internal/serveapp/") && path != "internal/serveapp/main.go" {
		tokens = append(tokens, `"database/sql"`)
	}
	if (strings.HasPrefix(path, "internal/serveapp/") || insideSelected) && strings.Contains(body, `"reflect"`) {
		tokens = append(tokens, `"reflect"`)
	}
	if strings.HasPrefix(path, "internal/serveapp/") || insideSelected {
		tokens = append(tokens, "BundleWriter")
	}
	if !insideStore && !insideTestSupport {
		tokens = append(tokens, "store.PostgresStore", "store.SQLiteRuntimeStore")
	}

	var failures []string
	lines := strings.Split(body, "\n")
	for lineIndex, line := range lines {
		for _, token := range tokens {
			if strings.Contains(line, token) {
				failures = append(failures, selectedStoreBoundaryFailure(path, lineIndex, token))
			}
		}
		if strings.Contains(line, `"github.com/division-sh/swarm/internal/store/construction"`) && !insideSelected && path != "internal/store/store.go" {
			failures = append(failures, selectedStoreBoundaryFailure(path, lineIndex, "direct construction import"))
		}
		if strings.Contains(line, `"github.com/division-sh/swarm/internal/store/selected"`) && !allowedSelectedImports[path] {
			failures = append(failures, selectedStoreBoundaryFailure(path, lineIndex, "unapproved selected owner consumer"))
		}
	}
	return failures
}

func selectedStoreBoundaryFailure(path string, lineIndex int, token string) string {
	return path + ":" + strconv.Itoa(lineIndex+1) + " contains " + strconv.Quote(token)
}

func selectedStoreRepoRoot(t testing.TB) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get selected-store package directory: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
