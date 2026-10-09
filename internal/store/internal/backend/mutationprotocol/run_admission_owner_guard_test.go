package mutationprotocol

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func admissionFunctionName(fn *ast.FuncDecl) string {
	name := fn.Name.Name
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return name
	}
	receiver := fn.Recv.List[0].Type
	if ptr, ok := receiver.(*ast.StarExpr); ok {
		receiver = ptr.X
	}
	if ident, ok := receiver.(*ast.Ident); ok {
		return ident.Name + "." + name
	}
	return name
}

func admissionOwnerCalls(path string, file *ast.File) map[string]int {
	aliases := map[string]bool{}
	for _, spec := range file.Imports {
		value, _ := strconv.Unquote(spec.Path.Value)
		if value != "github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol" {
			continue
		}
		alias := "mutationprotocol"
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		if alias == "." {
			return map[string]int{path + ":unqualified-admission-import": 1}
		}
		aliases[alias] = true
	}
	calls := map[string]int{}
	for _, decl := range file.Decls {
		name := "package"
		fn, ok := decl.(*ast.FuncDecl)
		if ok {
			name = admissionFunctionName(fn)
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if !ok || !aliases[receiver.Name] {
				return true
			}
			switch selector.Sel.Name {
			case "CacheActiveRunSource", "CachedActiveRunSource", "InvalidateActiveRunSource":
				calls[path+":"+name+":"+selector.Sel.Name]++
			}
			return true
		})
	}
	return calls
}

func TestActiveRunAdmissionHasClosedOwners(t *testing.T) {
	const mutationPath = "internal/store/internal/backend/runlifecycle/run_lifecycle_mutation.go"
	const statePath = "internal/store/internal/backend/runlifecycle/run_lifecycle_state.go"
	const leafPath = "internal/store/internal/backend/runlifecycle/sourceadmission/source.go"
	expected := map[string]int{}
	for _, method := range []string{"CacheActiveRunSource", "CachedActiveRunSource"} {
		expected[leafPath+":loadTx:"+method] = 1
	}
	expected[leafPath+":requireActiveNonlockingTx:CachedActiveRunSource"] = 1
	for _, dialect := range []string{"postgres", "sqlite"} {
		for _, method := range []string{"Create", "TransitionActive", "ReviseSource"} {
			expected[mutationPath+":"+dialect+"RunLifecycleMutation."+method+":InvalidateActiveRunSource"] = 1
		}
	}
	for _, backend := range []string{"Postgres", "SQLite"} {
		expected[mutationPath+":RunLifecycle"+backend+"Owner.InsertRunForkRunTx:InvalidateActiveRunSource"] = 1
		expected[statePath+":RunLifecycle"+backend+"Owner.markRunTerminalStateTx:InvalidateActiveRunSource"] = 1
	}
	expected[mutationPath+":deleteMaterializedForkRunTx:InvalidateActiveRunSource"] = 1
	expected["internal/store/internal/backend/runlifecycle/sourceadmission/test_fixture.go:FixtureSourceMutation.Invalidate:InvalidateActiveRunSource"] = 1
	root, err := filepath.Abs("../../../../..")
	if err != nil {
		t.Fatal(err)
	}
	err = checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for key, count := range admissionOwnerCalls(filepath.ToSlash(relative), file) {
			if expected[key] != count {
				return fmt.Errorf("unowned or duplicated run admission: %s (%d)", key, count)
			}
			delete(expected, key)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) != 0 {
		t.Fatalf("missing canonical admission/invalidation consumers: %v", expected)
	}
}

func TestRunAdmissionOwnerGuardFindsNewMintAndAliasedConsumer(t *testing.T) {
	const source = `package other
import admission "github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
func Mutable() { admission.CacheActiveRunSource(nil, nil, "foreign", fact) }
func ReadOnly() { admission.CachedActiveRunSource(nil, nil, "foreign") }
var escaped = admission.CacheActiveRunSource
`
	file, err := parser.ParseFile(token.NewFileSet(), "other.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	if calls := admissionOwnerCalls("other.go", file); calls["other.go:Mutable:CacheActiveRunSource"] != 1 || calls["other.go:ReadOnly:CachedActiveRunSource"] != 1 || calls["other.go:package:CacheActiveRunSource"] != 1 {
		t.Fatalf("aliased admission consumers escaped: %v", calls)
	}
	file, err = parser.ParseFile(token.NewFileSet(), "other.go", strings.ReplaceAll(source, `admission "github.com/`, `. "github.com/`), 0)
	if err != nil {
		t.Fatal(err)
	}
	if calls := admissionOwnerCalls("other.go", file); calls["other.go:unqualified-admission-import"] != 1 {
		t.Fatalf("unqualified admission import escaped: %v", calls)
	}
}

func runAdmissionDelegates(file *ast.File, owners map[string]string) error {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		name := admissionFunctionName(fn)
		owner, tracked := owners[name]
		if !tracked {
			continue
		}
		calls, raw := 0, false
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "sourceadmission" && selector.Sel.Name == owner {
				calls++
			}
			raw = raw || selector.Sel.Name == "QueryRowContext" || selector.Sel.Name == "Scan"
			return true
		})
		if calls != 1 || raw {
			return fmt.Errorf("%s must delegate once to %s without interpreting run rows", name, owner)
		}
		delete(owners, name)
	}
	if len(owners) != 0 {
		return fmt.Errorf("missing source-admission delegates: %v", owners)
	}
	return nil
}

func TestRunAdmissionLifecycleAndNestedGuardsHaveOneInterpreter(t *testing.T) {
	for path, owners := range map[string]map[string]string{
		"runlifecycle/run_lifecycle_mutation.go": {
			"postgresRunLifecycleMutation.loadSource":            "LoadPostgresTx",
			"sqliteRunLifecycleMutation.loadSource":              "LoadSQLiteTx",
			"postgresRunLifecycleMutation.requireSourceArtifact": "RequireArtifact",
			"sqliteRunLifecycleMutation.requireSourceArtifact":   "RequireArtifact",
		},
		"runstate/active_guard.go": {
			"RequirePostgresActiveTx": "LoadPostgresTx", "RequireSQLiteActiveTx": "LoadSQLiteTx",
			"RequirePostgresActiveQuery": "RequirePostgresActiveQuery", "RequireSQLiteActiveQuery": "RequireSQLiteActiveQuery",
			"RequirePostgresActiveNonlockingTx": "RequirePostgresActiveNonlockingTx",
			"RequireSQLiteActiveNonlockingTx":   "RequireSQLiteActiveNonlockingTx",
		},
	} {
		raw, err := os.ReadFile(filepath.Join("..", path))
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := runAdmissionDelegates(file, owners); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunAdmissionDelegateGuardRejectsCompetingInterpreter(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "guard.go", `package guard
func RequireActive(ctx, tx, runID any) {
 sourceadmission.LoadPostgresTx(ctx, tx, runID, true, false)
 tx.QueryRowContext(ctx, "SELECT status, bundle_hash FROM runs").Scan(&state, &source)
}`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAdmissionDelegates(file, map[string]string{"RequireActive": "LoadPostgresTx"}); err == nil {
		t.Fatal("competing interpreter next to canonical wrapper escaped guard")
	}
}
