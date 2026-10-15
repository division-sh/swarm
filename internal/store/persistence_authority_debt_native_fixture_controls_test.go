package store_test

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Excluded from the collector payload to avoid a self-referential fingerprint.
const debtNativeFixtureCollectorTo = "0bd1ab5a0b812a4ec0868c453f4a7ac572726261f9f1b738e72d49badc6c08af"

func TestPersistenceAuthorityDebtNativeFixtureExactClassificationDelta(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	fset := token.NewFileSet()
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Env: debtCensusEnvironment(), Fset: fset, Tests: true,
		BuildFlags: []string{"-trimpath"},
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, "./internal/runtime/pipeline", "./internal/runtime/runforkexecution")
	if err != nil || packages.PrintErrors(pkgs) != 0 {
		t.Fatalf("load native fixture evidence: %v", err)
	}
	seen := map[string]bool{}
	imports, constructions := 0, 0
	for _, pkg := range pkgs {
		for i, file := range pkg.Syntax {
			rel, err := filepath.Rel(root, pkg.CompiledGoFiles[i])
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.ToSlash(rel)
			reopen := debtReviewedNativeReopenDeclaration(path) != ""
			selected := debtReviewedNativeSelectedImport(path, "storeselected", file)
			if seen[path] || !reopen && !selected {
				continue
			}
			seen[path] = true
			foreign := filepath.ToSlash(filepath.Join(filepath.Dir(path), "unreviewed", filepath.Base(path)))
			var old, current []authorityFinding
			want := 1
			if selected {
				old, current = debtSelectedBoundarySource(foreign, file), debtSelectedBoundarySource(path, file)
				imports++
			} else {
				scan := debtNewAuthorityTypeScan()
				old = scan.debtCollectFixtureStoreConstruction(foreign, file, pkg.TypesInfo)
				current = scan.debtCollectFixtureStoreConstruction(path, file, pkg.TypesInfo)
				want = 2
				if calls := debtReviewedNativeReopenCalls(path, file, pkg.TypesInfo); len(calls) != want {
					t.Fatalf("%s: approved %d closure calls, want %d", path, len(calls), want)
				}
				constructions += want
			}
			for i := range old {
				old[i].File = path
			}
			before, after := authorityDebtSites(old), authorityDebtSites(current)
			removed := 0
			for key, original := range before {
				now := after[key]
				if now.Multiplicity > original.Multiplicity {
					t.Fatalf("%s: classification added debt: %+v", path, now)
				}
				delta := original.Multiplicity - now.Multiplicity
				if delta != 0 {
					if selected && (original.Kind != "selected-boundary" || !strings.HasPrefix(original.Operation, "import:")) ||
						reopen && (original.Kind != "selected-store-construction" || original.Declaration != debtReviewedNativeReopenDeclaration(path)) {
						t.Fatalf("unreviewed classification changed: %+v", original)
					}
					removed += delta
				}
			}
			for key := range after {
				if _, ok := before[key]; !ok {
					t.Fatalf("%s: unrelated identity changed: %+v", path, after[key])
				}
			}
			if removed != want {
				t.Fatalf("%s: removed %d occurrences, want %d", path, removed, want)
			}
		}
	}
	if len(seen) != 8 || imports != 4 || constructions != 8 {
		t.Fatalf("exact delta: files=%d imports=%d constructions=%d", len(seen), imports, constructions)
	}
	t.Log("same-source delta: exactly four imports and eight native factory-closure calls; all other findings retained")
}

func TestPersistenceAuthorityDebtNativeFixtureImportsRejectUnreviewedUses(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	for _, path := range []string{
		"internal/runtime/pipeline/fork_fan_out_crash_external_test.go",
		"internal/runtime/pipeline/fork_fan_out_refork_external_test.go",
		"internal/runtime/pipeline/fork_published_barrier_crash_external_test.go",
		"internal/runtime/pipeline/fork_published_barrier_failure_external_test.go",
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		for _, mutation := range []string{"exact", "foreign-path", "foreign-function", "extra-use", "changed-use", "concrete-recovery"} {
			t.Run(filepath.Base(path)+"/"+mutation, func(t *testing.T) {
				candidate, source := path, string(data)
				file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
				if err != nil {
					t.Fatal(err)
				}
				var reviewed *ast.FuncDecl
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok {
						continue
					}
					ast.Inspect(fn, func(n ast.Node) bool {
						if selector, ok := n.(*ast.SelectorExpr); ok {
							if alias, ok := selector.X.(*ast.Ident); ok && alias.Name == "storeselected" {
								reviewed = fn
							}
						}
						return true
					})
				}
				if reviewed == nil {
					t.Fatal("missing selected evidence")
				}
				switch mutation {
				case "foreign-path":
					candidate = "internal/runtime/pipeline/unreviewed_test.go"
				case "foreign-function":
					reviewed.Name.Name += "Unreviewed"
				case "extra-use":
					extra, err := parser.ParseFile(token.NewFileSet(), "extra.go", "package pipeline_test; func unreviewed() { _ = storeselected.RecoverConcrete }", 0)
					if err != nil {
						t.Fatal(err)
					}
					file.Decls = append(file.Decls, extra.Decls...)
				case "changed-use":
					changed := false
					ast.Inspect(reviewed, func(node ast.Node) bool {
						if selector, ok := node.(*ast.SelectorExpr); ok && !changed {
							if alias, ok := selector.X.(*ast.Ident); ok && alias.Name == "storeselected" {
								selector.Sel.Name += "Unreviewed"
								changed = true
							}
						}
						return true
					})
					if !changed {
						t.Fatal("changed-use probe did not alter a reviewed selected use")
					}
				case "concrete-recovery":
					reviewed.Body.List = append(reviewed.Body.List, &ast.ExprStmt{X: &ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent("storeselected"), Sel: ast.NewIdent("RecoverConcrete")}}})
				}
				if got := debtReviewedNativeSelectedImport(candidate, "storeselected", file); got != (mutation == "exact") {
					t.Fatalf("classified=%v", got)
				}
				if mutation != "exact" {
					found := false
					for _, finding := range debtSelectedBoundarySource(candidate, file) {
						found = found || finding.Kind == "selected-boundary" && strings.HasPrefix(finding.Member, "import:")
					}
					if !found {
						t.Fatal("unreviewed selected import disappeared")
					}
				}
			})
		}
	}
}

type debtNativeFixtureControlImporter struct {
	packages map[string]*types.Package
	fallback types.Importer
}

func (i debtNativeFixtureControlImporter) Import(path string) (*types.Package, error) {
	if pkg := i.packages[path]; pkg != nil {
		return pkg, nil
	}
	return i.fallback.Import(path)
}

func debtNativeFixtureTypedControl(t *testing.T, path, body string) (*ast.File, *types.Info) {
	t.Helper()
	const storePath = "github.com/division-sh/swarm/internal/store"
	const factoryPath = storePath + "/storetest"
	imports := debtNativeFixtureControlImporter{map[string]*types.Package{}, importer.Default()}
	check := func(path, source string) (*ast.File, *types.Info, *types.Package) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path+".go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
		pkg, err := (&types.Config{Importer: imports}).Check(path, fset, []*ast.File{file}, info)
		if err != nil {
			t.Fatal(err)
		}
		return file, info, pkg
	}
	_, _, native := check(storePath, "package store; type SQLiteRuntimeStore struct{}; type PostgresStore struct{}")
	imports.packages[storePath] = native
	_, _, factory := check(factoryPath, `package storetest
import s "`+storePath+`"
func StartSQLiteRuntimeStoreWithReopen() (*s.SQLiteRuntimeStore, func() *s.SQLiteRuntimeStore) { panic(0) }
func StartPostgresRuntimeStoreWithReopen() (*s.PostgresStore, func() *s.PostgresStore) { panic(0) }
`)
	imports.packages[factoryPath] = factory
	_, _, foreign := check("example.org/storetest", `package foreign
import s "`+storePath+`"
func StartSQLiteRuntimeStoreWithReopen() (*s.SQLiteRuntimeStore, func() *s.SQLiteRuntimeStore) { panic(0) }
`)
	imports.packages["example.org/storetest"] = foreign
	file, info, _ := check("github.com/division-sh/swarm/internal/runtime/runforkexecution", body)
	return file, info
}

func TestPersistenceAuthorityDebtNativeFixtureClosureProvenance(t *testing.T) {
	const path = "internal/runtime/runforkexecution/one_shot_reply_execution_test.go"
	const source = `package runforkexecution
import (
 s "github.com/division-sh/swarm/internal/store"
 factory "github.com/division-sh/swarm/internal/store/storetest"
 "database/sql"
)
func raw(db *sql.DB) *s.SQLiteRuntimeStore { panic(0) }
func reverse(s *s.SQLiteRuntimeStore, f func() *s.SQLiteRuntimeStore) (func() *s.SQLiteRuntimeStore, *s.SQLiteRuntimeStore) { return f, s }
func TestIssue642SupportedOneShotReturnsOwnChildReplyBothStores() {
 { s, again := factory.StartSQLiteRuntimeStoreWithReopen(); _ = s; _ = func() any { s = again(); return s } }
 { s, again := factory.StartPostgresRuntimeStoreWithReopen(); _ = s; _ = func() any { return again() } }
 { again := 1; _ = again }
}
`
	for _, mutation := range []string{"exact", "renamed", "foreign-path", "foreign-function", "foreign-factory", "shadowed-factory", "wrong-slot", "reassigned", "escaped", "extra-call", "raw-reconstruction", "concrete-recovery"} {
		t.Run(mutation, func(t *testing.T) {
			candidate, body, want := path, source, 2
			switch mutation {
			case "renamed":
				body = strings.ReplaceAll(body, "again", "openPeer")
			case "foreign-path":
				candidate, want = "internal/runtime/runforkexecution/unreviewed_test.go", 0
			case "foreign-function":
				body = strings.ReplaceAll(body, "TestIssue642SupportedOneShotReturnsOwnChildReplyBothStores", "unreviewed")
				want = 0
			case "foreign-factory":
				body = strings.Replace(body, "\"database/sql\"", "\"database/sql\"; foreign \"example.org/storetest\"", 1)
				body = strings.Replace(body, "factory.StartSQLiteRuntimeStoreWithReopen()", "foreign.StartSQLiteRuntimeStoreWithReopen()", 1)
				want = 0
			case "shadowed-factory":
				body = strings.Replace(body, "{ s, again := factory.StartSQLiteRuntimeStoreWithReopen()", "{ factory := struct { StartSQLiteRuntimeStoreWithReopen func() (*s.SQLiteRuntimeStore, func() *s.SQLiteRuntimeStore) }{factory.StartSQLiteRuntimeStoreWithReopen}; s, again := factory.StartSQLiteRuntimeStoreWithReopen()", 1)
				want = 0
			case "wrong-slot":
				body = strings.Replace(body, "s, again := factory.StartSQLiteRuntimeStoreWithReopen()", "again, s := reverse(factory.StartSQLiteRuntimeStoreWithReopen())", 1)
				want = 0
			case "reassigned":
				body = strings.Replace(body, "_ = s; _ = func() any { s = again()", "_ = s; again = factoryClosure(); _ = func() any { s = again()", 1)
				body += "\nfunc factoryClosure() func() *s.SQLiteRuntimeStore { panic(0) }\n"
				want = 1
			case "escaped":
				body = strings.Replace(body, "_ = s; _ = func() any { s = again()", "_ = s; escaped := again; _ = escaped; _ = func() any { s = again()", 1)
				want = 1
			case "extra-call":
				body = strings.Replace(body, "_ = s; _ = func() any { s = again()", "_ = s; _ = again(); _ = func() any { s = again()", 1)
				want = 1
			case "raw-reconstruction":
				body = strings.Replace(body, "{ again := 1; _ = again }", "{ again := 1; _ = again }; var db *sql.DB; _ = raw(db)", 1)
			case "concrete-recovery":
				body = strings.Replace(body, "{ again := 1; _ = again }", "{ again := 1; _ = again }; _ = recoverConcrete(nil)", 1)
				body += "\nfunc recoverConcrete(owner interface{ Concrete() *s.SQLiteRuntimeStore }) *s.SQLiteRuntimeStore { return owner.Concrete() }\n"
			}
			file, info := debtNativeFixtureTypedControl(t, candidate, body)
			if approved := debtReviewedNativeReopenCalls(candidate, file, info); len(approved) != want {
				t.Fatalf("approved=%d want=%d", len(approved), want)
			}
			findings := debtNewAuthorityTypeScan().debtCollectFixtureStoreConstruction(candidate, file, info)
			foreign := debtNewAuthorityTypeScan().debtCollectFixtureStoreConstruction(candidate+".unreviewed", file, info)
			if len(foreign)-len(findings) != want {
				t.Fatalf("construction delta=%d want=%d", len(foreign)-len(findings), want)
			}
			if mutation == "raw-reconstruction" {
				if len(findings) == 0 || !findings[len(findings)-1].RawSQL {
					t.Fatal("raw-input reconstruction disappeared")
				}
			}
			if mutation == "concrete-recovery" && len(findings) != 2 {
				t.Fatal("concrete recovery or its caller disappeared:", findings)
			}
		})
	}
}

func TestPersistenceAuthorityDebtNativeFixturePolicyIsFingerprintBound(t *testing.T) {
	root, copy := persistenceAuthorityRepoRoot(t), t.TempDir()
	predecessor := t.TempDir()
	for _, path := range []string{
		"internal/store/persistence_authority_debt_census_test.go",
		"internal/store/persistence_authority_debt_test.go",
		"internal/store/persistence_authority_debt_cache_test.go",
		"internal/store/persistence_authority_debt_sidecar_test.go",
		"internal/store/persistence_authority_debt_process_lease_test.go",
		"internal/store/persistence_authority_debt_native_fixture_test.go",
		"internal/checkoutsource/fixture_role.go",
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, path, string(data))
		if path != "internal/store/persistence_authority_debt_native_fixture_test.go" {
			old, err := debtGit(root, "show", "098a84acd3cff157fb6bcaefb1d55ee15994cdb7:"+path)
			if err != nil {
				t.Fatal(err)
			}
			debtWriteModuleSource(t, predecessor, path, string(old))
		}
	}
	if digest, err := debtCollectorDigest(predecessor); err != nil || digest != debtProcessLeaseCollectorTo {
		t.Fatalf("reviewed predecessor collector=%s error=%v", digest, err)
	}
	const policy = "internal/store/persistence_authority_debt_native_fixture_test.go"
	data, err := os.ReadFile(filepath.Join(copy, policy))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, policy, strings.Replace(string(data), "uses == 1", "uses >= 1", 1))
	if digest, err := debtCollectorDigest(copy); err != nil || digest == debtNativeFixtureCollectorTo {
		t.Fatalf("changed closure provenance not fingerprinted: %s %v", digest, err)
	}
	if err := os.Remove(filepath.Join(copy, policy)); err != nil {
		t.Fatal(err)
	}
	if _, err := debtCollectorDigest(copy); err == nil {
		t.Fatal("current collector accepted missing native provenance policy")
	}
	debtWriteModuleSource(t, copy, policy, "package store_test; func broken(")
	if _, err := debtCollectorDigest(copy); err == nil {
		t.Fatal("current collector accepted corrupt native provenance policy")
	}
}

func TestPersistenceAuthorityDebtNativeFixtureCollectorTransition(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	if digest, err := debtCollectorDigest(root); err != nil || digest != debtNativeFixtureCollectorTo {
		t.Fatalf("collector=%q error=%v", digest, err)
	}
	for _, mutation := range []string{"exact", "foreign-base", "foreign-head", "unrotated", "reverse", "rebootstrap", "addition", "removal", "increase", "decrease", "foreign-identity"} {
		t.Run(mutation, func(t *testing.T) {
			row := debtControlSite("call:QueryRow", 2)
			trusted := authorityDebtBaseline{BootstrapSource: debtBootstrapSource, Collector: debtProcessLeaseCollectorTo, Sites: debtControlSet(row)}
			head := trusted
			head.Collector, head.Sites = debtNativeFixtureCollectorTo, maps.Clone(trusted.Sites)
			current := head.Collector
			switch mutation {
			case "foreign-base":
				trusted.Collector = strings.Repeat("a", 64)
			case "foreign-head":
				head.Collector, current = strings.Repeat("b", 64), strings.Repeat("b", 64)
			case "unrotated":
				head.Collector = trusted.Collector
			case "reverse":
				trusted, head, current = head, trusted, trusted.Collector
			case "rebootstrap":
				head.BootstrapSource = strings.Repeat("c", 40)
			case "addition":
				added := debtControlSite("call:Exec", 1)
				head.Sites[added.identity()] = added
			case "removal":
				delete(head.Sites, row.identity())
			case "increase", "decrease", "foreign-identity":
				delete(head.Sites, row.identity())
				if mutation == "increase" {
					row.Multiplicity++
				} else if mutation == "decrease" {
					row.Multiplicity--
				} else {
					row.Resolved += ".foreign"
				}
				head.Sites[row.identity()] = row
			}
			if err := debtValidateCollectorIdentity(trusted.Collector, current, trusted, head, ""); (err == nil) != (mutation == "exact") {
				t.Fatalf("transition error=%v", err)
			}
		})
	}
}
