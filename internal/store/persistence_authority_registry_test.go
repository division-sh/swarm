package store_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const authorityRegistryUpdateEnv = "SWARM_UPDATE_PERSISTENCE_AUTHORITY_REGISTRY"

type authorityFinding struct {
	Kind        string
	File        string
	Enclosing   string
	Member      string
	Resolved    string
	RawSQL      bool
	Disposition string
}

func (f authorityFinding) key() string {
	raw := "typed"
	if f.RawSQL {
		raw = "raw-sql"
	}
	return strings.Join([]string{f.Kind, raw, f.File, f.Enclosing, f.Member, f.Resolved}, "\t")
}

func (f authorityFinding) registryLine() string {
	return f.Disposition + "\t" + f.key()
}

func TestPersistenceAuthorityFindingRegistry(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	findings := loadPersistenceAuthorityFindings(t, root)
	registryPath := filepath.Join(root, "internal", "store", "testdata", "persistence_authority_findings.tsv")
	expected := readAuthorityRegistryIfPresent(t, registryPath)
	if os.Getenv(authorityRegistryUpdateEnv) == "1" {
		writeAuthorityRegistry(t, registryPath, findings, expected)
	}
	expected = readAuthorityRegistry(t, registryPath)
	actual := make(map[string]authorityFinding, len(findings))
	for _, finding := range findings {
		key := finding.key()
		if prior, exists := actual[key]; exists {
			t.Fatalf("duplicate resolved authority finding:\n%s\n%s", prior.registryLine(), finding.registryLine())
		}
		actual[key] = finding
	}

	var unknown, stale, invalid []string
	for key, finding := range actual {
		disposition, ok := expected[key]
		if !ok {
			unknown = append(unknown, "unclassified\t"+key)
			continue
		}
		if disposition == "unclassified" {
			invalid = append(invalid, "unclassified\t"+key)
		}
		if finding.RawSQL && !rawSQLDispositionAllowed(disposition) {
			invalid = append(invalid, disposition+"\t"+key)
		}
	}
	for key, disposition := range expected {
		if _, ok := actual[key]; !ok {
			stale = append(stale, disposition+"\t"+key)
		}
	}
	sort.Strings(unknown)
	sort.Strings(stale)
	sort.Strings(invalid)
	if len(unknown)+len(stale)+len(invalid) != 0 {
		t.Fatalf("persistence authority registry mismatch (set %s=1 only after classifying every changed finding):\nunknown:\n%s\nstale:\n%s\ninvalid raw authority:\n%s",
			authorityRegistryUpdateEnv,
			strings.Join(unknown, "\n"),
			strings.Join(stale, "\n"),
			strings.Join(invalid, "\n"),
		)
	}
	t.Logf("verified %d exact resolved authority findings", len(findings))
}

func TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority(t *testing.T) {
	findings := loadPersistenceAuthorityFindings(t, persistenceAuthorityRepoRoot(t))
	var leaked []string
	for _, finding := range findings {
		if finding.Kind != "effective-method" || !finding.RawSQL {
			continue
		}
		publicFacade := finding.File == "internal/store/store.go"
		semanticOwner := strings.Contains(finding.File, "internal/store/internal/") && finding.File != "internal/store/store.go"
		if publicFacade || semanticOwner {
			leaked = append(leaked, finding.registryLine())
		}
	}
	sort.Strings(leaked)
	if len(leaked) != 0 {
		t.Fatalf("effective persistence method sets expose raw authority:\n%s", strings.Join(leaked, "\n"))
	}
}

func TestPersistenceAuthorityRegistryRejectsHostileResolvedTypes(t *testing.T) {
	fixtures := map[string]string{
		"postgres-field": `package fixture
import "database/sql"
type Store struct { DB *sql.DB }
`,
		"sqlite-alias": `package fixture
import "database/sql"
type Alias = sql.DB
type Store struct { DB *Alias }
`,
		"embedded-handle": `package fixture
import "database/sql"
type Store struct { *sql.DB }
`,
		"promoted-owner-alias": `package fixture
import (
  "context"
  "database/sql"
)
type Owner struct{}
func (*Owner) RequireActiveTx(context.Context, *sql.Tx, string) error { return nil }
type Private struct { *Owner }
type Public = Private
`,
		"interface-result": `package fixture
import "database/sql"
type Store interface { Transaction() *sql.Tx }
`,
		"function-field": `package fixture
import (
  "context"
  "database/sql"
)
type Store struct { Mutate func(context.Context, *sql.Tx) error }
`,
		"context-authority": `package fixture
import (
  "context"
  "database/sql"
)
func Bad(ctx context.Context, db *sql.DB) {
  _ = context.WithValue(ctx, "db", db)
  _ = ctx.Value((*sql.Tx)(nil))
}
`,
	}
	for name, source := range fixtures {
		t.Run(name, func(t *testing.T) {
			findings := authorityFindingsFromSource(t, "internal/store/fixture/"+name+".go", source)
			if len(findings) == 0 {
				t.Fatal("hostile fixture produced no resolved finding")
			}
			var hasRelevant bool
			for _, finding := range findings {
				if finding.RawSQL || strings.HasPrefix(finding.Kind, "context-") {
					hasRelevant = true
					break
				}
			}
			if !hasRelevant {
				t.Fatalf("hostile fixture was not resolved as raw/context authority: %+v", findings)
			}
		})
	}
}

func TestPersistenceAuthorityRegistryRejectsTransitiveMethodCarriers(t *testing.T) {
	const source = `package fixture
import (
  "context"
  "database/sql"
)
type Mutation struct { tx *sql.Tx }
type Backend struct { db *sql.DB }
type Dialect string
type Options struct { dialect Dialect }
type Store struct{}
func (*Store) Mutate(context.Context, *Mutation) error { return nil }
func (*Store) Backend() *Backend { return nil }
func (*Store) Configure(Options) error { return nil }
`
	findings := authorityFindingsFromSource(t, "internal/store/fixture/transitive-carriers.go", source)
	want := map[string]bool{"method:Mutate": false, "method:Backend": false, "method:Configure": false}
	for _, finding := range findings {
		if finding.Kind != "effective-method" || !finding.RawSQL {
			continue
		}
		if _, ok := want[finding.Member]; ok {
			want[finding.Member] = true
		}
	}
	for method, detected := range want {
		if !detected {
			t.Errorf("transitive raw carrier %s was not classified as raw authority", method)
		}
	}
}

func TestPersistenceAuthorityRegistryRejectsUnknownLocalOperationInAuthorizedFile(t *testing.T) {
	const source = `package postgres
import (
  "context"
  "database/sql"
)
func hostileLocalOperation(ctx context.Context) error {
  db, err := sql.Open("postgres", "")
  if err != nil { return err }
  defer db.Close()
  _, err = db.ExecContext(ctx, "DELETE FROM events")
  return err
}
`
	const path = "internal/store/internal/backend/postgres/backend.go"
	findings := authorityFindingsFromSource(t, path, source)
	wantMembers := map[string]bool{
		"call:database/sql.Open#1": false,
		"local:db#1":               false,
		"call:database/sql.(*database/sql.DB).ExecContext#1": false,
	}
	for _, finding := range findings {
		if _, ok := wantMembers[finding.Member]; ok {
			wantMembers[finding.Member] = true
		}
	}
	for member, found := range wantMembers {
		if !found {
			t.Errorf("hostile local SQL operation did not produce %s; findings=%+v", member, findings)
		}
	}
	registry := readAuthorityRegistry(t, filepath.Join(persistenceAuthorityRepoRoot(t), "internal", "store", "testdata", "persistence_authority_findings.tsv"))
	for _, finding := range findings {
		if strings.Contains(finding.Enclosing, "hostileLocalOperation") {
			if disposition, ok := registry[finding.key()]; ok {
				t.Fatalf("hostile local SQL operation unexpectedly inherited disposition %q: %s", disposition, finding.registryLine())
			}
		}
	}
}

func loadPersistenceAuthorityFindings(t *testing.T, root string) []authorityFinding {
	t.Helper()
	cfg := &packages.Config{
		Dir: root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatalf("load authority packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("load authority packages reported type errors")
	}
	var findings []authorityFinding
	for _, pkg := range pkgs {
		for index, file := range pkg.Syntax {
			if index >= len(pkg.CompiledGoFiles) || strings.HasSuffix(pkg.CompiledGoFiles[index], "_test.go") {
				continue
			}
			rel, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatalf("relativize %s: %v", pkg.CompiledGoFiles[index], err)
			}
			rel = filepath.ToSlash(rel)
			fileFindings := collectAuthorityFindings(rel, file, pkg.TypesInfo)
			if !authorityTypedContractScope(rel) {
				fileFindings = slices.DeleteFunc(fileFindings, func(finding authorityFinding) bool {
					return !finding.RawSQL
				})
			}
			findings = append(findings, fileFindings...)
		}
		findings = append(findings, collectEffectiveMethodSetFindings(root, pkg)...)
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].key() < findings[j].key() })
	return findings
}

func authorityTypedContractScope(path string) bool {
	return strings.HasPrefix(path, "internal/persistence/") ||
		strings.HasPrefix(path, "internal/runtime/") ||
		strings.HasPrefix(path, "internal/store/")
}

func authorityFindingsFromSource(t *testing.T, path, source string) []authorityFinding {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse hostile fixture: %v", err)
	}
	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Defs:       map[*ast.Ident]types.Object{},
		Uses:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	conf := types.Config{Importer: importer.Default()}
	pkg, err := conf.Check("github.com/division-sh/swarm/internal/store/fixture", fset, []*ast.File{file}, info)
	if err != nil {
		t.Fatalf("type-check hostile fixture: %v", err)
	}
	findings := collectAuthorityFindings(path, file, info)
	findings = append(findings, collectPackageEffectiveMethodSetFindings(path, pkg, true)...)
	return findings
}

func collectEffectiveMethodSetFindings(root string, pkg *packages.Package) []authorityFinding {
	if pkg == nil || pkg.Types == nil {
		return nil
	}
	const storePath = "github.com/division-sh/swarm/internal/store"
	if pkg.PkgPath == storePath {
		return collectPackageEffectiveMethodSetFindings("internal/store/store.go", pkg.Types, true)
	}
	if !semanticOwnerMethodSetScope(pkg.PkgPath) {
		return nil
	}
	path := "<effective-method-set>"
	if pkg.Fset != nil {
		path = filepath.ToSlash(filepath.Join("internal/store/internal", strings.TrimPrefix(pkg.PkgPath, storePath+"/internal/"), "<effective-method-set>"))
	}
	return collectPackageEffectiveMethodSetFindings(path, pkg.Types, false)
}

func semanticOwnerMethodSetScope(pkgPath string) bool {
	const prefix = "github.com/division-sh/swarm/internal/store/internal/"
	return strings.HasPrefix(pkgPath, prefix) && !strings.Contains(pkgPath, "/backend/")
}

func collectPackageEffectiveMethodSetFindings(path string, pkg *types.Package, includeTyped bool) []authorityFinding {
	if pkg == nil {
		return nil
	}
	var findings []authorityFinding
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		typeName, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || !typeName.Exported() {
			continue
		}
		seen := map[*types.Func]bool{}
		for _, receiver := range []types.Type{types.Unalias(typeName.Type()), types.NewPointer(types.Unalias(typeName.Type()))} {
			methodSet := types.NewMethodSet(receiver)
			for index := 0; index < methodSet.Len(); index++ {
				method, ok := methodSet.At(index).Obj().(*types.Func)
				if !ok || !method.Exported() || seen[method] {
					continue
				}
				seen[method] = true
				raw := containsRawAuthoritySignature(method.Type())
				if !includeTyped && !raw {
					continue
				}
				findings = append(findings, authorityFinding{
					Kind:      "effective-method",
					File:      path,
					Enclosing: pkg.Path() + "." + typeName.Name(),
					Member:    "method:" + method.Name(),
					Resolved:  resolvedTypeString(method.Type()),
					RawSQL:    raw,
				})
			}
		}
	}
	return findings
}

func containsRawAuthoritySignature(valueType types.Type) bool {
	signature, ok := types.Unalias(valueType).(*types.Signature)
	if !ok {
		return containsRawAuthorityType(valueType, map[types.Type]struct{}{})
	}
	return tupleContainsRawAuthority(signature.Params(), map[types.Type]struct{}{}) ||
		tupleContainsRawAuthority(signature.Results(), map[types.Type]struct{}{})
}

func tupleContainsRawAuthority(tuple *types.Tuple, seen map[types.Type]struct{}) bool {
	if tuple == nil {
		return false
	}
	for index := 0; index < tuple.Len(); index++ {
		if containsRawAuthorityType(tuple.At(index).Type(), seen) {
			return true
		}
	}
	return false
}

func containsRawAuthorityType(valueType types.Type, seen map[types.Type]struct{}) bool {
	if valueType == nil {
		return false
	}
	valueType = types.Unalias(valueType)
	if _, ok := seen[valueType]; ok {
		return false
	}
	seen[valueType] = struct{}{}
	switch typed := valueType.(type) {
	case *types.Named:
		object := typed.Obj()
		if object != nil && object.Pkg() != nil {
			if object.Pkg().Path() == "database/sql" {
				switch object.Name() {
				case "DB", "Tx", "Conn", "Rows", "Row", "Result":
					return true
				}
			}
			if (strings.HasPrefix(object.Pkg().Path(), "github.com/division-sh/swarm/internal/store/") ||
				strings.HasPrefix(object.Pkg().Path(), "github.com/division-sh/swarm/internal/persistence/")) &&
				(object.Name() == "Dialect" || strings.HasSuffix(object.Name(), "Queryer") || strings.HasSuffix(object.Name(), "Execer")) {
				return true
			}
		}
		return containsRawAuthorityType(typed.Underlying(), seen)
	case *types.Pointer:
		return containsRawAuthorityType(typed.Elem(), seen)
	case *types.Slice:
		return containsRawAuthorityType(typed.Elem(), seen)
	case *types.Array:
		return containsRawAuthorityType(typed.Elem(), seen)
	case *types.Map:
		return containsRawAuthorityType(typed.Key(), seen) || containsRawAuthorityType(typed.Elem(), seen)
	case *types.Chan:
		return containsRawAuthorityType(typed.Elem(), seen)
	case *types.Signature:
		return tupleContainsRawAuthority(typed.Params(), seen) || tupleContainsRawAuthority(typed.Results(), seen)
	case *types.Struct:
		for index := 0; index < typed.NumFields(); index++ {
			if containsRawAuthorityType(typed.Field(index).Type(), seen) {
				return true
			}
		}
	case *types.Interface:
		for index := 0; index < typed.NumMethods(); index++ {
			if containsRawAuthorityType(typed.Method(index).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func collectAuthorityFindings(path string, file *ast.File, info *types.Info) []authorityFinding {
	var findings []authorityFinding
	for _, decl := range file.Decls {
		switch node := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch typed := spec.(type) {
				case *ast.TypeSpec:
					findings = append(findings, collectTypeAuthorityFindings(path, typed, info)...)
				case *ast.ValueSpec:
					for index, name := range typed.Names {
						valueType := info.TypeOf(typed.Type)
						if valueType == nil && index < len(typed.Values) {
							valueType = info.TypeOf(typed.Values[index])
						}
						if finding, ok := authorityTypeFinding(path, "package", "value:"+name.Name, valueType); ok {
							findings = append(findings, finding)
						}
					}
				}
			}
		case *ast.FuncDecl:
			enclosing := functionAuthorityName(node, info)
			findings = append(findings, collectFieldAuthorityFindings(path, enclosing, "param", node.Type.Params, info)...)
			findings = append(findings, collectFieldAuthorityFindings(path, enclosing, "result", node.Type.Results, info)...)
			findings = append(findings, collectContextAuthorityFindings(path, enclosing, node.Body, info)...)
			findings = append(findings, collectOperationAuthorityFindings(path, enclosing, node.Body, info)...)
		}
	}
	return findings
}

func collectOperationAuthorityFindings(path, enclosing string, body *ast.BlockStmt, info *types.Info) []authorityFinding {
	if body == nil {
		return nil
	}
	counts := map[string]int{}
	var findings []authorityFinding
	ast.Inspect(body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			finding, ok := authorityCallFinding(path, enclosing, typed, info)
			if !ok {
				return true
			}
			counts[finding.Member]++
			finding.Member = fmt.Sprintf("%s#%d", finding.Member, counts[finding.Member])
			findings = append(findings, finding)
		case *ast.AssignStmt:
			for index, lhs := range typed.Lhs {
				ident, ok := lhs.(*ast.Ident)
				if !ok || ident.Name == "_" {
					continue
				}
				valueType := info.TypeOf(lhs)
				if valueType == nil && index < len(typed.Rhs) {
					valueType = info.TypeOf(typed.Rhs[index])
				}
				if !containsRawSQLType(valueType) {
					continue
				}
				member := "local:" + ident.Name
				counts[member]++
				findings = append(findings, authorityFinding{
					Kind:      "local-raw-type",
					File:      path,
					Enclosing: enclosing,
					Member:    fmt.Sprintf("%s#%d", member, counts[member]),
					Resolved:  resolvedTypeString(valueType),
					RawSQL:    true,
				})
			}
		case *ast.DeclStmt:
			declaration, ok := typed.Decl.(*ast.GenDecl)
			if !ok {
				return true
			}
			for _, spec := range declaration.Specs {
				values, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for index, name := range values.Names {
					valueType := info.TypeOf(name)
					if valueType == nil && index < len(values.Values) {
						valueType = info.TypeOf(values.Values[index])
					}
					if !containsRawSQLType(valueType) {
						continue
					}
					member := "local:" + name.Name
					counts[member]++
					findings = append(findings, authorityFinding{
						Kind:      "local-raw-type",
						File:      path,
						Enclosing: enclosing,
						Member:    fmt.Sprintf("%s#%d", member, counts[member]),
						Resolved:  resolvedTypeString(valueType),
						RawSQL:    true,
					})
				}
			}
		}
		return true
	})
	return findings
}

func authorityCallFinding(path, enclosing string, call *ast.CallExpr, info *types.Info) (authorityFinding, bool) {
	callType := info.TypeOf(call.Fun)
	name := authorityCallName(call.Fun, info)
	receiverType := authorityCallReceiverType(call.Fun, info)
	raw := containsRawSQLType(callType) || containsRawSQLType(receiverType)
	resolved := "func=" + resolvedTypeString(callType) + "|recv=" + resolvedTypeString(receiverType)
	for _, arg := range call.Args {
		argType := info.TypeOf(arg)
		raw = raw || containsRawSQLType(argType)
		resolved += "|arg=" + resolvedTypeString(argType)
	}
	if signature, ok := types.Unalias(callType).Underlying().(*types.Signature); ok {
		raw = raw || tupleContainsRawSQL(signature.Results(), map[types.Type]struct{}{})
		resolved += "|results=" + resolvedTupleString(signature.Results())
	}
	if !raw && !isSQLOperationName(name) {
		return authorityFinding{}, false
	}
	if !raw {
		return authorityFinding{}, false
	}
	return authorityFinding{
		Kind:      "raw-operation",
		File:      path,
		Enclosing: enclosing,
		Member:    "call:" + name,
		Resolved:  resolved,
		RawSQL:    true,
	}, true
}

func authorityCallName(expr ast.Expr, info *types.Info) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		if object := info.Uses[typed]; object != nil {
			return qualifiedAuthorityObjectName(object)
		}
		return typed.Name
	case *ast.SelectorExpr:
		if selection := info.Selections[typed]; selection != nil {
			return qualifiedAuthorityObjectName(selection.Obj())
		}
		if object := info.Uses[typed.Sel]; object != nil {
			return qualifiedAuthorityObjectName(object)
		}
		return typed.Sel.Name
	default:
		return fmt.Sprintf("%T", expr)
	}
}

func authorityCallReceiverType(expr ast.Expr, info *types.Info) types.Type {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	if selection := info.Selections[selector]; selection != nil {
		return selection.Recv()
	}
	return info.TypeOf(selector.X)
}

func qualifiedAuthorityObjectName(object types.Object) string {
	if object == nil {
		return "<unknown>"
	}
	name := object.Name()
	if signature, ok := object.Type().(*types.Signature); ok && signature.Recv() != nil {
		name = "(" + resolvedTypeString(signature.Recv().Type()) + ")." + name
	}
	if object.Pkg() == nil {
		return name
	}
	return object.Pkg().Path() + "." + name
}

func isSQLOperationName(name string) bool {
	for _, operation := range []string{"Open", "Conn", "Begin", "BeginTx", "Exec", "ExecContext", "Query", "QueryContext", "QueryRow", "QueryRowContext", "Prepare", "PrepareContext", "Commit", "Rollback"} {
		if name == operation || strings.HasSuffix(name, ")."+operation) || strings.HasSuffix(name, "."+operation) {
			return true
		}
	}
	return false
}

func resolvedTupleString(tuple *types.Tuple) string {
	if tuple == nil {
		return "()"
	}
	parts := make([]string, 0, tuple.Len())
	for index := 0; index < tuple.Len(); index++ {
		parts = append(parts, resolvedTypeString(tuple.At(index).Type()))
	}
	return "(" + strings.Join(parts, ",") + ")"
}

func collectTypeAuthorityFindings(path string, spec *ast.TypeSpec, info *types.Info) []authorityFinding {
	var findings []authorityFinding
	switch node := spec.Type.(type) {
	case *ast.FuncType:
		if finding, ok := authorityTypeFinding(path, spec.Name.Name, "named-function-type", info.TypeOf(node)); ok {
			findings = append(findings, finding)
		}
	case *ast.StructType:
		for index, field := range node.Fields.List {
			member := authorityFieldName("field", index, field)
			if finding, ok := authorityTypeFinding(path, spec.Name.Name, member, info.TypeOf(field.Type)); ok {
				findings = append(findings, finding)
			}
		}
	case *ast.InterfaceType:
		for index, field := range node.Methods.List {
			member := authorityFieldName("method", index, field)
			if finding, ok := authorityTypeFinding(path, spec.Name.Name, member, info.TypeOf(field.Type)); ok {
				finding.Kind = "interface-method"
				findings = append(findings, finding)
			}
		}
	}
	return findings
}

func collectFieldAuthorityFindings(path, enclosing, prefix string, list *ast.FieldList, info *types.Info) []authorityFinding {
	if list == nil {
		return nil
	}
	var findings []authorityFinding
	for index, field := range list.List {
		if finding, ok := authorityTypeFinding(path, enclosing, authorityFieldName(prefix, index, field), info.TypeOf(field.Type)); ok {
			findings = append(findings, finding)
		}
	}
	return findings
}

func authorityTypeFinding(path, enclosing, member string, valueType types.Type) (authorityFinding, bool) {
	if valueType == nil {
		return authorityFinding{}, false
	}
	raw := containsRawSQLType(valueType)
	callback := callableType(valueType)
	if !raw && !callback {
		return authorityFinding{}, false
	}
	kind := "raw-type"
	if callback {
		kind = "callback-type"
	}
	return authorityFinding{
		Kind:      kind,
		File:      path,
		Enclosing: enclosing,
		Member:    member,
		Resolved:  resolvedTypeString(valueType),
		RawSQL:    raw,
	}, true
}

func collectContextAuthorityFindings(path, enclosing string, body *ast.BlockStmt, info *types.Info) []authorityFinding {
	if body == nil {
		return nil
	}
	counts := map[string]int{}
	var findings []authorityFinding
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		operation := ""
		if object := info.Uses[selector.Sel]; object != nil && object.Pkg() != nil && object.Pkg().Path() == "context" && selector.Sel.Name == "WithValue" {
			operation = "WithValue"
		} else if selection := info.Selections[selector]; selection != nil && selection.Obj().Name() == "Value" && selection.Obj().Pkg() != nil && selection.Obj().Pkg().Path() == "context" {
			operation = "Value"
		}
		if operation == "" {
			return true
		}
		counts[operation]++
		resolved := resolvedTypeString(info.TypeOf(call.Fun))
		raw := false
		for _, arg := range call.Args {
			argType := info.TypeOf(arg)
			raw = raw || containsRawSQLType(argType)
			resolved += "|arg=" + resolvedTypeString(argType)
		}
		findings = append(findings, authorityFinding{
			Kind:      "context-" + strings.ToLower(operation),
			File:      path,
			Enclosing: enclosing,
			Member:    fmt.Sprintf("context:%s#%d", operation, counts[operation]),
			Resolved:  resolved,
			RawSQL:    raw,
		})
		return true
	})
	return findings
}

func authorityFieldName(prefix string, index int, field *ast.Field) string {
	if len(field.Names) == 0 {
		return fmt.Sprintf("%s:#%d", prefix, index+1)
	}
	names := make([]string, 0, len(field.Names))
	for _, name := range field.Names {
		names = append(names, name.Name)
	}
	return prefix + ":" + strings.Join(names, ",")
}

func functionAuthorityName(decl *ast.FuncDecl, info *types.Info) string {
	object, _ := info.Defs[decl.Name].(*types.Func)
	if object == nil {
		return decl.Name.Name
	}
	signature, _ := object.Type().(*types.Signature)
	if signature == nil || signature.Recv() == nil {
		return decl.Name.Name
	}
	return "(" + resolvedTypeString(signature.Recv().Type()) + ")." + decl.Name.Name
}

func callableType(valueType types.Type) bool {
	if valueType == nil {
		return false
	}
	_, ok := types.Unalias(valueType).Underlying().(*types.Signature)
	return ok
}

func containsRawSQLType(valueType types.Type) bool {
	return containsRawSQLTypeSeen(valueType, make(map[types.Type]struct{}))
}

func containsRawSQLTypeSeen(valueType types.Type, seen map[types.Type]struct{}) bool {
	if valueType == nil {
		return false
	}
	valueType = types.Unalias(valueType)
	if _, ok := seen[valueType]; ok {
		return false
	}
	seen[valueType] = struct{}{}
	switch typed := valueType.(type) {
	case *types.Named:
		object := typed.Obj()
		if object != nil && object.Pkg() != nil && object.Pkg().Path() == "database/sql" {
			switch object.Name() {
			case "DB", "Tx", "Conn", "Rows", "Row", "Result":
				return true
			}
		}
		// A non-alias named type is an opaque authority boundary. Its own fields
		// and methods are inspected where that type is declared.
		return false
	case *types.Pointer:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Slice:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Array:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Map:
		return containsRawSQLTypeSeen(typed.Key(), seen) || containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Chan:
		return containsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Signature:
		return tupleContainsRawSQL(typed.Params(), seen) || tupleContainsRawSQL(typed.Results(), seen)
	case *types.Struct:
		for index := 0; index < typed.NumFields(); index++ {
			if containsRawSQLTypeSeen(typed.Field(index).Type(), seen) {
				return true
			}
		}
	case *types.Interface:
		for index := 0; index < typed.NumMethods(); index++ {
			if containsRawSQLTypeSeen(typed.Method(index).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func tupleContainsRawSQL(tuple *types.Tuple, seen map[types.Type]struct{}) bool {
	if tuple == nil {
		return false
	}
	for index := 0; index < tuple.Len(); index++ {
		if containsRawSQLTypeSeen(tuple.At(index).Type(), seen) {
			return true
		}
	}
	return false
}

func resolvedTypeString(valueType types.Type) string {
	if valueType == nil {
		return "<nil>"
	}
	return types.TypeString(valueType, func(pkg *types.Package) string {
		if pkg == nil {
			return ""
		}
		return pkg.Path()
	})
}

func rawSQLDispositionAllowed(disposition string) bool {
	switch disposition {
	case "private-backend", "private-runtime-adapter", "private-domain-adapter", "construction-owner", "fixture-2151", "adjacent-2149":
		return true
	default:
		return false
	}
}

func readAuthorityRegistryIfPresent(t *testing.T, path string) map[string]string {
	t.Helper()
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return map[string]string{}
	} else if err != nil {
		t.Fatalf("stat persistence authority registry: %v", err)
	}
	return readAuthorityRegistry(t, path)
}

func readAuthorityRegistry(t *testing.T, path string) map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open persistence authority registry: %v", err)
	}
	defer file.Close()
	registry := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid persistence authority registry line %q", line)
		}
		if _, exists := registry[parts[1]]; exists {
			t.Fatalf("duplicate persistence authority registry finding %q", parts[1])
		}
		registry[parts[1]] = parts[0]
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read persistence authority registry: %v", err)
	}
	return registry
}

func writeAuthorityRegistry(t *testing.T, path string, findings []authorityFinding, prior map[string]string) {
	t.Helper()
	var buffer bytes.Buffer
	buffer.WriteString("# disposition\tkind\tauthority\tfile\tenclosing\tmember\tresolved-type\n")
	for _, finding := range findings {
		disposition := prior[finding.key()]
		if disposition == "" {
			disposition = "unclassified"
		}
		buffer.WriteString(disposition + "\t" + finding.key())
		buffer.WriteByte('\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create authority registry directory: %v", err)
	}
	if err := os.WriteFile(path, buffer.Bytes(), 0o644); err != nil {
		t.Fatalf("write persistence authority registry: %v", err)
	}
}

func persistenceAuthorityRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve persistence authority test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
