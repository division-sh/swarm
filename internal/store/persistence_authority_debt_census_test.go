package store_test

// Scanner mechanics extracted from B's approved #2151 integration census.
// The recorded-inventory guards keep their original scope; this collector
// supplies the separately enforced no-new-debt guard and final broad census.

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"golang.org/x/tools/go/packages"
)

func debtLoadPersistenceAuthorityFindings(t *testing.T, root string) []authorityFinding {
	t.Helper()
	cfg := &packages.Config{
		Dir: root,
		Env: debtCensusEnvironment(),
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests: true,
	}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		t.Fatalf("load authority packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("load authority packages reported type errors")
	}
	if len(pkgs) == 0 {
		t.Fatal("authority census matched no packages")
	}
	resolved := map[string]authorityFinding{}
	compiled := map[string]bool{}
	scan := debtNewAuthorityTypeScan()
	for _, pkg := range pkgs {
		if len(pkg.Syntax) != len(pkg.CompiledGoFiles) {
			t.Fatalf("incomplete authority syntax for %s: syntax=%d compiled=%d", pkg.ID, len(pkg.Syntax), len(pkg.CompiledGoFiles))
		}
		for index, file := range pkg.Syntax {
			if index >= len(pkg.CompiledGoFiles) {
				continue
			}
			rel, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatalf("relativize %s: %v", pkg.CompiledGoFiles[index], err)
			}
			rel = filepath.ToSlash(rel)
			if !filepath.IsLocal(rel) {

				continue
			}
			compiled[rel] = true
			fileFindings := scan.debtCollectAuthorityFindings(rel, file, pkg.TypesInfo)
			if !debtAuthorityTypedContractScope(rel) {
				fileFindings = slices.DeleteFunc(fileFindings, func(finding authorityFinding) bool {
					return !finding.RawSQL && finding.Kind != "forbidden-test-consumption" && finding.Kind != "selected-store-construction"
				})
			}
			for _, finding := range fileFindings {
				resolved[finding.key()] = finding
			}
		}
		for _, finding := range debtCollectEffectiveMethodSetFindings(root, pkg) {
			resolved[finding.key()] = finding
		}
	}
	for _, finding := range debtInactiveAuthorityFindings(t, root, compiled) {
		resolved[finding.key()] = finding
	}
	findings := make([]authorityFinding, 0, len(resolved))
	for _, finding := range resolved {
		findings = append(findings, finding)
	}
	sort.Slice(findings, func(i, j int) bool { return debtAuthorityFindingLess(findings[i], findings[j]) })
	return findings
}

func debtCensusEnvironment() []string {
	var env []string
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "GOFLAGS=") && !strings.HasPrefix(value, "GOWORK=") {
			env = append(env, value)
		}
	}
	return append(env, "GOFLAGS=", "GOWORK=off")
}

func debtInactiveAuthorityFindings(t *testing.T, root string, compiled map[string]bool) []authorityFinding {
	t.Helper()
	var findings []authorityFinding
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "testdata", "vendor", "node_modules", ".swarm":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if compiled[rel] {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			imported, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if imported != "database/sql" && !strings.HasPrefix(imported, "github.com/division-sh/swarm/internal/store/internal/") {
				continue
			}
			findings = append(findings, authorityFinding{
				Kind: "inactive-raw-source", File: rel, Enclosing: file.Name.Name,
				Member: "import:" + imported, Resolved: "unresolved inactive authority", RawSQL: true,
			})
		}
		findings = append(findings, debtCollectInactiveTestConsumptionFindings(rel, file)...)
		return nil
	})
	if err != nil {
		t.Fatalf("scan inactive persistence authority sources: %v", err)
	}
	return findings
}

func debtAuthorityTypedContractScope(path string) bool {
	return strings.HasPrefix(path, "internal/persistence/") ||
		strings.HasPrefix(path, "internal/runtime/") ||
		strings.HasPrefix(path, "internal/store/")
}

func debtAuthorityFindingsFromSource(t *testing.T, path, source string) []authorityFinding {
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
	findings := debtNewAuthorityTypeScan().debtCollectAuthorityFindings(path, file, info)
	findings = append(findings, debtCollectPackageEffectiveMethodSetFindings(path, pkg, true)...)
	return findings
}

func debtCollectEffectiveMethodSetFindings(root string, pkg *packages.Package) []authorityFinding {
	if pkg == nil || pkg.Types == nil {
		return nil
	}
	const storePath = "github.com/division-sh/swarm/internal/store"
	if pkg.PkgPath == storePath {
		return debtCollectPackageEffectiveMethodSetFindings("internal/store/store.go", pkg.Types, true)
	}
	if !debtSemanticOwnerMethodSetScope(pkg.PkgPath) {
		return nil
	}
	path := "<effective-method-set>"
	if pkg.Fset != nil {
		path = filepath.ToSlash(filepath.Join("internal/store/internal", strings.TrimPrefix(pkg.PkgPath, storePath+"/internal/"), "<effective-method-set>"))
	}
	return debtCollectPackageEffectiveMethodSetFindings(path, pkg.Types, false)
}

func debtSemanticOwnerMethodSetScope(pkgPath string) bool {
	const prefix = "github.com/division-sh/swarm/internal/store/internal/"
	return pkgPath == prefix+"backend/eventrecord" ||
		(strings.HasPrefix(pkgPath, prefix) && !strings.Contains(pkgPath, "/backend/"))
}

func debtCollectPackageEffectiveMethodSetFindings(path string, pkg *types.Package, includeTyped bool) []authorityFinding {
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
				raw := debtContainsRawAuthoritySignature(method.Type())
				if !includeTyped && !raw {
					continue
				}
				findings = append(findings, authorityFinding{
					Kind:      "effective-method",
					File:      path,
					Enclosing: pkg.Path() + "." + typeName.Name(),
					Member:    "method:" + method.Name(),
					Resolved:  debtResolvedTypeString(method.Type()),
					RawSQL:    raw,
				})
			}
		}
	}
	return findings
}

func debtContainsRawAuthoritySignature(valueType types.Type) bool {
	signature, ok := types.Unalias(valueType).(*types.Signature)
	if !ok {
		return debtContainsRawAuthorityType(valueType, map[types.Type]struct{}{})
	}
	return debtTupleContainsRawAuthority(signature.Params(), map[types.Type]struct{}{}) ||
		debtTupleContainsRawAuthority(signature.Results(), map[types.Type]struct{}{})
}

func debtTupleContainsRawAuthority(tuple *types.Tuple, seen map[types.Type]struct{}) bool {
	if tuple == nil {
		return false
	}
	for index := 0; index < tuple.Len(); index++ {
		if debtContainsRawAuthorityType(tuple.At(index).Type(), seen) {
			return true
		}
	}
	return false
}

func debtContainsRawAuthorityType(valueType types.Type, seen map[types.Type]struct{}) bool {
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
		return debtContainsRawAuthorityType(typed.Underlying(), seen)
	case *types.Pointer:
		return debtContainsRawAuthorityType(typed.Elem(), seen)
	case *types.Slice:
		return debtContainsRawAuthorityType(typed.Elem(), seen)
	case *types.Array:
		return debtContainsRawAuthorityType(typed.Elem(), seen)
	case *types.Map:
		return debtContainsRawAuthorityType(typed.Key(), seen) || debtContainsRawAuthorityType(typed.Elem(), seen)
	case *types.Chan:
		return debtContainsRawAuthorityType(typed.Elem(), seen)
	case *types.Signature:
		return debtTupleContainsRawAuthority(typed.Params(), seen) || debtTupleContainsRawAuthority(typed.Results(), seen)
	case *types.Struct:
		for index := 0; index < typed.NumFields(); index++ {
			if debtContainsRawAuthorityType(typed.Field(index).Type(), seen) {
				return true
			}
		}
	case *types.Interface:
		for index := 0; index < typed.NumMethods(); index++ {
			if debtContainsRawAuthorityType(typed.Method(index).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func (scan *debtAuthorityTypeScan) debtCollectAuthorityFindings(path string, file *ast.File, info *types.Info) []authorityFinding {
	findings := debtCollectTestAuthorityConsumptionFindings(path, file, info)
	findings = append(findings, scan.debtCollectFixtureStoreConstruction(path, file, info)...)
	for _, decl := range file.Decls {
		switch node := decl.(type) {
		case *ast.GenDecl:
			for _, spec := range node.Specs {
				switch typed := spec.(type) {
				case *ast.TypeSpec:
					findings = append(findings, scan.debtCollectTypeAuthorityFindings(path, typed, info)...)
					if typed.Name.IsExported() && debtSemanticOwnerRawExportScope(path) {
						if object := info.Defs[typed.Name]; object != nil {
							if finding, ok := scan.debtAuthorityTypeFinding(path, typed.Name.Name, "exported-type", object.Type()); ok && finding.RawSQL {
								finding.Kind = "raw-authority-export"
								findings = append(findings, finding)
							}
						}
					}
				case *ast.ValueSpec:
					for _, name := range typed.Names {
						object := info.Defs[name]
						if object == nil {
							continue
						}
						valueType := object.Type()
						if finding, ok := scan.debtAuthorityTypeFinding(path, "package", "value:"+name.Name, valueType); ok {
							if name.IsExported() && finding.RawSQL && debtSemanticOwnerRawExportScope(path) {
								finding.Kind = "raw-authority-export"
							}
							findings = append(findings, finding)
						}
					}
				}
			}
		case *ast.FuncDecl:
			enclosing := debtFunctionAuthorityName(node, info)
			params := scan.debtCollectFieldAuthorityFindings(path, enclosing, "param", node.Type.Params, info)
			results := scan.debtCollectFieldAuthorityFindings(path, enclosing, "result", node.Type.Results, info)
			if node.Name.IsExported() && debtSemanticOwnerRawExportScope(path) {
				callbackInputs := make(map[string]bool)
				if node.Type.Params != nil {
					for index, field := range node.Type.Params.List {
						callbackInputs[debtAuthorityFieldName("param", index, field)] = scan.debtContainsRawCallbackInput(info.TypeOf(field.Type), make(map[types.Type]struct{}))
					}
				}
				for index := range params {
					if params[index].RawSQL && callbackInputs[params[index].Member] {
						params[index].Kind = "raw-authority-export"
					}
				}
				for index := range results {
					if results[index].RawSQL {
						results[index].Kind = "raw-authority-export"
					}
				}
			}
			findings = append(findings, params...)
			findings = append(findings, results...)
			findings = append(findings, scan.debtCollectContextAuthorityFindings(path, enclosing, node.Body, info)...)
			findings = append(findings, scan.debtCollectOperationAuthorityFindings(path, enclosing, node.Body, info)...)
		}
	}
	return findings
}

func debtSemanticOwnerRawExportScope(path string) bool {
	return strings.HasPrefix(path, "internal/store/internal/") &&
		!strings.HasPrefix(path, "internal/store/internal/backend/") &&
		!strings.HasPrefix(path, "internal/store/internal/schemastore/")
}

func (scan *debtAuthorityTypeScan) debtContainsRawCallbackInput(valueType types.Type, seen map[types.Type]struct{}) bool {
	if valueType == nil {
		return false
	}
	valueType = types.Unalias(valueType)
	if _, exists := seen[valueType]; exists {
		return false
	}
	seen[valueType] = struct{}{}
	switch value := valueType.(type) {
	case *types.Signature:
		return scan.debtContainsRawSQLType(value)
	case *types.Named:
		return scan.debtContainsRawCallbackInput(value.Underlying(), seen)
	case *types.Pointer:
		return scan.debtContainsRawCallbackInput(value.Elem(), seen)
	case *types.Slice:
		return scan.debtContainsRawCallbackInput(value.Elem(), seen)
	case *types.Array:
		return scan.debtContainsRawCallbackInput(value.Elem(), seen)
	case *types.Map:
		return scan.debtContainsRawCallbackInput(value.Key(), seen) || scan.debtContainsRawCallbackInput(value.Elem(), seen)
	case *types.Chan:
		return scan.debtContainsRawCallbackInput(value.Elem(), seen)
	case *types.Struct:
		for index := 0; index < value.NumFields(); index++ {
			field := value.Field(index)
			if field.Exported() && scan.debtContainsRawCallbackInput(field.Type(), seen) {
				return true
			}
		}
	case *types.Interface:
		for index := 0; index < value.NumMethods(); index++ {
			method := value.Method(index).Type().(*types.Signature)

			if scan.debtTupleContainsRawSQL(method.Params(), make(map[types.Type]struct{})) {
				return true
			}
		}
	}
	return false
}

func (scan *debtAuthorityTypeScan) debtCollectFixtureStoreConstruction(path string, file *ast.File, info *types.Info) []authorityFinding {
	var findings []authorityFinding
	counts := map[string]int{}
	ast.Inspect(file, func(node ast.Node) bool {
		var result types.Type
		var inputs []ast.Expr
		switch node := node.(type) {
		case *ast.CallExpr:
			result = info.TypeOf(node)
			inputs = node.Args
		case *ast.CompositeLit:
			result = info.TypeOf(node)
			for _, element := range node.Elts {
				if field, ok := element.(*ast.KeyValueExpr); ok {
					inputs = append(inputs, field.Value)
				} else {
					inputs = append(inputs, element)
				}
			}
		default:
			return true
		}
		if debtRunFixtureConstructsSelectedStore(result) {
			enclosing := "package"
			for _, decl := range file.Decls {
				function, ok := decl.(*ast.FuncDecl)
				if ok && node.Pos() >= function.Pos() && node.End() <= function.End() {
					enclosing = debtFunctionAuthorityName(function, info)
					break
				}
			}
			raw := false
			for _, input := range inputs {
				raw = raw || scan.debtContainsRawSQLType(info.TypeOf(input))
			}
			if !raw && debtOwnedNativeFixtureConstruction(path, node, info) {
				return true
			}
			identity := enclosing + ":" + debtResolvedTypeString(result)
			counts[identity]++
			findings = append(findings, authorityFinding{
				Kind: "selected-store-construction", File: path,
				Enclosing: enclosing, Member: fmt.Sprintf("construction:%s#%d", debtResolvedTypeString(result), counts[identity]),
				Resolved: debtResolvedTypeString(result), RawSQL: raw,
			})
		}
		return true
	})
	return findings
}

func debtOwnedNativeFixtureConstruction(path string, node ast.Node, info *types.Info) bool {
	// Private constructors are legitimate only within their construction role.
	// Ordinary tests may consume the exact public native factories, not arbitrary
	// same-DB reconstruction. Raw-input constructors are never exempted here.
	if strings.HasPrefix(path, "internal/store/internal/") || strings.HasPrefix(path, "internal/store/construction/") || strings.HasPrefix(path, "internal/store/selected/") || strings.HasPrefix(path, "internal/testpostgres/") || filepath.ToSlash(filepath.Dir(path)) == "internal/testutil" {
		return true
	}
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return false
	}
	name := debtAuthorityCallName(call.Fun, info)
	switch name {
	case "github.com/division-sh/swarm/internal/store/storetest.StartPostgresRuntimeStore",
		"github.com/division-sh/swarm/internal/store/storetest.StartPostgresRuntimeStoreWithReopen",
		"github.com/division-sh/swarm/internal/store/storetest.StartSQLiteRuntimeStore",
		"github.com/division-sh/swarm/internal/store/storetest.StartSQLiteRuntimeStoreWithContext",
		"github.com/division-sh/swarm/internal/store/storetest.StartSQLiteRuntimeStoreWithReopen":
		return true
	}
	return false
}

func debtRunFixtureConstructsSelectedStore(result types.Type) bool {
	if result == nil {
		return false
	}
	switch result := types.Unalias(result).(type) {
	case *types.Pointer:
		return debtRunFixtureConstructsSelectedStore(result.Elem())
	case *types.Tuple:
		for i := 0; i < result.Len(); i++ {
			if debtRunFixtureConstructsSelectedStore(result.At(i).Type()) {
				return true
			}
		}
	case *types.Named:
		return result.Obj().Name() == "PostgresStore" || result.Obj().Name() == "SQLiteRuntimeStore"
	}
	return false
}

func debtTestFixturePackage(path string) bool {
	const module = "github.com/division-sh/swarm/internal/"
	internal := strings.HasPrefix(path, module) || strings.HasPrefix(path, "internal/")
	path = strings.TrimPrefix(path, module)
	path = strings.TrimPrefix(path, "internal/")
	publicTestSupport := !strings.HasPrefix(path, "store/internal/") && strings.HasSuffix(filepath.Base(path), "test")
	return internal && (publicTestSupport ||
		strings.Contains("/"+path+"/", "/testfixtures/") ||
		path == "store/storetest" || path == "store/testsql" ||
		path == "store/eventfixture" || strings.HasPrefix(path, "store/testutil/") ||
		strings.HasPrefix(path, "testutil/"))
}

func debtTestAuthorityConsumer(path, enclosing string) bool {
	if strings.HasSuffix(path, "_test.go") || debtTestFixturePackage(filepath.ToSlash(filepath.Dir(path))) {
		return true
	}

	return strings.HasPrefix(path, "internal/store/internal/") && strings.HasSuffix(enclosing, "ForTest")
}

func debtCollectTestAuthorityConsumptionFindings(path string, file *ast.File, info *types.Info) []authorityFinding {
	var findings []authorityFinding
	for _, imp := range file.Imports {
		imported, err := strconv.Unquote(imp.Path.Value)
		if err == nil && debtTestFixturePackage(imported) && !debtTestAuthorityConsumer(path, "package") && !debtPrivateFixtureImportIsConfined(path, file, info, imported) {
			findings = append(findings, authorityFinding{
				Kind: "forbidden-test-consumption", File: path, Enclosing: "package",
				Member: "import:" + imported, Resolved: "fixture authority imported by production",
			})
		}
	}
	for _, declaration := range file.Decls {
		enclosing := "package"
		if function, ok := declaration.(*ast.FuncDecl); ok {
			enclosing = function.Name.Name
		}
		if debtTestAuthorityConsumer(path, enclosing) {
			continue
		}
		counts := map[string]int{}
		ast.Inspect(declaration, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			object := info.Uses[identifier]
			if object == nil || object.Pkg() == nil || !strings.HasPrefix(object.Pkg().Path(), "github.com/division-sh/swarm/") {
				return true
			}
			_, function := object.(*types.Func)
			if !debtTestFixturePackage(object.Pkg().Path()) && (!function || !strings.HasSuffix(object.Name(), "ForTest")) {
				return true
			}
			name := debtQualifiedAuthorityObjectName(object)
			counts[name]++
			findings = append(findings, authorityFinding{
				Kind: "forbidden-test-consumption", File: path, Enclosing: enclosing,
				Member: fmt.Sprintf("reference:%s#%d", name, counts[name]), Resolved: debtResolvedTypeString(object.Type()),
			})
			return true
		})
	}
	return findings
}

func debtPrivateFixtureImportIsConfined(path string, file *ast.File, info *types.Info, imported string) bool {
	if !strings.HasPrefix(path, "internal/store/internal/") {
		return false
	}
	used, confined := false, true
	for _, declaration := range file.Decls {
		enclosing := "package"
		if function, ok := declaration.(*ast.FuncDecl); ok {
			enclosing = function.Name.Name
		}
		ast.Inspect(declaration, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			object := info.Uses[identifier]
			if object == nil || object.Pkg() == nil || object.Pkg().Path() != imported {
				return true
			}
			used = true
			confined = confined && debtTestAuthorityConsumer(path, enclosing)
			return true
		})
	}
	return used && confined
}

// Inactive source cannot rely on the current build's type graph. Resolve its
// import aliases syntactically and reject production fixture consumption rather
// than letting an OS/build-tag change bypass the active-source guard.
func debtCollectInactiveTestConsumptionFindings(path string, file *ast.File) []authorityFinding {
	if debtTestAuthorityConsumer(path, "package") {
		return nil
	}
	imports := map[string]string{}
	var findings []authorityFinding
	for _, imp := range file.Imports {
		imported, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		name := filepath.Base(imported)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		imports[name] = imported
		if debtTestFixturePackage(imported) {
			findings = append(findings, authorityFinding{
				Kind: "forbidden-test-consumption", File: path, Enclosing: "package",
				Member: "inactive-import:" + imported, Resolved: "fixture import in inactive production source",
			})
		}
	}
	for _, declaration := range file.Decls {
		enclosing := "package"
		if function, ok := declaration.(*ast.FuncDecl); ok {
			enclosing = function.Name.Name
		}
		if debtTestAuthorityConsumer(path, enclosing) {
			continue
		}
		counts := map[string]int{}
		ast.Inspect(declaration, func(node ast.Node) bool {
			name := ""
			switch reference := node.(type) {
			case *ast.SelectorExpr:
				if !strings.HasSuffix(reference.Sel.Name, "ForTest") {
					return true
				}
				qualifier, ok := reference.X.(*ast.Ident)
				if ok {
					imported := imports[qualifier.Name]
					if imported != "" && !strings.HasPrefix(imported, "github.com/division-sh/swarm/") {
						return false
					}
					if imported == "" {
						imported = file.Name.Name + "." + qualifier.Name
					}
					name = imported + "." + reference.Sel.Name
				} else {
					name = file.Name.Name + "." + reference.Sel.Name
				}
			case *ast.Ident:
				if !strings.HasSuffix(reference.Name, "ForTest") || imports[reference.Name] != "" {
					return true
				}
				if function, ok := declaration.(*ast.FuncDecl); ok && reference == function.Name {
					return true
				}
				if reference.Obj != nil && reference.Obj.Kind != ast.Fun {
					return true
				}
				name = file.Name.Name + "." + reference.Name
			}
			if name == "" {
				return true
			}
			counts[name]++
			findings = append(findings, authorityFinding{
				Kind: "forbidden-test-consumption", File: path, Enclosing: enclosing,
				Member: fmt.Sprintf("inactive-reference:%s#%d", name, counts[name]), Resolved: "named test operation in inactive production source",
			})
			_, selector := node.(*ast.SelectorExpr)
			return !selector
		})
	}
	return findings
}

func (scan *debtAuthorityTypeScan) debtCollectOperationAuthorityFindings(path, enclosing string, body *ast.BlockStmt, info *types.Info) []authorityFinding {
	if body == nil {
		return nil
	}
	counts := map[string]int{}
	var findings []authorityFinding
	ast.Inspect(body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			finding, ok := scan.debtAuthorityCallFinding(path, enclosing, typed, info)
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
				if !scan.debtContainsRawSQLType(valueType) {
					continue
				}
				member := "local:" + ident.Name
				counts[member]++
				findings = append(findings, authorityFinding{
					Kind:      "local-raw-type",
					File:      path,
					Enclosing: enclosing,
					Member:    fmt.Sprintf("%s#%d", member, counts[member]),
					Resolved:  debtResolvedTypeString(valueType),
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
					if !scan.debtContainsRawSQLType(valueType) {
						continue
					}
					member := "local:" + name.Name
					counts[member]++
					findings = append(findings, authorityFinding{
						Kind:      "local-raw-type",
						File:      path,
						Enclosing: enclosing,
						Member:    fmt.Sprintf("%s#%d", member, counts[member]),
						Resolved:  debtResolvedTypeString(valueType),
						RawSQL:    true,
					})
				}
			}
		}
		return true
	})
	return findings
}

func (scan *debtAuthorityTypeScan) debtAuthorityCallFinding(path, enclosing string, call *ast.CallExpr, info *types.Info) (authorityFinding, bool) {
	callType := info.TypeOf(call.Fun)
	name := debtAuthorityCallName(call.Fun, info)
	receiverType := debtAuthorityCallReceiverType(call.Fun, info)
	raw := scan.debtContainsRawSQLType(callType) || scan.debtContainsRawSQLType(receiverType)
	resolved := "func=" + debtResolvedTypeString(callType) + "|recv=" + debtResolvedTypeString(receiverType)
	for _, arg := range call.Args {
		argType := info.TypeOf(arg)
		raw = raw || scan.debtContainsRawSQLType(argType)
		resolved += "|arg=" + debtResolvedTypeString(argType)
	}
	if signature, ok := types.Unalias(callType).Underlying().(*types.Signature); ok {
		resolved += "|results=" + debtResolvedTupleString(signature.Results())
	}
	if !raw && !debtIsSQLOperationName(name) {
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

func debtAuthorityCallName(expr ast.Expr, info *types.Info) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		if object := info.Uses[typed]; object != nil {
			return debtQualifiedAuthorityObjectName(object)
		}
		return typed.Name
	case *ast.SelectorExpr:
		if selection := info.Selections[typed]; selection != nil {
			return debtQualifiedAuthorityObjectName(selection.Obj())
		}
		if object := info.Uses[typed.Sel]; object != nil {
			return debtQualifiedAuthorityObjectName(object)
		}
		return typed.Sel.Name
	default:
		return fmt.Sprintf("%T", expr)
	}
}

func debtAuthorityCallReceiverType(expr ast.Expr, info *types.Info) types.Type {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	if selection := info.Selections[selector]; selection != nil {
		return selection.Recv()
	}
	return info.TypeOf(selector.X)
}

func debtQualifiedAuthorityObjectName(object types.Object) string {
	if object == nil {
		return "<unknown>"
	}
	name := object.Name()
	if signature, ok := object.Type().(*types.Signature); ok && signature.Recv() != nil {
		name = "(" + debtResolvedTypeString(signature.Recv().Type()) + ")." + name
	}
	if object.Pkg() == nil {
		return name
	}
	return object.Pkg().Path() + "." + name
}

func debtIsSQLOperationName(name string) bool {
	for _, operation := range []string{"Open", "Conn", "Begin", "BeginTx", "Exec", "ExecContext", "Query", "QueryContext", "QueryRow", "QueryRowContext", "Prepare", "PrepareContext", "Commit", "Rollback"} {
		if name == operation || strings.HasSuffix(name, ")."+operation) || strings.HasSuffix(name, "."+operation) {
			return true
		}
	}
	return false
}

func debtResolvedTupleString(tuple *types.Tuple) string {
	if tuple == nil {
		return "()"
	}
	parts := make([]string, 0, tuple.Len())
	for index := 0; index < tuple.Len(); index++ {
		parts = append(parts, debtResolvedTypeString(tuple.At(index).Type()))
	}
	return "(" + strings.Join(parts, ",") + ")"
}

func (scan *debtAuthorityTypeScan) debtCollectTypeAuthorityFindings(path string, spec *ast.TypeSpec, info *types.Info) []authorityFinding {
	var findings []authorityFinding
	switch node := spec.Type.(type) {
	case *ast.FuncType:
		if finding, ok := scan.debtAuthorityTypeFinding(path, spec.Name.Name, "named-function-type", info.TypeOf(node)); ok {
			findings = append(findings, finding)
		}
	case *ast.StructType:
		for index, field := range node.Fields.List {
			member := debtAuthorityFieldName("field", index, field)
			if finding, ok := scan.debtAuthorityTypeFinding(path, spec.Name.Name, member, info.TypeOf(field.Type)); ok {
				findings = append(findings, finding)
			}
		}
	case *ast.InterfaceType:
		for index, field := range node.Methods.List {
			member := debtAuthorityFieldName("method", index, field)
			if finding, ok := scan.debtAuthorityTypeFinding(path, spec.Name.Name, member, info.TypeOf(field.Type)); ok {
				finding.Kind = "interface-method"
				findings = append(findings, finding)
			}
		}
	}
	return findings
}

func (scan *debtAuthorityTypeScan) debtCollectFieldAuthorityFindings(path, enclosing, prefix string, list *ast.FieldList, info *types.Info) []authorityFinding {
	if list == nil {
		return nil
	}
	var findings []authorityFinding
	for index, field := range list.List {
		if finding, ok := scan.debtAuthorityTypeFinding(path, enclosing, debtAuthorityFieldName(prefix, index, field), info.TypeOf(field.Type)); ok {
			findings = append(findings, finding)
		}
	}
	return findings
}

func (scan *debtAuthorityTypeScan) debtAuthorityTypeFinding(path, enclosing, member string, valueType types.Type) (authorityFinding, bool) {
	if valueType == nil {
		return authorityFinding{}, false
	}
	raw := scan.debtContainsRawSQLType(valueType)
	callback := debtCallableType(valueType)
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
		Resolved:  debtResolvedTypeString(valueType),
		RawSQL:    raw,
	}, true
}

func (scan *debtAuthorityTypeScan) debtCollectContextAuthorityFindings(path, enclosing string, body *ast.BlockStmt, info *types.Info) []authorityFinding {
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
		resolved := debtResolvedTypeString(info.TypeOf(call.Fun))
		raw := false
		for _, arg := range call.Args {
			argType := info.TypeOf(arg)
			raw = raw || scan.debtContainsRawSQLType(argType)
			resolved += "|arg=" + debtResolvedTypeString(argType)
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

func debtAuthorityFieldName(prefix string, index int, field *ast.Field) string {
	if len(field.Names) == 0 {
		return fmt.Sprintf("%s:#%d", prefix, index+1)
	}
	names := make([]string, 0, len(field.Names))
	for _, name := range field.Names {
		names = append(names, name.Name)
	}
	return prefix + ":" + strings.Join(names, ",")
}

func debtFunctionAuthorityName(decl *ast.FuncDecl, info *types.Info) string {
	object, _ := info.Defs[decl.Name].(*types.Func)
	if object == nil {
		return decl.Name.Name
	}
	signature, _ := object.Type().(*types.Signature)
	if signature == nil || signature.Recv() == nil {
		return decl.Name.Name
	}
	return "(" + debtResolvedTypeString(signature.Recv().Type()) + ")." + decl.Name.Name
}

func debtCallableType(valueType types.Type) bool {
	if valueType == nil {
		return false
	}
	_, ok := types.Unalias(valueType).Underlying().(*types.Signature)
	return ok
}

type debtAuthorityTypeScan struct {
	completed map[types.Type]bool
	methods   map[*types.Named][]types.Type
}

func debtNewAuthorityTypeScan() *debtAuthorityTypeScan {
	return &debtAuthorityTypeScan{
		completed: make(map[types.Type]bool),
		methods:   make(map[*types.Named][]types.Type),
	}
}

func (scan *debtAuthorityTypeScan) debtContainsRawSQLType(valueType types.Type) bool {
	if valueType == nil {
		return false
	}
	valueType = types.Unalias(valueType)
	if raw, ok := scan.completed[valueType]; ok {
		return raw
	}
	raw := scan.debtContainsRawSQLTypeSeen(valueType, make(map[types.Type]struct{}))

	scan.completed[valueType] = raw
	return raw
}

func (scan *debtAuthorityTypeScan) debtExportedMethods(named *types.Named) []types.Type {
	if methods, ok := scan.methods[named]; ok {
		return methods
	}
	var result []types.Type
	for _, receiver := range []types.Type{named, types.NewPointer(named)} {
		methods := types.NewMethodSet(receiver)
		for i := 0; i < methods.Len(); i++ {
			method := methods.At(i).Obj()
			if method.Exported() {
				result = append(result, method.Type())
			}
		}
	}
	scan.methods[named] = result
	return result
}

func (scan *debtAuthorityTypeScan) debtContainsRawSQLTypeSeen(valueType types.Type, seen map[types.Type]struct{}) bool {
	if valueType == nil {
		return false
	}
	valueType = types.Unalias(valueType)
	if raw, ok := scan.completed[valueType]; ok {
		return raw
	}
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

		switch underlying := typed.Underlying().(type) {
		case *types.Signature, *types.Interface:
			if scan.debtContainsRawSQLTypeSeen(underlying, seen) {
				return true
			}
		case *types.Struct:
			for i := 0; i < underlying.NumFields(); i++ {
				field := underlying.Field(i)
				if field.Exported() && scan.debtContainsRawSQLTypeSeen(field.Type(), seen) {
					return true
				}
			}
		}
		for _, method := range scan.debtExportedMethods(typed) {
			if scan.debtContainsRawSQLTypeSeen(method, seen) {
				return true
			}
		}
		return false
	case *types.Pointer:
		return scan.debtContainsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Slice:
		return scan.debtContainsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Array:
		return scan.debtContainsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Map:
		return scan.debtContainsRawSQLTypeSeen(typed.Key(), seen) || scan.debtContainsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Chan:
		return scan.debtContainsRawSQLTypeSeen(typed.Elem(), seen)
	case *types.Signature:
		return scan.debtTupleContainsRawSQL(typed.Params(), seen) || scan.debtTupleContainsRawSQL(typed.Results(), seen)
	case *types.Struct:
		for index := 0; index < typed.NumFields(); index++ {
			if scan.debtContainsRawSQLTypeSeen(typed.Field(index).Type(), seen) {
				return true
			}
		}
	case *types.Interface:
		for index := 0; index < typed.NumMethods(); index++ {
			if scan.debtContainsRawSQLTypeSeen(typed.Method(index).Type(), seen) {
				return true
			}
		}
	}
	return false
}

func (scan *debtAuthorityTypeScan) debtTupleContainsRawSQL(tuple *types.Tuple, seen map[types.Type]struct{}) bool {
	if tuple == nil {
		return false
	}
	for index := 0; index < tuple.Len(); index++ {
		if scan.debtContainsRawSQLTypeSeen(tuple.At(index).Type(), seen) {
			return true
		}
	}
	return false
}

func debtResolvedTypeString(valueType types.Type) string {
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

func debtRawSQLDispositionAllowed(finding authorityFinding, disposition string) bool {

	path := finding.File
	if finding.Kind == "inactive-raw-source" || finding.Kind == "raw-authority-export" {
		return false
	}
	insidePrivate := strings.HasPrefix(path, "internal/store/internal/")
	insideConstruction := strings.HasPrefix(path, "internal/store/construction/") ||
		strings.HasPrefix(path, "internal/store/selected/") ||
		strings.HasPrefix(path, "internal/store/platformschema/")
	insideInfrastructure := strings.HasPrefix(path, "internal/testpostgres/") ||
		filepath.ToSlash(filepath.Dir(path)) == "internal/testutil"
	if !insidePrivate && !insideConstruction && !insideInfrastructure {
		return false
	}
	switch disposition {
	case "private-backend", "private-runtime-adapter", "private-domain-adapter", "construction-owner", "fixture-2151", "adjacent-2149":
		return true
	default:
		return false
	}
}

func debtAuthorityFindingLess(a, b authorityFinding) bool {
	if a.File != b.File {
		return a.File < b.File
	}
	return a.key() < b.key()
}
