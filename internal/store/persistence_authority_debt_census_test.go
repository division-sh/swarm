package store_test

// Scanner mechanics extracted from B's approved #2151 integration census.
// The recorded-inventory guards keep their original scope; this collector
// supplies the separately enforced no-new-debt guard and final broad census.

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/build"
	"go/build/constraint"
	"go/format"
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
	sources := debtOwnedAuthoritySources(t, root)
	fset := token.NewFileSet()
	seenFiles := map[string]bool{}
	activeFiles := map[string]bool{}
	fileOccurrences := map[string]map[string][]authorityFinding{}
	methodSets := map[string]authorityFinding{}
	packagesByDir := map[string]*packages.Package{}
	imports := map[string]*types.Package{}
	var findings []authorityFinding
	scan := debtNewAuthorityTypeScan()
	{
		context := build.Default
		context.GOOS, context.GOARCH, context.CgoEnabled = "linux", "amd64", true
		context.BuildTags = nil
		context.ToolTags = []string{"amd64.v1"}
		dirs := map[string]bool{}
		for path := range sources {
			matches, err := context.MatchFile(filepath.Dir(path), filepath.Base(path))
			if err != nil {
				t.Fatalf("select owned authority source %s: %v", path, err)
			}
			if matches {
				activeFiles[path] = true
				dir, err := filepath.Rel(root, filepath.Dir(path))
				if err != nil {
					t.Fatal(err)
				}
				dirs["./"+filepath.ToSlash(dir)] = true
			}
		}
		var patterns []string
		for dir := range dirs {
			patterns = append(patterns, dir)
		}
		sort.Strings(patterns)
		if len(patterns) == 0 {
			t.Fatal("authority census matched no packages")
		}
		patterns = append(patterns, "database/sql", "context")
		cfg := &packages.Config{
			Dir: root, Env: debtCensusEnvironment(), Fset: fset,
			BuildFlags: []string{"-trimpath"},
			Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
				packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Tests: true,
		}
		pkgs, err := packages.Load(cfg, patterns...)
		if err != nil {
			t.Fatalf("load authority packages: %v", err)
		}
		if packages.PrintErrors(pkgs) > 0 {
			t.Fatal("load authority packages reported type errors")
		}
		for _, pkg := range pkgs {
			debtRememberAuthorityImports(pkg, imports)
			if len(pkg.Syntax) != len(pkg.CompiledGoFiles) {
				t.Fatalf("incomplete authority syntax for %s: syntax=%d compiled=%d", pkg.ID, len(pkg.Syntax), len(pkg.CompiledGoFiles))
			}
			for index, file := range pkg.Syntax {
				if index >= len(pkg.CompiledGoFiles) {
					continue
				}
				path := filepath.Clean(pkg.CompiledGoFiles[index])
				rel, owned := sources[path]
				if !owned {
					continue
				}
				packagesByDir[filepath.Dir(path)+"\t"+file.Name.Name] = pkg
				seenFiles[path] = true
				fileFindings := scan.debtCollectAuthorityFindings(rel, file, pkg.TypesInfo)
				if !debtAuthorityTypedContractScope(rel) {
					fileFindings = slices.DeleteFunc(fileFindings, func(finding authorityFinding) bool {
						return !finding.RawSQL && finding.Kind != "forbidden-test-consumption" && finding.Kind != "selected-store-construction"
					})
				}
				// Preserve each variant's evidence and within-file multiplicity.
				// Re-loading the same source cannot multiply its occurrences.
				if fileOccurrences[path] == nil {
					fileOccurrences[path] = map[string][]authorityFinding{}
				}
				variant := map[string][]authorityFinding{}
				for _, finding := range fileFindings {
					key := finding.key()
					variant[key] = append(variant[key], finding)
				}
				for key, occurrences := range variant {
					if len(occurrences) > len(fileOccurrences[path][key]) {
						fileOccurrences[path][key] = occurrences
					}
				}
			}
			for _, finding := range debtCollectEffectiveMethodSetFindings(root, pkg) {
				methodSets[finding.key()] = finding
			}
		}
	}
	for _, identities := range fileOccurrences {
		for _, occurrences := range identities {
			findings = append(findings, occurrences...)
		}
	}
	for path := range activeFiles {
		if !seenFiles[path] {
			t.Fatalf("canonical active authority source was not loaded: %s", sources[path])
		}
	}
	if len(seenFiles) == 0 {
		t.Fatal("authority census matched no packages")
	}
	for _, finding := range methodSets {
		findings = append(findings, finding)
	}
	var inactive []string
	for path := range sources {
		if !seenFiles[path] {
			inactive = append(inactive, path)
		}
	}
	sort.Strings(inactive)
	for _, path := range inactive {
		file, err := parser.ParseFile(fset, path, nil, parser.AllErrors|parser.ParseComments)
		if err != nil {
			t.Fatalf("parse inactive authority source: %v", err)
		}
		base := packagesByDir[filepath.Dir(path)+"\t"+file.Name.Name]
		var owner *types.Package
		if base != nil {
			owner = base.Types
		}
		info := debtPartialExcludedAuthorityInfo(file, fset, owner, imports)
		fileFindings := scan.debtCollectAuthorityFindings(sources[path], file, info)
		if !debtAuthorityTypedContractScope(sources[path]) {
			fileFindings = slices.DeleteFunc(fileFindings, func(finding authorityFinding) bool {
				return !finding.RawSQL && finding.Kind != "forbidden-test-consumption" && finding.Kind != "selected-store-construction"
			})
		}
		fileFindings = append(fileFindings, debtExcludedSourceFindings(t, sources[path], file, info)...)
		findings = append(findings, fileFindings...)
		seenFiles[path] = true
	}
	if len(seenFiles) != len(sources) {
		t.Fatalf("incomplete owned authority enumeration: sources=%d collected=%d", len(sources), len(seenFiles))
	}
	sort.Slice(findings, func(i, j int) bool { return debtAuthorityFindingLess(findings[i], findings[j]) })
	return findings
}

func debtCensusEnvironment() []string {
	var env []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "GOFLAGS", "GOWORK", "GOOS", "GOARCH", "GOAMD64", "GOEXPERIMENT", "CGO_ENABLED":
		default:
			env = append(env, value)
		}
	}
	return append(env, "GOFLAGS=", "GOWORK=off", "GOOS=linux", "GOARCH=amd64", "GOAMD64=v1", "GOEXPERIMENT=", "CGO_ENABLED=1")
}

func debtOwnedAuthoritySources(t *testing.T, root string) map[string]string {
	t.Helper()
	sources := map[string]string{}
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
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		sources[abs] = rel
		return nil
	})
	if err != nil {
		t.Fatalf("enumerate owned persistence authority sources: %v", err)
	}
	return sources
}

func debtRememberAuthorityImports(pkg *packages.Package, imports map[string]*types.Package) {
	if pkg == nil || pkg.Types == nil || imports[pkg.PkgPath] != nil {
		return
	}
	imports[pkg.PkgPath] = pkg.Types
	for _, imported := range pkg.Imports {
		debtRememberAuthorityImports(imported, imports)
	}
}

type debtAuthorityImporter map[string]*types.Package

func (imports debtAuthorityImporter) Import(path string) (*types.Package, error) {
	if pkg := imports[path]; pkg != nil {
		return pkg, nil
	}
	return nil, fmt.Errorf("%s is not available in the canonical active type view", path)
}

func debtPartialExcludedAuthorityInfo(file *ast.File, fset *token.FileSet, base *types.Package, imports map[string]*types.Package) *types.Info {
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	if base == nil {
		return info
	}
	declared := map[string]bool{}
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if decl.Recv == nil {
				declared[decl.Name.Name] = true
			}
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					declared[spec.Name.Name] = true
				case *ast.ValueSpec:
					for _, name := range spec.Names {
						declared[name.Name] = true
					}
				}
			}
		}
	}
	pkg := types.NewPackage(base.Path(), base.Name())
	for _, name := range base.Scope().Names() {
		if !declared[name] {
			pkg.Scope().Insert(base.Scope().Lookup(name))
		}
	}
	// This is partial evidence from the canonical scope, never foreign-platform
	// validation. AST uncertainty accounting below does not depend on its success.
	conf := &types.Config{Importer: debtAuthorityImporter(imports), Error: func(error) {}}
	partial := *file
	partial.Decls = nil
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv != nil {
			// A receiver copied from another checker has no local declaration
			// table. Its complete source is covered by AST uncertainty instead.
			continue
		}
		partial.Decls = append(partial.Decls, declaration)
	}
	_ = types.NewChecker(conf, fset, pkg, info).Files([]*ast.File{&partial})
	return info
}

func debtExcludedSourceFindings(t *testing.T, path string, file *ast.File, info *types.Info) []authorityFinding {
	t.Helper()
	var context []string
	for _, imported := range file.Imports {
		name := ""
		if imported.Name != nil {
			name = imported.Name.Name
		}
		value, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatalf("invalid excluded-source import %s: %v", path, err)
		}
		context = append(context, "import:"+name+":"+value)
	}
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !constraint.IsGoBuild(comment.Text) && !constraint.IsPlusBuild(comment.Text) {
				continue
			}
			expr, err := constraint.Parse(comment.Text)
			if err != nil {
				t.Fatalf("invalid excluded-source build constraint %s: %v", path, err)
			}
			context = append(context, "build:"+expr.String())
		}
	}
	sort.Strings(context)
	contextDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(context, "\n"))))
	refusals := map[types.Object]bool{}
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && debtExcludedTypedRefusal(function, info) {
			if object := info.Defs[function.Name]; object != nil {
				refusals[object] = true
			}
		}
	}
	var findings []authorityFinding
	record := func(node ast.Node, enclosing string) {
		if debtExcludedDeclarationIsLiteralOnly(node, info) || debtExcludedRefusalForwarder(node, info, refusals) || debtExcludedReadCloserFileRefusal(node, info) {
			return
		}
		// Comments and source positions do not define a permission or occurrence.
		ast.Inspect(node, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncDecl:
				node.Doc = nil
			case *ast.GenDecl:
				node.Doc = nil
			case *ast.TypeSpec:
				node.Doc, node.Comment = nil, nil
			case *ast.ValueSpec:
				node.Doc, node.Comment = nil, nil
			case *ast.Field:
				node.Doc, node.Comment = nil, nil
			}
			return true
		})
		var normalized bytes.Buffer
		if err := format.Node(&normalized, token.NewFileSet(), node); err != nil {
			t.Fatalf("normalize excluded authority source %s: %v", path, err)
		}
		findings = append(findings, authorityFinding{
			Kind: "unresolved-excluded-source", File: path, Enclosing: enclosing,
			Member:   fmt.Sprintf("declaration:%x", sha256.Sum256(normalized.Bytes())),
			Resolved: "unresolved|source-context:" + contextDigest,
		})
	}
	for _, declaration := range file.Decls {
		switch declaration := declaration.(type) {
		case *ast.FuncDecl:
			record(declaration, declaration.Name.Name)
		case *ast.GenDecl:
			if declaration.Tok == token.IMPORT {
				continue
			}
			for _, spec := range declaration.Specs {
				name := "package"
				if spec, ok := spec.(*ast.TypeSpec); ok {
					name = spec.Name.Name
				}
				record(&ast.GenDecl{Tok: declaration.Tok, Specs: []ast.Spec{spec}}, name)
			}
		}
	}
	return findings
}

func debtExcludedConstant(expr ast.Expr, info *types.Info) bool {
	switch expr := expr.(type) {
	case *ast.BasicLit:
		return true
	case *ast.Ident:
		_, constant := info.Uses[expr].(*types.Const)
		return constant || info.Uses[expr] == types.Universe.Lookup("nil")
	case *ast.SelectorExpr:
		_, constant := info.Uses[expr.Sel].(*types.Const)
		return constant
	}
	return false
}

// This closed transport shape either refuses a native file or returns the same
// io.ReadCloser unchanged. Every type, binding and executable statement is
// checked; file names, build tags and declaration digests grant no permission.
func debtExcludedReadCloserFileRefusal(node ast.Node, info *types.Info) bool {
	fn, ok := node.(*ast.FuncDecl)
	if !ok || fn.Recv != nil || fn.Type.TypeParams != nil || fn.Body == nil || len(fn.Body.List) != 2 {
		return false
	}
	object := info.Defs[fn.Name]
	if object == nil {
		return false
	}
	sig, ok := object.Type().(*types.Signature)
	if !ok || sig.Variadic() || sig.Params().Len() != 1 || sig.Results().Len() != 2 {
		return false
	}
	readCloser := func(t types.Type) bool {
		named, ok := types.Unalias(t).(*types.Named)
		return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "io" && named.Obj().Name() == "ReadCloser"
	}
	if !readCloser(sig.Params().At(0).Type()) || !types.Identical(sig.Params().At(0).Type(), sig.Results().At(0).Type()) || !types.Identical(sig.Results().At(1).Type(), types.Universe.Lookup("error").Type()) {
		return false
	}
	input := sig.Params().At(0)
	isInput := func(expr ast.Expr) bool {
		id, ok := expr.(*ast.Ident)
		return ok && info.Uses[id] == input
	}
	isNil := func(expr ast.Expr) bool {
		id, ok := expr.(*ast.Ident)
		return ok && info.Uses[id] == types.Universe.Lookup("nil")
	}
	guard, ok := fn.Body.List[0].(*ast.IfStmt)
	if !ok || guard.Else != nil || len(guard.Body.List) != 1 {
		return false
	}
	assign, ok := guard.Init.(*ast.AssignStmt)
	if !ok || assign.Tok != token.DEFINE || len(assign.Lhs) != 2 || len(assign.Rhs) != 1 {
		return false
	}
	discard, ok := assign.Lhs[0].(*ast.Ident)
	if !ok || discard.Name != "_" {
		return false
	}
	native, ok := assign.Lhs[1].(*ast.Ident)
	if !ok || native.Name == "_" || info.Defs[native] == nil {
		return false
	}
	condition, ok := guard.Cond.(*ast.Ident)
	if !ok || info.Uses[condition] != info.Defs[native] {
		return false
	}
	assertion, ok := assign.Rhs[0].(*ast.TypeAssertExpr)
	if !ok || assertion.Type == nil || !isInput(assertion.X) || info.TypeOf(assertion.Type) == nil {
		return false
	}
	pointer, ok := types.Unalias(info.TypeOf(assertion.Type)).(*types.Pointer)
	if !ok {
		return false
	}
	file, ok := types.Unalias(pointer.Elem()).(*types.Named)
	if !ok || file.Obj().Pkg() == nil || file.Obj().Pkg().Path() != "os" || file.Obj().Name() != "File" {
		return false
	}
	refusal, ok := guard.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(refusal.Results) != 2 || !isNil(refusal.Results[0]) {
		return false
	}
	call, ok := refusal.Results[1].(*ast.CallExpr)
	if !ok || call.Ellipsis.IsValid() || len(call.Args) != 1 {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	callee, ok := info.Uses[selector.Sel].(*types.Func)
	if !ok || callee.Pkg() == nil || callee.Pkg().Path() != "fmt" || callee.Name() != "Errorf" {
		return false
	}
	message, ok := call.Args[0].(*ast.BasicLit)
	if !ok || message.Kind != token.STRING {
		return false
	}
	text, err := strconv.Unquote(message.Value)
	if err != nil || strings.TrimSpace(text) == "" {
		return false
	}
	forward, ok := fn.Body.List[1].(*ast.ReturnStmt)
	return ok && len(forward.Results) == 2 && isInput(forward.Results[0]) && isNil(forward.Results[1])
}

func debtExcludedTypedRefusal(function *ast.FuncDecl, info *types.Info) bool {
	if function.Recv != nil || function.Body == nil || len(function.Body.List) != 1 {
		return false
	}
	ret, ok := function.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 2 || !debtExcludedConstant(ret.Results[0], info) {
		return false
	}
	pointer, ok := ret.Results[1].(*ast.UnaryExpr)
	if !ok || pointer.Op != token.AND {
		return false
	}
	literal, ok := pointer.X.(*ast.CompositeLit)
	object := info.Defs[function.Name]
	if !ok || object == nil || info.TypeOf(pointer) == nil || debtContainsRawAuthoritySignature(object.Type()) || debtContainsRawAuthoritySignature(info.TypeOf(pointer)) {
		return false
	}
	errorType := types.Universe.Lookup("error").Type().Underlying().(*types.Interface)
	if !types.Implements(info.TypeOf(pointer), errorType) {
		return false
	}
	for _, element := range literal.Elts {
		if pair, ok := element.(*ast.KeyValueExpr); ok {
			element = pair.Value
		}
		if !debtExcludedConstant(element, info) {
			return false
		}
	}
	return true
}

func debtExcludedRefusalForwarder(node ast.Node, info *types.Info, refusals map[types.Object]bool) bool {
	function, ok := node.(*ast.FuncDecl)
	if !ok || function.Recv != nil || function.Body == nil || len(function.Body.List) != 2 {
		return false
	}
	object := info.Defs[function.Name]
	if object == nil || debtContainsRawAuthoritySignature(object.Type()) {
		return false
	}
	assignment, ok := function.Body.List[0].(*ast.AssignStmt)
	if !ok || assignment.Tok != token.DEFINE || len(assignment.Lhs) != 2 || len(assignment.Rhs) != 1 {
		return false
	}
	discard, ok := assignment.Lhs[0].(*ast.Ident)
	if !ok || discard.Name != "_" {
		return false
	}
	errName, ok := assignment.Lhs[1].(*ast.Ident)
	if !ok || errName.Name == "_" {
		return false
	}
	call, ok := assignment.Rhs[0].(*ast.CallExpr)
	if !ok {
		return false
	}
	callee, ok := call.Fun.(*ast.Ident)
	if !ok || !refusals[info.Uses[callee]] {
		return false
	}
	for _, arg := range call.Args {
		if !debtExcludedConstant(arg, info) {
			return false
		}
	}
	ret, ok := function.Body.List[1].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 2 || !debtExcludedConstant(ret.Results[0], info) {
		return false
	}
	returned, ok := ret.Results[1].(*ast.Ident)
	return ok && info.Defs[errName] != nil && info.Uses[returned] == info.Defs[errName]
}

func debtExcludedDeclarationIsLiteralOnly(node ast.Node, info *types.Info) bool {
	// A deliberately narrow, source-evidenced benign classification. References,
	// calls, selectors, aliases, callbacks and non-literal bodies stay uncertain.
	literal := func(expr ast.Expr) bool {
		_, ok := expr.(*ast.BasicLit)
		return ok
	}
	builtin := func(expr ast.Expr) bool {
		ident, ok := expr.(*ast.Ident)
		if !ok {
			return false
		}
		object, ok := info.Uses[ident].(*types.TypeName)
		if !ok || object != types.Universe.Lookup(ident.Name) {
			return false
		}
		_, ok = object.Type().(*types.Basic)
		return ok
	}
	switch node := node.(type) {
	case *ast.GenDecl:
		for _, spec := range node.Specs {
			values, ok := spec.(*ast.ValueSpec)
			if !ok || (values.Type != nil && !builtin(values.Type)) || len(values.Values) == 0 {
				return false
			}
			for _, value := range values.Values {
				if !literal(value) {
					return false
				}
			}
		}
		return true
	case *ast.FuncDecl:
		if node.Recv != nil || node.Type.TypeParams != nil || node.Body == nil {
			return false
		}
		for _, list := range []*ast.FieldList{node.Type.Params, node.Type.Results} {
			if list != nil {
				for _, field := range list.List {
					if !builtin(field.Type) {
						return false
					}
				}
			}
		}
		for _, statement := range node.Body.List {
			ret, ok := statement.(*ast.ReturnStmt)
			if !ok {
				return false
			}
			for _, expr := range ret.Results {
				if !literal(expr) {
					return false
				}
			}
		}
		return true
	}
	return false
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
					for _, value := range typed.Values {
						findings = append(findings, scan.debtCollectContextAuthorityFindings(path, "package", value, info)...)
						findings = append(findings, scan.debtCollectOperationAuthorityFindings(path, "package", value, info)...)
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

func (scan *debtAuthorityTypeScan) debtCollectOperationAuthorityFindings(path, enclosing string, body ast.Node, info *types.Info) []authorityFinding {
	if body == nil || body == (*ast.BlockStmt)(nil) {
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
	if callType != nil {
		if signature, ok := types.Unalias(callType).Underlying().(*types.Signature); ok {
			resolved += "|results=" + debtResolvedTupleString(signature.Results())
		}
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

func (scan *debtAuthorityTypeScan) debtCollectContextAuthorityFindings(path, enclosing string, body ast.Node, info *types.Info) []authorityFinding {
	if body == nil || body == (*ast.BlockStmt)(nil) {
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
