// fixture-codemod migrates this batch's finite, audited conformance patterns.
// It is not a SQL translator or a registry of future migration transforms.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/tools/go/packages"
)

const conformancePackage = "github.com/division-sh/swarm/internal/runtime/conformance"
const storetestPackage = "github.com/division-sh/swarm/internal/store/storetest"

type change struct {
	File        string `json:"file"`
	Function    string `json:"function"`
	Line        int    `json:"line"`
	Field       string `json:"field"`
	start, end  int
	replacement string
}

type migrationFile struct {
	path   string
	source []byte
	edits  []change
}

func main() {
	if err := runCodemod(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runCodemod() error {
	write := flag.Bool("write", false, "apply exact matched rewrites (default: report only)")
	census := flag.String("classify", "", "classify an existing authority-census JSON file without changing source")
	only := flag.String("only", "", "comma-separated audited rewrite families to apply")
	flag.Parse()
	if *census != "" {
		if *write || flag.NArg() != 0 {
			return fmt.Errorf("-classify cannot be combined with a rewrite")
		}
		return classify(*census)
	}
	if flag.NArg() != 1 || flag.Arg(0) != "./internal/runtime/conformance" {
		return fmt.Errorf("usage: go run ./tools/fixture-codemod [-write] [-only families] ./internal/runtime/conformance")
	}
	selected := map[string]bool{}
	for _, family := range strings.Split(*only, ",") {
		if family != "" {
			selected[family] = true
		}
	}
	started := time.Now()
	changes, err := migrateSelected(flag.Arg(0), *write, selected)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Write          bool     `json:"write"`
		ElapsedSeconds float64  `json:"elapsed_seconds"`
		Changes        []change `json:"changes"`
	}{*write, time.Since(started).Seconds(), changes})
}

func classify(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var findings []struct{ Kind, Member string }
	if err := json.Unmarshal(data, &findings); err != nil {
		return err
	}
	counts := map[string]int{}
	for _, finding := range findings {
		if finding.Kind != "raw-operation" {
			continue
		}
		member := finding.Member
		if index := strings.LastIndex(member, "#"); index >= 0 {
			if _, err := strconv.Atoi(member[index+1:]); err == nil {
				member = member[:index]
			}
		}
		counts[patternClass(member)]++
	}
	return json.NewEncoder(os.Stdout).Encode(counts)
}

func patternClass(member string) string {
	if strings.HasPrefix(member, "call:database/sql.") {
		method := member[strings.LastIndex(member, ".")+1:]
		switch method {
		case "Scan", "Next", "Err", "Columns":
			return "cursor-plumbing"
		case "Close":
			if strings.Contains(member, "sql.Rows)") {
				return "cursor-plumbing"
			}
			return "open-and-connection-lifetime"
		case "QueryRow", "QueryRowContext", "Query", "QueryContext":
			return "sql-read-endpoints"
		case "Exec", "ExecContext", "RowsAffected":
			return "sql-write-endpoints"
		case "Begin", "BeginTx", "Commit", "Rollback", "PrepareContext":
			return "transaction-protocol"
		case "Open", "OpenDB", "Ping", "PingContext", "Conn", "Stats", "SetMaxOpenConns", "SetMaxIdleConns", "Driver":
			return "open-and-connection-lifetime"
		default:
			return "unclassified-sql"
		}
	}
	switch member {
	case "call:github.com/division-sh/swarm/internal/store/storetest.DatabaseForTest",
		"call:github.com/division-sh/swarm/internal/store/storetest.Database",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.(*github.com/division-sh/swarm/internal/runtime/pipeline.workflowInstanceStore).testDB":
		return "generic-raw-getter-calls"
	case "call:github.com/division-sh/swarm/internal/testutil.StartPostgres",
		"call:github.com/division-sh/swarm/internal/store/storetest.AdmitPostgresRuntimeStore",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.newPostgresWorkflowInstanceStoreForTest",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.newSQLiteWorkflowInstanceStoreTestDB",
		"call:github.com/division-sh/swarm/internal/runtime/pipeline.newSQLiteWorkflowInstanceStoreForTest",
		"call:github.com/division-sh/swarm/internal/serveapp.selectedRuntimeStoreForTest":
		return "raw-setup-helper-calls"
	default:
		return "other-raw-bearing-shared-helper-calls"
	}
}

func migrateSelected(pattern string, write bool, selected map[string]bool) ([]change, error) {
	if pattern != "./internal/runtime/conformance" {
		return nil, fmt.Errorf("this batch has no transforms for %s", pattern)
	}
	root, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	pkgs, err := packages.Load(&packages.Config{
		Tests: true,
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
	}, pattern)
	if err != nil {
		return nil, err
	}
	if packages.PrintErrors(pkgs) != 0 {
		return nil, fmt.Errorf("package type checking failed; no files changed")
	}
	files, err := collectMigrationFiles(root, pkgs, selected)
	if err != nil {
		return nil, err
	}
	changes := []change{}
	for _, file := range files {
		changes = append(changes, file.edits...)
	}
	if write {
		if err := applyMigrationFiles(files); err != nil {
			return nil, err
		}
	}
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].File == changes[j].File {
			return changes[i].Line < changes[j].Line
		}
		return changes[i].File < changes[j].File
	})
	return changes, nil
}

func collectMigrationFiles(root string, pkgs []*packages.Package, selected map[string]bool) ([]migrationFile, error) {
	var files []migrationFile
	seen := map[string]bool{}
	for _, pkg := range pkgs {
		if pkg.Types == nil || pkg.Types.Path() != conformancePackage {
			continue
		}
		family := ignoredConformanceDatabaseFamily(pkg.TypesInfo, pkg.Syntax)
		for _, file := range pkg.Syntax {
			filename := pkg.Fset.Position(file.Pos()).Filename
			if seen[filename] || !strings.HasSuffix(filename, "_test.go") {
				continue
			}
			seen[filename] = true
			plan, err := collectMigrationFile(root, filename, pkg, file, selected, family)
			if err != nil {
				return nil, err
			}
			files = append(files, plan)
		}
	}
	return files, nil
}

func collectMigrationFile(root, filename string, pkg *packages.Package, file *ast.File, selected map[string]bool, family map[*types.Func]bool) (migrationFile, error) {
	source, err := os.ReadFile(filename)
	if err != nil {
		return migrationFile{}, err
	}
	plan := migrationFile{path: filename, source: source}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		replacement, field, matched := rewriteSelectedConformanceOwner(pkg.Fset, pkg.TypesInfo, fn, selected, family)
		if matched {
			edit, err := migrationChange(root, filename, pkg.Fset, fn, fn.Name.Name, field, replacement)
			if err != nil {
				return migrationFile{}, err
			}
			plan.edits = append(plan.edits, edit)
			continue
		}
		edits, err := collectConformanceCallChanges(root, filename, pkg, fn, selected, family)
		if err != nil {
			return migrationFile{}, err
		}
		plan.edits = append(plan.edits, edits...)
	}
	return plan, nil
}

func collectConformanceCallChanges(root, filename string, pkg *packages.Package, fn *ast.FuncDecl, selected map[string]bool, family map[*types.Func]bool) ([]change, error) {
	var edits []change
	var failure error
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if failure != nil {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		field, replacement := rewriteConformanceCall(pkg.Fset, pkg.TypesInfo, fn, call, family)
		if field == "" || len(selected) != 0 && !selected[field] {
			return true
		}
		var edit change
		edit, failure = migrationChange(root, filename, pkg.Fset, call, fn.Name.Name, field, replacement)
		if failure != nil {
			return false
		}
		edits = append(edits, edit)
		return false
	})
	return edits, failure
}

func rewriteConformanceCall(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, call *ast.CallExpr, family map[*types.Func]bool) (string, string) {
	id, _ := call.Fun.(*ast.Ident)
	var called *types.Func
	if id != nil {
		called, _ = info.Uses[id].(*types.Func)
	}
	if family[called] && ignoredConformanceDatabaseCall(info, call) {
		return "UnusedConformanceDatabase", renderCallWithoutArgument(fset, call, 2)
	}
	if owner, ok := matchConformanceMutationColumnOwner(info, fn, call); ok {
		return "CanonicalMutationColumnOwner", fmt.Sprintf("requireMutationSurface(%s, %s)", goNodeString(fset, call.Args[0]), owner)
	}
	if index, ok := matchNotifyExecutionCaller(info, call); ok {
		return "NotifyExecutionReadOwner", renderCallWithoutArgument(fset, call, index)
	}
	if helper, ok := matchNotifyObserverOwner(info, call); ok {
		return helper, renderCallWithoutArgument(fset, call, 3)
	}
	return "", ""
}

func renderCallWithoutArgument(fset *token.FileSet, call *ast.CallExpr, index int) string {
	var args []string
	for i, arg := range call.Args {
		if i != index {
			args = append(args, goNodeString(fset, arg))
		}
	}
	if call.Ellipsis.IsValid() {
		args[len(args)-1] += "..."
	}
	return fmt.Sprintf("%s(%s)", goNodeString(fset, call.Fun), strings.Join(args, ", "))
}

func goNodeString(fset *token.FileSet, node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, fset, node); err != nil {
		panic(err)
	}
	return out.String()
}

func migrationChange(root, filename string, fset *token.FileSet, node ast.Node, function, field, replacement string) (change, error) {
	relative, err := filepath.Rel(root, filename)
	if err != nil {
		return change{}, err
	}
	position := fset.Position(node.Pos())
	return change{File: relative, Function: function, Line: position.Line, Field: field,
		start: position.Offset, end: fset.Position(node.End()).Offset, replacement: replacement}, nil
}

// Format every file before writing any of them. Finite family refusal must not
// leave earlier files rewritten because a later planned edit cannot be rendered.
func applyMigrationFiles(files []migrationFile) error {
	prepared := make([]migrationFile, 0, len(files))
	for _, file := range files {
		if len(file.edits) == 0 {
			continue
		}
		updated, err := formatMigrationFile(file)
		if err != nil {
			return err
		}
		prepared = append(prepared, migrationFile{path: file.path, source: updated})
	}
	for _, file := range prepared {
		if err := os.WriteFile(file.path, file.source, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func formatMigrationFile(file migrationFile) ([]byte, error) {
	sort.Slice(file.edits, func(i, j int) bool { return file.edits[i].start > file.edits[j].start })
	updated := file.source
	for _, edit := range file.edits {
		updated = append(append(append([]byte{}, updated[:edit.start]...), []byte(edit.replacement)...), updated[edit.end:]...)
	}
	return format.Source(updated)
}

func rewriteSelectedConformanceOwner(fset *token.FileSet, info *types.Info, fn *ast.FuncDecl, selected map[string]bool, unusedDatabaseFamily map[*types.Func]bool) (string, string, bool) {
	if len(selected) == 0 || selected["ConformanceMutationProjection"] {
		if replacement, matched := rewriteConformanceMutationProjectionOwner(fset, info, fn); matched {
			return replacement, "ConformanceMutationProjection", true
		}
	}
	if len(selected) == 0 || selected["UnusedConformanceDatabase"] {
		if replacement, matched := rewriteIgnoredConformanceDatabase(fset, info, fn, unusedDatabaseFamily); matched {
			return replacement, "UnusedConformanceDatabase", true
		}
	}
	if len(selected) == 0 || selected["CanonicalStorageColumns"] {
		if replacement, matched := rewriteConformanceColumnOwner(fset, info, fn); matched {
			return replacement, "CanonicalStorageColumns", true
		}
	}
	return "", "", false
}

func matchNotifyObserverOwner(info *types.Info, call *ast.CallExpr) (string, bool) {
	name, ok := call.Fun.(*ast.Ident)
	if !ok {
		return "", false
	}
	fn, ok := info.Uses[name].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != conformancePackage {
		return "", false
	}
	count := 0
	switch fn.Name() {
	case "dumpNotifyAllChildrenRuntimeState":
		count = 4
	case "loadNotifyAllChildrenItemEvents":
		count = 6
	case "assertNotifyAllChildrenMetadata":
		count = 7
	default:
		return "", false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Variadic() || sig.Params().Len() != count || len(call.Args) != count || call.Ellipsis.IsValid() || !pureFixtureReference(call.Args[3]) {
		return "", false
	}
	if types.TypeString(sig.Params().At(3).Type(), func(p *types.Package) string { return p.Path() }) != "*database/sql.DB" {
		return "", false
	}
	return fn.Name(), true
}

func pureFixtureReference(expr ast.Expr) bool {
	switch value := expr.(type) {
	case *ast.Ident:
		return true
	case *ast.SelectorExpr:
		return pureFixtureReference(value.X)
	default:
		return false
	}
}

func storetestAlias(file *ast.File) string {
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != storetestPackage {
			continue
		}
		if imp.Name == nil {
			return "storetest"
		}
		if imp.Name.Name != "." && imp.Name.Name != "_" {
			return imp.Name.Name
		}
	}
	return ""
}
