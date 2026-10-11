// pipeline-observations applies only this batch's reviewed function snapshots.
package main

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

//go:embed recipes.json
var recipeBytes []byte

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string              `json:"Successor,omitempty"`
	Removed                               bool                `json:"Removed,omitempty"`
	Mechanical                            *mechanicalSnapshot `json:"Mechanical,omitempty"`
}

// Mechanical is proof history only; migrate never applies it to current source.
type mechanicalSnapshot struct {
	SourceCommit string
	BeforeSHA256 string
	After        string
}

type pendingFile struct {
	path string
	data []byte
}

func main() {
	write := flag.Bool("write", false, "apply the finite reviewed rewrites after complete preflight")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: go run ./tools/fixture-codemod/pipeline-observations [-write]")
		os.Exit(1)
	}
	if err := migrate(*write); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func migrate(write bool) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	var recipes []recipe
	if err := json.Unmarshal(recipeBytes, &recipes); err != nil {
		return err
	}
	files, changed, err := prepareFiles(root, recipes)
	if err != nil {
		return err
	}
	if err := applyPendingFiles(root, files, write); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Write   bool
		Changes []recipe
	}{write, changed})
}

func applyPendingFiles(root string, files []pendingFile, write bool) error {
	if len(files) != 0 {
		if err := checkTypes(root, files); err != nil {
			return err
		}
	}
	if write {
		for _, file := range files {
			if err := os.WriteFile(file.path, file.data, 0644); err != nil {
				return err
			}
		}
	}
	return nil
}

func prepareFiles(root string, recipes []recipe) ([]pendingFile, []recipe, error) {
	grouped := map[string][]recipe{}
	for _, row := range recipes {
		if !filepath.IsLocal(row.File) || filepath.ToSlash(filepath.Clean(row.File)) != row.File {
			return nil, nil, fmt.Errorf("noncanonical recipe path %q", row.File)
		}
		if err := validateRecipeSnapshots(row); err != nil {
			return nil, nil, err
		}
		grouped[row.File] = append(grouped[row.File], row)
	}
	var paths []string
	for path := range grouped {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var files []pendingFile
	changed := []recipe{}
	for _, path := range paths {
		absolute := filepath.Join(root, path)
		info, err := os.Lstat(absolute)
		if err != nil {
			if os.IsNotExist(err) && allRecipesRemoved(grouped[path]) {
				continue
			}
			return nil, nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, nil, fmt.Errorf("recipe source is not a regular file: %s", path)
		}
		source, err := os.ReadFile(absolute)
		if err != nil {
			return nil, nil, err
		}
		current, err := checkpointRecipes(source, grouped[path])
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", path, err)
		}
		result, applied, err := rewriteSource(path, source, current)
		if err != nil {
			return nil, nil, err
		}
		changed = append(changed, applied...)
		if len(applied) != 0 {
			files = append(files, pendingFile{absolute, result})
		}
	}
	return files, changed, nil
}

func validateRecipeSnapshots(row recipe) error {
	if row.Removed && row.Successor != "" {
		return fmt.Errorf("retired recipe also has a successor: %s", row.Function)
	}
	for _, source := range []string{row.Before, row.After} {
		if _, err := canonicalFunction(source); err != nil {
			return err
		}
	}
	if row.Mechanical != nil {
		if len(row.Mechanical.SourceCommit) != 40 || strings.Trim(row.Mechanical.SourceCommit, "0123456789abcdef") != "" ||
			row.Mechanical.BeforeSHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) {
			return fmt.Errorf("invalid mechanical snapshot provenance: %s", row.Function)
		}
		if _, err := canonicalFunction(row.Mechanical.After); err != nil {
			return fmt.Errorf("invalid mechanical snapshot: %s: %w", row.Function, err)
		}
	}
	if row.Successor != "" {
		_, err := canonicalFunction(row.Successor)
		return err
	}
	return nil
}

func allRecipesRemoved(rows []recipe) bool {
	for _, row := range rows {
		if !row.Removed {
			return false
		}
	}
	return len(rows) > 0
}

// Historical transformations stay pinned; later owner retirement has its own cut.
func checkpointRecipes(source []byte, rows []recipe) ([]recipe, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "checkpoint.go", source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var current []recipe
	for _, row := range rows {
		if row.Removed {
			if row.Successor != "" {
				return nil, fmt.Errorf("retired recipe also has a successor: %s", row.Function)
			}
			if _, err := uniqueRecipeFunction(file, row); err == nil || !strings.HasPrefix(err.Error(), "missing original/replacement function ") {
				return nil, fmt.Errorf("retired function survives or is ambiguous: %s", row.Function)
			}
			continue
		}
		if row.Successor != "" {
			row.Before, row.After = row.After, row.Successor
			row.Successor = ""
		}
		current = append(current, row)
	}
	return current, nil
}

func rewriteSource(path string, source []byte, recipes []recipe) ([]byte, []recipe, error) {
	changed := []recipe{}
	for _, row := range recipes {
		updated, applied, err := rewriteFunction(path, source, row)
		if err != nil {
			return nil, nil, err
		}
		source = updated
		if applied {
			row.Before, row.After = "", ""
			changed = append(changed, row)
		}
	}
	if len(changed) == 0 {
		return source, changed, nil
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source, parser.ParseComments|parser.AllErrors)
	if err != nil {
		return nil, nil, err
	}
	const storetestImport = "github.com/division-sh/swarm/internal/store/storetest"
	hasStoretest := false
	for _, imported := range file.Imports {
		if imported.Path.Value == `"`+storetestImport+`"` {
			hasStoretest = true
		}
	}
	if !hasStoretest {
		astutil.AddImport(set, file, storetestImport)
	}
	var out bytes.Buffer
	if err := format.Node(&out, set, file); err != nil {
		return nil, nil, err
	}
	result, err := imports.Process(path, out.Bytes(), &imports.Options{Comments: true, TabIndent: true, TabWidth: 8})
	return result, changed, err
}

func rewriteFunction(path string, source []byte, row recipe) ([]byte, bool, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source, parser.AllErrors)
	if err != nil {
		return nil, false, err
	}
	fn, err := uniqueRecipeFunction(file, row)
	if err != nil {
		return nil, false, fmt.Errorf("%s: %w", path, err)
	}
	start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
	before, err := canonicalFunction(row.Before)
	if err != nil {
		return nil, false, err
	}
	after, err := canonicalFunction(row.After)
	if err != nil {
		return nil, false, err
	}
	actual, err := canonicalFunction(string(source[start:end]))
	if err != nil {
		return nil, false, err
	}
	if actual == after {
		return source, false, nil
	}
	if actual != before {
		return nil, false, fmt.Errorf("%s: %s differs from both reviewed snapshots; no files written", path, row.Function)
	}
	return bytes.Join([][]byte{source[:start], []byte(row.After), source[end:]}, nil), true, nil
}

func uniqueRecipeFunction(file *ast.File, row recipe) (*ast.FuncDecl, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "after.go", "package probe\n"+row.After, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	if len(parsed.Decls) != 1 {
		return nil, fmt.Errorf("recipe must contain exactly one replacement function")
	}
	after, ok := parsed.Decls[0].(*ast.FuncDecl)
	if !ok {
		return nil, fmt.Errorf("replacement is not a function")
	}
	var found *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != row.Function && fn.Name.Name != after.Name.Name) || (after.Recv != nil && !sameRecipeReceiver(fn, after)) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("ambiguous original/replacement function %s", row.Function)
		}
		found = fn
	}
	if found == nil {
		return nil, fmt.Errorf("missing original/replacement function %s", row.Function)
	}
	return found, nil
}

func sameRecipeReceiver(left, right *ast.FuncDecl) bool {
	if left.Recv == nil || right.Recv == nil {
		return left.Recv == nil && right.Recv == nil
	}
	if len(left.Recv.List) != 1 || len(right.Recv.List) != 1 {
		return false
	}
	var first, second bytes.Buffer
	if err := format.Node(&first, token.NewFileSet(), left.Recv.List[0].Type); err != nil {
		return false
	}
	if err := format.Node(&second, token.NewFileSet(), right.Recv.List[0].Type); err != nil {
		return false
	}
	return first.String() == second.String()
}

func uniqueFunction(file *ast.File, name string) (*ast.FuncDecl, error) {
	var found *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("ambiguous function %s", name)
		}
		found = fn
	}
	if found == nil {
		return nil, fmt.Errorf("missing function %s", name)
	}
	return found, nil
}

func canonicalFunction(source string) (string, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "function.go", "package probe\n"+source, parser.AllErrors)
	if err != nil {
		return "", err
	}
	if len(file.Decls) != 1 {
		return "", fmt.Errorf("recipe must contain exactly one function")
	}
	fn, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok {
		return "", fmt.Errorf("recipe is not a function")
	}
	var out bytes.Buffer
	err = format.Node(&out, token.NewFileSet(), fn)
	return out.String(), err
}

func checkTypes(root string, files []pendingFile) error {
	overlay, patterns, err := candidateTypeCheckInputs(root, files)
	if err != nil {
		return fmt.Errorf("candidate type checking failed: %w; no files written", err)
	}
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Tests: true, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedTypes | packages.NeedImports,
	}, patterns...)
	if err != nil {
		return fmt.Errorf("candidate type checking failed: %w; no files written", err)
	}
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		return fmt.Errorf("candidate type checking failed; no files written")
	}
	covered := make(map[string]bool)
	for _, pkg := range pkgs {
		if pkg.Types == nil {
			continue
		}
		for _, path := range pkg.CompiledGoFiles {
			covered[filepath.Clean(path)] = true
		}
	}
	for _, file := range files {
		path, err := filepath.Abs(file.path)
		if err != nil || !covered[path] {
			return fmt.Errorf("candidate type checking failed: %s is not represented in a typed package variant; no files written", file.path)
		}
	}
	return nil
}

func candidateTypeCheckInputs(root string, files []pendingFile) (map[string][]byte, []string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, nil, err
	}
	if len(files) == 0 {
		return nil, nil, fmt.Errorf("no pending Go files")
	}
	overlay := make(map[string][]byte)
	directories := make(map[string]bool)
	for _, file := range files {
		path, err := filepath.Abs(file.path)
		if err != nil {
			return nil, nil, err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || !filepath.IsLocal(relative) || !strings.HasSuffix(relative, ".go") {
			return nil, nil, fmt.Errorf("candidate must be a Go file within the checkout: %s", file.path)
		}
		if _, duplicate := overlay[path]; duplicate {
			return nil, nil, fmt.Errorf("duplicate candidate file: %s", file.path)
		}
		overlay[path] = file.data
		directories["./"+filepath.ToSlash(filepath.Dir(relative))] = true
	}
	patterns := make([]string, 0, len(directories))
	for path := range directories {
		patterns = append(patterns, path)
	}
	sort.Strings(patterns)
	return overlay, patterns, nil
}
