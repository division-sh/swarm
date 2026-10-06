// pipeline-observations applies only this batch's reviewed function snapshots.
package main

import (
	"bytes"
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

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

//go:embed recipes.json
var recipeBytes []byte

type recipe struct {
	Family, File, Function, Before, After string
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
	return json.NewEncoder(os.Stdout).Encode(struct {
		Write   bool
		Changes []recipe
	}{write, changed})
}

func prepareFiles(root string, recipes []recipe) ([]pendingFile, []recipe, error) {
	grouped := map[string][]recipe{}
	for _, row := range recipes {
		if !filepath.IsLocal(row.File) || filepath.ToSlash(filepath.Clean(row.File)) != row.File {
			return nil, nil, fmt.Errorf("noncanonical recipe path %q", row.File)
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
			return nil, nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, nil, fmt.Errorf("recipe source is not a regular file: %s", path)
		}
		source, err := os.ReadFile(absolute)
		if err != nil {
			return nil, nil, err
		}
		result, applied, err := rewriteSource(path, source, grouped[path])
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
	astutil.AddImport(set, file, "github.com/division-sh/swarm/internal/store/storetest")
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
	fn, err := uniqueFunction(file, row.Function)
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
	overlay := map[string][]byte{}
	for _, file := range files {
		overlay[file.path] = file.data
	}
	pkgs, err := packages.Load(&packages.Config{
		Dir: root, Tests: true, Overlay: overlay,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedTypes | packages.NeedImports,
	}, "./internal/runtime", "./internal/runtime/pipeline")
	if err != nil {
		return err
	}
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		return fmt.Errorf("candidate type checking failed; no files written")
	}
	return nil
}
