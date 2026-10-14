package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
	"golang.org/x/tools/go/packages"
)

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-served-settlement [-write]")
	}
	root, err := os.Getwd()
	must(err)
	originals := signedSources(root)
	uses := typedUses(root, originals)
	outputs, pins := candidate(originals, uses)
	validateCandidate(root, outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	calls := 0
	for _, pin := range pins {
		calls += pin.Calls
	}
	fmt.Printf("bodies=%d calls=%d sha256=%x\n", len(pins), calls, sha256.Sum256(raw))
	if write {
		_, err := servedsettlement.Decode(raw)
		must(err)
		for path, raw := range outputs {
			must(os.WriteFile(path, raw, 0644))
		}
		must(os.WriteFile(servedsettlement.Path, raw, 0644))
	}
}

func signedSources(root string) map[string][]byte {
	result := map[string][]byte{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		// New bridge files are not source donors for an old helper rewrite.
		check := exec.Command("git", "cat-file", "-e", servedsettlement.Predecessor+":"+filepath.ToSlash(relative))
		if check.Run() != nil {
			return nil
		}
		raw, err := exec.Command("git", "show", servedsettlement.Predecessor+":"+filepath.ToSlash(relative)).Output()
		if err == nil {
			result[relative] = raw
		}
		return err
	})
	must(err)
	return result
}

func load(root string, outputs map[string][]byte) []*packages.Package {
	overlay := map[string][]byte{}
	for path, raw := range outputs {
		overlay[filepath.Join(root, path)] = raw
	}
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay, Tests: true, Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedSyntax}, "./internal/serveapp")
	must(err)
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		panic("served settlement package/test overlay does not type-check")
	}
	return pkgs
}

func typedUses(root string, originals map[string][]byte) map[string]bool {
	uses := map[string]bool{}
	for _, pkg := range load(root, originals) {
		for ident, object := range pkg.TypesInfo.Uses {
			fn, ok := object.(*types.Func)
			if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "github.com/division-sh/swarm/internal/serveapp" {
				continue
			}
			if _, ok := servedsettlement.Changes[fn.Name()]; ok {
				pos := pkg.Fset.Position(ident.Pos())
				relative, err := filepath.Rel(root, pos.Filename)
				must(err)
				uses[fmt.Sprintf("%s:%d", relative, pos.Offset)] = true
			}
		}
	}
	return uses
}

func candidate(originals map[string][]byte, uses map[string]bool) (map[string][]byte, []runphysical.Pin) {
	outputs := map[string][]byte{}
	var pins []runphysical.Pin
	paths := make([]string, 0, len(originals))
	for path := range originals {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		out, rows := fileCandidate(path, originals[path], uses)
		if len(rows) > 0 {
			outputs[path] = out
			pins = append(pins, rows...)
		}
	}
	return outputs, pins
}

func fileCandidate(path string, raw []byte, uses map[string]bool) ([]byte, []runphysical.Pin) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.AllErrors)
	must(err)
	var edits []servedsettlement.Splice
	var pins []runphysical.Pin
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil {
			continue
		}
		body, calls := rewriteDeclaration(path, set, fn, raw, uses)
		if body == "" {
			continue
		}
		before := requestkey.Node(fn)
		after, err := requestkey.Normalize(body)
		must(err)
		start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
		current, err := os.ReadFile(path)
		must(err)
		validateCurrent(path, current, fn.Name.Name, before, after)
		edits = append(edits, servedsettlement.Splice{Start: start, End: end, After: body})
		pins = append(pins, runphysical.Pin{File: path, Function: fn.Name.Name, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: calls})
	}
	if len(edits) == 0 {
		return raw, nil
	}
	out := apply(raw, edits)
	out, err = format.Source(out)
	must(err)
	return out, pins
}

func rewriteDeclaration(path string, set *token.FileSet, fn *ast.FuncDecl, raw []byte, uses map[string]bool) (string, int) {
	var edits []servedsettlement.Splice
	if _, ok := servedsettlement.Changes[fn.Name.Name]; ok {
		edit, err := servedsettlement.Signature(set, fn)
		must(err)
		edits = append(edits, edit)
	}
	calls := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		if _, target := servedsettlement.Changes[ident.Name]; !target {
			return true
		}
		pos := set.Position(ident.Pos())
		if !uses[fmt.Sprintf("%s:%d", path, pos.Offset)] {
			panic("untyped/excluded/shadowed served settlement call: " + path + "/" + fn.Name.Name)
		}
		edit, err := servedsettlement.Call(set, call, fn.Name.Name, ident.Name)
		must(err)
		edits = append(edits, edit)
		calls++
		return true
	})
	if len(edits) == 0 && fn.Name.Name != "seedServedRunControlPendingRunWithAgentDelivery" {
		return "", 0
	}
	start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
	for index := range edits {
		edits[index].Start -= start
		edits[index].End -= start
	}
	body := string(apply(raw[start:end], edits))
	body, err := servedsettlement.OwnerBody(fn.Name.Name, body)
	must(err)
	return body, calls
}

func apply(raw []byte, edits []servedsettlement.Splice) []byte {
	slices.SortFunc(edits, func(a, b servedsettlement.Splice) int { return b.Start - a.Start })
	last := len(raw)
	for _, edit := range edits {
		if edit.Start < 0 || edit.End > last || edit.Start >= edit.End {
			panic("served settlement overlapping/invalid AST splices")
		}
		raw = append(append(append([]byte{}, raw[:edit.Start]...), edit.After...), raw[edit.End:]...)
		last = edit.Start
	}
	return raw
}

func validateCurrent(path string, raw []byte, name, before, after string) {
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
	must(err)
	matches := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != name {
			continue
		}
		matches++
		current := requestkey.Node(fn)
		if current != before && current != after {
			panic("served settlement destination escaped exact signed/candidate body: " + name)
		}
	}
	if matches != 1 {
		panic("served settlement destination missing or ambiguous: " + name)
	}
}

func validateCandidate(root string, outputs map[string][]byte) { load(root, outputs) }

func must(err error) {
	if err != nil {
		panic(err)
	}
}
