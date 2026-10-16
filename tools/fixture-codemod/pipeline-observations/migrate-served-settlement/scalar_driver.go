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
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

func migrateScalar(write bool) {
	root, err := os.Getwd()
	must(err)
	oldPredecessor := sourcePredecessor
	sourcePredecessor = servedsettlement.ScalarPredecessor
	defer func() { sourcePredecessor = oldPredecessor }()
	originals := signedSources(root)
	typed := scalarUses(root, originals)
	outputs, pins := scalarCandidates(originals, typed)
	validateCandidate(root, outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	calls := 0
	for _, pin := range pins {
		calls += pin.Calls
		fmt.Printf("source\t%s\t%s\t%d\n", pin.File, pin.Function, pin.Calls)
	}
	fmt.Printf("bodies=%d scalar-reads=%d sha256=%x\n", len(pins), calls, sha256.Sum256(raw))
	if !write {
		return
	}
	for path, candidate := range outputs {
		must(os.WriteFile(path, candidate, 0644))
	}
	must(os.WriteFile(servedsettlement.ScalarPath, raw, 0644))
}

func scalarUses(root string, originals map[string][]byte) map[string]string {
	uses := map[string]string{}
	for _, pkg := range load(root, originals) {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(node ast.Node) bool {
				scan, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				result := scalarDestinationType(scan, pkg.TypesInfo)
				if result == "" {
					return true
				}
				pos := pkg.Fset.Position(scan.Pos())
				path, err := filepath.Rel(root, pos.Filename)
				must(err)
				uses[fmt.Sprintf("%s:%d", filepath.ToSlash(path), pos.Offset)] = result
				return true
			})
		}
	}
	return uses
}

func scalarDestinationType(scan *ast.CallExpr, info *types.Info) string {
	selector, ok := scan.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Scan" || len(scan.Args) != 1 {
		return ""
	}
	query, ok := selector.X.(*ast.CallExpr)
	if !ok {
		return ""
	}
	method, ok := query.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	field, ok := method.X.(*ast.SelectorExpr)
	if !ok || field.Sel.Name != "DB" {
		return ""
	}
	owner, ok := info.TypeOf(field.X).(*types.Named)
	if !ok || owner.Obj().Pkg() == nil || owner.Obj().Name() != "servedControlProofRuntime" || owner.Obj().Pkg().Path() != "github.com/division-sh/swarm/internal/serveapp" {
		return ""
	}
	if types.TypeString(info.TypeOf(field), nil) != "*database/sql.DB" {
		return ""
	}
	address, ok := scan.Args[0].(*ast.UnaryExpr)
	if !ok || address.Op != token.AND {
		return ""
	}
	basic, ok := info.TypeOf(address.X).(*types.Basic)
	if !ok || (basic.Kind() != types.Int && basic.Kind() != types.String) {
		return ""
	}
	return basic.Name()
}

func scalarCandidates(originals map[string][]byte, typed map[string]string) (map[string][]byte, []runphysical.Pin) {
	paths := make([]string, 0, len(originals))
	for path := range originals {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	outputs := map[string][]byte{}
	var pins []runphysical.Pin
	for _, path := range paths {
		rows := scalarFilePins(path, originals[path], typed)
		if len(rows) == 0 {
			continue
		}
		out, err := destinationCandidate(path, rows)
		must(err)
		outputs[path] = scalarImports(out)
		pins = append(pins, rows...)
	}
	return outputs, pins
}

func scalarFilePins(path string, raw []byte, typed map[string]string) []runphysical.Pin {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.AllErrors)
	must(err)
	var pins []runphysical.Pin
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Body == nil {
			continue
		}
		var edits []servedsettlement.Splice
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			result := typed[fmt.Sprintf("%s:%d", path, set.Position(call.Pos()).Offset)]
			if result == "" {
				return true
			}
			edit, _, err := servedsettlement.ScalarCall(set, call, result)
			if err == nil {
				edits = append(edits, edit)
			}
			return true
		})
		if len(edits) == 0 {
			continue
		}
		start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
		for index := range edits {
			edits[index].Start -= start
			edits[index].End -= start
		}
		after, err := requestkey.Normalize(string(apply(raw[start:end], edits)))
		must(err)
		before := requestkey.Node(fn)
		pins = append(pins, runphysical.Pin{File: path, Function: fn.Name.Name, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: len(edits)})
	}
	return pins
}

func scalarImports(raw []byte) []byte {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	for _, path := range []string{"context", "github.com/division-sh/swarm/internal/store/storetest"} {
		found := false
		for _, imported := range file.Imports {
			found = found || imported.Path.Value == fmt.Sprintf("%q", path)
		}
		if !found {
			if strings.Count(string(raw), "import (\n") != 1 {
				panic("served scalar imports lost their exact group")
			}
			raw = []byte(strings.Replace(string(raw), "import (\n", fmt.Sprintf("import (\n%q\n", path), 1))
		}
	}
	out, err := format.Source(raw)
	must(err)
	return out
}
