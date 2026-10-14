package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
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

var changes = servedsettlement.Changes
var sourcePredecessor = servedsettlement.Predecessor
var manifestPath = servedsettlement.Path
var ownerBody = servedsettlement.OwnerBody

func main() {
	write := flag.Bool("write", false, "write the type-checked finite candidate")
	lookup := flag.Bool("lookup", false, "apply the finite publication lookup extension")
	flag.Parse()
	if flag.NArg() != 0 {
		panic("usage: migrate-served-settlement [-lookup] [-write]")
	}
	if *lookup {
		changes, sourcePredecessor, manifestPath, ownerBody = servedsettlement.LookupChanges, servedsettlement.LookupPredecessor, servedsettlement.LookupPath, servedsettlement.LookupOwnerBody
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
	if *write {
		if *lookup {
			_, err := servedsettlement.DecodeLookup(raw)
			must(err)
		} else {
			_, err := servedsettlement.Decode(raw)
			must(err)
		}
		for path, raw := range outputs {
			must(os.WriteFile(path, raw, 0644))
		}
		must(os.WriteFile(manifestPath, raw, 0644))
	}
}

func signedSources(root string) map[string][]byte {
	result, err := collectSignedSources(root, sourcePredecessor)
	must(err)
	return result
}

func collectSignedSources(root, predecessor string) (map[string][]byte, error) {
	listed, err := exec.Command("git", "-C", root, "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	if err != nil {
		return nil, fmt.Errorf("served source inventory cannot read the signed tree: %w", err)
	}
	tracked := map[string]bool{}
	for _, path := range strings.Fields(string(listed)) {
		if strings.HasSuffix(path, ".go") {
			tracked[path] = true
		}
	}
	if len(tracked) == 0 {
		return nil, fmt.Errorf("served source inventory has no signed Go files")
	}
	result := map[string][]byte{}
	err = checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !tracked[relative] {
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return validateNewSource(path, raw)
		}
		raw, err := exec.Command("git", "-C", root, "show", predecessor+":"+relative).Output()
		if err == nil {
			result[relative] = raw
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(result) != len(tracked) {
		return nil, fmt.Errorf("served signed source enumeration is incomplete")
	}
	return result, nil
}

func validateNewSource(path string, raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
	if err != nil {
		return err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok {
			if _, target := changes[ident.Name]; target {
				err = fmt.Errorf("new/excluded source contains an unrecorded served helper reference: %s/%s", path, ident.Name)
			}
		}
		return true
	})
	return err
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
			if _, ok := changes[fn.Name()]; ok {
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
		pins = append(pins, runphysical.Pin{File: path, Function: fn.Name.Name, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: calls})
	}
	if len(pins) == 0 {
		return raw, nil
	}
	out, err := destinationCandidate(path, pins)
	must(err)
	out, err = format.Source(out)
	must(err)
	return out, pins
}

func rewriteDeclaration(path string, set *token.FileSet, fn *ast.FuncDecl, raw []byte, uses map[string]bool) (string, int) {
	var edits []servedsettlement.Splice
	if _, ok := changes[fn.Name.Name]; ok {
		edit, err := servedsettlement.SignatureWithChanges(set, fn, changes)
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
		if _, target := changes[ident.Name]; !target {
			return true
		}
		pos := set.Position(ident.Pos())
		if !uses[fmt.Sprintf("%s:%d", path, pos.Offset)] {
			panic("untyped/excluded/shadowed served settlement call: " + path + "/" + fn.Name.Name)
		}
		edit, err := servedsettlement.CallWithChanges(set, call, fn.Name.Name, ident.Name, changes)
		must(err)
		edits = append(edits, edit)
		calls++
		return true
	})
	if len(edits) == 0 && (sourcePredecessor != servedsettlement.Predecessor || fn.Name.Name != "seedServedRunControlPendingRunWithAgentDelivery") {
		return "", 0
	}
	start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
	for index := range edits {
		edits[index].Start -= start
		edits[index].End -= start
	}
	body := string(apply(raw[start:end], edits))
	body, err := ownerBody(fn.Name.Name, body)
	must(err)
	return body, calls
}

func apply(raw []byte, edits []servedsettlement.Splice) []byte {
	out, err := servedsettlement.Apply(raw, edits)
	must(err)
	return out
}

func destinationCandidate(path string, pins []runphysical.Pin) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var edits []servedsettlement.Splice
	for _, pin := range pins {
		fn, err := uniqueDestination(file, pin.Function)
		if err != nil {
			return nil, err
		}
		current := requestkey.Node(fn)
		if current == pin.After {
			continue
		}
		if current != pin.Before {
			if err := exactLaterLookup(pin, current); err != nil {
				return nil, err
			}
			continue
		}
		edits = append(edits, servedsettlement.Splice{Start: set.Position(fn.Pos()).Offset, End: set.Position(fn.End()).Offset, After: pin.After})
	}
	return servedsettlement.Apply(raw, edits)
}

func uniqueDestination(file *ast.File, name string) (*ast.FuncDecl, error) {
	var found *ast.FuncDecl
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			if found != nil {
				return nil, fmt.Errorf("served destination is ambiguous: %s", name)
			}
			found = fn
		}
	}
	if found == nil {
		return nil, fmt.Errorf("served destination is missing: %s", name)
	}
	return found, nil
}

func exactLaterLookup(pin runphysical.Pin, current string) error {
	raw, err := os.ReadFile(servedsettlement.LookupPath)
	if err != nil {
		return err
	}
	pins, err := servedsettlement.DecodeLookup(raw)
	if err != nil {
		return err
	}
	for _, later := range pins {
		if later.File == pin.File && later.Function == pin.Function && later.Before == pin.After && later.After == current {
			return nil
		}
	}
	return fmt.Errorf("served destination escaped its exact signed/candidate body: %s", pin.Function)
}

func validateCandidate(root string, outputs map[string][]byte) { load(root, outputs) }

func must(err error) {
	if err != nil {
		panic(err)
	}
}
