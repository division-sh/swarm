package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/rundelivery"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-compose-history" {
		composeHistory()
		return
	}
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-run-delivery-cardinality [-write]")
	}
	outputs := map[string][]byte{}
	var pins []rundelivery.Pin
	for _, rule := range rundelivery.Rules {
		old, err := exec.Command("git", "show", rundelivery.Predecessor+":"+rule.File).Output()
		must(err)
		rawBefore, before := declaration(old, rule.Function)
		after, err := runphysical.Transform(rule, before)
		must(err)
		current := outputs[rule.File]
		if current == nil {
			current, err = os.ReadFile(rule.File)
			must(err)
		}
		_, candidate := declaration(current, rule.Function)
		if candidate != before && candidate != after {
			panic("unreviewed run delivery cardinality input: " + rule.Function)
		}
		rawAfter, err := runphysical.ApplyRaw(rule, rawBefore)
		must(err)
		outputs[rule.File] = replace(current, rule.Function, rawAfter)
		pins = append(pins, rundelivery.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: rule.Calls})
	}
	validateCandidate(outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("bodies=%d calls=6 sha256=%x\n", len(pins), sha256.Sum256(raw))
	if write {
		_, err := rundelivery.Decode(raw)
		must(err)
		for path, out := range outputs {
			must(os.WriteFile(path, out, 0644))
		}
		must(os.WriteFile(rundelivery.Path, raw, 0644))
	}
}

func composeHistory() {
	raw, err := os.ReadFile(rundelivery.Path)
	must(err)
	pins, err := rundelivery.Decode(raw)
	must(err)
	for _, pin := range pins {
		current, err := os.ReadFile(pin.File)
		must(err)
		must(rundelivery.ValidateCurrentSource(pin, current))
	}
	for _, path := range []string{"tools/fixture-codemod/pipeline-observations/recipes.json", "tools/fixture-codemod/pipeline-observations/family145-transitions.json"} {
		prior, err := exec.Command("git", "show", rundelivery.HistoryPredecessor+":"+path).Output()
		must(err)
		out, err := rundelivery.ComposeHistory(prior, pins, path == "tools/fixture-codemod/pipeline-observations/recipes.json")
		must(err)
		out = append(out, '\n')
		current, err := os.ReadFile(path)
		must(err)
		var a, b []map[string]any
		must(json.Unmarshal(current, &a))
		must(json.Unmarshal(out, &b))
		if !bytes.Equal(current, prior) && !reflect.DeepEqual(a, b) {
			panic("unreviewed history outside four run delivery endpoints")
		}
		must(os.WriteFile(path, out, 0644))
		fmt.Printf("composed four finite run delivery endpoints: %s\n", path)
	}
}

func declaration(raw []byte, name string) (string, string) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return string(raw[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset]), requestkey.Node(fn)
		}
	}
	panic("missing run delivery cardinality declaration: " + name)
}

func replace(raw []byte, name, after string) []byte {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			start, end := fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset
			out := append(append(append([]byte{}, raw[:start]...), after...), raw[end:]...)
			file, err = parser.ParseFile(fset, "source.go", out, parser.ParseComments|parser.AllErrors)
			must(err)
			if name == "TestSelectedDeploymentExternalEffectRecoveryBothStores" {
				astutil.AddImport(fset, file, "github.com/division-sh/swarm/internal/store/storetest")
			}
			var buffer bytes.Buffer
			must(format.Node(&buffer, fset, file))
			return buffer.Bytes()
		}
	}
	panic("missing run delivery cardinality destination: " + name)
}

func validateCandidate(outputs map[string][]byte) {
	root, err := os.Getwd()
	must(err)
	overlay := map[string][]byte{}
	for path, raw := range outputs {
		absolute, err := filepath.Abs(path)
		must(err)
		overlay[absolute] = raw
	}
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay, Tests: true, Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}, "./internal/serveapp", "./internal/runtime/conformance")
	must(err)
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		panic("candidate run delivery cardinality package/test overlay does not type-check")
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
