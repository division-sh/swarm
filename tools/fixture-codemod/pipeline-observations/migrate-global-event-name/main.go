package main

import (
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
	"strconv"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/globalname"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"golang.org/x/tools/go/packages"
)

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-global-event-name [-write]")
	}
	outputs := map[string][]byte{}
	var pins []globalname.Pin
	for _, rule := range globalname.Rules {
		old, err := exec.Command("git", "show", globalname.Predecessor+":"+rule.File).Output()
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
			panic("global event name escaped exact predecessor/output: " + rule.Function)
		}
		rawAfter, err := runphysical.ApplyRaw(rule, rawBefore)
		must(err)
		outputs[rule.File] = replace(current, rule.Function, rawAfter)
		pins = append(pins, globalname.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: rule.Calls})
	}
	path := "internal/serveapp/channel_input_process_supported_surface_test.go"
	outputs[path] = contextImport(outputs[path])
	validateCandidate(outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("bodies=7 reads=8 sha256=%x\n", sha256.Sum256(raw))
	if write {
		_, err := globalname.Decode(raw)
		must(err)
		for path, out := range outputs {
			must(os.WriteFile(path, out, 0644))
		}
		must(os.WriteFile(globalname.Path, raw, 0644))
	}
}

func contextImport(raw []byte) []byte {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	for _, item := range file.Imports {
		path, err := strconv.Unquote(item.Path.Value)
		must(err)
		if path == "context" {
			return raw
		}
	}
	if strings.Count(string(raw), "import (\n") != 1 {
		panic("channel input process imports lost their exact shape")
	}
	out, err := format.Source([]byte(strings.Replace(string(raw), "import (\n", "import (\n\"context\"\n", 1)))
	must(err)
	return out
}

func declaration(raw []byte, name string) (string, string) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return string(raw[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset]), requestkey.Node(fn)
		}
	}
	panic("missing global event name declaration: " + name)
}

func replace(raw []byte, name, after string) []byte {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			start, end := set.Position(fn.Pos()).Offset, set.Position(fn.End()).Offset
			out, err := format.Source(append(append(append([]byte{}, raw[:start]...), after...), raw[end:]...))
			must(err)
			return out
		}
	}
	panic("missing global event name destination: " + name)
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
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay, Tests: true, Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}, "./internal/serveapp", "./internal/store/storetest")
	must(err)
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		panic("global event name candidate package/test overlays do not type-check")
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
