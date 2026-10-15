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
	"reflect"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourcefork"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "-compose-history" {
		composeHistory()
		return
	}
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-source-fork-cardinality [-write]")
	}
	outputs := map[string][]byte{}
	var pins []sourcefork.Pin
	count := 0
	for _, rule := range sourcefork.Rules {
		old, err := exec.Command("git", "show", sourcefork.Predecessor+":"+rule.File).Output()
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
			panic("source fork cardinality escaped exact predecessor/output: " + rule.Function)
		}
		rawAfter, err := runphysical.ApplyRaw(rule, rawBefore)
		must(err)
		outputs[rule.File] = replace(current, rule.Function, rawAfter)
		pins = append(pins, sourcefork.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: rule.Calls})
		count += rule.Calls
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	if len(pins) != 6 || count != 8 {
		panic("source fork cardinality inventory escaped six bodies/eight reads")
	}
	fmt.Printf("bodies=%d calls=%d sha256=%x\n", len(pins), count, sha256.Sum256(raw))
	if write {
		_, err := sourcefork.Decode(raw)
		must(err)
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(sourcefork.Path, raw, 0644))
	}
}

func composeHistory() {
	raw, err := os.ReadFile(sourcefork.Path)
	must(err)
	pins, err := sourcefork.Decode(raw)
	must(err)
	for _, pin := range pins {
		current, err := os.ReadFile(pin.File)
		must(err)
		_, body := declaration(current, pin.Function)
		if body != pin.After {
			panic("source fork current history endpoint changed")
		}
	}
	for _, path := range []string{"tools/fixture-codemod/pipeline-observations/recipes.json", "tools/fixture-codemod/pipeline-observations/family145-transitions.json"} {
		prior, err := exec.Command("git", "show", sourcefork.HistoryPredecessor+":"+path).Output()
		must(err)
		out, err := sourcefork.ComposeHistory(prior, pins, path == "tools/fixture-codemod/pipeline-observations/recipes.json")
		must(err)
		out = append(out, '\n')
		current, err := os.ReadFile(path)
		must(err)
		if !bytes.Equal(current, prior) && !sameHistory(current, out) {
			panic("unreviewed source fork history outside finite endpoints")
		}
		must(os.WriteFile(path, out, 0644))
		fmt.Printf("composed six finite source fork endpoints: %s\n", path)
	}
}

func sameHistory(left, right []byte) bool {
	var a, b []map[string]any
	must(json.Unmarshal(left, &a))
	must(json.Unmarshal(right, &b))
	return reflect.DeepEqual(a, b)
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
	panic("missing source fork cardinality declaration: " + name)
}

func replace(raw []byte, name, after string) []byte {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			start, end := fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset
			out, err := format.Source(append(append(append([]byte{}, raw[:start]...), after...), raw[end:]...))
			must(err)
			return out
		}
	}
	panic("missing source fork cardinality destination: " + name)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
