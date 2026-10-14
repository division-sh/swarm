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

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicrollback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-public-rollback [-write]")
	}
	path := publicrollback.Rules[0].File
	old, err := exec.Command("git", "show", publicrollback.Predecessor+":"+path).Output()
	must(err)
	current, err := os.ReadFile(path)
	must(err)
	var pins []publicrollback.Pin
	for _, rule := range publicrollback.Rules {
		_, before := declaration(old, rule.Function)
		after, err := runphysical.Transform(rule, before)
		must(err)
		_, candidate := declaration(current, rule.Function)
		if candidate != before && candidate != after {
			panic("public rollback escaped exact predecessor/output: " + rule.Function)
		}
		rawBefore, _ := declaration(old, rule.Function)
		rawAfter, err := runphysical.ApplyRaw(rule, rawBefore)
		must(err)
		current = replace(current, rule.Function, rawAfter)
		pins = append(pins, publicrollback.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: rule.Calls})
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("bodies=%d cuts=7 callers=2 sha256=%x\n", len(pins), sha256.Sum256(raw))
	if write {
		_, err := publicrollback.Decode(raw)
		must(err)
		must(os.WriteFile(path, current, 0644))
		must(os.WriteFile(publicrollback.Path, raw, 0644))
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
	panic("missing public rollback declaration: " + name)
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
	panic("missing public rollback destination: " + name)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
