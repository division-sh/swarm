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

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/standingdiagnostic"
)

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-standing-diagnostics [-write]")
	}
	var manifest standingdiagnostic.Manifest
	outputs := map[string][]byte{}
	for _, rule := range standingdiagnostic.Rules {
		raw, err := exec.Command("git", "show", standingdiagnostic.Predecessor+":"+rule.File).Output()
		must(err)
		before := declaration(raw, rule.Function)
		after, err := standingdiagnostic.Transform(rule, before)
		must(err)
		manifest.Pins = append(manifest.Pins, standingdiagnostic.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before)))})
		current := outputs[rule.File]
		if current == nil {
			current, err = os.ReadFile(rule.File)
			must(err)
		}
		body := declaration(current, rule.Function)
		if body != before && body != after {
			panic("standing diagnostic escaped its predecessor/endpoint: " + rule.Function)
		}
		if before != after {
			current = replaceDeclaration(current, rule.Function, after)
		}
		outputs[rule.File] = current
	}
	for _, endpoint := range standingdiagnostic.Endpoints {
		raw, err := os.ReadFile(endpoint.File)
		must(err)
		endpoint.After = declaration(raw, endpoint.Function)
		manifest.Endpoints = append(manifest.Endpoints, endpoint)
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("pins=%d endpoints=%d sha256=%x\n", len(manifest.Pins), len(manifest.Endpoints), sha256.Sum256(raw))
	if write {
		_, err := standingdiagnostic.Decode(raw)
		must(err)
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(standingdiagnostic.Path, raw, 0644))
	}
}

func declaration(raw []byte, name string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return requestkey.Node(fn)
		}
	}
	panic("missing standing diagnostic declaration: " + name)
}

func replaceDeclaration(raw []byte, name, after string) []byte {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != name {
			continue
		}
		start, end := fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset
		out := append(append(append([]byte(nil), raw[:start]...), after...), raw[end:]...)
		out, err = format.Source(out)
		must(err)
		return out
	}
	panic("missing standing diagnostic destination: " + name)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
