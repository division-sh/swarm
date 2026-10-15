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
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourceblob"
	"golang.org/x/tools/go/packages"
)

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-source-blob [-write]")
	}
	outputs := map[string][]byte{}
	var pins []sourceblob.Pin
	for _, rule := range sourceblob.Rules {
		old, err := exec.Command("git", "show", sourceblob.Predecessor+":"+rule.File).Output()
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
			panic("source blob escaped exact predecessor/output: " + rule.Function)
		}
		rawAfter, err := runphysical.ApplyRaw(rule, rawBefore)
		must(err)
		outputs[rule.File] = replace(current, rule.Function, rawAfter)
		pins = append(pins, sourceblob.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: rule.Calls})
	}
	path := sourceblob.Rules[0].File
	outputs[path] = captureField(outputs[path])
	printFixtureType(outputs[path])
	validateCandidate(outputs)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("bodies=4 reads=6 sha256=%x\n", sha256.Sum256(raw))
	if write {
		_, err := sourceblob.Decode(raw)
		must(err)
		for path, out := range outputs {
			must(os.WriteFile(path, out, 0644))
		}
		must(os.WriteFile(sourceblob.Path, raw, 0644))
	}
}

func printFixtureType(raw []byte) {
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", raw, parser.AllErrors)
	must(err)
	for _, declaration := range file.Decls {
		if group, ok := declaration.(*ast.GenDecl); ok {
			for _, spec := range group.Specs {
				if typed, ok := spec.(*ast.TypeSpec); ok && typed.Name.Name == "servedControlProofRuntime" {
					fmt.Printf("fixture_type_sha256=%x\n", sha256.Sum256([]byte(requestkey.Node(typed))))
				}
			}
		}
	}
}

func captureField(raw []byte) []byte {
	before := "type servedControlProofRuntime struct {\n\tEndpoint"
	after := "type servedControlProofRuntime struct {\n\tSourceArtifacts sourceArtifactReader\n\tEndpoint"
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range group.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != "servedControlProofRuntime" {
				continue
			}
			structure, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				panic("source blob shared fixture is not a struct")
			}
			if sourceFieldExists(structure) {
				return raw
			}
		}
	}
	if strings.Count(string(raw), before) != 1 {
		panic("source blob shared fixture field lost its exact predecessor")
	}
	out, err := format.Source([]byte(strings.Replace(string(raw), before, after, 1)))
	must(err)
	return out
}

func sourceFieldExists(structure *ast.StructType) bool {
	for _, field := range structure.Fields.List {
		for _, name := range field.Names {
			if name.Name == "SourceArtifacts" {
				if requestkey.Node(field.Type) != "sourceArtifactReader" {
					panic("source blob fixture binds a foreign role")
				}
				return true
			}
		}
	}
	return false
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
	panic("missing source blob declaration: " + name)
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
	panic("missing source blob destination: " + name)
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
	pkgs, err := packages.Load(&packages.Config{Dir: root, Overlay: overlay, Tests: true, Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}, "./internal/serveapp")
	must(err)
	if len(pkgs) == 0 || packages.PrintErrors(pkgs) != 0 {
		panic("source blob candidate package/test overlay does not type-check")
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
