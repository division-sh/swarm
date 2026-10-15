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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runidentity"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-run-identity-counts [-write]")
	}
	var pins []runidentity.Pin
	calls := 0
	outputs := map[string][]byte{}
	for _, rule := range runidentity.Rules {
		raw, err := exec.Command("git", "show", runidentity.Predecessor+":"+rule.File).Output()
		must(err)
		before := requestkey.Node(function(raw, rule.Function))
		after, err := runphysical.Transform(rule, before)
		must(err)
		pins = append(pins, runidentity.Pin{File: rule.File, Function: rule.Function, Before: before, After: after, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(before))), Calls: rule.Calls})
		calls += rule.Calls
		current, err := os.ReadFile(rule.File)
		must(err)
		body := requestkey.Node(function(current, rule.Function))
		if body != before && body != after {
			panic("run identity source escaped its predecessor/output: " + rule.File)
		}
		output, err := runphysical.ApplyRaw(rule, sourceDeclaration(raw, rule.Function))
		must(err)
		outputs[rule.File] = replaceDeclaration(current, rule.Function, output)
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	if len(pins) != 5 || calls != 8 {
		panic("run identity rule escaped its five-body/eight-call inventory")
	}
	fmt.Printf("bodies=%d calls=%d sha256=%x\n", len(pins), calls, sha256.Sum256(raw))
	reader, err := os.ReadFile("internal/serveapp/run_identity_observation_test.go")
	must(err)
	fmt.Printf("native-reader-sha256=%x\n", sha256.Sum256([]byte(requestkey.Node(function(reader, "readServedRunPhysicalIdentity")))))
	if write {
		_, err = runidentity.Decode(raw)
		must(err)
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(runidentity.Path, raw, 0644))
	}
}

func function(raw []byte, name string) *ast.FuncDecl {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return fn
		}
	}
	panic("missing run identity declaration: " + name)
}

func sourceDeclaration(raw []byte, name string) string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "source.go", raw, parser.AllErrors)
	must(err)
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return string(raw[fset.Position(fn.Pos()).Offset:fset.Position(fn.End()).Offset])
		}
	}
	panic("missing run identity source: " + name)
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
	panic("missing run identity destination: " + name)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
