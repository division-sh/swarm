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
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

type pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-run-physical-counts [-write]")
	}
	var pins []pin
	calls := 0
	outputs := map[string][]byte{}
	for _, rule := range runphysical.Rules {
		raw, err := exec.Command("git", "show", runphysical.Predecessor+":"+rule.File).Output()
		must(err)
		before := declaration(raw, rule.Function)
		after, err := runphysical.Transform(rule, before)
		must(err)
		pins = append(pins, pin{rule.File, rule.Function, before, after, fmt.Sprintf("%x", sha256.Sum256([]byte(before))), rule.Calls})
		calls += rule.Calls
		current, err := os.ReadFile(rule.File)
		must(err)
		body := declaration(current, rule.Function)
		if body != before && body != after {
			panic("physical count source escaped its predecessor/output: " + rule.File)
		}
		output, err := runphysical.ApplyRaw(rule, sourceDeclaration(raw, rule.Function))
		must(err)
		outputs[rule.File] = replaceDeclaration(current, rule.Function, output)
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	if len(pins) != 8 || calls != 10 {
		panic("physical count rule escaped its eight-body/ten-call inventory")
	}
	fmt.Printf("bodies=%d calls=%d sha256=%x\n", len(pins), calls, sha256.Sum256(raw))
	if write {
		if _, err := runphysical.Decode(raw); err != nil {
			panic(err)
		}
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(runphysical.Path, raw, 0644))
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
	panic("missing physical count declaration: " + name)
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
	panic("missing physical count source text: " + name)
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
		if strings.Contains(after, "storetest.") && !strings.Contains(string(out), `"github.com/division-sh/swarm/internal/store/storetest"`) {
			out = []byte(strings.Replace(string(out), `"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"`, "\"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting\"\n\"github.com/division-sh/swarm/internal/store/storetest\"", 1))
		}
		out, err = format.Source(out)
		must(err)
		return out
	}
	panic("missing physical count destination: " + name)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
