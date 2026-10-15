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
	"io/fs"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicendpoint"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicjoin"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

const predecessor = "9c964b502e52681d01d02c678b67ba996e5aed8a"
const ledger = "tools/fixture-codemod/pipeline-observations/internal/publicendpoint/preimages.json"

type edit struct {
	start, end int
	after      string
}
type result struct {
	path  string
	pins  []publicendpoint.Pin
	exact map[string]string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-public-a2-endpoints [-write]")
	}
	var results []result
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	must(err)
	must(checkoutsource.WalkDir(".", "internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !hasGoSuffix(path) || !strings.Contains("\n"+string(snapshot), "\n"+path+"\n") {
			return err
		}
		raw, err := exec.Command("git", "show", predecessor+":"+path).Output()
		must(err)
		r := transform(path, raw)
		if len(r.pins) != 0 {
			results = append(results, r)
		}
		return nil
	}))
	var pins []publicendpoint.Pin
	var outputs [][]byte
	calls := 0
	for _, r := range results {
		pins = append(pins, r.pins...)
		for _, p := range r.pins {
			calls += p.Calls
		}
		outputs = append(outputs, currentOutput(r))
	}
	if calls != 67 {
		panic(fmt.Sprintf("public endpoint inventory=%d, want67", calls))
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("calls=%d bodies=%d source_manifest_sha256=%x\n", calls, len(pins), sha256.Sum256(raw))
	if write {
		for index, r := range results {
			must(os.WriteFile(r.path, outputs[index], 0644))
		}
		must(os.WriteFile(ledger, raw, 0644))
	}
}

func hasGoSuffix(path string) bool { return len(path) > 3 && path[len(path)-3:] == ".go" }

func transform(path string, raw []byte) result {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
	must(err)
	r := result{path: path, exact: map[string]string{}}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		start, end := fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset
		before := string(raw[start:end])
		after, calls, err := publicendpoint.Transform(fset, function, before)
		must(err)
		if after == before {
			continue
		}
		original := nodeString(function)
		r.pins = append(r.pins, publicendpoint.Pin{File: path, Function: function.Name.Name, Before: original, After: normalized(after), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(original))), Calls: calls})
		r.exact[function.Name.Name] = after
	}
	return r
}

func normalized(source string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "declaration.go", "package proof\n"+source, parser.AllErrors)
	must(err)
	if len(file.Decls) != 1 {
		panic("invalid whole declaration")
	}
	return nodeString(file.Decls[0])
}

func currentOutput(r result) []byte {
	raw, err := os.ReadFile(r.path)
	must(err)
	joinBytes, err := os.ReadFile(publicjoin.Path)
	must(err)
	joinPins, err := publicjoin.Decode(joinBytes)
	must(err)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, r.path, raw, parser.AllErrors)
	must(err)
	rules := map[string]publicendpoint.Pin{}
	for _, p := range r.pins {
		rules[p.Function] = p
	}
	var edits []edit
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		pin, selected := rules[function.Name.Name]
		if !selected {
			continue
		}
		actual := nodeString(function)
		comparison, err := servedsettlement.ObservationEarlierSource(".", r.path, pin.Function, actual)
		must(err)
		preceding := publicjoin.Previous(joinPins, r.path, pin.Function, comparison)
		if preceding != pin.Before && preceding != pin.After {
			panic("caller changed outside exact endpoint transform: " + r.path + "/" + pin.Function)
		}
		delete(rules, function.Name.Name)
		if actual == preceding {
			edits = append(edits, edit{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, r.exact[pin.Function]})
		}
	}
	if len(rules) != 0 {
		panic("owned caller disappeared")
	}
	slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
	for _, e := range edits {
		raw = append(raw[:e.start], append([]byte(e.after), raw[e.end:]...)...)
	}
	out, err := format.Source(raw)
	must(err)
	return out
}

func nodeString(node ast.Node) string {
	var out bytes.Buffer
	must(format.Node(&out, token.NewFileSet(), node))
	return out.String()
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
