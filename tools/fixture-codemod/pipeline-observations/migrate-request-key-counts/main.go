package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/deliverystatus"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runscalar"
)

const predecessor = "80d42e2aba875393cae592e3b3f7a37f6af3d892"
const ledger = "tools/fixture-codemod/pipeline-observations/family145-request-key-preimages.json"

type pin struct {
	File, Function, Before, After, SHA256, Source string
	Calls                                         int
}
type edit struct {
	start, end int
	after      string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-request-key-counts [-write]")
	}
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	must(err)
	var pins []pin
	must(checkoutsource.WalkDir(".", "internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || !strings.Contains("\n"+string(snapshot), "\n"+path+"\n") {
			return err
		}
		raw, err := exec.Command("git", "show", predecessor+":"+path).Output()
		if err != nil {
			return err
		}
		pins = append(pins, sourcePins(path, raw)...)
		return nil
	}))
	count := 0
	for _, pin := range pins {
		count += pin.Calls
	}
	if count != 39 {
		panic(fmt.Sprintf("request-key source inventory=%d want39", count))
	}
	outputs := prepare(pins)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("calls=%d bodies=%d manifest_sha256=%x\n", count, len(pins), sha256.Sum256(raw))
	if write && fmt.Sprintf("%x", sha256.Sum256(raw)) != requestkey.Digest {
		panic("request-key historical inventory escaped its reviewed complete source")
	}
	if write {
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(ledger, raw, 0644))
	}
}

func sourcePins(path string, raw []byte) []pin {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
	must(err)
	var pins []pin
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		before := requestkey.Node(fn)
		after, calls, err := requestkey.Transform(before)
		must(err)
		if after == before {
			continue
		}
		pins = append(pins, pin{path, requestkey.Key(fn), before, after, fmt.Sprintf("%x", sha256.Sum256([]byte(before))), after, calls})
	}
	return pins
}

func prepare(pins []pin) map[string][]byte {
	byPath := map[string][]pin{}
	for _, pin := range pins {
		byPath[pin.File] = append(byPath[pin.File], pin)
	}
	outputs := map[string][]byte{}
	rawPins, err := os.ReadFile(runscalar.Path)
	must(err)
	scalarPins, err := runscalar.Decode(rawPins)
	must(err)
	for path, rows := range byPath {
		raw, err := os.ReadFile(path)
		must(err)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
		must(err)
		rules := map[string]pin{}
		for _, row := range rows {
			rules[row.Function] = row
		}
		var edits []edit
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			row, ok := rules[requestkey.Key(fn)]
			if !ok {
				continue
			}
			current := requestkey.Node(fn)
			previous := requestKeyPrevious(scalarPins, path, row.Function, current)
			if previous != row.Before && previous != row.After {
				panic("request-key source escaped exact predecessor/successor: " + path + "/" + row.Function)
			}
			delete(rules, requestkey.Key(fn))
			if current != previous {
				row.Source = current
			}
			edits = append(edits, edit{fset.Position(fn.Pos()).Offset, fset.Position(fn.End()).Offset, row.Source})
		}
		if len(rules) != 0 {
			panic("required request-key source is missing: " + path)
		}
		sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
		for _, e := range edits {
			raw = append(append(append([]byte(nil), raw[:e.start]...), e.after...), raw[e.end:]...)
		}
		verifyOutput(path, raw, rows, scalarPins)
		outputs[path] = raw
	}
	return outputs
}

func verifyOutput(path string, raw []byte, rows []pin, scalarPins []runscalar.Pin) {
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
	must(err)
	declarations := map[string]string{}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			declarations[requestkey.Key(fn)] = requestkey.Node(fn)
		}
	}
	for _, row := range rows {
		if requestKeyPrevious(scalarPins, path, row.Function, declarations[row.Function]) != row.After {
			panic("request-key candidate output escaped its typed rule: " + path + "/" + row.Function)
		}
	}
}

func requestKeyPrevious(scalarPins []runscalar.Pin, path, function, current string) string {
	previous, err := deliverystatus.ReadPrevious(path, function, current)
	must(err)
	return runscalar.Previous(scalarPins, path, function, previous)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
