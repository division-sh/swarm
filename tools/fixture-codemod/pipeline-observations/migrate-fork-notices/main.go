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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
)

const predecessor = "62f09eb5e15afb9a9b9ed9838336254944dd6be7"
const ledger = "tools/fixture-codemod/pipeline-observations/family145-fork-notice-preimages.json"
const helper = "readForkReceiverNoticeDomain"
const helperFile = "internal/serveapp/fork_receiver_notice_effect_test.go"
const helperSource = `func readForkReceiverNoticeDomain(t *testing.T, reader receiverProofStateReader, runID string) []string {
	t.Helper()
	out, err := storetest.ReadForkReceiverNoticeStorage(context.Background(), reader, runID)
	if err != nil {
		t.Fatal(err)
	}
	return out
}`
const helperHeader = `package serveapp

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

`

type pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
	Source                                string
}
type edit struct {
	start, end int
	after      string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-fork-notices [-write]")
	}
	var pins []pin
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	must(err)
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
	calls := 0
	for _, pin := range pins {
		calls += pin.Calls
	}
	if calls != 16 || len(pins) != 4 {
		panic(fmt.Sprintf("notice inventory calls=%d bodies=%d", calls, len(pins)))
	}
	outputs := prepare(pins)
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("calls=%d bodies=%d manifest_sha256=%x\n", calls, len(pins), sha256.Sum256(raw))
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
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		source := string(raw[fset.Position(function.Pos()).Offset:fset.Position(function.End()).Offset])
		after, calls := transform(fset, function, source)
		if after == source {
			continue
		}
		before := nodeString(function)
		parsed, err := parser.ParseFile(token.NewFileSet(), path, "package proof\n"+after, parser.AllErrors)
		must(err)
		pins = append(pins, pin{path, function.Name.Name, before, nodeString(parsed.Decls[0]), fmt.Sprintf("%x", sha256.Sum256([]byte(before))), calls, after})
	}
	return pins
}

func transform(fset *token.FileSet, function *ast.FuncDecl, source string) (string, int) {
	if function.Name.Name == helper {
		return helperSource, 0
	}
	start := fset.Position(function.Pos()).Offset
	var edits []edit
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || name.Name != helper {
			return true
		}
		if len(call.Args) != 3 || call.Ellipsis.IsValid() || nodeString(call.Args[0]) != "t" || nodeString(call.Args[1]) != "rt" || name.Obj != nil && name.Obj.Kind != ast.Fun {
			panic("unclassified notice caller: " + function.Name.Name)
		}
		argument := call.Args[1]
		edits = append(edits, edit{fset.Position(argument.Pos()).Offset - start, fset.Position(argument.End()).Offset - start, "rt.ReceiverStateReader"})
		return true
	})
	slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
	for _, e := range edits {
		source = source[:e.start] + e.after + source[e.end:]
	}
	return source, len(edits)
}

func prepare(pins []pin) map[string][]byte {
	grouped := map[string][]pin{}
	for _, pin := range pins {
		grouped[pin.File] = append(grouped[pin.File], pin)
	}
	outputs := map[string][]byte{}
	for path, rows := range grouped {
		raw, err := os.ReadFile(path)
		must(err)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
		must(err)
		var edits []edit
		for _, pin := range rows {
			edits = append(edits, currentEdit(fset, file, pin))
		}
		slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
		for _, e := range edits {
			raw = append(raw[:e.start], append([]byte(e.after), raw[e.end:]...)...)
		}
		if path == helperFile {
			if len(rows) != 1 || len(file.Decls) != 2 {
				panic("notice-only fixture file grew an unknown dependency")
			}
			raw = []byte(helperHeader + helperSource + "\n")
		}
		outputs[path], err = format.Source(raw)
		must(err)
	}
	return outputs
}

func currentEdit(fset *token.FileSet, file *ast.File, pin pin) edit {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != pin.Function {
			continue
		}
		current := nodeString(function)
		comparison, err := servedsettlement.ObservationEarlierSource(".", pin.File, pin.Function, current)
		must(err)
		if comparison != pin.Before && comparison != pin.After {
			panic("notice consumer escaped complete source: " + pin.File + "/" + pin.Function)
		}
		if current != comparison {
			return edit{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, current}
		}
		return edit{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, pin.Source}
	}
	panic("notice declaration disappeared")
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
