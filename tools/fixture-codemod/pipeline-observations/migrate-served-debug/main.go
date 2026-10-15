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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelintent"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/deliverystatus"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forkcompanion"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forknotice"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicendpoint"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/receiverreadback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runscalar"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/serveddebug"
)

const predecessor = "bfdcdc112f7053494ea597c053aaa49678804bbc"
const ledger = "tools/fixture-codemod/pipeline-observations/family145-served-debug-preimages.json"

type pin struct {
	File, Function, SHA256  string
	Before, After           string
	DebugCalls, HelperCalls int
}
type edit struct {
	start, end int
	after      string
}
type result struct {
	path   string
	source []byte
	pins   []pin
	debug  int
	needs  []string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-served-debug [-write]")
	}
	var results []result
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	must(err)
	must(checkoutsource.WalkDir(".", "internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		if !strings.Contains("\n"+string(snapshot), "\n"+path+"\n") {
			return nil // New files have no body in this immutable historical inventory.
		}
		source, err := exec.Command("git", "show", predecessor+":"+path).Output()
		if err != nil {
			return fmt.Errorf("read signed predecessor %s: %w", path, err)
		}
		r := transformFile(path, source)
		if len(r.pins) != 0 {
			results = append(results, r)
		}
		return nil
	}))
	var pins []pin
	var needs []string
	debug := 0
	for _, r := range results {
		pins = append(pins, r.pins...)
		needs = append(needs, r.needs...)
		debug += r.debug
	}
	slices.Sort(needs)
	for _, need := range needs {
		fmt.Println("NEEDS ORIGINAL READER: " + need)
	}
	if debug != 63 {
		panic(fmt.Sprintf("debug calls=%d, want 63", debug))
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	fmt.Printf("debug=%d bodies=%d preimages_sha256=%x\n", debug, len(pins), sha256.Sum256(raw))
	if !write {
		return
	}
	if len(needs) != 0 {
		panic("reader capture decisions incomplete; no files written")
	}
	var outputs [][]byte
	for _, r := range results {
		outputs = append(outputs, currentOutput(r))
	}
	for index, r := range results {
		must(os.WriteFile(r.path, outputs[index], 0644))
	}
	must(os.WriteFile(ledger, append(raw, '\n'), 0644))
}

func transformFile(path string, source []byte) result {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, parser.AllErrors)
	must(err)
	r := result{path: path}
	var edits []edit
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		start, end := fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset
		before := nodeString(function)
		if function.Name.Name == "servedEventPublishDebugSummary" || function.Name.Name == "servedEventPublishDebugQuery" {
			r.pins = append(r.pins, pin{File: path, Function: function.Name.Name, SHA256: hash(before), Before: before})
			edits = append(edits, edit{start, end, ""})
			continue
		}
		after, debug, calls, needs := serveddebug.TransformFunction(fset, function, string(source[start:end]))
		if after == string(source[start:end]) {
			continue
		}
		if needs {
			r.needs = append(r.needs, path+"/"+function.Name.Name)
		}
		r.debug += debug
		r.pins = append(r.pins, pin{File: path, Function: function.Name.Name, SHA256: hash(before), Before: before, DebugCalls: debug, HelperCalls: calls})
		edits = append(edits, edit{start, end, after})
	}
	r.source = apply(source, edits)
	if path == "internal/serveapp/main_runtime_test.go" {
		addCarrier(file, &r)
	}
	r.source, err = format.Source(r.source)
	must(err)
	afterDecls := declarations(r.source)
	for index := range r.pins {
		r.pins[index].After = afterDecls[r.pins[index].Function]
	}
	return r
}

func addCarrier(file *ast.File, r *result) {
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.TYPE {
			continue
		}
		for _, spec := range group.Specs {
			if spec.(*ast.TypeSpec).Name.Name != "servedSessionCleanupProof" {
				continue
			}
			before := nodeString(group)
			r.pins = append(r.pins, pin{File: r.path, Function: "type/servedSessionCleanupProof", SHA256: hash(before), Before: before})
			r.source = bytes.Replace(r.source, []byte("type servedSessionCleanupProof struct {\n"), []byte("type servedSessionCleanupProof struct {\n\tReceiverStateReader receiverProofStateReader\n"), 1)
			return
		}
	}
	panic("original session proof carrier disappeared")
}

func declarations(source []byte) map[string]string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.AllErrors)
	must(err)
	out := map[string]string{}
	for _, declaration := range file.Decls {
		if key := declarationKey(declaration); key != "" {
			out[key] = nodeString(declaration)
		}
	}
	return out
}

func currentOutput(r result) []byte {
	current, err := os.ReadFile(r.path)
	must(err)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, r.path, current, parser.AllErrors)
	must(err)
	rules := map[string]pin{}
	for _, p := range r.pins {
		rules[p.Function] = p
	}
	var edits []edit
	for _, declaration := range file.Decls {
		key := declarationKey(declaration)
		p, ok := rules[key]
		if !ok {
			continue
		}
		actual := nodeString(declaration)
		manifest, err := os.ReadFile(receiverreadback.Path)
		must(err)
		pins, err := receiverreadback.Decode(manifest)
		must(err)
		companionManifest, err := os.ReadFile(forkcompanion.Path)
		must(err)
		companionPins, err := forkcompanion.Decode(companionManifest)
		must(err)
		noticeManifest, err := os.ReadFile(forknotice.Path)
		must(err)
		noticePins, err := forknotice.Decode(noticeManifest)
		must(err)
		intentManifest, err := os.ReadFile(channelintent.Path)
		must(err)
		intentPins, err := channelintent.Decode(intentManifest)
		must(err)
		requestKeyManifest, err := os.ReadFile(requestkey.Path)
		must(err)
		requestKeyPins, err := requestkey.Decode(requestKeyManifest)
		must(err)
		scalarManifest, err := os.ReadFile(runscalar.Path)
		must(err)
		scalarPins, err := runscalar.Decode(scalarManifest)
		must(err)
		previous, err := deliverystatus.ReadPrevious(r.path, key, actual)
		must(err)
		previous = runscalar.Previous(scalarPins, r.path, key, previous)
		previous = requestkey.Previous(requestKeyPins, r.path, key, previous)
		previous = channelintent.Previous(intentPins, r.path, key, previous)
		previous = forknotice.Previous(noticePins, r.path, key, previous)
		previous = forkcompanion.Previous(companionPins, r.path, key, previous)
		previous = receiverreadback.Previous(pins, key, previous)
		previous = publicendpoint.Previous(r.path, key, previous)
		if actual != p.Before && previous != p.After {
			panic("current declaration differs from exact predecessor/successor: " + r.path + "/" + key)
		}
		delete(rules, key)
		if actual != previous {
			continue // Keep the exact newer endpoint-only successor, never rewind it.
		}
		edits = append(edits, edit{fset.Position(declaration.Pos()).Offset, fset.Position(declaration.End()).Offset, exactSource(r.source, key)})
	}
	for key, p := range rules {
		if p.After != "" {
			panic("current declaration disappeared: " + key)
		}
	}
	out, err := format.Source(apply(current, edits))
	must(err)
	return out
}

func declarationKey(declaration ast.Decl) string {
	if function, ok := declaration.(*ast.FuncDecl); ok && function.Recv == nil {
		return function.Name.Name
	}
	if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 {
		return "type/" + group.Specs[0].(*ast.TypeSpec).Name.Name
	}
	return ""
}

func exactSource(source []byte, key string) string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "successor.go", source, parser.AllErrors)
	must(err)
	for _, declaration := range file.Decls {
		if declarationKey(declaration) == key {
			return string(source[fset.Position(declaration.Pos()).Offset:fset.Position(declaration.End()).Offset])
		}
	}
	return ""
}

func apply(source []byte, edits []edit) []byte {
	slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
	for _, e := range edits {
		source = append(source[:e.start], append([]byte(e.after), source[e.end:]...)...)
	}
	return source
}
func nodeString(node ast.Node) string {
	var out bytes.Buffer
	must(format.Node(&out, token.NewFileSet(), node))
	return out.String()
}
func hash(source string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(source))) }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
