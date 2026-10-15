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

const predecessor = "13f976220fbe24e0271434c10e2f6eba29665657"
const helperFile = "internal/serveapp/receiver_public_readback_test.go"
const helper = "requireReceiverPublicReadback"
const native = "requireWorkspaceProofReceiverPublicReadback"
const ledger = "tools/fixture-codemod/pipeline-observations/family145-receiver-public-readback-preimages.json"

type transition struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
	Removed                               bool `json:"Removed,omitempty"`
	Source                                string
}
type edit struct {
	start, end int
	after      string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-receiver-public-readback [-write]")
	}
	nativeBody := nativeSource()
	var changes []transition
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	must(err)
	must(checkoutsource.WalkDir(".", "internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || !strings.Contains("\n"+string(snapshot), "\n"+path+"\n") {
			return err
		}
		raw, err := exec.Command("git", "show", predecessor+":"+path).Output()
		if err != nil {
			return fmt.Errorf("read immutable source %s: %w", path, err)
		}
		changes = append(changes, transitions(path, raw, nativeBody)...)
		return nil
	}))
	calls := 0
	for _, change := range changes {
		calls += change.Calls
	}
	if calls != 15 {
		panic(fmt.Sprintf("receiver readback call census=%d, want15", calls))
	}
	outputs := prepare(changes)
	raw, err := json.MarshalIndent(changes, "", "  ")
	must(err)
	raw = append(raw, '\n')
	fmt.Printf("calls=%d bodies=%d manifest_sha256=%x\n", calls, len(changes), sha256.Sum256(raw))
	if write {
		for path, output := range outputs {
			must(os.WriteFile(path, output, 0644))
		}
		must(os.WriteFile(ledger, raw, 0644))
	}
}

func nativeSource() string {
	raw, err := exec.Command("git", "show", predecessor+":"+helperFile).Output()
	must(err)
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, helperFile, raw, parser.AllErrors)
	must(err)
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Name.Name == native {
			source := string(raw[fset.Position(function.Pos()).Offset:fset.Position(function.End()).Offset])
			from := "func " + native + "(t *testing.T, endpoint string, owner runtimebus.EventStore, runID string)"
			to := "func " + helper + "(t *testing.T, endpoint string, owner receiverProofStateReader, runID string)"
			if strings.Count(source, from) != 1 {
				panic("native reader signature changed")
			}
			return strings.Replace(source, from, to, 1)
		}
	}
	panic("native receiver observation owner disappeared")
}

func transitions(path string, raw []byte, nativeBody string) []transition {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
	must(err)
	var changes []transition
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		start, end := fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset
		source := string(raw[start:end])
		after, calls := callerSource(fset, function, source)
		removed := false
		if path == helperFile && function.Name.Name == helper {
			after = nativeBody
		}
		if path == helperFile && function.Name.Name == native {
			after, removed = "", true
		}
		if after == source {
			continue
		}
		before := nodeString(function)
		normalized := ""
		if !removed {
			normalized = normalize(after)
		}
		changes = append(changes, transition{path, function.Name.Name, before, normalized, fmt.Sprintf("%x", sha256.Sum256([]byte(before))), calls, removed, after})
	}
	return changes
}

func callerSource(fset *token.FileSet, function *ast.FuncDecl, source string) (string, int) {
	start := fset.Position(function.Pos()).Offset
	var edits []edit
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || name.Name != helper && name.Name != native {
			return true
		}
		if call.Ellipsis.IsValid() || name.Obj != nil && name.Obj.Kind != ast.Fun {
			panic("escaped receiver helper call")
		}
		after := replacementCall(function.Name.Name, name.Name, call)
		edits = append(edits, edit{fset.Position(call.Pos()).Offset - start, fset.Position(call.End()).Offset - start, after})
		return true
	})
	slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
	for _, e := range edits {
		source = source[:e.start] + e.after + source[e.end:]
	}
	if len(edits) != 0 {
		source = ownerCapture(function.Name.Name, source)
	}
	return source, len(edits)
}

func replacementCall(function, name string, call *ast.CallExpr) string {
	if name == native {
		if len(call.Args) != 4 || nodeString(call.Args[0]) != "t" {
			panic("foreign native receiver readback call")
		}
		return helper + "(t, " + nodeString(call.Args[1]) + ", requireReceiverProofStateReader(t, " + nodeString(call.Args[2]) + "), " + nodeString(call.Args[3]) + ")"
	}
	if len(call.Args) != 3 || nodeString(call.Args[0]) != "t" {
		panic("foreign broad receiver readback call")
	}
	endpoint, owner := "rt.Endpoint", "rt.ReceiverStateReader"
	argument := nodeString(call.Args[1])
	if argument != "rt" {
		switch function {
		case "TestReceiverCompositionMixedAgentBothStores":
			if argument != "servedControlProofRuntime{ReadRunDeliveries: deliveryReader, Endpoint: endpoint, DB: db, Backend: backend, Runtime: runtime, BundleHash: bundle}" {
				panic("mixed receiver capture changed")
			}
		case "TestReceiverCompositionRestartBothStores":
			if argument != "servedControlProofRuntime{ReadRunDeliveries: deliveryReader, Endpoint: endpoint, DB: db, Backend: backend}" {
				panic("restart receiver capture changed")
			}
		default:
			panic("unclassified broad receiver argument")
		}
		endpoint, owner = "endpoint", "reader"
	}
	return helper + "(t, " + endpoint + ", " + owner + ", " + nodeString(call.Args[2]) + ")"
}

func ownerCapture(function, source string) string {
	switch function {
	case "TestReceiverCompositionMixedAgentBothStores":
		return replaceOnce(source, "endpoint, runtime := startOwnedMockLifecycleFollowUpRuntime(t, opts)", "endpoint, _ := startOwnedMockLifecycleFollowUpRuntime(t, opts)")
	case "TestReceiverCompositionRestartBothStores":
		source = replaceOnce(source, "var deliveryReader servedRunDeliveryReader", "var reader receiverProofStateReader\n\t\t\tvar deliveryReader servedRunDeliveryReader")
		return replaceOnce(source, "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })", "captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {\n\t\t\t\tdb, _, _ = selectedRuntimeStoreForTest(t, p)\n\t\t\t\treader = requireReceiverProofStateReader(t, p.deps.EventStore)\n\t\t\t})")
	}
	return source
}

func prepare(changes []transition) map[string][]byte {
	grouped := map[string][]transition{}
	for _, change := range changes {
		grouped[change.File] = append(grouped[change.File], change)
	}
	outputs := map[string][]byte{}
	for path, rows := range grouped {
		raw, err := os.ReadFile(path)
		must(err)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, raw, parser.AllErrors)
		must(err)
		var edits []edit
		for _, row := range rows {
			edits = append(edits, currentEdit(fset, file, row)...)
		}
		slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
		for _, e := range edits {
			raw = append(raw[:e.start], append([]byte(e.after), raw[e.end:]...)...)
		}
		if path == helperFile {
			importLine := []byte("\truntimebus \"github.com/division-sh/swarm/internal/runtime/bus\"\n")
			if bytes.Count(raw, importLine) > 1 {
				panic("duplicate retired receiver import")
			}
			raw = bytes.Replace(raw, importLine, nil, 1)
		}
		outputs[path], err = format.Source(raw)
		must(err)
	}
	return outputs
}

func currentEdit(fset *token.FileSet, file *ast.File, row transition) []edit {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != row.Function {
			continue
		}
		actual := nodeString(function)
		comparison, err := servedsettlement.ObservationEarlierSource(".", row.File, row.Function, actual)
		must(err)
		if comparison != row.Before && comparison != row.After {
			panic("receiver caller changed outside complete rule: " + row.File + "/" + row.Function)
		}
		if actual != comparison {
			return []edit{{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, actual}}
		}
		return []edit{{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, row.Source}}
	}
	if row.Removed {
		return nil
	}
	panic("receiver caller disappeared: " + row.File + "/" + row.Function)
}

func replaceOnce(source, before, after string) string {
	if strings.Count(source, before) != 1 {
		panic("non-equivalent reviewed cut: " + before)
	}
	return strings.Replace(source, before, after, 1)
}

func normalize(source string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", "package proof\n"+source, parser.AllErrors)
	must(err)
	if len(file.Decls) != 1 {
		panic("invalid complete declaration")
	}
	return nodeString(file.Decls[0])
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
