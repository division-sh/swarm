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
)

const predecessor = "daf6590ef1013decce2a7562ffb480dd9ca5cf32"
const ledger = "tools/fixture-codemod/pipeline-observations/family145-fork-completion-preimages.json"
const helper = "waitForkReceiverSourceCompletion"
const rawRead = "rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND\n\t\t\t(d.status IN ('pending','in_progress') OR d.continuation_handoff_at IS NULL OR NOT EXISTS\n\t\t\t(SELECT 1 FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'))`, runID).Scan(&incomplete)"
const nativeRead = `reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
			var err error
			incomplete, err = storetest.ReadServedIncompletePipelineHandoffCount(ctx, reader, runID)
			return err
		})`

type transformation struct{ File, Function, Before, After, Source, SHA256 string }
type preimage struct{ File, Function, SHA256 string }
type edit struct {
	start, end int
	after      string
}

func main() {
	write := len(os.Args) == 2 && os.Args[1] == "-write"
	if !write && len(os.Args) != 1 {
		panic("usage: migrate-fork-completion [-write]")
	}
	var rules []transformation
	calls := 0
	snapshot, err := exec.Command("git", "ls-tree", "-r", "--name-only", predecessor, "--", "internal/serveapp").Output()
	must(err)
	must(checkoutsource.WalkDir(".", "internal/serveapp", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || !strings.Contains("\n"+string(snapshot), "\n"+path+"\n") {
			return err
		}
		source, err := exec.Command("git", "show", predecessor+":"+path).Output()
		if err != nil {
			return fmt.Errorf("read signed predecessor %s: %w", path, err)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, source, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			before := nodeString(function)
			n := countCalls(function)
			if n == 0 && function.Name.Name != helper {
				continue
			}
			calls += n
			original := string(source[fset.Position(function.Pos()).Offset:fset.Position(function.End()).Offset])
			afterSource := transformSource(function.Name.Name, before, original)
			if n != strings.Count(afterSource, helper+"(t, rt.ReceiverStateReader, ") {
				panic("non-equivalent caller layout: " + path + "/" + function.Name.Name)
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, "package proof\n"+afterSource, parser.AllErrors)
			if err != nil {
				return err
			}
			rules = append(rules, transformation{path, function.Name.Name, before, nodeString(parsed.Decls[0]), afterSource, fmt.Sprintf("%x", sha256.Sum256([]byte(before)))})
		}
		return nil
	}))
	if calls != 30 {
		panic(fmt.Sprintf("signed completion call count %d, want 30", calls))
	}
	var pins []preimage
	for _, rule := range rules {
		pins = append(pins, preimage{rule.File, rule.Function, rule.SHA256})
	}
	raw, err := json.MarshalIndent(pins, "", "  ")
	must(err)
	fmt.Printf("callers=%d bodies=%d preimages_sha256=%x\n", calls, len(pins), sha256.Sum256(raw))
	if write {
		applyRules(rules)
		must(os.WriteFile(ledger, append(raw, '\n'), 0644))
	}
}

func transformSource(function, before, original string) string {
	if function != helper {
		return strings.ReplaceAll(original, helper+"(t, rt, ", helper+"(t, rt.ReceiverStateReader, ")
	}
	if strings.Count(before, rawRead) != 1 {
		panic("completion owner no longer has its exact physical predecessor")
	}
	return strings.NewReplacer("rt servedControlProofRuntime", "reader receiverProofStateReader", rawRead, nativeRead,
		"servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID)", "servedInspectionRunDebugSummary(reader, runID)").Replace(original)
}

func countCalls(function *ast.FuncDecl) int {
	count := 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || nodeString(call.Fun) != helper {
			return true
		}
		if len(call.Args) != 3 || nodeString(call.Args[0]) != "t" || nodeString(call.Args[1]) != "rt" {
			panic("non-equivalent completion caller: " + function.Name.Name)
		}
		count++
		return true
	})
	return count
}

func applyRules(rules []transformation) {
	byFile := map[string]map[string]transformation{}
	for _, rule := range rules {
		if byFile[rule.File] == nil {
			byFile[rule.File] = map[string]transformation{}
		}
		byFile[rule.File][rule.Function] = rule
	}
	for path, fileRules := range byFile {
		source, err := os.ReadFile(path)
		must(err)
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, source, parser.AllErrors)
		must(err)
		var edits []edit
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			rule, ok := fileRules[function.Name.Name]
			if !ok {
				continue
			}
			actual := nodeString(function)
			if actual != rule.Before && actual != rule.After {
				panic("changed caller outside signed transform: " + path + "/" + rule.Function)
			}
			delete(fileRules, rule.Function)
			edits = append(edits, edit{fset.Position(function.Pos()).Offset, fset.Position(function.End()).Offset, rule.Source})
		}
		if len(fileRules) != 0 {
			panic("signed completion caller disappeared: " + path)
		}
		slices.SortFunc(edits, func(a, b edit) int { return b.start - a.start })
		for _, change := range edits {
			source = append(source[:change.start], append([]byte(change.after), source[change.end:]...)...)
		}
		formatted, err := format.Source(source)
		must(err)
		must(os.WriteFile(path, formatted, 0644))
	}
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
