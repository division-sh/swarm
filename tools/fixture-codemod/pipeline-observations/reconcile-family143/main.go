package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"strings"
)

type recipe struct {
	Family, File, Function, Before, After string
	Successor                             string              `json:"Successor,omitempty"`
	Removed                               bool                `json:"Removed,omitempty"`
	Mechanical                            *mechanicalSnapshot `json:"Mechanical,omitempty"`
}

type mechanicalSnapshot struct {
	SourceCommit, BeforeSHA256, After string
}

func main() {
	data, err := os.ReadFile("tools/fixture-codemod/pipeline-observations/recipes.json")
	must(err)
	var rows []recipe
	must(json.Unmarshal(data, &rows))
	changed := 0
	allowed := map[string]bool{
		"constructor_unit_fixture_test.go/seedConstructorUnitInstance":                                                        true,
		"workflow_instance_store_sqlite_test.go/TestSQLiteWorkflowInstanceStore_runPipelineMutationUsesRuntimeMutationRunner": true,
		"workflow_instance_store_sqlite_test.go/RunRuntimeMutationContextAcknowledged":                                        true,
		"workflow_compiled_lifecycle_evidence_test.go/TestCompiledTransitionEvidenceRoundTripOnBothStores":                    true,
		"mutation_logging_test.go/TestMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTable":                      true,
		"run_scoped_test_helpers_test.go/testPipelineCoordinatorRunContext":                                                   true,
	}
	for i := range rows {
		row := &rows[i]
		if !strings.HasPrefix(row.File, "internal/runtime/pipeline/") {
			continue
		}
		previous, err := exec.Command("git", "show", "b670e2b5fc519b3cf290652cd96732ca513e3a7a:"+row.File).Output()
		must(err)
		expected := body(previous, row.Function, row.After)
		if expected == "" {
			continue
		}
		if row.Function == "verifyWorkflowTimerPublishedOccurrenceRecovery" {
			old := `t.Fatalf("dispatch deadline did not expire at the transition cut: %v", fireCtx.Err())`
			new := `t.Fatalf("dispatch deadline did not expire at the transition cut: outer=%v dispatch=%v intercept=%v entry=%v exit=%v mutation_calls=%d", fireCtx.Err(), err, capture.interceptErr, capture.entryErr, capture.exitErr, owner.calls)`
			if strings.Count(row.After, old) != 1 || nodeFunction(strings.Replace(row.After, old, new, 1)) != expected {
				panic("upstream timer diagnostic changed more than its failure message")
			}
			row.Successor = expected
			fmt.Printf("diagnostic-successor\t%s\t%s\n", row.File, row.Function)
		}
		current, err := os.ReadFile(row.File)
		if err != nil && !os.IsNotExist(err) {
			must(err)
		}
		actual := ""
		if err == nil {
			actual = body(current, row.Function, row.After)
		}
		if actual == expected {
			continue
		}
		key := strings.TrimPrefix(row.File, "internal/runtime/pipeline/") + "/" + row.Function
		if !allowed[key] {
			panic("unreviewed family143 successor or retirement: " + key)
		}
		delete(allowed, key)
		changed++
		if actual == "" {
			row.Removed = true
			fmt.Printf("removed\t%s\t%s\n", row.File, row.Function)
		} else {
			row.Successor = actual
			fmt.Printf("successor\t%s\t%s\n", row.File, row.Function)
		}
	}
	if changed != 6 || len(allowed) != 0 {
		panic(fmt.Sprintf("family143 reconciliation shape changed: %d, want 6", changed))
	}
	fmt.Fprintf(os.Stderr, "changed %d\n", changed)
	if len(os.Args) > 1 && os.Args[1] == "-write" {
		data, err = json.MarshalIndent(rows, "", "  ")
		must(err)
		must(os.WriteFile("tools/fixture-codemod/pipeline-observations/recipes.json", append(data, '\n'), 0644))
	}
}
func body(source []byte, name, after string) string {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "source.go", source, parser.AllErrors)
	must(err)
	other, err := parser.ParseFile(token.NewFileSet(), "after.go", "package x\n"+after, parser.AllErrors)
	must(err)
	replacement := other.Decls[0].(*ast.FuncDecl)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != name && fn.Name.Name != replacement.Name.Name) {
			continue
		}
		if receiver(fn) != receiver(replacement) {
			continue
		}
		return node(fn)
	}
	return ""
}
func node(n ast.Node) string {
	if n == nil {
		return ""
	}
	var out bytes.Buffer
	must(format.Node(&out, token.NewFileSet(), n))
	return out.String()
}
func nodeFunction(source string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "function.go", "package probe\n"+source, parser.AllErrors)
	must(err)
	return node(file.Decls[0])
}
func receiver(fn *ast.FuncDecl) string {
	if fn.Recv == nil {
		return ""
	}
	return node(fn.Recv.List[0].Type)
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
