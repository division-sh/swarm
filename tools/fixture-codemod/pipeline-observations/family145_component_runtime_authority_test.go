package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ComponentRuntimeAuthorityError(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "component.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return err
	}
	want := map[string]int{
		"runfork.RunForkSelectedContractRuntimeFingerprint(fork)":                                                     1,
		"runfork.RunForkSelectedContractRuntimeFingerprint(agents.Records)":                                           1,
		"runfork.RunForkSelectedContractRuntimeFingerprint(agents.Options.Config)":                                    1,
		"prepared.bind(ctx, fork.ForkRunID, loaded, agents)":                                                          1,
		"selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, \"component-runtime-test\", time.Minute)": 1,
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if _, exact := want[family145NodeString(call)]; exact {
				want[family145NodeString(call)]--
			}
		}
		return true
	})
	for name, remaining := range want {
		if remaining != 0 {
			return fmt.Errorf("component authority lost its exact admitted-input derivation or claim: %s", name)
		}
	}
	for _, raw := range []string{"*sql.DB", "db.", "strings.Repeat", "storetest.Database", "InsertExistingRunRootEventRecord", "ExecContext", "run_fork_selected_contract_bindings"} {
		if strings.Contains(source, raw) {
			return fmt.Errorf("component authority retained raw or fabricated setup: %s", raw)
		}
	}
	return nil
}

func TestFamily145ComponentRuntimeAuthorityRetiresPoolBindingAndFabricatedFingerprints(t *testing.T) {
	data, err := os.ReadFile("../../../internal/runtime/runforkexecution/selected_execution_authority_fixture_test.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "component.go", data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	functions := map[string]string{}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok {
			functions[fn.Name.Name] = family145NodeString(fn)
		}
	}
	source := functions["selectedContractTestRuntimeAuthority"]
	if err := family145ComponentRuntimeAuthorityError(source); err != nil {
		t.Fatal(err)
	}
	for _, hostile := range []string{
		strings.Replace(source, "RuntimeFingerprint(fork)", "RuntimeFingerprint(foreignFork)", 1),
		strings.Replace(source, "RuntimeFingerprint(agents.Records)", "RuntimeFingerprint(nil)", 1),
		strings.Replace(source, "RuntimeFingerprint(agents.Options.Config)", "RuntimeFingerprint(nil)", 1),
		strings.Replace(source, "ClaimRunForkSelectedContractRuntimeExecution(ctx, issued,", "ClaimRunForkSelectedContractRuntimeExecution(ctx, foreignIssued,", 1),
		strings.Replace(source, "prepared.bind(ctx, fork.ForkRunID, loaded, agents)", "prepared.bind(ctx, foreignRun, loaded, agents)", 1),
	} {
		if err := family145ComponentRuntimeAuthorityError(hostile); err == nil {
			t.Fatal("foreign or fabricated component authority accepted")
		}
	}
	fork := functions["selectedContractTestRuntimeFork"]
	for _, native := range []string{"storetest.RequireRun(t, ctx, selected,", "storetest.CommitSemanticEvent(t, ctx, selected,", "captureSelectedExecutionSourceRevision(t, selected, sourceRun)", "selected.MaterializeRunFork(ctx,", "SourceArtifactFact: source", "EffectiveSourceIdentity: loaded.EffectiveSourceIdentity"} {
		if !strings.Contains(fork, native) {
			t.Fatalf("component fork lost original native ownership or source identity: %s", native)
		}
	}
	for _, raw := range []string{"*sql.DB", "ExecContext", "AdmitPostgresRuntimeStore", "run_fork_selected_contract_bindings"} {
		if strings.Contains(string(data), raw) {
			t.Fatalf("component fixture retained %s", raw)
		}
	}
}
