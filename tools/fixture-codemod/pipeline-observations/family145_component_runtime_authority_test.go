package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
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
