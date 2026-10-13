package runforkexecution

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Native provider journeys prove the actual grant-protected settlement. This
// guard preserves the direct call order and rejects the original inversion.
func TestSelectedTerminalManagerJoinPrecedesExecutorQuiesce(t *testing.T) {
	source, err := os.ReadFile("runtime_container.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedTerminalManagerOrder(string(source)); err != nil {
		t.Fatal(err)
	}
	join := "\t\t\tif manager := c.attachment.runtime.manager; manager != nil {\n\t\t\t\tif err := manager.Shutdown(); err != nil {\n\t\t\t\t\treturn err\n\t\t\t\t}\n\t\t\t\tc.attachment.runtime.manager = nil\n\t\t\t}\n"
	if !strings.Contains(string(source), join) {
		t.Fatal("hostile probe lost its exact manager join")
	}
	hostile := strings.Replace(string(source), join, "", 1)
	hostile = strings.Replace(hostile, "\t\t\treturn c.Close(ctx)", join+"\t\t\treturn c.Close(ctx)", 1)
	if err := selectedTerminalManagerOrder(hostile); err == nil {
		t.Fatal("quiescing the execution before manager self-release passed")
	}
}

func selectedTerminalManagerOrder(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "runtime_container.go", source, 0)
	if err != nil {
		return err
	}
	var actual []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "awaitTerminalSettlement" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch method.Sel.Name {
			case "WaitForQuiescence", "RequireRunForkSelectedContractExecutionSettlement", "Shutdown", "Quiesce", "Close", "stopExecutionWork":
				actual = append(actual, method.Sel.Name)
			}
			return true
		})
	}
	want := []string{"WaitForQuiescence", "RequireRunForkSelectedContractExecutionSettlement", "Shutdown", "Quiesce", "Close"}
	if !reflect.DeepEqual(actual, want) {
		return fmt.Errorf("selected terminal manager must join before executor quiesce/close, without joining serving work: got %v", actual)
	}
	return nil
}
