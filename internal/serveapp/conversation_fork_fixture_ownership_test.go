package serveapp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestServedConversationForkFixtureUsesSelectedWriteOwner(t *testing.T) {
	raw, err := os.ReadFile("main_runtime_test.go")
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "main_runtime_test.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "seedServedConversationForkSource" {
			continue
		}
		body := string(raw[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
		for _, required := range []string{"servedControlProofAuthorActivityContext", "WorkOccurrence().Begin(ctx)", "lease.Context()", "lease.Done()", "storetest.SeedConversationForkSource", "storetest.PersistManagedAgentTurnFixture"} {
			if !strings.Contains(body, required) {
				t.Errorf("live fixture lost %s", required)
			}
		}
		for _, forbidden := range []string{"ExecContext", "BeginTx", "InsertExistingRunRootEventRecord", "context.Background()"} {
			if strings.Contains(body, forbidden) {
				t.Errorf("live fixture bypasses its selected write/scope owner with %s", forbidden)
			}
		}
		return
	}
	t.Fatal("served conversation fixture entrypoint missing")
}
