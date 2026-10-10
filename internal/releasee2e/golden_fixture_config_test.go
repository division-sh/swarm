package releasee2e

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGoldenPossessionConfigConsumesOnlySharedListenerOwner(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/golden_agent_workload_test.go"), nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "TestGoldenSQLitePossessionServeJourney" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 3 {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok || name.Name != "writeReleaseFile" {
				return true
			}
			path, ok := call.Args[1].(*ast.Ident)
			if !ok || (path.Name != "configPath" && path.Name != "devConfigPath") {
				return true
			}
			checked++
			owner, ok := call.Args[2].(*ast.CallExpr)
			if !ok {
				t.Fatal("possession config adds an independent block to the shared owner")
			}
			producer, ok := owner.Fun.(*ast.Ident)
			if !ok || producer.Name != "goldenRuntimeConfig" || len(owner.Args) != 1 {
				t.Fatal("possession config bypasses the shared listener owner")
			}
			return true
		})
	}
	if checked != 2 {
		t.Fatalf("possession config owners=%d, want normal and dev", checked)
	}
}

func TestGoldenRuntimeConfigUsesSharedEphemeralListeners(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			store := goldenStoreSelection{name: backend, configYAML: "store:\n  backend: " + backend + "\n"}
			var document map[string]map[string]any
			if err := yaml.Unmarshal([]byte(goldenRuntimeConfig(store)), &document); err != nil {
				t.Fatal(err)
			}
			if len(document) != 5 || len(document["serve"]) != 2 ||
				document["serve"]["api_listen_addr"] != "127.0.0.1:0" ||
				document["serve"]["mcp_listen_addr"] != "127.0.0.1:0" ||
				document["store"]["backend"] != backend ||
				document["runtime"]["recovery_on_startup"] != true ||
				document["llm"]["backend"] != "claude_cli" ||
				document["workspace"]["backend"] != "host" {
				t.Fatalf("golden listener policy, backend or workload profile changed: %#v", document)
			}
		})
	}
}
