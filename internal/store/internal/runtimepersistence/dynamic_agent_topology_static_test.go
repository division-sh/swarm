package runtimepersistence

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentTopologyMutationProductionConsumersStatic(t *testing.T) {
	storeCalls, agentWrites := inspectAgentTopologyProductionPackage(t, ".")
	agentOwnerDir := filepath.Join(repoRootForRuntimeWriterGuard(t), "internal", "store", "internal", "backend", "agentpersistence")
	agentCalls, ownerWrites := inspectAgentTopologyProductionPackage(t, agentOwnerDir)
	effectOwnerDir := filepath.Join(repoRootForRuntimeWriterGuard(t), "internal", "store", "internal", "backend", "effectpersistence")
	effectCalls, effectWrites := inspectAgentTopologyProductionPackage(t, effectOwnerDir)
	for name, count := range agentCalls {
		storeCalls[name] += count
	}
	for name, count := range effectCalls {
		storeCalls[name] += count
	}
	for name, count := range ownerWrites {
		agentWrites[name] += count
	}
	for name, count := range effectWrites {
		agentWrites[name] += count
	}
	for name, want := range map[string]int{
		"authorizeAgentTopologyMutation":                2,
		"AuthorizePostgresDynamicAgentTopologyMutation": 0,
		"AuthorizeSQLiteDynamicAgentTopologyMutation":   0,
		"AuthorizePostgresRawAgentTopologyMutation":     0,
		"AuthorizeSQLiteRawAgentTopologyMutation":       0,
	} {
		if got := storeCalls[name]; got != want {
			t.Fatalf("production %s calls = %d, want %d", name, got, want)
		}
	}
	wantWrites := map[string]int{
		"lifecycle.go":                       4,
		"external_effect_authority_store.go": 2,
		"provider_drain.go":                  2,
	}
	if len(agentWrites) != len(wantWrites) {
		t.Fatalf("production agents-table writers = %#v, want %#v", agentWrites, wantWrites)
	}
	for file, want := range wantWrites {
		if got := agentWrites[file]; got != want {
			t.Fatalf("agents-table writes in %s = %d, want %d", file, got, want)
		}
	}

	managerCalls, _ := inspectAgentTopologyProductionPackage(t, filepath.Join(repoRootForRuntimeWriterGuard(t), "internal", "runtime", "manager"))
	if got := managerCalls["UpsertAgent"]; got != 0 {
		t.Fatalf("production UpsertAgent calls in manager = %d, want zero raw topology writers", got)
	}
}

func inspectAgentTopologyProductionPackage(t *testing.T, dir string) (map[string]int, map[string]int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]int{}
	agentWrites := map[string]int{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				switch fn := call.Fun.(type) {
				case *ast.Ident:
					calls[fn.Name]++
				case *ast.SelectorExpr:
					calls[fn.Sel.Name]++
				}
			}
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			raw := strings.ToUpper(literal.Value)
			if strings.Contains(raw, "INSERT INTO AGENTS") || strings.Contains(raw, "UPDATE AGENTS") || strings.Contains(raw, "DELETE FROM AGENTS") {
				agentWrites[name]++
			}
			return true
		})
	}
	return calls, agentWrites
}
