package runtimepersistence

import (
	"bytes"
	"go/ast"
	"go/format"
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
		"authorizeAgentTopologyMutation":                4,
		"AuthorizePostgresDynamicAgentTopologyMutation": 1,
		"AuthorizeSQLiteDynamicAgentTopologyMutation":   1,
		"AuthorizePostgresRawAgentTopologyMutation":     1,
		"AuthorizeSQLiteRawAgentTopologyMutation":       1,
	} {
		if got := storeCalls[name]; got != want {
			t.Fatalf("production %s calls = %d, want %d", name, got, want)
		}
	}
	wantWrites := map[string]int{
		"lifecycle.go":                       4,
		"catalog.go":                         1,
		"external_effect_authority_store.go": 2,
		"sqlite_catalog.go":                  1,
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
	if got := managerCalls["UpsertAgent"]; got != 3 {
		t.Fatalf("production UpsertAgent calls in manager = %d, want three classified lifecycle/fallback consumers", got)
	}
}

func TestTerminalTopologyClassificationUsesTypedTargetPhase(t *testing.T) {
	path := filepath.Join(repoRootForRuntimeWriterGuard(t), "internal", "store", "internal", "backend", "agentpersistence", "dynamic_topology.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var classification ast.Expr
	for _, declaration := range parsed.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "lifecycleAgentTopologyMutation" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			element, ok := node.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, ok := element.Key.(*ast.Ident)
			if ok && key.Name == "changesDesiredSet" {
				classification = element.Value
				return false
			}
			return true
		})
	}
	if classification == nil {
		t.Fatal("lifecycle topology classification is missing")
	}
	var rendered bytes.Buffer
	if err := format.Node(&rendered, token.NewFileSet(), classification); err != nil {
		t.Fatal(err)
	}
	const want = "req.Agent != nil || req.TargetPhase == runtimemanager.AgentLifecycleTerminated"
	if got := rendered.String(); got != want {
		t.Fatalf("lifecycle topology classification = %q, want %q", got, want)
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
