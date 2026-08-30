package tools

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

var retiredDynamicAgentToolTestNames = []string{"agent_hire", "agent_fire", "agent_reconfigure"}

func TestValidateRetiredDynamicAgentToolReferencesRejectsEveryAuthoredSurface(t *testing.T) {
	for _, name := range retiredDynamicAgentToolTestNames {
		for _, scope := range []string{"root", "flow"} {
			t.Run(name+"/"+scope+"/agent_tools", func(t *testing.T) {
				assertRetiredDynamicAgentToolRejected(t, name, retiredToolSourceForScope(scope,
					map[string]runtimecontracts.AgentRegistryEntry{"worker": {ID: "worker", Tools: []string{name}}},
					nil,
					runtimecontracts.PolicyDocument{},
				))
			})
			t.Run(name+"/"+scope+"/direct_permissions", func(t *testing.T) {
				assertRetiredDynamicAgentToolRejected(t, name, retiredToolSourceForScope(scope,
					map[string]runtimecontracts.AgentRegistryEntry{"worker": {ID: "worker", Permissions: []string{name}}},
					nil,
					runtimecontracts.PolicyDocument{},
				))
			})
			t.Run(name+"/"+scope+"/permission_bundle", func(t *testing.T) {
				assertRetiredDynamicAgentToolRejected(t, name, retiredToolSourceForScope(scope, nil, nil,
					runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
						"permission_bundles": {Value: map[string]any{
							"operators": map[string]any{"permissions": []any{name}},
						}},
					}},
				))
			})
		}
	}
}

func TestValidateRetiredDynamicAgentToolReferencesRejectsEveryToolHandlerDisposition(t *testing.T) {
	for _, name := range retiredDynamicAgentToolTestNames {
		for _, scope := range []string{"root", "flow"} {
			for _, test := range []struct {
				name  string
				entry runtimecontracts.ToolSchemaEntry
			}{
				{name: "platform_builtin", entry: retiredToolEntry(runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerPlatformBuiltin))},
				{name: "http", entry: retiredToolEntry(
					runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
					runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: "POST", URL: "https://example.invalid"}),
				)},
				{name: "mcp", entry: retiredToolEntry(
					runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerMCP),
					runtimecontracts.WithToolMCP(runtimecontracts.MustToolMCPBinding("test", "mutate")),
				)},
				{name: "channel", entry: retiredToolEntry(
					runtimecontracts.WithToolCategory("channel_operation"),
					runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerChannel),
				)},
				{name: "unspecified", entry: retiredToolEntry()},
			} {
				t.Run(name+"/"+scope+"/"+test.name, func(t *testing.T) {
					assertRetiredDynamicAgentToolRejected(t, name, retiredToolSourceForScope(scope, nil,
						map[string]runtimecontracts.ToolSchemaEntry{name: test.entry},
						runtimecontracts.PolicyDocument{},
					))
				})
			}
		}
	}
}

type retiredDynamicAgentToolSource struct {
	semanticview.Source
	rootTools  map[string]runtimecontracts.ToolSchemaEntry
	rootPolicy runtimecontracts.PolicyDocument
	flows      []semanticview.FlowScope
}

func (s retiredDynamicAgentToolSource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	return s.rootTools
}

func (s retiredDynamicAgentToolSource) ResolvedPolicyForFlow(string) runtimecontracts.PolicyDocument {
	return s.rootPolicy
}

func (s retiredDynamicAgentToolSource) FlowScopes() []semanticview.FlowScope { return s.flows }

func retiredToolSourceForScope(
	scope string,
	agents map[string]runtimecontracts.AgentRegistryEntry,
	tools map[string]runtimecontracts.ToolSchemaEntry,
	policy runtimecontracts.PolicyDocument,
) semanticview.Source {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	source := retiredDynamicAgentToolSource{}
	ownerURI := "test://retired-tool-scope/worker"
	registerRootAgent := func() {
		root := &runtimecontracts.FlowContractView{
			Paths:     runtimecontracts.FlowContractPaths{FlowPath: ".", AgentsFile: "agents.yaml"},
			Agents:    agents,
			AgentURIs: map[string]string{"worker": ownerURI},
		}
		bundle.FlowTree.Root = root
		bundle.FlowTree.ByID = map[string]*runtimecontracts.FlowContractView{".": root}
		bundle.URIRegistry = runtimecontracts.ContractURIRegistry{
			Agents: map[string]runtimecontracts.ContractURIRef{"worker": {Kind: "agent", LocalID: "worker", Full: ownerURI}},
			ByURI:  map[string]runtimecontracts.ContractURIRef{ownerURI: {Kind: "agent", LocalID: "worker", Full: ownerURI}},
		}
	}
	switch scope {
	case "root":
		bundle.Agents = agents
		registerRootAgent()
		source.rootTools, source.rootPolicy = tools, policy
	case "flow":
		flow := &runtimecontracts.FlowContractView{
			Paths:     runtimecontracts.FlowContractPaths{FlowPath: "flow-fixture", AgentsFile: "flow-fixture/agents.yaml"},
			Path:      "flow-fixture",
			Schema:    runtimecontracts.FlowSchemaDocument{Mode: runtimecontracts.FlowModeStatic},
			Agents:    agents,
			AgentURIs: map[string]string{"worker": ownerURI},
		}
		bundle.FlowTree.Root = flow
		bundle.FlowTree.ByID = map[string]*runtimecontracts.FlowContractView{"flow-fixture": flow}
		bundle.URIRegistry = runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			ownerURI: {Kind: "agent", FlowID: "flow-fixture", LocalID: "worker", Full: ownerURI},
		}}
		source.flows = []semanticview.FlowScope{{ID: "flow-fixture", Agents: agents, AgentURIs: map[string]string{"worker": ownerURI}, Tools: tools, Policy: policy}}
	default:
		panic("unsupported test scope: " + scope)
	}
	source.Source = semanticview.Wrap(bundle)
	return source
}

func TestRetiredDynamicAgentToolsAreAbsentFromNormalProviderInventories(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	contractDefs, err := ContractDefinitionsForSource(source)
	if err != nil {
		t.Fatalf("ContractDefinitionsForSource: %v", err)
	}
	exec := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
	actorDefs := exec.ToolDefinitionsForActor(models.AgentConfig{ExecutionMode: "live", ID: "worker"})
	for _, name := range retiredDynamicAgentToolTestNames {
		for surface, defs := range map[string]any{"contract": contractDefs, "actor": actorDefs} {
			if strings.Contains(fmt.Sprint(defs), name) {
				t.Fatalf("%s provider inventory retained %s: %#v", surface, name, defs)
			}
		}
	}
}

type retiredToolManagerProbe struct{ resolveCalls int }

func (m *retiredToolManagerProbe) ResolveAgentConfig(string, string) (models.AgentConfig, error) {
	m.resolveCalls++
	return models.AgentConfig{}, fmt.Errorf("unexpected manager resolution")
}

func TestRetiredDynamicAgentToolCallsNeverReachManager(t *testing.T) {
	for _, name := range retiredDynamicAgentToolTestNames {
		for form, callName := range map[string]string{
			"canonical": name,
			"prefixed":  "mcp__runtime-tools__" + name,
		} {
			t.Run(name+"/"+form, func(t *testing.T) {
				manager := &retiredToolManagerProbe{}
				exec := NewExecutor(nil, nil, manager)
				actor := models.AgentConfig{ExecutionMode: "live", ID: "hostile", Tools: []string{name}}
				ctx := WithActor(context.Background(), actor)
				if _, err := exec.Execute(ctx, callName, map[string]any{"agent_id": "target"}); err == nil {
					t.Fatalf("hostile call %q was accepted", callName)
				}
				if manager.resolveCalls != 0 {
					t.Fatalf("hostile call %q reached manager %d time(s)", callName, manager.resolveCalls)
				}
			})
		}
	}
}

func TestRetiredDynamicAgentToolsFailClosedAtProviderProjectionAndDispatch(t *testing.T) {
	for _, name := range retiredDynamicAgentToolTestNames {
		t.Run(name, func(t *testing.T) {
			var transportCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				transportCalls.Add(1)
			}))
			defer server.Close()

			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": {ID: "worker", Tools: []string{name}}},
				Tools: map[string]runtimecontracts.ToolSchemaEntry{name: retiredToolEntry(
					runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
					runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: server.URL}),
				)},
			})
			if _, err := ContractDefinitionsForSource(source); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("ContractDefinitionsForSource error = %v, want retirement of %s", err, name)
			}

			manager := &retiredToolManagerProbe{}
			exec := NewExecutorWithOptions(nil, ExecutorOptions{Manager: manager, WorkflowSource: source})
			actor := models.AgentConfig{ExecutionMode: "live", ID: "worker", Tools: []string{name}}
			if defs := exec.ToolDefinitionsForActor(actor); len(defs) != 0 {
				t.Fatalf("provider projection retained %s: %#v", name, defs)
			}
			ctx := WithActor(context.Background(), actor)
			for _, callName := range []string{name, "mcp__runtime-tools__" + name} {
				if _, err := exec.Execute(ctx, callName, map[string]any{"agent_id": "target"}); err == nil {
					t.Fatalf("hostile call %q was accepted", callName)
				}
			}
			if manager.resolveCalls != 0 {
				t.Fatalf("hostile calls reached manager %d time(s)", manager.resolveCalls)
			}
			if got := transportCalls.Load(); got != 0 {
				t.Fatalf("hostile calls reached HTTP transport %d time(s)", got)
			}
		})
	}
}

func TestValidateRetiredDynamicAgentToolReferencesUsesExactNamesOnly(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {ID: "worker", Tools: []string{"agent_reconfigured"}},
		},
	}
	if errs := ValidateRetiredDynamicAgentToolReferences(semanticview.Wrap(bundle)); len(errs) != 0 {
		t.Fatalf("unrelated lifecycle fact rejected: %v", errs)
	}
}

func TestRetiredDynamicAgentToolTokensRemainOnlyInRetirementEvidence(t *testing.T) {
	root := retiredDynamicAgentToolRepoRoot(t)
	allowed := map[string]struct{}{
		"internal/runtime/workflow_validation_test.go":                          {},
		"internal/runtime/tools/retired_dynamic_agent_tools.go":                 {},
		"internal/runtime/tools/retired_dynamic_agent_tools_test.go":            {},
		"internal/runtime/mcp/retired_dynamic_agent_tools_test.go":              {},
		"internal/runtime/runforkexecution/retired_dynamic_agent_tools_test.go": {},
		"platform-spec.yaml": {},
	}
	pattern := regexp.MustCompile(`\bagent_(hire|fire|reconfigure)\b`)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "test-results" {
				return filepath.SkipDir
			}
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		switch ext {
		case ".go", ".yaml", ".yml", ".json", ".tsv", ".md":
		default:
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !pattern.Match(contents) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, ok := allowed[rel]; !ok {
			t.Errorf("retired dynamic-agent tool token survives in %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRetiredDynamicAgentMutationSymbolsAreAbsent(t *testing.T) {
	root := retiredDynamicAgentToolRepoRoot(t)
	banned := map[string]struct{}{
		"SpawnAgentForEntity":       {},
		"ReconfigureAgentTarget":    {},
		"TeardownAgentTarget":       {},
		"AgentTargetMutationResult": {},
		"AuthorizeManagement":       {},
		"AuthorizeRouting":          {},
	}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || path == filepath.Join(root, "internal/runtime/tools/retired_dynamic_agent_tools_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if _, forbidden := banned[ident.Name]; forbidden {
				t.Errorf("retired mutation symbol %s survives in %s", ident.Name, path)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func retiredDynamicAgentToolRepoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve retirement test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func assertRetiredDynamicAgentToolRejected(t *testing.T, name string, source semanticview.Source) {
	t.Helper()
	errs := ValidateRetiredDynamicAgentToolReferences(source)
	if len(errs) == 0 {
		t.Fatalf("%s was accepted", name)
	}
	joined := make([]string, 0, len(errs))
	for _, err := range errs {
		joined = append(joined, err.Error())
	}
	message := strings.Join(joined, "\n")
	for _, want := range []string{name, "RETIRED", "agents.yaml", "flow lifecycle/readiness", "typed fan-out"} {
		if !strings.Contains(message, want) {
			t.Fatalf("retirement error = %q, want %q", message, want)
		}
	}
	if _, err := ValidateToolImplementations(source); err == nil || !strings.Contains(err.Error(), name) {
		t.Fatalf("ValidateToolImplementations error = %v, want retirement of %s", err, name)
	}
}

func retiredToolEntry(options ...runtimecontracts.ToolSchemaEntryOption) runtimecontracts.ToolSchemaEntry {
	base := []runtimecontracts.ToolSchemaEntryOption{
		runtimecontracts.WithToolDescription("negative retirement fixture"),
		runtimecontracts.WithToolSchemas(
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
		),
	}
	return runtimecontracts.MustToolSchemaEntry(append(base, options...)...)
}
