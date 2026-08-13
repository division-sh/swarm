package semanticview_test

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

type agentNameOwnershipFinding struct {
	Path      string
	Enclosing string
	Kind      string
}

func (f agentNameOwnershipFinding) key() string {
	return strings.Join([]string{f.Path, f.Enclosing, f.Kind}, "::")
}

func TestDeclaredAgentNameOwnershipBoundary(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	findings := loadAgentNameOwnershipFindings(t, root, nil)
	if len(findings) != 0 {
		keys := make([]string, len(findings))
		for i, finding := range findings {
			keys[i] = finding.key()
		}
		t.Fatalf("declared-agent-name ownership bypasses remain:\n%s", strings.Join(keys, "\n"))
	}
}

func TestDeclaredAgentNameOwnershipBoundaryRejectsHostileBypasses(t *testing.T) {
	root := agentNameGuardRepoRoot(t)
	path := filepath.Join(root, "internal", "runtime", "semanticview", "agent_name_guard_hostile.go")
	overlay := map[string][]byte{path: []byte(`package semanticview

import runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"

func hostileRawAgentID(entry runtimecontracts.AgentRegistryEntry) string {
	return entry.ID
}

func hostileMapKeyAgentID(bundle *runtimecontracts.WorkflowContractBundle) []string {
	var out []string
	for key := range bundle.AgentEntries() {
		out = append(out, key)
	}
	return out
}

func hostileScopedMapKeyAgentID(view runtimecontracts.FlowContractView) []string {
	var out []string
	for key := range view.Agents {
		out = append(out, key)
	}
	return out
}

func hostileLegacyContractLookup(bundle *runtimecontracts.WorkflowContractBundle, actorID string) {
	bundle.AgentContractSource(actorID)
	bundle.ScopedAgentContractSource(runtimecontracts.ContractItemSource{FlowID: "hostile"}, actorID)
}
`)}
	findings := loadAgentNameOwnershipFindings(t, root, overlay)
	want := map[string]bool{"raw_agent_registry_id": false, "unclassified_agent_map_range": false, "legacy_agent_contract_lookup": false}
	for _, finding := range findings {
		if _, ok := want[finding.Kind]; ok && strings.Contains(finding.Enclosing, "hostile") {
			want[finding.Kind] = true
		}
	}
	for kind, found := range want {
		if !found {
			t.Fatalf("hostile ownership guard did not detect %s: %#v", kind, findings)
		}
	}
}

func loadAgentNameOwnershipFindings(t *testing.T, root string, overlay map[string][]byte) []agentNameOwnershipFinding {
	t.Helper()
	cfg := &packages.Config{
		Dir: root,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests:   false,
		Overlay: overlay,
	}
	patterns := []string{
		"./internal/cliapp",
		"./internal/providerconnectors",
		"./internal/serveapp",
		"./internal/runtime/...",
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		t.Fatalf("load declared-agent-name ownership packages: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("load declared-agent-name ownership packages reported type errors")
	}
	var findings []agentNameOwnershipFinding
	for _, pkg := range pkgs {
		for index, file := range pkg.Syntax {
			if index >= len(pkg.CompiledGoFiles) || strings.HasSuffix(pkg.CompiledGoFiles[index], "_test.go") {
				continue
			}
			rel, err := filepath.Rel(root, pkg.CompiledGoFiles[index])
			if err != nil {
				t.Fatal(err)
			}
			findings = append(findings, collectAgentNameOwnershipFindings(filepath.ToSlash(rel), file, pkg.TypesInfo)...)
		}
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].key() < findings[j].key() })
	return findings
}

func collectAgentNameOwnershipFindings(path string, file *ast.File, info *types.Info) []agentNameOwnershipFinding {
	var findings []agentNameOwnershipFinding
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		enclosing := agentNameGuardFunctionName(function)
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if statement, ok := node.(*ast.RangeStmt); ok && agentNameGuardIsAgentMapRange(statement, info) && !agentNameGuardAgentMapRangeAllowed(path, enclosing) {
				findings = append(findings, agentNameOwnershipFinding{Path: path, Enclosing: enclosing, Kind: "unclassified_agent_map_range"})
			}
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if agentNameGuardIsRawID(selector, info) && !agentNameGuardRawIDAllowed(path, enclosing) {
				findings = append(findings, agentNameOwnershipFinding{Path: path, Enclosing: enclosing, Kind: "raw_agent_registry_id"})
			}
			if agentNameGuardIsLegacyContractLookup(selector, info) && !agentNameGuardLegacyContractLookupAllowed(path, enclosing) {
				findings = append(findings, agentNameOwnershipFinding{Path: path, Enclosing: enclosing, Kind: "legacy_agent_contract_lookup"})
			}
			return true
		})
	}
	return findings
}

func agentNameGuardIsLegacyContractLookup(selector *ast.SelectorExpr, info *types.Info) bool {
	if selector == nil || selector.Sel == nil || info == nil {
		return false
	}
	if selector.Sel.Name != "AgentContractSource" && selector.Sel.Name != "ScopedAgentContractSource" {
		return false
	}
	function, ok := info.Uses[selector.Sel].(*types.Func)
	return ok && function.Pkg() != nil && function.Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/contracts"
}

func agentNameGuardLegacyContractLookupAllowed(path, enclosing string) bool {
	if path != "internal/runtime/semanticview/agent_contract_projection.go" {
		return false
	}
	return enclosing == "ScopedAgentContractProjection" || enclosing == "exactAgentContractSource"
}

func agentNameGuardIsAgentMapRange(statement *ast.RangeStmt, info *types.Info) bool {
	if statement == nil || statement.X == nil || info == nil {
		return false
	}
	typ := info.TypeOf(statement.X)
	named, ok := typ.(*types.Named)
	if ok {
		typ = named.Underlying()
	}
	mapping, ok := typ.(*types.Map)
	if !ok {
		return false
	}
	key, ok := mapping.Key().Underlying().(*types.Basic)
	if !ok || key.Kind() != types.String {
		return false
	}
	element := mapping.Elem()
	if pointer, ok := element.(*types.Pointer); ok {
		element = pointer.Elem()
	}
	entry, ok := element.(*types.Named)
	return ok && entry.Obj() != nil && entry.Obj().Pkg() != nil &&
		entry.Obj().Name() == "AgentRegistryEntry" &&
		entry.Obj().Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/contracts"
}

func agentNameGuardIsRawID(selector *ast.SelectorExpr, info *types.Info) bool {
	if selector == nil || selector.Sel == nil || selector.Sel.Name != "ID" || info == nil {
		return false
	}
	typ := info.TypeOf(selector.X)
	if pointer, ok := typ.(*types.Pointer); ok {
		typ = pointer.Elem()
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj() == nil || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Name() == "AgentRegistryEntry" && named.Obj().Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/contracts"
}

func agentNameGuardRawIDAllowed(path, enclosing string) bool {
	return path == "internal/runtime/contracts/workflow_contract_effective.go" && enclosing == "DeclaredAgentID"
}

func agentNameGuardAgentMapRangeAllowed(path, enclosing string) bool {
	// These exact functions use the map key only as an authored declaration
	// coordinate, or ignore it while inspecting non-identity entry fields. Any
	// new range over the wire map must be classified here or consume a name plan.
	allowed := map[string]struct{}{
		"internal/runtime/authoringview/view.go::agentViews":                                                     {},
		"internal/runtime/bootverify/checks.go::(*checkerContext).invalidFieldDetection":                         {},
		"internal/runtime/bootverify/checks.go::intentFindingsForScope":                                          {},
		"internal/runtime/bootverify/workflow_entity_contract_coverage_checks.go::wave1ScopedAgentRecords":       {},
		"internal/runtime/bootverify/workflow_flow_boundary_checks.go::flowHasScopedInputEscapeHatch":            {},
		"internal/runtime/contracts/agent_intent_resolution.go::materializeAgentIntents":                         {},
		"internal/runtime/contracts/criteria_validation.go::validateAgentCriteriaCitationConsumption":            {},
		"internal/runtime/contracts/criteria_validation.go::validateAgentCriteriaReferences":                     {},
		"internal/runtime/contracts/mock_performance_loading.go::materializeAgentMockPerformances":               {},
		"internal/runtime/contracts/workflow_contract_effective.go::EffectiveAgentRegistryEntries":               {},
		"internal/runtime/contracts/workflow_contract_merging.go::mergeAgentContracts":                           {},
		"internal/runtime/contracts/workflow_contract_paths.go::cloneAgentRegistryEntryMap":                      {},
		"internal/runtime/flowdata/flowdata.go::ValidateSource":                                                  {},
		"internal/runtime/flowdata/flowdata.go::sortedAgentLocalIDs":                                             {},
		"internal/runtime/manager/flow_activation.go::(*AgentManager).flowInstanceAgentRecords":                  {},
		"internal/runtime/manager/flow_activation.go::StaticAgentMaterializationRecords":                         {},
		"internal/runtime/manager/flow_activation.go::flowAgentNamePlans":                                        {},
		"internal/runtime/manager/flow_activation.go::projectAgentNamePlans":                                     {},
		"internal/runtime/manager/flow_activation.go::staticAgentsForScope":                                      {},
		"internal/runtime/manager/flow_activation.go::staticFlowLocalEventSet":                                   {},
		"internal/runtime/semanticview/agent_declarations.go::AgentDeclarations":                                 {},
		"internal/runtime/semanticview/import_boundary_wildcards.go::importBoundaryFlowWildcardSubscriptions":    {},
		"internal/runtime/semanticview/import_boundary_wildcards.go::importBoundaryProjectWildcardSubscriptions": {},
		"internal/runtime/semanticviewtest/source.go::WrapRootAgents":                                            {},
		"internal/runtime/tools/emit_runtime.go::NewEmitRegistry":                                                {},
	}
	_, ok := allowed[path+"::"+enclosing]
	return ok
}

func agentNameGuardFunctionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) == 0 {
		return function.Name.Name
	}
	receiver := function.Recv.List[0].Type
	switch typed := receiver.(type) {
	case *ast.Ident:
		return "(" + typed.Name + ")." + function.Name.Name
	case *ast.StarExpr:
		if name, ok := typed.X.(*ast.Ident); ok {
			return "(*" + name.Name + ")." + function.Name.Name
		}
	}
	return function.Name.Name
}

func agentNameGuardRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
