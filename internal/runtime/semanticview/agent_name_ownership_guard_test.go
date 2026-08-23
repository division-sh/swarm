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
	flowBoundaryPath := "internal/runtime/bootverify/workflow_flow_boundary_checks.go"
	flowBoundaryFunction := "flowHasScopedInputEscapeHatch"
	if agentNameGuardRawAgentScopeMapAllowed(flowBoundaryPath, flowBoundaryFunction) || agentNameGuardAgentMapRangeAllowed(flowBoundaryPath, flowBoundaryFunction) {
		t.Fatal("retired flow-boundary agent-map interpreter remains exempt from ownership guard")
	}
	if agentNameGuardAgentMapRangeAllowed("internal/runtime/manager/flow_activation.go", "(*AgentManager).flowInstanceAgentRecords") {
		t.Fatal("canonical flow-instance agent consumer remains exempt from ownership guard")
	}
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

func hostileArbitraryReceiverIndex(arbitrary FlowScope, id string) runtimecontracts.AgentRegistryEntry {
	return arbitrary.Agents[id]
}

func hostileContractViewIndex(arbitrary runtimecontracts.FlowContractView, id string) runtimecontracts.AgentRegistryEntry {
	return arbitrary.Agents[id]
}

func hostileLegacyContractLookup(bundle *runtimecontracts.WorkflowContractBundle, actorID string) {
	bundle.AgentContractSource(actorID)
	bundle.ScopedAgentContractSource(runtimecontracts.ContractItemSource{FlowID: "hostile"}, actorID)
}
	`)}
	toolsPath := filepath.Join(root, "internal", "runtime", "tools", "agent_source_guard_hostile.go")
	overlay[toolsPath] = []byte(`package tools

import (
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func hostileSixthAgentSourceProducer(source semanticview.Source, actor models.AgentConfig) {
	runtimepinrouting.AdmitAgentExecutionRoutingSource(source, actor, "entity")
}
`)
	authoringPath := filepath.Join(root, "internal", "runtime", "authoringview", "agent_name_guard_hostile.go")
	overlay[authoringPath] = []byte(`package authoringview

import "github.com/division-sh/swarm/internal/runtime/semanticview"

func hostileAuthoringRawAgentMap(arbitrary semanticview.FlowScope, id string) {
	_ = arbitrary.Agents[id]
}
`)
	requiredPath := filepath.Join(root, "internal", "runtime", "requiredagents", "agent_name_guard_hostile.go")
	overlay[requiredPath] = []byte(`package requiredagents

import "github.com/division-sh/swarm/internal/runtime/semanticview"

func hostileRequiredAgentRawMap(arbitrary semanticview.ProjectScope) []string {
	var out []string
	for id := range arbitrary.Agents {
		out = append(out, id)
	}
	return out
}
`)
	findings := loadAgentNameOwnershipFindings(t, root, overlay)
	want := map[string]bool{
		"raw_agent_registry_id":             false,
		"raw_agent_scope_map_read":          false,
		"unclassified_agent_map_range":      false,
		"legacy_agent_contract_lookup":      false,
		"unapproved_agent_source_admission": false,
	}
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
	consumerBypasses := map[string]bool{
		"internal/runtime/authoringview/agent_name_guard_hostile.go::hostileAuthoringRawAgentMap::raw_agent_scope_map_read":     false,
		"internal/runtime/requiredagents/agent_name_guard_hostile.go::hostileRequiredAgentRawMap::raw_agent_scope_map_read":     false,
		"internal/runtime/requiredagents/agent_name_guard_hostile.go::hostileRequiredAgentRawMap::unclassified_agent_map_range": false,
	}
	for _, finding := range findings {
		if _, ok := consumerBypasses[finding.key()]; ok {
			consumerBypasses[finding.key()] = true
		}
	}
	for bypass, found := range consumerBypasses {
		if !found {
			t.Fatalf("hostile consumer bypass was not detected at %s: %#v", bypass, findings)
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
			if call, ok := node.(*ast.CallExpr); ok && agentNameGuardIsAgentSourceAdmission(call, info) && !agentNameGuardAgentSourceAdmissionAllowed(path, enclosing) {
				findings = append(findings, agentNameOwnershipFinding{Path: path, Enclosing: enclosing, Kind: "unapproved_agent_source_admission"})
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
			if agentNameGuardIsRawAgentScopeMap(selector, info) && !agentNameGuardRawAgentScopeMapAllowed(path, enclosing) {
				findings = append(findings, agentNameOwnershipFinding{Path: path, Enclosing: enclosing, Kind: "raw_agent_scope_map_read"})
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
	return false
}

func agentNameGuardIsAgentSourceAdmission(call *ast.CallExpr, info *types.Info) bool {
	if call == nil || info == nil {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel == nil || selector.Sel.Name != "AdmitAgentExecutionRoutingSource" {
		return false
	}
	function, ok := info.Uses[selector.Sel].(*types.Func)
	return ok && function.Pkg() != nil && function.Pkg().Path() == "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
}

func agentNameGuardAgentSourceAdmissionAllowed(path, enclosing string) bool {
	allowed := map[string]struct{}{
		"internal/runtime/tools/channel_runtime.go::(*Executor).execChannelOperation": {},
		"internal/runtime/tools/executor_agents.go::(*Executor).execSchedule":         {},
		"internal/runtime/tools/executor_emit.go::(*Executor).handleEmitTool":         {},
		"internal/runtime/tools/executor_human_tasks.go::(*Executor).execAskHuman":    {},
	}
	_, ok := allowed[path+"::"+enclosing]
	return ok
}

func agentNameGuardIsRawAgentScopeMap(selector *ast.SelectorExpr, info *types.Info) bool {
	if selector == nil || selector.Sel == nil || selector.Sel.Name != "Agents" || info == nil {
		return false
	}
	typ := info.TypeOf(selector)
	if named, ok := typ.(*types.Named); ok {
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

func agentNameGuardRawAgentScopeMapAllowed(path, enclosing string) bool {
	allowed := map[string]struct{}{
		"internal/runtime/contracts/agent_registry_resolution.go::bundleAgentRecords":                            {},
		"internal/runtime/contracts/agent_intent_resolution.go::materializeAgentIntents":                         {},
		"internal/runtime/contracts/criteria_validation.go::validateAgentCriteriaCitationConsumption":            {},
		"internal/runtime/contracts/criteria_validation.go::validateAgentCriteriaReferences":                     {},
		"internal/runtime/contracts/mock_performance_loading.go::materializeAgentMockPerformances":               {},
		"internal/runtime/contracts/workflow_contract_accessors.go::(*WorkflowContractBundle).AgentEntries":      {},
		"internal/runtime/contracts/workflow_contract_accessors.go::(*WorkflowContractBundle).AgentEntry":        {},
		"internal/runtime/contracts/workflow_contract_effective.go::EffectiveAgentRegistryEntries":               {},
		"internal/runtime/contracts/workflow_contract_merging.go::mergeAgentContracts":                           {},
		"internal/runtime/contracts/workflow_contract_paths.go::cloneAgentRegistryEntryMap":                      {},
		"internal/runtime/contracts/workflow_contract_tree.go::buildFlowTree":                                    {},
		"internal/runtime/contracts/workflow_contract_tree.go::loadFlowContractView":                             {},
		"internal/runtime/contracts/workflow_contract_tree.go::loadProjectContractView":                          {},
		"internal/runtime/contracts/workflow_contract_tree.go::populateMergedPackageViews":                       {},
		"internal/runtime/semanticview/bundle_source.go::(bundleSource).ProjectScopes":                           {},
		"internal/runtime/semanticview/scopes.go::flowScopeFromView":                                             {},
		"internal/runtime/semanticview/import_boundary_wildcards.go::importBoundaryFlowWildcardSubscriptions":    {},
		"internal/runtime/semanticview/import_boundary_wildcards.go::importBoundaryProjectWildcardSubscriptions": {},
		"internal/runtime/semanticviewtest/source.go::WrapRootAgents":                                            {},
	}
	_, ok := allowed[path+"::"+enclosing]
	return ok
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
		"internal/runtime/contracts/agent_intent_resolution.go::materializeAgentIntents":                         {},
		"internal/runtime/contracts/criteria_validation.go::validateAgentCriteriaCitationConsumption":            {},
		"internal/runtime/contracts/criteria_validation.go::validateAgentCriteriaReferences":                     {},
		"internal/runtime/contracts/mock_performance_loading.go::materializeAgentMockPerformances":               {},
		"internal/runtime/contracts/workflow_contract_effective.go::EffectiveAgentRegistryEntries":               {},
		"internal/runtime/contracts/workflow_contract_merging.go::mergeAgentContracts":                           {},
		"internal/runtime/contracts/workflow_contract_paths.go::cloneAgentRegistryEntryMap":                      {},
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
