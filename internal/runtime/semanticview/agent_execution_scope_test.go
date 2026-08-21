package semanticview

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestLoadAgentExecutionSemanticScopeMatrix(t *testing.T) {
	for _, mode := range []string{
		runtimecontracts.FlowModeStatic,
		runtimecontracts.FlowModeSingleton,
		runtimecontracts.FlowModeTemplate,
	} {
		for _, packageParts := range [][]string{
			nil,
			{"parent", "child"},
			{"parent", "child", "grandchild"},
		} {
			depth := "root"
			if len(packageParts) > 0 {
				depth = strings.Join(packageParts, "_")
			}
			t.Run(mode+"/"+depth, func(t *testing.T) {
				source := loadAgentExecutionSemanticScopeFixture(t, mode, packageParts)
				declarations := AgentDeclarations(source)
				if len(declarations) != 1 {
					t.Fatalf("declarations = %#v, want one physical declaration", declarations)
				}
				declaration := declarations[0]
				wantPackage := "."
				if len(packageParts) > 0 {
					wantPackage = strings.Join(packageParts, "/")
				}
				if declaration.Source.PackageKey != wantPackage || declaration.Source.FlowID != "support" || declaration.OwnerFlowID != "support" {
					t.Fatalf("declaration = %#v, want package %q flow support", declaration, wantPackage)
				}
				plan, err := ScopedAgentNamePlan(source, declaration)
				if err != nil {
					t.Fatalf("ScopedAgentNamePlan: %v", err)
				}
				flowPath := strings.Trim(strings.TrimSpace(source.FlowPath("support")), "/")
				instanceID := "support"
				instancePath := flowPath
				if mode == runtimecontracts.FlowModeTemplate {
					instanceID = "instance-1"
					instancePath = flowPath + "/" + instanceID
				}
				actor := models.AgentConfig{
					ID:       plan.AgentID,
					FlowID:   "support",
					FlowPath: instancePath,
					Identity: agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, flowPath, instanceID, instancePath),
				}
				scope, err := ResolveAgentExecutionSemanticScope(source, actor)
				if err != nil {
					t.Fatalf("ResolveAgentExecutionSemanticScope: %v", err)
				}
				if scope.ContractSource() != declaration.Source || scope.Identity() != actor.Identity {
					t.Fatalf("execution scope = %#v, want exact declaration and concrete identity", scope)
				}
			})
		}
	}
}

func TestResolveAgentExecutionSemanticScopeAdmitsRootStaticSingletonAndImportedTemplate(t *testing.T) {
	rootSource, _ := loadSameFlowSiblingAgentPackages(t)
	rootDeclaration := executionDeclarationByLocalID(t, rootSource, "root-worker")
	rootPlan, err := ScopedAgentNamePlan(rootSource, rootDeclaration)
	if err != nil {
		t.Fatal(err)
	}
	rootActor := models.AgentConfig{
		ID:       rootPlan.AgentID,
		FlowID:   "",
		FlowPath: "",
		Identity: agentidentitytest.RootDeclared(t, rootPlan.AgentID, rootPlan.OwnerURI),
	}
	rootScope, err := ResolveAgentExecutionSemanticScope(rootSource, rootActor)
	if err != nil {
		t.Fatalf("root execution scope: %v", err)
	}
	if _, ok := rootScope.OwningFlow(); ok || rootScope.ContractSource().Layer != "project" {
		t.Fatalf("root execution scope = %#v, want project-owned root declaration", rootScope)
	}

	for _, tc := range []struct {
		name         string
		mode         string
		packageKey   string
		flowID       string
		flowPath     string
		instanceID   string
		instancePath string
	}{
		{name: "static", mode: runtimecontracts.FlowModeStatic, packageKey: ".", flowID: "support", flowPath: "support", instanceID: "support", instancePath: "support"},
		{name: "singleton", mode: runtimecontracts.FlowModeSingleton, packageKey: "services", flowID: "ingress", flowPath: "services/ingress", instanceID: "ingress", instancePath: "services/ingress"},
		{name: "imported template", mode: runtimecontracts.FlowModeTemplate, packageKey: "bot", flowID: "telegram-chat", flowPath: "telegram-ingress/telegram-chat", instanceID: "chat-1", instancePath: "telegram-ingress/telegram-chat/chat-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, declaration, actor := executionFlowAgentFixture(t, tc.mode, tc.packageKey, tc.flowID, tc.flowPath, tc.instanceID, tc.instancePath)
			scope, err := ResolveAgentExecutionSemanticScope(source, actor)
			if err != nil {
				t.Fatalf("ResolveAgentExecutionSemanticScope: %v", err)
			}
			flow, ok := scope.OwningFlow()
			if !ok || flow.ID != tc.flowID || scope.Declaration().OwnerURI != declaration.OwnerURI ||
				scope.ContractSource().PackageKey != tc.packageKey || scope.ContractSource().FlowID != tc.flowID ||
				scope.Identity().Route.InstancePath != tc.instancePath {
				t.Fatalf("scope = %#v, flow = %#v ok=%v", scope, flow, ok)
			}
		})
	}
}

func TestResolveAgentExecutionSemanticScopeRejectsHostileIdentityContradictions(t *testing.T) {
	source, declaration, valid := executionFlowAgentFixture(
		t,
		runtimecontracts.FlowModeTemplate,
		"bot",
		"telegram-chat",
		"telegram-ingress/telegram-chat",
		"chat-1",
		"telegram-ingress/telegram-chat/chat-1",
	)
	runtimeName, err := agentidentity.RuntimeName(valid.ID, declaration.OwnerURI)
	if err != nil {
		t.Fatal(err)
	}
	wrongOwner, err := agentidentity.DeclaredName(valid.ID, "test://hostile/other-owner")
	if err != nil {
		t.Fatal(err)
	}
	wrongRoute, err := agentidentity.PresentRoute("telegram-ingress/sibling", "chat-1", "telegram-ingress/sibling/chat-1")
	if err != nil {
		t.Fatal(err)
	}
	baseRoute, err := agentidentity.PresentRoute("telegram-ingress/telegram-chat", "telegram-chat", "telegram-ingress/telegram-chat")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		mutate   func(models.AgentConfig) models.AgentConfig
		contains string
	}{
		{name: "runtime-created name collision", contains: "declared agent identity", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.Identity.Name = runtimeName
			return actor
		}},
		{name: "wrong declaration owner", contains: "no exact declaration", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.Identity.Name = wrongOwner
			return actor
		}},
		{name: "runtime path used as declaration flow", contains: "conflicts with declaration owner flow", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.FlowID = actor.FlowPath
			return actor
		}},
		{name: "sibling route", contains: "not a concrete instance", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.FlowPath = wrongRoute.InstancePath
			actor.Identity.Route = wrongRoute
			return actor
		}},
		{name: "template base route", contains: "not a concrete instance", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.FlowPath = baseRoute.InstancePath
			actor.Identity.Route = baseRoute
			return actor
		}},
		{name: "root route for flow declaration", contains: "flow_path", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.Identity.Route = agentidentity.RootRoute()
			return actor
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveAgentExecutionSemanticScope(source, tc.mutate(valid))
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("error = %v, want %q", err, tc.contains)
			}
		})
	}
}

func executionFlowAgentFixture(t *testing.T, mode, packageKey, flowID, flowPath, instanceID, instancePath string) (Source, AgentDeclaration, models.AgentConfig) {
	t.Helper()
	ownerURI := "test://agent-execution/" + strings.Trim(packageKey, "/") + "/" + flowID + "/worker"
	entry := runtimecontracts.EffectiveAgentRegistryEntry("worker", runtimecontracts.AgentRegistryEntry{ID: "worker", Role: "worker"})
	view := runtimecontracts.FlowContractView{
		Path:      flowPath,
		Schema:    runtimecontracts.FlowSchemaDocument{Mode: mode},
		Paths:     runtimecontracts.FlowContractPaths{ID: flowID, Flow: flowID, PackageKey: packageKey, AgentsFile: "/contracts/" + packageKey + "/" + flowID + "/agents.yaml"},
		Agents:    map[string]runtimecontracts.AgentRegistryEntry{"worker": entry},
		AgentURIs: map[string]string{"worker": ownerURI},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{view}},
			ByID: map[string]*runtimecontracts.FlowContractView{flowID: &view},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			ownerURI: {Kind: "agent", FlowID: flowID, LocalID: "worker", Full: ownerURI},
		}},
	}
	source := Wrap(bundle)
	declaration := executionDeclarationByLocalID(t, source, "worker")
	plan, err := ScopedAgentNamePlan(source, declaration)
	if err != nil {
		t.Fatal(err)
	}
	identity := agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, flowPath, instanceID, instancePath)
	return source, declaration, models.AgentConfig{ID: plan.AgentID, FlowID: flowID, FlowPath: instancePath, Identity: identity}
}

func executionDeclarationByLocalID(t *testing.T, source Source, localID string) AgentDeclaration {
	t.Helper()
	var found AgentDeclaration
	for _, declaration := range AgentDeclarations(source) {
		if declaration.LocalID != localID {
			continue
		}
		if found.LocalID != "" {
			t.Fatalf("agent declaration %q is ambiguous: %#v", localID, AgentDeclarations(source))
		}
		found = declaration
	}
	if found.LocalID == "" {
		t.Fatalf("agent declaration %q not found: %#v", localID, AgentDeclarations(source))
	}
	return found
}

func loadAgentExecutionSemanticScopeFixture(t *testing.T, mode string, packageParts []string) Source {
	t.Helper()
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	root := t.TempDir()
	write := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", path, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", path, err)
		}
	}
	packageRoot := root
	for index := 0; index <= len(packageParts); index++ {
		name := "agent-execution-root"
		if index > 0 {
			name = packageParts[index-1]
		}
		manifest := "name: " + name + "\nversion: \"1.0.0\"\n"
		if index == 0 {
			manifest += "platform_version: \">=0.7.0 <0.8.0\"\n"
		}
		if index < len(packageParts) {
			manifest += "packages:\n  - {path: " + packageParts[index] + "}\n"
		} else {
			manifest += "flows:\n  - {id: support, flow: support, mode: " + mode + "}\n"
		}
		write(filepath.Join(packageRoot, "package.yaml"), manifest)
		if index < len(packageParts) {
			packageRoot = filepath.Join(packageRoot, packageParts[index])
		}
	}
	write(filepath.Join(root, "schema.yaml"), "name: agent-execution-root\n")
	for _, name := range []string{"agents.yaml", "entities.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml"} {
		write(filepath.Join(root, name), "{}\n")
	}
	flowRoot := filepath.Join(packageRoot, "flows", "support")
	write(filepath.Join(flowRoot, "package.yaml"), "name: support\nversion: \"1.0.0\"\nflows: []\n")
	write(filepath.Join(flowRoot, "schema.yaml"), "name: support\nmode: "+mode+"\ninitial_state: active\nstates: [active]\n")
	write(filepath.Join(flowRoot, "events.yaml"), "work.requested: {}\n")
	write(filepath.Join(flowRoot, "agents.yaml"), `
worker:
  role: worker
  intent: {inline: Exercise strict agent execution scope loading.}
  model: regular
  memory: false
  subscriptions: [work.requested]
`)
	for _, name := range []string{"nodes.yaml", "policy.yaml", "tools.yaml"} {
		write(filepath.Join(flowRoot, name), "{}\n")
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return Wrap(bundle)
}
