package semanticview

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
)

func TestResolveAgentExecutionSemanticScopeUsesFilesystemFlowOwnerForEveryMode(t *testing.T) {
	for _, test := range []struct {
		name         string
		flowPath     string
		mode         string
		instanceID   string
		instancePath string
	}{
		{name: "root", flowPath: ".", mode: runtimecontracts.FlowModeStatic},
		{name: "static", flowPath: "support", mode: runtimecontracts.FlowModeStatic, instanceID: "support", instancePath: "support"},
		{name: "singleton", flowPath: "services/ingress", mode: runtimecontracts.FlowModeSingleton, instanceID: "ingress", instancePath: "services/ingress"},
		{name: "template", flowPath: "telegram/telegram-chat", mode: runtimecontracts.FlowModeTemplate, instanceID: "chat-1", instancePath: "telegram/telegram-chat/chat-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, declaration, actor := executionScopeFixture(t, test.flowPath, test.mode, test.instanceID, test.instancePath)
			scope, err := ResolveAgentExecutionSemanticScope(source, actor)
			if err != nil {
				t.Fatal(err)
			}
			flow, ok := scope.OwningFlow()
			if !ok || flow.ID != test.flowPath || scope.ContractSource().FlowPath != test.flowPath || scope.Declaration().OwnerURI != declaration.OwnerURI || scope.Identity() != actor.Identity {
				t.Fatalf("scope = %#v flow = %#v ok=%v", scope, flow, ok)
			}
		})
	}
}

func TestResolveAgentExecutionSemanticScopeRejectsIdentityAndRouteContradictions(t *testing.T) {
	source, declaration, valid := executionScopeFixture(t, "telegram/telegram-chat", runtimecontracts.FlowModeTemplate, "chat-1", "telegram/telegram-chat/chat-1")
	runtimeName, err := agentidentity.RuntimeName(valid.ID, declaration.OwnerURI)
	if err != nil {
		t.Fatal(err)
	}
	wrongOwner, err := agentidentity.DeclaredName(valid.ID, "test://hostile/other-owner")
	if err != nil {
		t.Fatal(err)
	}
	wrongRoute, err := agentidentity.PresentRoute("telegram/sibling", "chat-1", "telegram/sibling/chat-1")
	if err != nil {
		t.Fatal(err)
	}
	baseRoute, err := agentidentity.PresentRoute("telegram/telegram-chat", "telegram-chat", "telegram/telegram-chat")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		contains string
		mutate   func(models.AgentConfig) models.AgentConfig
	}{
		{name: "runtime name", contains: "declared agent identity", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.Identity.Name = runtimeName
			return actor
		}},
		{name: "wrong owner", contains: "no exact declaration", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.Identity.Name = wrongOwner
			return actor
		}},
		{name: "wrong flow", contains: "conflicts with declaration owner flow", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.FlowID = "telegram/sibling"
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
		{name: "root route", contains: "requires concrete identity", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.Identity.Route = agentidentity.RootRoute()
			return actor
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ResolveAgentExecutionSemanticScope(source, test.mutate(valid))
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("error = %v, want %q", err, test.contains)
			}
		})
	}
}

func executionScopeFixture(t *testing.T, flowPath, mode, instanceID, instancePath string) (Source, AgentDeclaration, models.AgentConfig) {
	t.Helper()
	ownerURI := "test://agent-execution/" + strings.ReplaceAll(flowPath, "/", "-") + "/worker"
	entry := runtimecontracts.EffectiveAgentRegistryEntry("worker", runtimecontracts.AgentRegistryEntry{ID: "worker", Role: "worker"})
	view := runtimecontracts.FlowContractView{
		Path:      flowPath,
		Schema:    runtimecontracts.FlowSchemaDocument{Mode: mode},
		Paths:     runtimecontracts.FlowContractPaths{FlowPath: flowPath, AgentsFile: strings.TrimPrefix(flowPath+"/agents.yaml", "./")},
		Agents:    map[string]runtimecontracts.AgentRegistryEntry{"worker": entry},
		AgentURIs: map[string]string{"worker": ownerURI},
	}
	root := &view
	if flowPath != "." {
		root = &runtimecontracts.FlowContractView{Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}, Children: []runtimecontracts.FlowContractView{view}}
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root:   root,
			ByID:   map[string]*runtimecontracts.FlowContractView{flowPath: &view},
			ByPath: map[string]*runtimecontracts.FlowContractView{flowPath: &view},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			ownerURI: {Kind: "agent", FlowID: flowPath, LocalID: "worker", Full: ownerURI},
		}},
	}
	source := Wrap(bundle)
	declarations := AgentDeclarations(source)
	if len(declarations) != 1 {
		t.Fatalf("declarations = %#v", declarations)
	}
	declaration := declarations[0]
	plan, err := ScopedAgentNamePlan(source, declaration)
	if err != nil {
		t.Fatal(err)
	}
	actor := models.AgentConfig{ID: plan.AgentID, FlowID: flowPath, FlowPath: instancePath}
	if flowPath == "." {
		actor.Identity = agentidentitytest.RootDeclared(t, plan.AgentID, plan.OwnerURI)
	} else {
		actor.Identity = agentidentitytest.Declared(t, plan.AgentID, plan.OwnerURI, flowPath, instanceID, instancePath)
	}
	return source, declaration, actor
}
