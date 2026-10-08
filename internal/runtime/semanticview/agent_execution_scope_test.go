package semanticview_test

import (
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
		{name: "singleton", flowPath: "services/ingress", mode: runtimecontracts.FlowModeStatic, instanceID: "ingress", instancePath: "services/ingress"},
		{name: "template", flowPath: "telegram/telegram-chat", mode: runtimecontracts.FlowModeTemplate, instanceID: "chat-1", instancePath: "telegram/telegram-chat/chat-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, declaration, actor := executionScopeFixture(t, test.flowPath, test.mode, test.instanceID, test.instancePath)
			construction := executionScopeConstruction(t, source, actor)
			scope, err := semanticview.ResolveAgentExecutionSemanticScope(source, actor, construction)
			if err != nil {
				t.Fatal(err)
			}
			flow, ok := scope.OwningFlow()
			if !ok || flow.ID != test.flowPath || scope.ContractSource().FlowPath != test.flowPath || scope.Declaration().OwnerURI != declaration.OwnerURI || scope.Identity() != actor.Identity {
				t.Fatalf("scope = %#v flow = %#v ok=%v", scope, flow, ok)
			}
			plan, err := actor.Identity.Plan()
			if err != nil {
				t.Fatal(err)
			}
			planned, err := semanticview.ResolveAgentPlanExecutionSemanticScope(source, actor.Identity.RunID, plan, construction)
			if err != nil || !reflect.DeepEqual(planned, scope) {
				t.Fatalf("plan/actor scope disagreement: %+v %v", planned, err)
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
		{name: "sibling route", contains: "conflicts with its exact constructed flow", mutate: func(actor models.AgentConfig) models.AgentConfig {
			actor.FlowPath = wrongRoute.InstancePath
			actor.Identity.Route = wrongRoute
			return actor
		}},
		{name: "template base route", contains: "conflicts with its exact constructed flow", mutate: func(actor models.AgentConfig) models.AgentConfig {
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
			construction := executionScopeConstruction(t, source, valid)
			_, err := semanticview.ResolveAgentExecutionSemanticScope(source, test.mutate(valid), construction)
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("error = %v, want %q", err, test.contains)
			}
			// An explicit config FlowID is not part of a runless plan. Every
			// identity/route contradiction must also fail for the plan carrier.
			if test.name != "wrong flow" {
				actor := test.mutate(valid)
				plan := agentidentity.Plan{Name: actor.Identity.Name, Route: actor.Identity.Route}
				if _, err := semanticview.ResolveAgentPlanExecutionSemanticScope(source, actor.Identity.RunID, plan, construction); err == nil {
					t.Fatal("contradictory agent plan acquired semantic scope")
				}
			}
		})
	}
}

func executionScopeFixture(t *testing.T, flowPath, mode, instanceID, instancePath string) (semanticview.Source, semanticview.AgentDeclaration, models.AgentConfig) {
	t.Helper()
	ownerURI := "test://agent-execution/" + strings.ReplaceAll(flowPath, "/", "-") + "/worker"
	entry := runtimecontracts.EffectiveAgentRegistryEntry("worker", runtimecontracts.AgentRegistryEntry{ID: "worker", Role: "worker"})
	view := runtimecontracts.FlowContractView{
		Path:      flowPath,
		Schema:    runtimecontracts.FlowSchemaDocument{},
		Paths:     runtimecontracts.FlowContractPaths{FlowPath: flowPath, AgentsFile: strings.TrimPrefix(flowPath+"/agents.yaml", "./")},
		Agents:    map[string]runtimecontracts.AgentRegistryEntry{"worker": entry},
		AgentURIs: map[string]string{"worker": ownerURI},
	}
	if mode == runtimecontracts.FlowModeTemplate {
		field, err := runtimecontracts.ParseTemplateInstanceField("instance_id")
		if err != nil {
			t.Fatal(err)
		}
		view.Schema.Instance = field
	}
	root := &view
	if flowPath != "." {
		root = &runtimecontracts.FlowContractView{Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: "."}}
		parent := root
		parts := strings.Split(flowPath, "/")
		for i := range parts {
			path := strings.Join(parts[:i+1], "/")
			child := runtimecontracts.FlowContractView{Path: path, Paths: runtimecontracts.FlowContractPaths{FlowPath: path}}
			if path == flowPath {
				child = view
			}
			parent.Children = []runtimecontracts.FlowContractView{child}
			parent.Children[0].Parent = parent
			parent = &parent.Children[0]
		}
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema:  &root.Schema,
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{},
		FlowTree: runtimecontracts.FlowTree{
			Root:   root,
			ByID:   map[string]*runtimecontracts.FlowContractView{flowPath: &view},
			ByPath: map[string]*runtimecontracts.FlowContractView{flowPath: &view},
		},
		URIRegistry: runtimecontracts.ContractURIRegistry{ByURI: map[string]runtimecontracts.ContractURIRef{
			ownerURI: {Kind: "agent", FlowID: flowPath, LocalID: "worker", Full: ownerURI},
		}},
	}
	var index func(*runtimecontracts.FlowContractView)
	index = func(node *runtimecontracts.FlowContractView) {
		bundle.FlowSchemas[node.Paths.FlowPath] = node.Schema
		bundle.FlowTree.ByID[node.Paths.FlowPath], bundle.FlowTree.ByPath[node.Paths.FlowPath] = node, node
		for i := range node.Children {
			index(&node.Children[i])
		}
	}
	index(root)
	source := semanticview.Wrap(bundle)
	declarations := semanticview.AgentDeclarations(source)
	if len(declarations) != 1 {
		t.Fatalf("declarations = %#v", declarations)
	}
	declaration := declarations[0]
	plan, err := semanticview.ScopedAgentNamePlan(source, declaration)
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

func executionScopeConstruction(t *testing.T, source semanticview.Source, actor models.AgentConfig) semanticview.AgentExecutionConstruction {
	t.Helper()
	if actor.FlowID == "." {
		return nil
	}
	schema, found := source.FlowSchemaByID(actor.FlowID)
	if !found {
		t.Fatal("fixture has no construction schema")
	}
	if !schema.Instance.Empty() {
		bundle, found := semanticview.Bundle(source)
		if !found {
			t.Fatal("fixture requires its admitted flow tree")
		}
		view, found := bundle.FlowViewByID(actor.FlowID)
		if !found || view.Parent == nil {
			t.Fatal("fixture requires its exact parent declaration")
		}
		parent, err := flowidentity.StandingForGeneration(source, view.Parent.Paths.FlowPath, actor.Identity.RunID)
		if err != nil {
			t.Fatal(err)
		}
		child, err := flowidentity.KeyedChild(source, parent, actor.FlowID, actor.Identity.Route.InstanceID)
		if err != nil {
			t.Fatal(err)
		}
		return child
	}
	instance, err := flowidentity.StandingForGeneration(source, actor.FlowID, actor.Identity.RunID)
	if err != nil {
		t.Fatal(err)
	}
	return instance
}
