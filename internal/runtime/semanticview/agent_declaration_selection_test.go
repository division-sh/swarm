package semanticview

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func TestAgentDeclarationSelectionIgnoresRoleCardinality(t *testing.T) {
	for _, sameRole := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			source := agentNamePlanNestedSource(false)
			bundle, _ := Bundle(source)
			flow := bundle.FlowTree.ByID["parent/child"]
			entry := flow.Agents["worker"]
			entry.Role = "shared-worker"
			flow.Agents["worker"] = entry
			if sameRole {
				flow.Agents["second"] = runtimecontracts.AgentRegistryEntry{Role: "shared-worker"}
				ref := runtimecontracts.ContractURIRef{Kind: "agent", FlowID: "parent/child", LocalID: "second", Full: "test://agent-name/child/second"}
				bundle.URIRegistry.Agents[ref.Full] = ref
				bundle.URIRegistry.ByURI[ref.Full] = ref
			}
			if reverse {
				flow.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
				if sameRole {
					flow.Agents["second"] = runtimecontracts.AgentRegistryEntry{Role: "shared-worker"}
				}
				flow.Agents["worker"] = entry
			}
			for _, id := range []string{"", "unknown", "worker"} {
				for _, role := range []string{"shared-worker", "SHARED-WORKER", "shared_worker", "other"} {
					for _, scope := range []string{"", "parent/child"} {
						if id == "worker" && scope == "" {
							// The parent's effective public name is legitimately worker.
							continue
						}
						actor := models.AgentConfig{ID: id, Role: role, FlowID: scope}
						if _, ok := ResolveAgentDeclaration(source, actor); ok {
							t.Errorf("role inferred ownership: id=%q role=%q scope=%q two=%v reverse=%v", id, role, scope, sameRole, reverse)
						}
					}
				}
			}
			actor := models.AgentConfig{ID: "public-worker", Role: "unrelated", FlowID: "parent/child"}
			declaration, ok := ResolveAgentDeclaration(source, actor)
			if !ok || declaration.LocalID != "worker" || declaration.OwnerFlowID != "parent/child" {
				t.Fatalf("exact scoped public name lost: %#v, %v", declaration, ok)
			}
			plan, err := ScopedAgentNamePlan(source, declaration)
			if err != nil {
				t.Fatal(err)
			}
			name, err := plan.Materialize()
			if err != nil {
				t.Fatal(err)
			}
			actor.ID = ""
			actor.Identity = agentidentity.Identity{Name: name}
			if got, ok := ResolveAgentDeclaration(source, actor); !ok || got.OwnerURI != declaration.OwnerURI {
				t.Fatal("valid typed declared name with absent redundant ID rejected")
			}
			actor.Identity.Name.Owner = "test://agent-name/parent/worker"
			if _, ok := ResolveAgentDeclaration(source, actor); ok {
				t.Fatal("wrong typed owner accepted")
			}
			actor.Identity.Name = name
			actor.FlowID = "parent"
			if _, ok := ResolveAgentDeclaration(source, actor); ok {
				t.Fatal("wrong flow accepted")
			}
			for _, id := range []string{"unknown", "", "public-worker"} {
				if key, remapped := CredentialStoreKeyForActorFlow(source, id, "", "  provider-key  "); key != "provider-key" || remapped {
					t.Fatalf("credential lookup acquired declaration semantics: %q %v", key, remapped)
				}
			}
		}
	}
}
