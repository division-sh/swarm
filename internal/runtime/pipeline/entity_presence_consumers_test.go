package pipeline

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEntityPresenceAtGateConsumer(t *testing.T) {
	source := semanticview.Wrap(&rc.WorkflowContractBundle{
		RootTypes:    rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Profile": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}},
		RootEntities: rc.EntityContractsDocument{"work": {Fields: map[string]rc.EntityFieldDecl{"profile": {Type: "Profile"}}}},
	})
	for _, consumer := range []string{"gate"} {
		for _, safe := range []bool{false, true} {
			name, expr := consumer+"/unsafe", `entity.profile.note`
			if safe {
				name, expr = consumer+"/decision", `entity.profile.?note.orValue("fallback")`
			}
			t.Run(name, func(t *testing.T) {
				fields := map[string]any{"profile": map[string]any{}}
				if !safe {
					// A populated sample must not authorize an optional read without a decision.
					fields["profile"] = map[string]any{"note": "present"}
				}
				entityID := identity.NormalizeEntityID(eventtest.UUID("presence-entity"))
				expression := rc.CELExpression(expr)
				var got any
				var err error
				switch consumer {
				case "gate":
					instance := materializedWorkflowInstanceForTest(WorkflowInstance{StorageRef: testPipelineRunID, EntityID: entityID.String(), WorkflowName: ".", EntityType: "work", CurrentState: "review", Fields: fields})
					got, err = evalWorkflowGateContext(expression, testWorkflowInstanceRoute(testPipelineRunID), entityID, instance, source, ".")
				}
				if safe && (err != nil || got != "fallback") {
					t.Fatalf("got %v, error %v", got, err)
				}
				if !safe && (err == nil || !strings.Contains(err.Error(), "presence decision")) {
					t.Fatalf("unsafe read error: %v", err)
				}
			})
		}
	}
}
