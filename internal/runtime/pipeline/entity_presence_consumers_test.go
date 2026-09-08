package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEntityPresenceAtMailboxArtifactAndGateConsumers(t *testing.T) {
	source := semanticview.Wrap(&rc.WorkflowContractBundle{
		RootTypes:    rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Profile": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}},
		RootEntities: rc.EntityContractsDocument{"work": {Fields: map[string]rc.EntityFieldDecl{"profile": {Type: "Profile"}}}},
	})
	entityType, err := semanticview.ResolveEntityStructuralType(source, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, consumer := range []string{"mailbox", "artifact", "gate"} {
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
				base := values.NewContext()
				base.Entity = values.Wrap(fields)
				entityID := identity.NormalizeEntityID(eventtest.UUID("presence-entity"))
				evt := eventtest.ExistingRunRootIngress("presence-consumer", "work.received", "", "", []byte(`{}`), 0, eventtest.UUID("presence-run"), events.EventEnvelope{EntityID: entityID.String()}, time.Time{})
				execCtx := runtimeengine.ExecutionContext{Base: base, EntityType: entityType, Request: runtimeengine.ExecutionRequest{Event: evt, EntityID: entityID, Node: pipelineNode(t, "", "worker")}}
				expression := rc.CELExpression(expr)
				var got any
				var err error
				switch consumer {
				case "mailbox":
					materializer := &recordingMailboxWriteMaterializer{}
					pc := &PipelineCoordinator{mailboxMaterializer: materializer}
					err = pc.materializeMailboxItem(context.Background(), rc.ActionSpec{Mailbox: &rc.MailboxWriteSpec{ItemType: rc.LiteralExpression("review_request"), Summary: expression}}, execCtx)
					if safe && len(materializer.rows()) == 1 {
						got = materializer.rows()[0].Summary
					}
					if !safe && materializer.calls != 0 {
						t.Fatal("unsafe expression reached persistence")
					}
				case "artifact":
					got, err = artifactNamespace(execCtx, &rc.ArtifactRepoSpec{Namespace: expression})
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
