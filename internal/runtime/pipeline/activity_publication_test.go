package pipeline

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestActivityPublicationProjectsBeforeIdentityAndPreservesJournal(t *testing.T) {
	for _, row := range []struct {
		name, flow, instance, declaration, want string
		template                                bool
	}{
		{"static", "research", "research", "work.done", "research/work.done", false},
		{"template_local", "research", "research/ti-one", "work.done", "research/ti-one/work.done", true},
		{"template_reference", "research", "research/ti-two", "research/work.done", "research/ti-two/work.done", true},
		{"nested", "outer/research", "outer/research/ti-one", "outer/research/work.done", "outer/research/ti-one/work.done", true},
	} {
		t.Run(row.name, func(t *testing.T) {
			intent := testActivityIntent("https://example.test/source")
			intent.ExecutionFlowID = identity.NormalizeFlowID(row.flow)
			intent.FlowInstance = row.instance
			mode := "static"
			if row.template {
				mode = "template"
			}
			intent.RoutingSource = mustActionResultRoutingSource(t, mode, events.RouteIdentity{
				FlowID: row.flow, FlowInstance: row.instance, EntityID: intent.EntityID.String(),
			})
			intent.SuccessEvent = row.declaration
			before := intent
			emissions := &pipelineEmissionPlan{}
			dispatcher := pipelineActivityDispatcher{emissions: emissions}
			payload := map[string]any{"value": "frozen"}
			if err := dispatcher.publishActivityResult(context.Background(), intent, row.declaration, payload); err != nil {
				t.Fatal(err)
			}
			published := emissions.immutableEvents()
			if len(published) != 1 {
				t.Fatalf("events = %d, want 1", len(published))
			}
			first := published[0]
			if string(first.Type()) != row.want || first.ID() != activityResultEventID(intent, row.want) {
				t.Fatalf("publication = %s %s, want final spelling %s before ID", first.Type(), first.ID(), row.want)
			}
			if first.RoutingSource() != intent.RoutingSource || !reflect.DeepEqual(before, intent) {
				t.Fatal("publication changed source or frozen declaration")
			}
			// No semantic source is installed: journal recovery must copy the
			// final result, not consult or specialize current declarations.
			record := ActivityAttemptRecord{
				Status: ActivityAttemptStatusSucceeded, Attempt: intent.Attempt,
				ResultEventID: first.ID(), ResultEventType: string(first.Type()), ResultPayload: payload,
			}
			if err := dispatcher.publishJournaledActivityResult(context.Background(), intent, record); err != nil {
				t.Fatal(err)
			}
			published = emissions.immutableEvents()
			if len(published) != 2 || published[1].ID() != first.ID() || published[1].Type() != first.Type() ||
				published[1].RoutingSource() != first.RoutingSource() || string(published[1].Payload()) != string(first.Payload()) {
				t.Fatal("journal replay changed final publication facts")
			}
		})
	}
}

func TestActivityPublicationRejectsForeignOrAlreadySpecializedDeclarationBeforeEmission(t *testing.T) {
	for _, declaration := range []string{"other/work.done", "research/entity-1/work.done", "research/research/work.done", ""} {
		t.Run(declaration, func(t *testing.T) {
			intent := testActivityIntent("https://example.test/source")
			emissions := &pipelineEmissionPlan{}
			dispatcher := pipelineActivityDispatcher{emissions: emissions}
			if err := dispatcher.publishActivityResult(context.Background(), intent, declaration, map[string]any{}); err == nil {
				t.Fatal("foreign or runtime spelling accepted as declaration")
			}
			if len(emissions.immutableEvents()) != 0 {
				t.Fatal("rejected publication emitted an event")
			}
		})
	}
}
