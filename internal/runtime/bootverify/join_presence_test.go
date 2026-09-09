package bootverify

import (
	"context"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"testing"
)

func TestJoinPresenceAdmissionCompleteAndTimeout(t *testing.T) {
	for _, phase := range []string{"complete_when", "on_complete", "timeout"} {
		for _, safe := range []bool{false, true} {
			name := phase + "/unsafe"
			expr := `join.results.exists(r, r.note == "fallback")`
			if safe {
				name = phase + "/decision"
				expr = `join.results.exists(r, r.?note.orValue("fallback") == "fallback")`
			}
			t.Run(name, func(t *testing.T) {
				bundle := joinValidationBundle()
				bundle.RootTypes = rc.TypeCatalogDocument{Types: map[string]rc.NamedTypeDecl{"Result": {Fields: map[string]rc.TypeFieldSpec{"note": {Type: "text", IsOptional: true}}}}}
				event := bundle.Events["item.completed"]
				event.Payload.Properties["result"] = rc.EventFieldSpec{Type: "Result"}
				bundle.Events["item.completed"] = event
				handler := bundle.Nodes["join-node"].EventHandlers["item.completed"]
				switch phase {
				case "complete_when":
					handler.Join.CompleteWhen = expr
					handler.Join.Remaining = rc.JoinRemainingIgnore
				case "on_complete":
					handler.Join.OnComplete.Emit.Fields["results"] = rc.CELExpression(expr)
				case "timeout":
					handler.Join.Timeout.Outcome.Emit.Fields["missing"] = rc.CELExpression(expr)
				}
				bundle.Nodes["join-node"].EventHandlers["item.completed"] = handler
				rebuildJoinValidationTopology(bundle)
				report := Run(context.Background(), semanticviewtest.WrapRootAgents(bundle), Options{})
				found := reportContains(report.Errors(), joinValidationCheckID, "without a presence decision")
				if found == safe {
					t.Fatalf("presence findings=%#v safe=%v", report.Errors(), safe)
				}
				if safe && reportContains(report.Errors(), joinValidationCheckID, "") {
					t.Fatalf("safe join expression rejected: %#v", report.Errors())
				}
			})
		}
	}
}
