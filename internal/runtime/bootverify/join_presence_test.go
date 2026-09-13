package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestEntityDefiniteAssignmentJoinOutcomeIdentity(t *testing.T) {
	for _, variant := range []string{"separate destinations", "same destination missing timeout write", "same destination both write"} {
		t.Run(variant, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.FanInBarrier)
			entityPath := filepath.Join(root, "portfolio", "entities.yaml")
			entity, err := os.ReadFile(entityPath)
			if err != nil {
				t.Fatal(err)
			}
			writeBootverifyFixtureFile(t, entityPath, string(entity)+"  summary: text\n")
			nodePath := filepath.Join(root, "portfolio", "nodes.yaml")
			nodes, err := os.ReadFile(nodePath)
			if err != nil {
				t.Fatal(err)
			}
			writer := "          data_accumulation:\n            writes:\n              - {target_field: summary, value: collected}\n"
			body := strings.Replace(string(nodes), "        on_complete:\n", "        on_complete:\n"+writer, 1)
			if strings.HasPrefix(variant, "same destination") {
				body = strings.Replace(body, "          advances_to: failed\n", "          advances_to: complete\n", 1)
			}
			if variant == "same destination both write" {
				body = strings.Replace(body, "          after: 5m\n", "          after: 5m\n"+writer, 1)
			}
			writeBootverifyFixtureFile(t, nodePath, body)
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, rc.DefaultPlatformSpecFile(repo))
			analysis, err := engine.BuildEntityAssignmentAnalysis(semanticview.Wrap(bundle), "portfolio")
			if err != nil {
				t.Fatal(err)
			}
			if analysis.StageFacts("awaiting").Has("summary") {
				t.Fatal("join output certified before an outcome")
			}
			want := variant != "same destination missing timeout write"
			if analysis.StageFacts("complete").Has("summary") != want {
				t.Fatalf("complete facts=%#v, want summary assigned=%v", analysis.StageFacts("complete"), want)
			}
			if analysis.StageFacts("failed").Has("summary") {
				t.Fatal("timeout borrowed complete outcome's assignment")
			}
		})
	}
}

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
