package bootverify

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func loadAccumulateTarget2293(t *testing.T, target, payload string) *c.WorkflowContractBundle {
	t.Helper()
	root, repo := t.TempDir(), repoRootForBootverifyTest(t)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: collect-reviews\nstages: []\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "types.yaml"), "Review:\n  reviewer_id: text\n  rating: Rating\nRating: ['low', 'high']\nOtherRating: ['other', 'high']\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "Work:\n  "+target+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "review.received:\n  "+strings.ReplaceAll(payload, "\n", "\n  ")+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "collector:\n  execution_type: system_node\n  subscribes_to: [review.received]\n  event_handlers:\n    review.received:\n      accumulate: {target: reviews, key: reviewer_id}\n")
	return loadFixtureBundleAt(t, repo, root, c.DefaultPlatformSpecFile(repo))
}

func Test2293Q1VerifyBindsAccumulateTargetFromSource(t *testing.T) {
	for _, tc := range []struct {
		name, target, payload, want string
	}{
		{"matching", `reviews: {type: '[Review]?'}`, "reviewer_id: text\nrating: Rating", ""},
		{"missing_target", "other: text", "reviewer_id: text\nrating: Rating", "reviews"},
		{"non_list", "reviews: Review", "reviewer_id: text\nrating: Rating", "list"},
		{"map_not_list", "reviews: map[text]Review", "reviewer_id: text\nrating: Rating", "list"},
		{"extra_payload", `reviews: {type: '[Review]?'}`, "reviewer_id: text\nrating: Rating\nextra: text", "payload must equal"},
		{"missing_payload", `reviews: {type: '[Review]?'}`, "reviewer_id: text", "payload must equal"},
		{"optional_payload", `reviews: {type: '[Review]?'}`, "reviewer_id: text\nrating: Rating?", "payload must equal"},
		{"different_enum", `reviews: {type: '[Review]?'}`, "reviewer_id: text\nrating: OtherRating", "payload must equal"},
		{"plain_text", `reviews: {type: '[Review]?'}`, "reviewer_id: text\nrating: text", "payload must equal"},
		{"optional_key", `reviews: {type: '[Review]?'}`, "reviewer_id: text?\nrating: Rating", "required scalar"},
		{"non_scalar_key", `reviews: {type: '[Review]?'}`, "reviewer_id: [text]\nrating: Rating", "required scalar"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadAccumulateTarget2293(t, tc.target, tc.payload)
			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
			if tc.want == "" {
				if reportContains(report.HardInvalidities(), "accumulator_entity_projection", "") {
					t.Fatalf("matching state-list payload refused: %#v", report.HardInvalidities())
				}
				return
			}
			if !reportContains(report.HardInvalidities(), "accumulator_entity_projection", tc.want) {
				t.Fatalf("missing target admission refusal %q: %#v", tc.want, report.HardInvalidities())
			}
			if strings.Contains(tc.want, "payload must equal") && !reportContains(report.HardInvalidities(), "accumulator_entity_projection", "review.received") {
				t.Fatal("target mismatch lost its authored event")
			}
		})
	}
}

func Test2293Q1AccumulateSelectorsAreDeclarationsNotExpressions(t *testing.T) {
	for _, name := range []string{"reviews", "entity", "payload", "policy", "state", "item", "computed"} {
		t.Run(name, func(t *testing.T) {
			handler := c.SystemNodeEventHandler{Accumulate: &c.AccumulateSpec{Target: name, Key: name}}
			readers := handlerExecutableReaderExpressionsForSource(nil, identitytest.RootNode(t, "collector"), "review.received", handler)
			if len(readers) != 0 {
				t.Fatalf("bare field declarations entered the expression census: %#v", readers)
			}
		})
	}
	for _, tc := range []struct{ name, source, want string }{
		{"rooted_key", "accumulate: {target: reviews, key: policy.missing}", "one bare item field"},
		{"retired_source", "accumulate: {target: reviews, key: reviewer_id, from: policy.missing}", "from"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := admitBootHandlerFixture(t, tc.source)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "nodes.yaml:") {
				t.Fatalf("old selector/source escaped source-located admission: %v", err)
			}
		})
	}
	// Exercise the real source entrance as well as the closed reader inventory.
	bundle := loadAccumulateTarget2293(t, `reviews: {type: '[Review]?'}`, "reviewer_id: text\nrating: Rating")
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.HardInvalidities(), "executable_reader_expression_validation", "accumulate.") {
		t.Fatalf("canonical collection declaration was evaluated as CEL: %#v", report.HardInvalidities())
	}
}

func Test2293Q1VerifyAccumulateUsesEffectiveExactSchema(t *testing.T) {
	for _, imported := range []bool{false, true} {
		for _, matching := range []bool{false, true} {
			name := "authored"
			if imported {
				name = "imported"
			}
			if matching {
				name += "/matching"
			} else {
				name += "/different_enum"
			}
			t.Run(name, func(t *testing.T) {
				bundle := loadAccumulateTarget2293(t, `reviews: {type: '[Review]?'}`, "reviewer_id: text\nrating: Rating")
				entry := bundle.Events["review.received"]
				values := []any{"other", "high"}
				if matching {
					values = []any{"low", "high"}
				}
				exact := c.MustToolInputSchema(c.ToolSchemaString, c.ToolSchemaEnum(values...))
				entry.Payload.Properties["rating"] = c.EventFieldSpec{ExactSchema: &exact}
				var source semanticview.Source
				if imported {
					schema, err := c.CompileImportedEventSchema(".", "review.received", entry, c.CompiledEventSchemaSource{Layer: "provider_trigger_pack"})
					if err != nil {
						t.Fatal(err)
					}
					delete(bundle.Events, "review.received")
					source = loopEffectiveSchemaSource{
						Source:  semanticview.Wrap(bundle),
						entries: map[string]c.EventCatalogEntry{"review.received": entry},
						schemas: map[string]c.CompiledEventSchema{"review.received": schema},
					}
				} else {
					bundle.Events["review.received"] = entry
					source = compileBootverifyRootSource(bundle)
				}
				report := Run(context.Background(), source, Options{})
				refused := reportContains(report.HardInvalidities(), "accumulator_entity_projection", "")
				if refused == matching {
					t.Fatalf("effective payload equality: matching=%v findings=%#v", matching, report.HardInvalidities())
				}
			})
		}
	}
}
