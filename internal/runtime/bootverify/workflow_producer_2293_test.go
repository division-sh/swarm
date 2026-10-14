package bootverify

import (
	"context"
	"reflect"
	"strings"
	"testing"

	c "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"gopkg.in/yaml.v3"
)

func TestDerivedNodeProducerVerificationNeedsNoAssertion(t *testing.T) {
	source := compileBootverifyRootSource(derivedNodeProducerBundle2293())
	want := map[string]semanticview.EventEndpointKind{
		"work.completed":   semanticview.EventEndpointNodeHandler,
		"work.escalated":   semanticview.EventEndpointNodeHandler,
		"lookup.succeeded": semanticview.EventEndpointNodeGenerated,
		"lookup.failed":    semanticview.EventEndpointNodeGenerated,
	}
	seen := map[string]int{}
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).Producers() {
		if kind, expected := want[endpoint.Event.Authored]; expected {
			if endpoint.Kind != kind {
				t.Fatalf("producer %s has kind %s, want %s", endpoint.Event.Authored, endpoint.Kind, kind)
			}
			seen[endpoint.Event.Authored]++
		}
	}
	for event := range want {
		if seen[event] != 1 {
			t.Fatalf("producer %s has %d endpoints, want exactly one", event, seen[event])
		}
	}
	report := Run(context.Background(), source, Options{})
	if report.HasErrors() {
		t.Fatalf("derived producers require no authored assertion: %#v", report.Errors())
	}
	for event := range want {
		if reportContains(report.Warnings(), "event_producer_exists", event) || reportContains(report.Warnings(), "event_chain_integrity", event) {
			t.Fatalf("derived producer %s lost its real output evidence: %#v", event, report.Warnings())
		}
	}
}

func TestDerivedNodeProducerNamespaceRefusalsRemain(t *testing.T) {
	for _, target := range []string{"platform.boot", "platform.forbidden"} {
		for _, site := range []string{"handler", "guard", "rule"} {
			t.Run(target+"/"+site, func(t *testing.T) {
				bundle := derivedNodeProducerBundle2293()
				bundle.Platform = admittedCatalogTestSpec(t, map[string]yaml.Node{"platform.boot": {}})
				node := bundle.Nodes["worker"]
				handler := node.EventHandlers["work.requested"]
				switch site {
				case "handler":
					handler.Emit = c.EmitSpec{Event: target}
				case "guard":
					handler.Guard = &c.GuardSpec{Check: "true", OnFail: "escalate:" + target}
				case "rule":
					handler.Emit = c.EmitSpec{}
					handler.Rules = []c.HandlerRuleEntry{{Condition: "true", Emit: c.EmitSpec{Event: target}}}
				}
				node.EventHandlers["work.requested"] = handler
				bundle.Nodes["worker"] = node
				source := compileBootverifyRootSource(bundle)
				findings := newCheckerContext(context.Background(), source, Options{}).platformNamespace()
				if len(findings) != 1 || findings[0].CheckID != "platform_namespace_violation" || findings[0].Severity != "error" ||
					!strings.Contains(findings[0].Message, target) || !strings.Contains(findings[0].Message, "node worker handler work.requested emits") {
					t.Fatalf("real %s producer lost its exact namespace refusal: %#v", site, findings)
				}
			})
		}
	}
}

func TestProducesAssertionChecksAndDTOAreRetired(t *testing.T) {
	for _, name := range []string{"Produces", "ProducesDeclared"} {
		t.Run("DTO/"+name, func(t *testing.T) {
			if _, exists := reflect.TypeFor[c.SystemNodeContract]().FieldByName(name); exists {
				t.Fatalf("authored producer authority survived in node DTO: %s", name)
			}
		})
	}
	for _, retired := range []string{"produces_drift", "phantom_produces"} {
		t.Run("check/"+retired, func(t *testing.T) {
			for _, check := range bootCheckRegistry {
				if check.ID == retired {
					t.Fatalf("retired authored-output assertion check survived: %s", check.ID)
				}
			}
		})
	}
	want := map[string]bool{"event_producer_exists": false, "event_chain_integrity": false, "platform_namespace_violation": false}
	for _, check := range bootCheckRegistry {
		if _, required := want[check.ID]; required {
			want[check.ID] = true
		}
	}
	for check, present := range want {
		if !present {
			t.Fatalf("real producer verification was removed with the authored assertion: %s", check)
		}
	}
}

func TestDerivedNodeProducerEvidenceDoesNotComeFromCatalogMetadata(t *testing.T) {
	bundle := derivedNodeProducerBundle2293()
	bundle.Events["work.unused"] = c.EventCatalogEntry{}
	node := bundle.Nodes["worker"]
	handler := node.EventHandlers["work.requested"]
	handler.Emit = c.EmitSpec{Event: "work.missing_schema"}
	node.EventHandlers["work.requested"] = handler
	bundle.Nodes["worker"] = node
	source := compileBootverifyRootSource(bundle)
	census := semanticview.BuildAuthoredEventEndpointCensus(source)
	if got := census.MatchingProducers(".", "work.unused"); len(got) != 0 {
		t.Fatalf("catalog metadata fabricated an executable producer: %#v", got)
	}
	report := Run(context.Background(), source, Options{})
	if !reportContains(report.Warnings(), "event_chain_integrity", "work.missing_schema") ||
		!reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "work.unused") {
		t.Fatalf("real missing-schema/dead-declaration checks disappeared with produces assertions: %#v", report.Warnings())
	}
}

func derivedNodeProducerBundle2293() *c.WorkflowContractBundle {
	bundle := &c.WorkflowContractBundle{
		RootSchema: &c.FlowSchemaDocument{Name: "derived-producer", Pins: c.FlowPins{Inputs: c.FlowInputPins{
			EventPins: []c.FlowInputEventPin{{Event: "work.requested"}, {Event: "lookup.requested"}},
		}}},
		Nodes: map[string]c.SystemNodeContract{
			"worker": {EventHandlers: map[string]c.SystemNodeEventHandler{
				"work.requested": {Emit: c.EmitSpec{Event: "work.completed"}, Guard: &c.GuardSpec{Check: "true", OnFail: "escalate:work.escalated"}},
				"work.completed": {}, "work.escalated": {},
			}},
			"reader": {EventHandlers: map[string]c.SystemNodeEventHandler{
				"lookup.requested": {Activity: c.ActivitySpec{ID: "lookup", Tool: "lookup"}},
			}},
		},
		Events: map[string]c.EventCatalogEntry{
			"work.requested": {}, "work.completed": {}, "work.escalated": {}, "lookup.requested": {},
		},
		Tools: map[string]c.ToolSchemaEntry{"lookup": c.MustToolSchemaEntry(
			c.WithToolHandler(c.ToolHandlerHTTP), c.WithToolEffect(c.ActivityEffectClassReadOnly),
			c.WithToolSchemas(c.MustToolInputSchema(c.ToolSchemaObject), c.MustToolInputSchema(c.ToolSchemaObject)),
			c.WithToolHTTP(c.HTTPToolSpec{Method: "GET", URL: "https://example.test"}),
		)},
	}
	bundle.Platform.Platform.Name, bundle.Platform.Platform.Version = "test", "1.0.0"
	return bundle
}
