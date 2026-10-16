package runtime

import (
	"fmt"
	"os"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func s1EffectiveTriggerMembers() map[string][]byte {
	members := s1EffectiveProviderScopeMembers()
	provider := packfixture.NeutralProviderMembers()
	for directory, marker := range map[string]string{
		"team-bot": "root_marker", "team_bot": "underscore_marker",
		"sibling/team-bot": "sibling_marker", "child/child-bot": "child_marker",
	} {
		members[directory+"/manifest.yaml"] = provider["manifest.yaml"]
		body := append([]byte(nil), provider["trigger.yaml"]...)
		members[directory+"/trigger.yaml"] = append(body, []byte(fmt.Sprintf("      %s: {from: %s, schema: {type: string}}\n", marker, marker))...)
	}
	return members
}

func TestS1EffectiveProviderTriggerSchemasUseWholeScopedDirectory(t *testing.T) {
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, s1EffectiveTriggerMembers())
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
		Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()),
	})
	if err != nil {
		t.Fatal(err)
	}
	source := projection.Source()
	const event = "inbound.team-bot.text_message"
	for _, cell := range []struct{ flow, marker string }{
		{".", "root_marker"}, {"child", ""}, {"child/deep", ""}, {"sibling", "sibling_marker"},
	} {
		t.Run(cell.flow, func(t *testing.T) {
			resolution := semanticview.ResolveEventSchema(source, cell.flow, event)
			if resolution.HasCompiled != (cell.marker != "") {
				t.Fatalf("provider schema visibility borrowed or lost its selected directory: %+v", resolution)
			}
			if cell.marker != "" && !compiledSchemaHasField(resolution.CompiledSchema, cell.marker) {
				t.Fatalf("provider schema borrowed a sibling BODY in %s", cell.flow)
			}
			_, _, present := source.ResolveFlowEventCatalogEntry(cell.flow, event)
			scope, found := source.FlowScopeByID(cell.flow)
			_, scoped := scope.Events[event]
			if !found || present != resolution.HasCompiled || scoped != present {
				t.Fatal("scoped schema readers disagree")
			}
			other := semanticview.ResolveEventSchema(source, cell.flow, "inbound.team_bot.text_message")
			if !other.HasCompiled || !compiledSchemaHasField(other.CompiledSchema, "underscore_marker") {
				t.Fatal("hyphenated provider selection replaced the distinct underscore namespace")
			}
		})
	}
	if _, ambiguous := source.EventEntry(event); ambiguous {
		t.Fatal("different scoped trigger BODYs acquired one global schema alias")
	}
	if semanticview.ResolveEventSchema(source, ".", "inbound.child-bot.text_message").HasSchema {
		t.Fatal("root schema inspection borrowed a descendant provider")
	}
	for _, flow := range []string{"child", "child/deep"} {
		resolution := semanticview.ResolveEventSchema(source, flow, "inbound.child-bot.text_message")
		if !resolution.HasCompiled || !compiledSchemaHasField(resolution.CompiledSchema, "child_marker") {
			t.Fatalf("child provider lost its exact descendant visibility: %s", flow)
		}
	}
	if bindings := source.SemanticCapabilities().ProviderTriggerOutputBindings(); len(bindings) != 0 {
		t.Fatalf("schema visibility without holder inputs created output authority: %+v", bindings)
	}
	if inputs := source.SemanticCapabilities().ProviderIngressEvents(); len(inputs) != 0 {
		t.Fatalf("unused provider declarations created ingress: %+v", inputs)
	}
}

func TestS1EffectiveProviderTriggerSchemasRetainCapturedAndRestoredSelection(t *testing.T) {
	bundle, rooted, directory := loadS1EffectiveProviderScope(t, s1EffectiveTriggerMembers())
	fact := sourceartifactfixture.FactFor(rooted.Artifact())
	first, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	const event = "inbound.team-bot.text_message"
	before := semanticview.ResolveEventSchema(first.Source(), "sibling", event)
	if !before.HasCompiled || !compiledSchemaHasField(before.CompiledSchema, "sibling_marker") {
		t.Fatal("fixture did not enter the directory trigger compiler")
	}
	for _, scope := range first.Source().FlowScopes() {
		delete(scope.Events, event)
	}
	if err := os.WriteFile(directory+"/sibling/team-bot/trigger.yaml", []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(directory); err != nil {
		t.Fatal(err)
	}
	again, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: first.Source(), SourceArtifactFact: fact})
	if err != nil || !again.Identity().Equal(first.Identity()) {
		t.Fatalf("repeated composition borrowed mutable readback or host bytes: %v", err)
	}
	row, err := sourceartifact.PersistedFromArtifact(rooted.Artifact(), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := sourceartifact.RestoreRootedSource(row, rooted.Admission().Reference())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), restored, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, compiled)
	after, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(compiled), SourceArtifactFact: fact})
	if err != nil || !after.Identity().Equal(first.Identity()) {
		t.Fatalf("closed retained reconstruction changed effective trigger identity: %v", err)
	}
	resolution := semanticview.ResolveEventSchema(after.Source(), "sibling", event)
	if !resolution.HasCompiled || !compiledSchemaHasField(resolution.CompiledSchema, "sibling_marker") {
		t.Fatal("retained reconstruction selected another provider BODY")
	}
}

func TestS1EffectiveProviderTriggerSchemasBindRawAndTemplatePinsWithoutIngress(t *testing.T) {
	for _, cell := range []struct {
		name, body, event string
	}{
		{"normalized-and-raw", string(packfixture.NeutralProviderMembers()["trigger.yaml"]), "raw"},
		{"raw-only", "secret: {required: true}\nsignature: {type: token_equality, header: X-Signature}\nevent_name: {literal: raw}\n", "raw"},
		{"template", "secret: {required: true}\nsignature: {type: token_equality, header: X-Signature}\nevent_name: {template: 'raw.{event_type}'}\nevent_type: {literal: message}\n", "raw.message"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			members := s1EffectiveProviderScopeMembers()
			members["team-bot/manifest.yaml"] = packfixture.NeutralProviderMembers()["manifest.yaml"]
			members["team-bot/trigger.yaml"] = []byte(cell.body)
			event := "inbound.team-bot." + cell.event
			for _, flow := range []string{"", "child/"} {
				members[flow+"schema.yaml"] = []byte(fmt.Sprintf("stages: []\npins:\n  inputs: [%s]\n  outputs: [%s]\n", event, event))
			}
			bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
			projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact())})
			if err != nil {
				t.Fatal(err)
			}
			source := projection.Source()
			for _, flow := range []string{".", "child"} {
				input, found := source.FlowInputEventPin(flow, event)
				if !found {
					t.Fatalf("%s lost its authored input pin", flow)
				}
				producer, bound := input.ProducerEventSchema()
				output, outputFound := source.FlowOutputEventPin(flow, event)
				emitted, outputBound := output.EventSchema()
				wantBound := flow == "."
				if !outputFound || bound != wantBound || outputBound != wantBound {
					t.Fatalf("%s borrowed or lost a whole-directory pin schema: input=%t output=%t", flow, bound, outputBound)
				}
				if wantBound && (producer.Classification() != runtimecontracts.CompiledEventSchemaImported || emitted.AcceptanceSchemaDigest() != producer.AcceptanceSchemaDigest() || !compiledSchemaHasField(producer, "provider")) {
					t.Fatal("raw input/output schemas differ from the imported raw event")
				}
			}
			for _, item := range source.SemanticCapabilities().ProviderTriggerEventProvenance() {
				if item.Provider == "team-bot" && len(item.FlowScopes) == 1 && item.FlowScopes[0] == "." && item.Event == event && item.Kind != "raw" {
					t.Fatalf("raw output was reclassified as normalized: %+v", item)
				}
			}
			if cell.name != "normalized-and-raw" && semanticview.ResolveEventSchema(source, ".", "inbound.team-bot.text_message").HasSchema {
				t.Fatal("raw-only directory acquired a fabricated normalized event")
			}
			if len(source.SemanticCapabilities().ProviderTriggerOutputBindings()) != 0 || len(source.SemanticCapabilities().ProviderIngressEvents()) != 0 {
				t.Fatal("static input/schema composition created authenticated output or enabled ingress")
			}
		})
	}
}

func TestS1EffectiveProviderTriggerSchemaReadbackIsDefensive(t *testing.T) {
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, s1EffectiveTriggerMembers())
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact())})
	if err != nil {
		t.Fatal(err)
	}
	source := projection.Source()
	const event = "inbound.team-bot.text_message"
	for _, readback := range []map[string]runtimecontracts.EventCatalogEntry{
		source.EventEntries(), source.ResolvedEventCatalog(), source.FlowScopes()[0].Events,
	} {
		delete(readback, event)
	}
	for _, flow := range []string{".", "sibling"} {
		scope, found := source.FlowScopeByID(flow)
		if !found {
			t.Fatal("fixture lost flow")
		}
		entry := scope.Events[event]
		delete(entry.Payload.Properties, "text")
		delete(scope.Events, event)
		delete(scope.Events, "inbound.team_bot.text_message")
		after, _ := source.FlowScopeByID(flow)
		entry, present := after.Events[event]
		if _, found := entry.Payload.Properties["text"]; !present || !found {
			t.Errorf("%s returned flow metadata mutated the retained provider schema", flow)
		}
		if _, present := after.Events["inbound.team_bot.text_message"]; !present {
			t.Errorf("%s returned flow metadata deleted another visible provider", flow)
		}
		resolution := semanticview.ResolveEventSchema(source, flow, event)
		if !resolution.HasCompiled || !compiledSchemaHasField(resolution.CompiledSchema, "text") {
			t.Errorf("%s diagnostic readback changed the compiled contract", flow)
		}
	}
}

func TestS1EffectiveProviderTriggerSchemasRefuseNoncanonicalFlowReaders(t *testing.T) {
	members := s1EffectiveTriggerMembers()
	const event = "inbound.team-bot.text_message"
	members["schema.yaml"] = []byte("stages: []\npins:\n  inputs: [" + event + "]\n  outputs: [" + event + "]\n")
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact())})
	if err != nil {
		t.Fatal(err)
	}
	source := projection.Source()
	for _, flow := range []string{"", " ", " . ", "./", "/", "/child", "child/", "child/../sibling", "foreign", "team-bot"} {
		t.Run(fmt.Sprintf("%q", flow), func(t *testing.T) {
			if pins := source.FlowInputEventPins(flow); len(pins) != 0 {
				t.Errorf("noncanonical flow %q borrowed %d input pins", flow, len(pins))
			}
			if pins := source.FlowOutputEventPins(flow); len(pins) != 0 {
				t.Errorf("noncanonical flow %q borrowed %d output pins", flow, len(pins))
			}
			if _, found := source.FlowInputEventPin(flow, event); found {
				t.Errorf("noncanonical flow %q acquired an input pin", flow)
			}
			if _, found := source.FlowOutputEventPin(flow, event); found {
				t.Errorf("noncanonical flow %q acquired an output pin", flow)
			}
			if _, found, err := source.ResolveEffectiveCompiledFlowEventSchema(flow, event); err != nil || found {
				t.Errorf("noncanonical flow %q acquired a compiled provider schema: %v", flow, err)
			}
			if _, _, found := source.ResolveFlowEventCatalogEntry(flow, event); found {
				t.Errorf("noncanonical flow %q acquired scoped catalog evidence", flow)
			}
			if flow != "" && semanticview.ResolveEventSchema(source, flow, event).HasSchema {
				t.Errorf("noncanonical flow %q borrowed schema through the general resolver", flow)
			}
		})
	}
	input, found := source.FlowInputEventPin(".", event)
	if _, bound := input.ProducerEventSchema(); !found || !bound {
		t.Fatal("exact root lost its imported input pin")
	}
	readback := semanticview.ResolveEventSchema(source, "", "inbound.team_bot.text_message")
	if !readback.HasCompiled || !compiledSchemaHasField(readback.CompiledSchema, "underscore_marker") {
		t.Fatal("target-free inspection lost the unambiguous typed catalog")
	}
}
