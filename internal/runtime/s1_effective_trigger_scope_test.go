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
