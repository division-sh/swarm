package runtime

import (
	"bytes"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1EffectiveProviderDirectoryScopeUsesWholeSelectedBody(t *testing.T) {
	members := map[string][]byte{
		"schema.yaml":            []byte("stages: []\n"),
		"child/schema.yaml":      []byte("stages: []\n"),
		"child/deep/schema.yaml": []byte("stages: []\n"),
		"sibling/schema.yaml":    []byte("stages: []\n"),
	}
	provider := packfixture.NeutralProviderMembers()
	for _, key := range []string{"team-bot", "team_bot"} {
		members[key+"/manifest.yaml"] = []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {call_provider_actions: [send_message]}, cannot: [bypass journal]}\n")
		members[key+"/connector.yaml"] = provider["connector.yaml"]
	}
	members["child/team-bot/manifest.yaml"] = bytes.Replace(members["team-bot/manifest.yaml"], []byte("1.0.0"), []byte("2.0.0"), 1)
	members["child/team-bot/connector.yaml"] = bytes.Replace(provider["connector.yaml"], []byte("example.test"), []byte("child.example.test"), 1)
	members["sibling/team-bot/manifest.yaml"] = []byte("version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw, text_message]}, cannot: [bypass journal]}\n")
	members["sibling/team-bot/trigger.yaml"] = provider["trigger.yaml"]
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	rooted, _ := sourceartifactfixture.Rooted(t, "scoped-provider-shop", artifact)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), rooted, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundle)
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
		Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()),
	})
	if err != nil {
		t.Fatalf("admit directory-owned effective source without imports: %v", err)
	}
	source := projection.Source()
	for _, cell := range []struct{ flow, host string }{
		{".", "https://example.test/messages"},
		{"child", "https://child.example.test/messages"},
		{"child/deep", "https://child.example.test/messages"},
		{"sibling", ""},
	} {
		t.Run(cell.flow, func(t *testing.T) {
			node, err := runtimeidentity.AdmitExecutableNodeDeclaration(cell.flow, "sender")
			if err != nil {
				t.Fatal(err)
			}
			tool, present := source.ActivityToolEntry(node, "team-bot.send_message")
			if present != (cell.host != "") {
				t.Fatalf("nearest complete directory lost or borrowed a connector: present=%t want_host=%q", present, cell.host)
			}
			scope, found := source.FlowScopeByID(cell.flow)
			if !found {
				t.Fatal("effective source lost its admitted flow")
			}
			_, visible := scope.Tools["team-bot.send_message"]
			if visible != present {
				t.Fatal("scope readback contradicts the frozen activity compiler")
			}
			if present {
				http, found := tool.HTTP()
				if !found || http.URL != cell.host {
					t.Fatalf("effective tool borrowed a different selected endpoint: %+v", http)
				}
			}
			other, present := source.ActivityToolEntry(node, "team_bot.send_message")
			http, found := other.HTTP()
			if !present || !found || http.URL != "https://example.test/messages" {
				t.Fatal("hyphenated scope selection stole the distinct underscore provider")
			}
		})
	}
	if _, present := source.ToolEntries()["team-bot.send_message"]; present {
		t.Fatal("different scoped connector bodies acquired one global execution alias")
	}
	compiled, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("effective source lost its compiled source")
	}
	actual, err := compiled.RootedSource()
	if err != nil || !actual.Admission().Equal(rooted.Admission()) {
		t.Fatalf("composition replaced the genuine selected source/root: %v", err)
	}
}
