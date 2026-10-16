package runtime

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func s1EffectiveProviderScopeMembers() map[string][]byte {
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
	return members
}

func loadS1EffectiveProviderScope(t *testing.T, members map[string][]byte) (*runtimecontracts.WorkflowContractBundle, sourceartifact.RootedSource, string) {
	t.Helper()
	artifact, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		t.Fatal(err)
	}
	rooted, directory := sourceartifactfixture.Rooted(t, "scoped-provider-shop", artifact)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), rooted, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, bundle)
	return bundle, rooted, directory
}

func TestS1EffectiveProviderDirectoryScopeUsesWholeSelectedBody(t *testing.T) {
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, s1EffectiveProviderScopeMembers())
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

func TestS1EffectiveProviderDirectoryScopeRetainsFrozenEvidenceAndRestart(t *testing.T) {
	bundle, rooted, directory := loadS1EffectiveProviderScope(t, s1EffectiveProviderScopeMembers())
	request := EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact())}
	first, err := AdmitEffectiveSourceProjection(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range []struct{ flow, label, version string }{
		{".", "team-bot/connector.yaml", "1.0.0"},
		{"child", "child/team-bot/connector.yaml", "2.0.0"},
		{"child/deep", "child/team-bot/connector.yaml", "2.0.0"},
	} {
		capabilities := first.Source().SemanticCapabilities()
		label, present := capabilities.ConnectorImportSource(cell.flow, "team-bot.send_message")
		identity, hasIdentity := capabilities.ConnectorPackProvenance(cell.flow, "team-bot.send_message")
		if !present || !hasIdentity || label.URI() != cell.label || identity.Source != cell.label || identity.Version != cell.version || identity.PackID != "team-bot" {
			t.Fatalf("scoped connector metadata contradicts its captured BODY: %s %+v", cell.flow, identity)
		}
		identity.Version = "99.0.0"
		scope, _ := first.Source().FlowScopeByID(cell.flow)
		delete(scope.Tools, "team-bot.send_message")
	}
	for _, flow := range []string{"sibling", "foreign", "", " child", "child/"} {
		if _, present := first.Source().SemanticCapabilities().ConnectorImportSource(flow, "team-bot.send_message"); present {
			t.Fatalf("connector metadata borrowed another scope: %q", flow)
		}
	}
	if err := os.WriteFile(directory+"/child/team-bot/connector.yaml", []byte("changed: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(directory); err != nil {
		t.Fatal(err)
	}
	again, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: first.Source(), SourceArtifactFact: request.SourceArtifactFact})
	if err != nil || !again.Identity().Equal(first.Identity()) {
		t.Fatalf("repeated admission reselected the tree or consumed mutated readbacks: %v", err)
	}
	row, err := sourceartifact.PersistedFromArtifact(rooted.Artifact(), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := sourceartifact.RestoreRootedSource(row, rooted.Admission().Reference())
	if err != nil {
		t.Fatal(err)
	}
	retained, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), restored, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	admitRuntimeTestBundle(t, retained)
	after, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(retained), SourceArtifactFact: request.SourceArtifactFact})
	if err != nil || !after.Identity().Equal(first.Identity()) {
		t.Fatalf("retained reconstruction lost exact scoped identity: %v", err)
	}
}

type callerDirectedConnectorScope struct {
	semanticview.Source
	scopes []semanticview.FlowScope
}

func (s callerDirectedConnectorScope) FlowScopes() []semanticview.FlowScope { return s.scopes }

func TestS1EffectiveProviderDirectoryScopeRefusesMissingRepeatedAndForeignFlows(t *testing.T) {
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, s1EffectiveProviderScopeMembers())
	source := semanticview.Wrap(bundle)
	for _, name := range []string{"missing", "repeated", "foreign"} {
		t.Run(name, func(t *testing.T) {
			scopes := source.FlowScopes()
			switch name {
			case "missing":
				scopes = scopes[:len(scopes)-1]
			case "repeated":
				scopes = append(scopes, scopes[0])
			case "foreign":
				scopes = append(scopes, semanticview.FlowScope{ID: "foreign"})
			}
			projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
				Source: callerDirectedConnectorScope{Source: source, scopes: scopes}, SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()),
			})
			if err == nil || projection.Source() != nil {
				t.Fatalf("replacement scope inventory published a partial effective source: %v", err)
			}
		})
	}
}

func TestS1EffectiveProviderDirectoryScopeRefusesPartialRoleAndAuthoredNamespace(t *testing.T) {
	for _, name := range []string{"bad sibling role", "authored namespace"} {
		t.Run(name, func(t *testing.T) {
			members := s1EffectiveProviderScopeMembers()
			if name == "bad sibling role" {
				members["sibling/team-bot/trigger.yaml"] = []byte("unknown: true\n")
			} else {
				members["sibling/tools.yaml"] = []byte("team-bot.invented:\n  handler_type: http\n  effect_class: read_only\n  http: {method: GET, url: 'https://foreign.example.test'}\n")
			}
			bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
			projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
				Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()),
			})
			if err == nil || projection.Source() != nil {
				t.Fatalf("invalid directory composition published a partial source: %v", err)
			}
			if name == "authored namespace" && (!strings.Contains(err.Error(), "connector.yaml") || !strings.Contains(err.Error(), "sibling")) {
				t.Fatalf("provider namespace refusal lost its exact teaching location: %v", err)
			}
		})
	}
}

func TestS1EffectiveProviderDirectoryScopeFreezesAuthoredActivityContracts(t *testing.T) {
	for _, withProvider := range []bool{false, true} {
		t.Run(map[bool]string{false: "local-only", true: "directory-and-local"}[withProvider], func(t *testing.T) {
			members := packfixture.ScopedActivityMembers()
			if withProvider {
				members["team-bot/manifest.yaml"] = s1EffectiveProviderScopeMembers()["team-bot/manifest.yaml"]
				members["team-bot/connector.yaml"] = packfixture.NeutralProviderMembers()["connector.yaml"]
			}
			bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
			projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{
				Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()),
			})
			if err != nil {
				t.Fatal(err)
			}
			source := projection.Source()
			compiled, found := semanticview.Bundle(source)
			if !found {
				t.Fatal("effective source lost its frozen contract compiler")
			}
			for _, cell := range []struct{ flow, event, origin string }{
				{".", "send.succeeded", "root"},
				{"child", "child/send.succeeded", "child"},
				{"sibling", "sibling/send.succeeded", "sibling"},
				{"inherited", "inherited/send.succeeded", "root"},
			} {
				node, err := runtimeidentity.AdmitExecutableNodeDeclaration(cell.flow, "sender")
				if err != nil {
					t.Fatal(err)
				}
				tool, present := source.ActivityToolEntry(node, "shared")
				origin, hasOrigin := tool.OutputSchema().Property("origin")
				expected := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaEnum(cell.origin))
				if !present || !hasOrigin || !origin.Equal(expected) {
					t.Fatalf("effective execution lost its exact %s contract", cell.flow)
				}
				entry, present := compiled.GeneratedActivityEventEntries()[cell.event]
				result := entry.Payload.Properties["result"].ExactSchema
				if !present || result == nil || !result.Equal(tool.OutputSchema()) {
					t.Fatalf("effective schema and execution contract disagree in %s", cell.flow)
				}
				view, present := bundle.FlowViewByID(cell.flow)
				if !present {
					t.Fatal("fixture lost its original flow")
				}
				delete(view.Tools, "shared")
				scope, _ := source.FlowScopeByID(cell.flow)
				delete(scope.Tools, "shared")
				after, present := source.ActivityToolEntry(node, "shared")
				if !present || !after.OutputSchema().Equal(tool.OutputSchema()) {
					t.Fatalf("effective binding depends on mutable authored/readback tools in %s", cell.flow)
				}
			}
		})
	}
}
