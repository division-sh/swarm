package runtime

import (
	"os"
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

func s1ReadmissionMembers(withProvider bool) map[string][]byte {
	members := packfixture.ScopedActivityMembers()
	if withProvider {
		provider := packfixture.NeutralProviderMembers()
		members["team-bot/manifest.yaml"] = provider["manifest.yaml"]
		members["team-bot/connector.yaml"] = provider["connector.yaml"]
		members["team-bot/trigger.yaml"] = provider["trigger.yaml"]
	}
	return members
}

func assertS1FrozenActivityContracts(t *testing.T, source semanticview.Source, expected map[string]runtimecontracts.ToolSchemaEntry) {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("effective source lost its frozen compiler")
	}
	for flow, before := range expected {
		node, err := runtimeidentity.AdmitExecutableNodeDeclaration(flow, "sender")
		if err != nil {
			t.Fatal(err)
		}
		after, found := source.ActivityToolEntry(node, "shared")
		beforeHash, err := before.CanonicalHash()
		if err != nil {
			t.Fatal(err)
		}
		afterHash, err := after.CanonicalHash()
		beforeHTTP, _ := before.HTTP()
		afterHTTP, _ := after.HTTP()
		if !found || err != nil || beforeHash != afterHash || beforeHTTP.URL != afterHTTP.URL {
			t.Fatalf("re-admission changed %s frozen tool: before=%s after=%s err=%v", flow, beforeHTTP.URL, afterHTTP.URL, err)
		}
		event := "send.succeeded"
		if flow != "." {
			event = flow + "/" + event
		}
		entry, found := bundle.GeneratedActivityEventEntries()[event]
		result := entry.Payload.Properties["result"].ExactSchema
		if !found || result == nil || !result.Equal(before.OutputSchema()) {
			t.Fatalf("re-admission changed generated result contract in %s", flow)
		}
		if _, found := source.ActivityToolEntry(node, "inserted"); found {
			t.Fatalf("inserted readback acquired execution authority in %s", flow)
		}
	}
}

func TestS1EffectiveReadmissionRejectsAuthoredContractMutation(t *testing.T) {
	for _, withProvider := range []bool{false, true} {
		for _, flow := range []string{".", "child", "inherited"} {
			for _, change := range []string{"replace", "delete", "insert"} {
				name := map[bool]string{false: "local-only", true: "mixed"}[withProvider] + "/" + flow + "/" + change
				t.Run(name, func(t *testing.T) {
					bundle, rooted, directory := loadS1EffectiveProviderScope(t, s1ReadmissionMembers(withProvider))
					fact := sourceartifactfixture.FactFor(rooted.Artifact())
					first, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
					if err != nil {
						t.Fatal(err)
					}
					expected := map[string]runtimecontracts.ToolSchemaEntry{}
					for _, owner := range []string{".", "child", "sibling", "inherited"} {
						node, _ := runtimeidentity.AdmitExecutableNodeDeclaration(owner, "sender")
						tool, found := first.Source().ActivityToolEntry(node, "shared")
						if !found {
							t.Fatalf("initial fixture lost %s contract", owner)
						}
						expected[owner] = tool
					}
					assertS1FrozenActivityContracts(t, first.Source(), expected)
					exposed, _ := semanticview.Bundle(first.Source())
					mutateFlow := flow
					if flow == "inherited" && change == "delete" {
						mutateFlow = "."
						// This declaration is the inherited flow's actual owner.
					}
					view, found := exposed.FlowViewByID(mutateFlow)
					if !found {
						t.Fatal("missing authored readback")
					}
					if view.Tools == nil {
						view.Tools = map[string]runtimecontracts.ToolSchemaEntry{}
					}
					other := expected["child"]
					if flow == "child" {
						other = expected["."]
					}
					switch change {
					case "replace":
						view.Tools["shared"] = other
					case "delete":
						delete(view.Tools, "shared")
					case "insert":
						view.Tools["inserted"] = other
					}
					again, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: first.Source(), SourceArtifactFact: fact})
					if err == nil {
						if !again.Identity().Equal(first.Identity()) {
							t.Fatal("readback mutation was blessed as a new effective identity")
						}
						assertS1FrozenActivityContracts(t, again.Source(), expected)
					} else if again.Source() != nil {
						t.Fatalf("contradictory admission published a partial source: %v", err)
					}
					assertS1FrozenActivityContracts(t, first.Source(), expected)
					if err := os.RemoveAll(directory); err != nil {
						t.Fatal(err)
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
					recovered, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(retained), SourceArtifactFact: fact})
					if err != nil || !recovered.Identity().Equal(first.Identity()) {
						t.Fatalf("closed retained source lost its original identity: %v", err)
					}
					assertS1FrozenActivityContracts(t, recovered.Source(), expected)
				})
			}
		}
	}
}

func TestS1EffectiveReadmissionPreservesUnchangedAndFreshSources(t *testing.T) {
	for _, withProvider := range []bool{false, true} {
		t.Run(map[bool]string{false: "local-only", true: "mixed"}[withProvider], func(t *testing.T) {
			bundle, rooted, _ := loadS1EffectiveProviderScope(t, s1ReadmissionMembers(withProvider))
			fact := sourceartifactfixture.FactFor(rooted.Artifact())
			first, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
			if err != nil {
				t.Fatal(err)
			}
			expected := map[string]runtimecontracts.ToolSchemaEntry{}
			for _, flow := range []string{".", "child", "sibling", "inherited"} {
				node, _ := runtimeidentity.AdmitExecutableNodeDeclaration(flow, "sender")
				tool, found := first.Source().ActivityToolEntry(node, "shared")
				if !found {
					t.Fatalf("unchanged fixture lost %s contract", flow)
				}
				expected[flow] = tool
			}
			again, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: first.Source(), SourceArtifactFact: fact})
			if err != nil || !again.Identity().Equal(first.Identity()) {
				t.Fatalf("unchanged repeated admission did not retain identity: %v", err)
			}
			assertS1FrozenActivityContracts(t, again.Source(), expected)
			members := s1ReadmissionMembers(withProvider)
			members["child/tools.yaml"] = members["tools.yaml"]
			fresh, newRoot, _ := loadS1EffectiveProviderScope(t, members)
			changed, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(fresh), SourceArtifactFact: sourceartifactfixture.FactFor(newRoot.Artifact())})
			if err != nil || changed.Identity().Equal(first.Identity()) {
				t.Fatalf("genuine new source was confused with repeated admission: %v", err)
			}
			node, _ := runtimeidentity.AdmitExecutableNodeDeclaration("child", "sender")
			before, _ := first.Source().ActivityToolEntry(node, "shared")
			after, _ := changed.Source().ActivityToolEntry(node, "shared")
			if before.OutputSchema().Equal(after.OutputSchema()) {
				t.Fatal("fresh source failed to admit its actual changed contract")
			}
			expected["child"] = expected["."]
			assertS1FrozenActivityContracts(t, changed.Source(), expected)
		})
	}
}
