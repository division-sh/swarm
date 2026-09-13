package bootverify

import (
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func TestArtifactWriterEvidenceCoversOnlySupportedActionSites(t *testing.T) {
	node, err := identity.ParseExecutableNode("child", "writer")
	if err != nil {
		t.Fatal(err)
	}
	action := contracts.ActionSpec{ID: "artifact_repo_commit", ArtifactRepo: &contracts.ArtifactRepoSpec{
		Output: contracts.ArtifactRepoOutputSpec{
			RepoURL: "url", CurrentRef: "ref", FileManifest: "manifest", Status: "state",
			Failure: "error", LastRequestID: "request", LastSourceEventID: "source",
		},
	}}
	for _, tc := range []struct {
		name    string
		handler contracts.SystemNodeEventHandler
		allowed bool
	}{
		{"handler", contracts.SystemNodeEventHandler{Action: action}, true},
		{"rule", contracts.SystemNodeEventHandler{Rules: []contracts.HandlerRuleEntry{{Action: action}}}, true},
		{"completion", contracts.SystemNodeEventHandler{OnComplete: []contracts.HandlerRuleEntry{{Action: action}}}, false},
		{"join", contracts.SystemNodeEventHandler{Join: &contracts.JoinSpec{OnComplete: contracts.HandlerRuleEntry{Action: action}}}, false},
		{"timeout", contracts.SystemNodeEventHandler{Join: &contracts.JoinSpec{Timeout: contracts.JoinTimeoutSpec{Outcome: contracts.HandlerRuleEntry{Action: action}}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, target := range wave1HandlerWriteTargets(node, "request", tc.handler) {
				if !target.Node.Equal(node) || !target.Entity {
					t.Fatalf("writer lost exact receiving owner: %+v", target)
				}
				got = append(got, target.Target)
			}
			sort.Strings(got)
			var want []string
			if tc.allowed {
				want = []string{"error", "manifest", "ref", "request", "source", "state", "url"}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("writer targets = %v, want %v", got, want)
			}
		})
	}
}
