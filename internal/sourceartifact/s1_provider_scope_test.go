package sourceartifact

import (
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/packmodel"
)

func TestS1ProviderScopeSelectsNearestWholeDirectory(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "schema.yaml", "stages: []\n")
	writeTestFile(t, root, "child/deep/schema.yaml", "stages: []\n")
	writeTestFile(t, root, "sibling/schema.yaml", "stages: []\n")
	for _, directory := range []string{"zeta", "team", "child/team", "sibling/other", "child/deep/local"} {
		writeTestFile(t, root, directory+"/manifest.yaml", s1ProviderManifest)
		writeTestFile(t, root, directory+"/trigger.yaml", "{}\n")
	}
	for _, directory := range []string{"zeta", "team"} {
		writeTestFile(t, root, directory+"/connector.yaml", "{}\n")
		writeTestFile(t, root, directory+"/channel.yaml", "{}\n")
	}
	artifact, err := AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := DecodeLogical(artifact.LogicalBlob())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	for _, admitted := range []*AdmittedSourceArtifact{artifact, restored} {
		for _, tc := range []struct {
			flow  string
			paths []string
		}{
			{".", []string{"team", "zeta"}},
			{"child", []string{"child/team", "zeta"}},
			{"child/deep", []string{"child/deep/local", "child/team", "zeta"}},
			{"sibling", []string{"sibling/other", "team", "zeta"}},
		} {
			providers, err := admitted.ProviderDirectoriesInScope(tc.flow)
			if err != nil {
				t.Fatal(err)
			}
			var paths []string
			for _, provider := range providers {
				paths = append(paths, provider.Path())
				if provider.Path() == "child/team" {
					if _, channel := provider.Body(packmodel.BodyChannel); channel {
						t.Fatal("nearest trigger acquired ancestor channel role")
					}
				}
			}
			if !reflect.DeepEqual(paths, tc.paths) {
				t.Fatalf("scope %s=%v want %v", tc.flow, paths, tc.paths)
			}
			providers[0] = ProviderDirectory{}
			again, err := admitted.ProviderDirectoriesInScope(tc.flow)
			if err != nil || again[0].Path() != tc.paths[0] {
				t.Fatalf("caller mutated selected membership: %v %v", again, err)
			}
		}
		for _, invalid := range []string{"", "./child", "child/", "child//deep", "child/../sibling", "missing", "team", "child/team", "/child"} {
			if got, err := admitted.ProviderDirectoriesInScope(invalid); err == nil || got != nil {
				t.Fatalf("non-flow selector %q gained membership: %v %v", invalid, got, err)
			}
		}
	}

	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 20 {
				providers, err := artifact.ProviderDirectoriesInScope("child/deep")
				if err != nil || len(providers) != 3 {
					t.Errorf("concurrent scope read: %v %v", providers, err)
					return
				}
				providers[0] = ProviderDirectory{}
			}
		})
	}
	readers.Wait()
}

func TestS1ProviderScopeRefusesNonFlowArtifacts(t *testing.T) {
	provider, err := AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte(s1ProviderManifest),
		"trigger.yaml":  []byte("{}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []*AdmittedSourceArtifact{nil, provider} {
		if got, err := artifact.ProviderDirectoriesInScope("."); err == nil || got != nil {
			t.Fatalf("non-flow artifact acquired executable scope: %v %v", got, err)
		}
	}
}
