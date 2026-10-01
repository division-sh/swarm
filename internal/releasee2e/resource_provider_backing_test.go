package releasee2e

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestResourceProviderBackingRejectsMissingAuthority(t *testing.T) {
	root := t.TempDir()
	key := "swarm-claude-state-v1-" + strings.Repeat("a", 64)
	head := uuid.NewString()
	name := releaseE2EFixtureAgent + "-claude-" + strings.Repeat("a", 24)
	withFakeDockerState(root, func(state *fakeDockerState) {
		state.Containers[name] = fakeDockerContainer{Running: true, ProviderKey: key}
		state.ProviderVolumes = map[string]fakeProviderVolume{key: {Marker: key, Heads: map[string]bool{}}}
	})
	if err := fakeResourceProviderHead(root, name, head, false); err == nil {
		t.Fatal("missing transcript accepted")
	}
	if err := fakeResourceProviderHead(root, name, head, true); err != nil {
		t.Fatal(err)
	}
	if err := fakeResourceProviderHead(root, name, head, false); err != nil {
		t.Fatal(err)
	}
	withFakeDockerState(root, func(state *fakeDockerState) { delete(state.Containers, name) })
	if err := fakeResourceProviderHead(root, name, head, false); err == nil {
		t.Fatal("absent container accepted")
	}
	withFakeDockerState(root, func(state *fakeDockerState) {
		state.Containers[name] = fakeDockerContainer{Running: true, ProviderKey: key}
	})
	if err := fakeResourceProviderHead(root, name, head, false); err != nil {
		t.Fatalf("container removal destroyed backing: %v", err)
	}
	withFakeDockerState(root, func(state *fakeDockerState) {
		volume := state.ProviderVolumes[key]
		volume.Marker = "foreign"
		state.ProviderVolumes[key] = volume
	})
	if err := fakeResourceProviderHead(root, name, head, false); err == nil {
		t.Fatal("foreign marker accepted")
	}
	withFakeDockerState(root, func(state *fakeDockerState) { delete(state.ProviderVolumes, key) })
	if fakeResourceProviderVolumeInspect(root, []string{"volume", "inspect", key}) == 0 {
		t.Fatal("absent volume accepted")
	}
}
