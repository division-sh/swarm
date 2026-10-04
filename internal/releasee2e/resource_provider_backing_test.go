package releasee2e

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestResourceProviderBackingRejectsMissingAuthority(t *testing.T) {
	t.Run("worker mount never becomes provider backing", func(t *testing.T) {
		t.Setenv(releaseResourceReadEnv, "1")
		workerPath, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		key := "swarm-claude-state-v1-" + strings.Repeat("a", 64)
		for _, retained := range []bool{false, true} {
			root := t.TempDir()
			name := releaseE2EFixtureAgent + "-claude-" + strings.Repeat("a", 24)
			args := []string{"create", "--name", name, "--network", releaseE2ENetwork, "-w", releaseE2EAgentWorkdir, "--mount", "type=bind,source=" + workerPath + ",destination=/opt/swarm/bin/swarm,readonly"}
			wantKey := ""
			if retained {
				wantKey = key
				args = append(args, "--mount", "type=volume,source="+key+",target=/opt/swarm/provider/claude")
			} else {
				args = append(args, "--tmpfs", "/opt/swarm/provider/claude:rw,mode=0700,uid=10001,gid=10001")
			}
			args = append(args, releaseE2EWorkspaceImage, "sleep", "infinity")
			if code := fakeDockerCreate(root, args); code != 0 {
				t.Fatalf("fixture create retained=%t: exit=%d", retained, code)
			}
			withFakeDockerState(root, func(state *fakeDockerState) {
				container := state.Containers[name]
				if container.WorkerPath != workerPath || container.ProviderKey != wantKey {
					t.Fatalf("fixture mount identities retained=%t: %#v", retained, container)
				}
				if !retained && len(state.ProviderVolumes) != 0 {
					t.Fatal("disposable provider acquired retained backing")
				}
			})
		}
	})
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
