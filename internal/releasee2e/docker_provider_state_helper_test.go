package releasee2e

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// The resource journey models exact backing and transcript presence; it is not
// credit for real Docker filesystem integrity or genuine provider retention.
type fakeProviderVolume struct {
	Marker string          `json:"marker"`
	Heads  map[string]bool `json:"heads"`
}

func fakeResourceProviderVolumeInspect(root string, args []string) int {
	var exists bool
	withFakeDockerState(root, func(state *fakeDockerState) { _, exists = state.ProviderVolumes[args[2]] })
	recordFakeDocker(root, fakeDockerRecord{Class: "provider_volume_inspect", Args: args})
	if !exists {
		fmt.Fprintln(os.Stderr, "provider volume missing")
		return 1
	}
	fmt.Fprintln(os.Stdout, "[]")
	return 0
}

func fakeResourceProviderHead(root, container, head string, write bool) error {
	var failure error
	withFakeDockerState(root, func(state *fakeDockerState) {
		bound := state.Containers[container]
		volume, exists := state.ProviderVolumes[bound.ProviderKey]
		if !bound.Running || !exists || volume.Marker != bound.ProviderKey || !validReleaseUUID(head) {
			failure = fmt.Errorf("provider head lacks exact running backing")
			return
		}
		if !write && !volume.Heads[head] {
			failure = fmt.Errorf("provider transcript missing")
			return
		}
		if write {
			volume.Heads[head] = true
			state.ProviderVolumes[bound.ProviderKey] = volume
		}
	})
	return failure
}

// These fixtures check process protocol and ownership, not transcript retention.
// Provider filesystem lifetime is proven by the real Docker and live restart tests.
func releaseProviderContainerBase(name string) (string, bool) {
	base, suffix, found := strings.Cut(name, "-claude-")
	if !found || len(suffix) != 24 || suffix != strings.ToLower(suffix) {
		return "", false
	}
	_, err := hex.DecodeString(suffix)
	return base, err == nil
}

func validateReleaseProviderCreate(root, base string, create releaseDockerCreate) error {
	if !releaseE2EContainerName(base) || create.volumesFrom != base || create.privileged || len(create.mounts) != 0 {
		return fmt.Errorf("provider container must inherit only its exact base workspace")
	}
	var parent fakeDockerContainer
	withFakeDockerState(root, func(state *fakeDockerState) { parent = state.Containers[base] })
	if !parent.Running || len(parent.Labels) != len(create.labels) {
		return fmt.Errorf("provider container base is not admitted")
	}
	for key, value := range parent.Labels {
		if key == "dev.swarm.container.name" {
			value = create.name
		}
		if create.labels[key] != value {
			return fmt.Errorf("provider container changed base identity %s", key)
		}
	}
	kind, _, _ := releaseE2EContainerIdentity(base)
	wantWorkdir := releaseE2EAgentWorkdir
	if kind == "system" {
		wantWorkdir = releaseE2ESystemWorkdir
	}
	if create.workdir != wantWorkdir {
		return fmt.Errorf("provider container changed base workdir")
	}
	if create.providerTmpfs != "" {
		if create.providerMount != "" || create.providerTmpfs != "/opt/swarm/provider/claude:rw,mode=0700,uid=10001,gid=10001" {
			return fmt.Errorf("invalid disposable provider backing")
		}
		return nil
	}
	key := strings.TrimSuffix(strings.TrimPrefix(create.providerMount, "type=volume,source="), ",target=/opt/swarm/provider/claude")
	if !releaseProviderKey(key) || create.providerMount != "type=volume,source="+key+",target=/opt/swarm/provider/claude" || !strings.HasSuffix(create.name, "-claude-"+strings.TrimPrefix(key, "swarm-claude-state-v1-")[:24]) {
		return fmt.Errorf("invalid exact provider volume")
	}
	return nil
}

func releaseProviderKey(key string) bool {
	digest := strings.TrimPrefix(key, "swarm-claude-state-v1-")
	if len(digest) != 64 || digest == key || digest != strings.ToLower(digest) {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func fakeReleaseProviderStateExec(root string, args []string, input []byte) (bool, int) {
	index := 1
	prepare := len(args) > 3 && args[1] == "--user" && args[2] == "0"
	if prepare {
		index = 3
	}
	if len(args) <= index+2 || args[index+1] != "node" {
		return false, 0
	}
	refuse := func(reason string) (bool, int) { return true, fakeDockerUnexpected(root, args, reason) }
	if len(input) != 0 || !releaseE2EContainerName(args[index]) || args[index+2] != "-e" {
		return refuse("invalid provider state check")
	}
	if _, provider := releaseProviderContainerBase(args[index]); !provider {
		return refuse("state check on base workspace")
	}
	wantLength := index + 8
	if prepare {
		wantLength = index + 7
	}
	if len(args) != wantLength || args[index+4] != "/opt/swarm/provider/claude" || !releaseProviderKey(args[index+5]) {
		return refuse("invalid provider state authority")
	}
	script := args[index+3]
	if !strings.Contains(script, ".swarm-authority") {
		return refuse("unknown provider state script")
	}
	if prepare {
		if (args[index+6] != "true" && args[index+6] != "false") || !strings.Contains(script, "fs.chownSync") {
			return refuse("invalid provider preparation")
		}
	} else if !validReleaseUUID(args[index+7]) || !strings.Contains(script, "readline") {
		return refuse("invalid provider head check")
	}
	recordFakeDocker(root, fakeDockerRecord{Class: "provider_state_check", Args: redactDockerArgs(args)})
	if os.Getenv(releaseResourceReadEnv) == "1" {
		if !prepare {
			if err := fakeResourceProviderHead(root, args[index], args[index+7], false); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return true, 1
			}
		} else {
			var invalid bool
			withFakeDockerState(root, func(state *fakeDockerState) {
				container := state.Containers[args[index]]
				// Capability probes use disposable tmpfs, not retained backing.
				if container.ProviderKey == "" {
					return
				}
				volume, exists := state.ProviderVolumes[container.ProviderKey]
				key := args[index+5]
				invalid = !exists || container.ProviderKey != key || (volume.Marker == "" && args[index+6] == "true") || (volume.Marker != "" && volume.Marker != key)
				if !invalid {
					volume.Marker = key
					state.ProviderVolumes[key] = volume
				}
			})
			if invalid {
				fmt.Fprintln(os.Stderr, "provider backing authority missing or mismatched")
				return true, 1
			}
		}
	}
	return true, 0
}
