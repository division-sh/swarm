package releasee2e

import (
	"encoding/hex"
	"fmt"
	"strings"
)

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
	return true, 0
}
