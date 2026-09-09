package releasee2e

import (
	"strings"
	"testing"
)

func TestReleaseDockerProjectionInventoryRequiresExactScope(t *testing.T) {
	root := t.TempDir()
	args := []string{"container", "ls", "--all", "--filter", "label=dev.swarm.owner=runtime", "--filter", "label=dev.swarm.bundle_hash=bundle-v2:sha256:" + strings.Repeat("a", 64), "--filter", "label=dev.swarm.source_projection=runtime-projection-v1:" + strings.Repeat("b", 32), "--format", "{{.ID}}", "--no-trunc"}
	if err := validateReleaseDockerCommand(root, args); err != nil {
		t.Fatalf("exact projection inventory rejected: %v", err)
	}
	for index, value := range map[int]string{2: "--quiet", 4: "label=dev.swarm.owner=foreign", 6: "label=dev.swarm.bundle_hash=bad", 8: "label=dev.swarm.source_projection=bad", 10: "{{.Names}}", 11: "--size"} {
		changed := append([]string{}, args...)
		changed[index] = value
		if err := validateReleaseDockerCommand(root, changed); err == nil {
			t.Fatalf("malformed or unscoped projection inventory accepted: %q", changed)
		}
	}
	if err := validateReleaseDockerCommand(root, args[:len(args)-1]); err == nil {
		t.Fatal("truncated Docker IDs accepted")
	}
}
