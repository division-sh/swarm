package apispec

import (
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/platform"
)

func TestPlatformSpecWorkspaceDataProjectionOwnsActorIsolation(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	workspace := mustMappingValue(t, root, "workspace_model")
	authority := mustMappingValue(t, workspace, "data_projection_authority")
	identity := scalarValue(mustMappingValue(t, authority, "identity_and_permissions"))
	for _, fragment := range []string{
		"content identity",
		"exact concrete actor",
		"isolated container",
		"share its writable /workspace volume",
		"no write bits",
	} {
		if !strings.Contains(identity, fragment) {
			t.Fatalf("data projection identity authority missing %q:\n%s", fragment, identity)
		}
	}

	dataMount := mustYAMLPath(t, workspace, "standard_mounts", "/data")
	assertScalarContains(t, mustMappingValue(t, dataMount, "scope"), "actor-specific")
	assertScalarContains(t, mustMappingValue(t, dataMount, "lifecycle"), "canonical bundle/store facts")
	assertScalarContains(t, mustYAMLPath(t, workspace, "deployment_mapping", "docker", "/data"), "actor-isolated execution container")
	assertScalarContains(t, mustYAMLPath(t, workspace, "managed_container_identity", "labels", "lineage"), "dev.swarm.data_projection_id")
}

func TestPlatformSpecRejectsRetiredGlobalDataProjectionClaims(t *testing.T) {
	rawBytes, err := os.ReadFile(platform.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatalf("read authoritative platform spec: %v", err)
	}
	raw := string(rawBytes)
	for _, retired := range []string{
		"scope: global — identical content visible to every agent",
		"An agent in any class sees identical /data contents",
		"/data: Named volume or bind mount, shared across all containers",
		"/data mount exists and is readable",
	} {
		if strings.Contains(raw, retired) {
			t.Fatalf("platform spec retains retired global /data authority %q", retired)
		}
	}
}
