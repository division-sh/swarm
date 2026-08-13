package canonicalrouting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStatelessSingletonFixturesContainNoCoordinatorCeremony(t *testing.T) {
	standing := CopyStandingTelegramServe(t, "https://telegram.example.test")
	assertCanonicalFileExcludes(t, filepath.Join(standing, "flows", "telegram-ingress", "entities.yaml"), "active_chats")

	memory := CopyStandingTelegramMemoryServe(t, "https://telegram.example.test")
	memoryEntities := readCanonicalFixtureFile(t, filepath.Join(memory, "flows", "memory-singleton", "entities.yaml"))
	if strings.TrimSpace(memoryEntities) != "memory_state: {}" {
		t.Fatalf("memory singleton entities = %q, want exactly empty primary entity", memoryEntities)
	}

	matrix := CopyInboundAdmissionPolicyMatrix(t)
	assertCanonicalFileExcludes(t, filepath.Join(matrix, "flows", "matrix", "entities.yaml"), "records")

	stream := ExampleRoot(t, FanInStream)
	entities := readCanonicalFixtureFile(t, filepath.Join(stream, "flows", "portfolio", "default", "entities.yaml"))
	if strings.Contains(entities, "_unused_reason") || !strings.Contains(entities, "reports:") {
		t.Fatalf("fan-in stream reports declaration = %q, want real field without _unused_reason", entities)
	}
	nodes := readCanonicalFixtureFile(t, filepath.Join(stream, "flows", "portfolio", "default", "nodes.yaml"))
	for _, want := range []string{"op: set", "target: entity.reports", "payload.operating_id", "value: {ref: payload}"} {
		if !strings.Contains(nodes, want) {
			t.Fatalf("fan-in stream nodes missing %q:\n%s", want, nodes)
		}
	}
}

func assertCanonicalFileExcludes(t *testing.T, path, forbidden string) {
	t.Helper()
	if source := readCanonicalFixtureFile(t, path); strings.Contains(source, forbidden) {
		t.Fatalf("%s still contains retired ceremony %q:\n%s", path, forbidden, source)
	}
}

func readCanonicalFixtureFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}
