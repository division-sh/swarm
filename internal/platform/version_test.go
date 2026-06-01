package platform

import "testing"

func TestPlatformVersionFromYAML(t *testing.T) {
	got, err := PlatformVersionFromYAML([]byte("platform:\n  name: swarm-orchestrator\n  version: 1.6.0\n"))
	if err != nil {
		t.Fatalf("PlatformVersionFromYAML() error = %v", err)
	}
	if got != "1.6.0" {
		t.Fatalf("PlatformVersionFromYAML() = %q, want 1.6.0", got)
	}
}

func TestPlatformVersionFromYAMLRequiresVersion(t *testing.T) {
	if _, err := PlatformVersionFromYAML([]byte("platform:\n  name: swarm-orchestrator\n")); err == nil {
		t.Fatal("PlatformVersionFromYAML() error = nil, want missing version error")
	}
}
