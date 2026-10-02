package platform

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

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

func TestPlatformVersionAdmissionPreservesScalarKindsAndUniqueness(t *testing.T) {
	for _, value := range []string{"1", "1.5", "true", "null", "''", "[]", "{}"} {
		if _, err := PlatformVersionFromYAML([]byte("platform: {version: " + value + "}\n")); err == nil {
			t.Fatalf("invalid version %s admitted", value)
		}
	}
	for _, body := range []string{
		"platform: {version: 1.6.0, version: 1.7.0}\n",
		"platform: {version: 1.6.0}\nplatform: {version: 1.7.0}\n",
		"platform: {<<: {version: 1.6.0}, version: 1.7.0}\n",
	} {
		if _, err := PlatformVersionFromYAML([]byte(body)); err == nil {
			t.Fatal("duplicate version admitted")
		}
	}
	for _, body := range []string{"platform: {version: '1.6.0'}\n", "platform: {<<: {version: 1.6.0}}\n"} {
		version, err := PlatformVersionFromYAML([]byte(body))
		if err != nil || version != "1.6.0" {
			t.Fatalf("typed version rejected: %s %v", version, err)
		}
	}
}

// TestPlatformSpecDigestMatchesMaterializedFilename ensures the version stamp
// and the content-addressed materialized filename derive from the same digest,
// so the two spellings of the binary↔spec stamp cannot diverge (#2182).
func TestPlatformSpecDigestMatchesMaterializedFilename(t *testing.T) {
	spec := PlatformSpecYAML()
	if len(spec) == 0 {
		t.Fatal("embedded platform spec is empty")
	}
	for _, field := range []string{
		"payload_schema_bundle_hash",
		"payload_schema_event_key",
		"payload_schema_digest",
	} {
		if !strings.Contains(string(spec), field) {
			t.Fatalf("embedded platform spec is missing event admission field %s", field)
		}
	}
	digest := sha256.Sum256(spec)
	want := hex.EncodeToString(digest[:])
	if got := PlatformSpecDigest(); got != want {
		t.Fatalf("PlatformSpecDigest() = %q, want %q (sha256 of embedded spec bytes)", got, want)
	}
	path, err := MaterializePlatformSpecFile()
	if err != nil {
		t.Fatalf("MaterializePlatformSpecFile(): %v", err)
	}
	name := path[strings.LastIndex(path, "/")+1:]
	if !strings.HasPrefix(name, "platform-spec-"+want[:16]) {
		t.Fatalf("materialized filename %q does not derive its prefix from PlatformSpecDigest %q", name, want)
	}
}
