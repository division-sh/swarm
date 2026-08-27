package cliapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func decodeAuthoritativeYAMLFileForTest(t testing.TB, path string, target any) {
	t.Helper()
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		t.Fatalf("read authoritative YAML %s: %v", path, err)
	}
	if err := source.Decode(target); err != nil {
		t.Fatalf("decode authoritative YAML %s: %v", path, err)
	}
}
