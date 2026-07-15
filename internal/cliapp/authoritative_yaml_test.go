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

func decodeAuthoritativeYAMLBytesForTest(t testing.TB, raw []byte, target any) {
	t.Helper()
	source, err := yamlsource.Load(raw)
	if err != nil {
		t.Fatalf("parse authoritative YAML: %v", err)
	}
	if err := source.Decode(target); err != nil {
		t.Fatalf("decode authoritative YAML: %v", err)
	}
}
