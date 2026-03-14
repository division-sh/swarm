package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultContractsPathExists(t *testing.T) {
	path := filepath.Join(repoRoot(), defaultContractsPath, "package.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("default contracts path missing package.yaml: %v", err)
	}
}
