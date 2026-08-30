package conformance

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func conformanceRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root with go.mod not found")
		}
		dir = parent
	}
}

func loadMatrixOpenRPCMethods(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read openrpc artifact: %v", err)
	}
	var doc struct {
		Methods []struct {
			Name string `json:"name"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse openrpc artifact: %v", err)
	}
	out := map[string]bool{}
	for _, method := range doc.Methods {
		name := strings.TrimSpace(method.Name)
		if name == "" {
			t.Fatal("openrpc method missing name")
		}
		out[name] = true
	}
	return out
}
