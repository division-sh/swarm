package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPipelineArchitecture_EmpireBoundary(t *testing.T) {
	t.Helper()

	allowed := map[string]struct{}{
		"coordinator.go":       {},
		"coordinator_scoring.go": {},
		"payload_factory.go":   {},
	}

	matches, err := filepath.Glob(filepath.Join(".", "*.go"))
	if err != nil {
		t.Fatalf("glob pipeline files: %v", err)
	}
	for _, path := range matches {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		if !strings.Contains(string(data), "internal/runtime/pipeline/empire") {
			continue
		}
		if _, ok := allowed[base]; ok {
			continue
		}
		t.Fatalf("%s imports pipeline/empire; keep Empire-specific policy out of generic pipeline files", base)
	}
}
