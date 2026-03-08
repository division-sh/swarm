package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPipelineArchitecture_EmpireBoundary(t *testing.T) {
	t.Helper()

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
		t.Fatalf("%s imports pipeline/empire; keep Empire-specific policy out of generic pipeline files", base)
	}
}

func TestPipelineArchitecture_GenericTestsOnlyUseEmpireViaDefaultModuleBridge(t *testing.T) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(".", "*_test.go"))
	if err != nil {
		t.Fatalf("glob pipeline test files: %v", err)
	}
	for _, path := range matches {
		base := filepath.Base(path)
		if base == "pipeline_architecture_test.go" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		if !strings.Contains(string(data), "internal/runtime/pipeline/empire") {
			continue
		}
		if base == "module_default_test.go" {
			continue
		}
		t.Fatalf("%s imports pipeline/empire; keep Empire-specific tests under pipeline/empire and reserve module_default_test.go for the generic test bridge", base)
	}
}
