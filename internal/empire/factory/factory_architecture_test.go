package factory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFactoryProductionFilesContainNoEmpireCoordinatorLiteral(t *testing.T) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(".", "*.go"))
	if err != nil {
		t.Fatalf("glob factory files: %v", err)
	}
	for _, path := range matches {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") || base == "contracts_policy.go" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		if strings.Contains(string(data), "empire-coordinator") {
			t.Fatalf("%s still contains empire-coordinator; derive recipients from contracts instead", base)
		}
	}
}

func TestFactoryScanModeTaxonomyIsCentralized(t *testing.T) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(".", "*.go"))
	if err != nil {
		t.Fatalf("glob factory files: %v", err)
	}
	forbidden := []string{"local_services", "saas_gap", "saas_trend", "automation_micro", "corpus"}
	for _, path := range matches {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") || base == "contracts_policy.go" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		text := string(data)
		for _, token := range forbidden {
			if strings.Contains(text, token) {
				t.Fatalf("%s still contains scan mode literal %q; keep taxonomy centralized in contracts_policy.go", base, token)
			}
		}
	}
}

func TestFactoryProductionFilesContainNoEmpireTaxonomy(t *testing.T) {
	t.Helper()

	forbidden := []string{
		"empire-",
		"empire_",
		"Empire",
		"saas_gap",
		"saas_trend",
		"local_services",
		"automation_micro",
	}

	matches, err := filepath.Glob(filepath.Join(".", "*.go"))
	if err != nil {
		t.Fatalf("glob factory files: %v", err)
	}
	for _, path := range matches {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") || base == "contracts_policy.go" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		text := string(data)
		for _, token := range forbidden {
			if strings.Contains(text, token) {
				t.Fatalf("%s still contains Empire/taxonomy token %q; move product logic into approved product packages", base, token)
			}
		}
	}
}
