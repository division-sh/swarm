package cataloge2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testcatalog"
	"gopkg.in/yaml.v3"
)

func TestCatalogReplayProofReferences(t *testing.T) {
	fixtures := map[string]testcatalog.Fixture{}
	for _, fixture := range catalogReplayCleanFixtures(t) {
		fixtures[fixture.Name] = fixture
	}
	raw, err := os.ReadFile(filepath.Join(repoRootFromCatalogE2E(t),
		"internal/runtime/conformance/testdata/producer_routing_retirement_ledger.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var ledger struct {
		Rows []struct {
			ID    string `yaml:"id"`
			Path  string `yaml:"path"`
			Proof string `yaml:"proof"`
		} `yaml:"rows"`
	}
	if err := yaml.Unmarshal(raw, &ledger); err != nil {
		t.Fatal(err)
	}
	if len(ledger.Rows) != 197 {
		t.Fatalf("routing ledger rows = %d, want 197", len(ledger.Rows))
	}
	for _, row := range ledger.Rows {
		if err := validateCatalogReplayProofReference(row.Proof, row.Path, fixtures); err != nil {
			t.Errorf("%s: %v", row.ID, err)
		}
	}

	fixture := catalogReplayCleanFixtures(t)[0]
	valid := fmt.Sprintf("TestCatalogReplayClean_SelectedStores%d/%s", catalogReplayPartition(fixture.Name), fixture.Name)
	path := fixture.RelativePath + "/nodes.yaml"
	for _, tc := range []struct {
		name      string
		proof     string
		path      string
		wantError bool
	}{
		{"exact live fixture", valid, path, false},
		{"removed entrypoint", "TestCatalogReplayClean_SelectedStores/" + fixture.Name, path, true},
		{"wrong partition", fmt.Sprintf("TestCatalogReplayClean_SelectedStores%d/%s", catalogReplayPartition(fixture.Name)%3+1, fixture.Name), path, true},
		{"missing fixture", "TestCatalogReplayClean_SelectedStores1/test-not-in-catalog", path, true},
		{"retired fixture", "TestCatalogReplayClean_SelectedStores1/test-child-flow-loads", path, true},
		{"invented nested leaf", valid + "/not-executed", path, true},
		{"different source", valid, "tests/different-fixture/nodes.yaml", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCatalogReplayProofReference(tc.proof, tc.path, fixtures)
			if (err != nil) != tc.wantError {
				t.Fatalf("proof %q: error=%v, wantError=%t", tc.proof, err, tc.wantError)
			}
		})
	}
}

func validateCatalogReplayProofReference(proof, sourcePath string, fixtures map[string]testcatalog.Fixture) error {
	const prefix = "TestCatalogReplayClean_SelectedStores"
	if !strings.HasPrefix(proof, prefix) {
		return nil
	}
	_, name, _ := strings.Cut(proof, "/")
	fixture, exists := fixtures[name]
	if !exists {
		return fmt.Errorf("replay proof %q names no live executable replay fixture", proof)
	}
	want := fmt.Sprintf("%s%d/%s", prefix, catalogReplayPartition(name), name)
	if proof != want || !strings.HasPrefix(sourcePath, fixture.RelativePath+"/") {
		return fmt.Errorf("replay proof %q for %s must name %q in %s", proof, sourcePath, want, fixture.RelativePath)
	}
	return nil
}
