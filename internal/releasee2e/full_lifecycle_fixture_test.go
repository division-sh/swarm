package releasee2e

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

const fullLifecycleFixtureSource = "standing_telegram"

func TestFullLifecycleJourneyMatrixIsClosed(t *testing.T) {
	want := map[string]fullLifecycleJourney{
		"J1-sqlite-graceful":                  {name: "J1-sqlite-graceful", backend: "sqlite", kind: fullLifecycleGraceful},
		"J2-postgres-graceful":                {name: "J2-postgres-graceful", backend: "postgres", kind: fullLifecycleGraceful},
		"J3-sqlite-crash-intrinsic-recover":   {name: "J3-sqlite-crash-intrinsic-recover", backend: "sqlite", kind: fullLifecycleCrashIntrinsic},
		"J4-postgres-crash-intrinsic-recover": {name: "J4-postgres-crash-intrinsic-recover", backend: "postgres", kind: fullLifecycleCrashIntrinsic},
		"J5-sqlite-dev-fresh":                 {name: "J5-sqlite-dev-fresh", backend: "sqlite", kind: fullLifecycleDevFresh},
	}
	if len(fullLifecycleJourneys) != len(want) {
		t.Fatalf("full lifecycle journey count = %d, want %d", len(fullLifecycleJourneys), len(want))
	}
	seen := make(map[string]bool, len(fullLifecycleJourneys))
	for _, journey := range fullLifecycleJourneys {
		if seen[journey.name] {
			t.Fatalf("duplicate full lifecycle journey %s", journey.name)
		}
		seen[journey.name] = true
		if expected, ok := want[journey.name]; !ok || journey != expected {
			t.Fatalf("unsupported full lifecycle journey %#v", journey)
		}
	}
}

func fullLifecycleFixtureRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(releaseE2ERepoRoot(t), "internal", "releasee2e", "testdata", "full_lifecycle")
}

func fullLifecycleExecutableSource(t *testing.T) string {
	t.Helper()
	return filepath.Join(fullLifecycleFixtureRoot(t), fullLifecycleFixtureSource)
}

func mutateFullLifecycleFixtureWithoutExactConnectorResponse(t *testing.T, contracts string) {
	t.Helper()
	connectorPath := filepath.Join(contracts, "telegram", "connector.yaml")
	raw, err := os.ReadFile(connectorPath)
	if err != nil {
		t.Fatalf("read lifecycle connector BODY: %v", err)
	}
	var body struct {
		Tools map[string]map[string]any `yaml:"tools"`
	}
	if err := yaml.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	tool, present := body.Tools["send_message"]
	if !present || tool["category"] != "provider_connector" {
		t.Fatal("lifecycle connector BODY has no exact send_message connector")
	}
	tool["category"] = "provider_registration"
	mutated, err := yaml.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseFile(t, connectorPath, string(mutated))
}
