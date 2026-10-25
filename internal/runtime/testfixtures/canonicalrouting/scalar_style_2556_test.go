package canonicalrouting

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScalar2556GeneratedIdentifiersRemainData(t *testing.T) {
	root := CopyAgentSlugAdmission(t, "scalar-style", "worker", "api-worker")
	bundle := loadA2IndependentProducer(t, root)
	agent, found := bundle.Agents["worker"]
	if !found || agent.ID != "api-worker" || agent.Role != "api-worker" || agent.ResolvedIntent.Coordinate != "prompts/api-worker.md" {
		t.Fatalf("agent identifier was interpreted as an expression literal: %#v", bundle.Agents)
	}
	if _, err := os.Stat(filepath.Join(root, "prompts", "api-worker.md")); err != nil {
		t.Fatalf("intent filename lost its data identity: %v", err)
	}
}
