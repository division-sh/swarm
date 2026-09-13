package pipeline

import (
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"testing"
)

func requireResolvedSelection(t *testing.T, observation handlerselection.Observation) handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	fact, err := observation.ResolvedFact()
	if err != nil {
		t.Fatalf("expected resolved selection observation: %v", err)
	}
	return fact
}
