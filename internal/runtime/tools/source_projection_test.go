package tools

import (
	"testing"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func toolTestRuntimeSourceProjection(t testing.TB, root string) *sourceartifact.RuntimeProjection {
	t.Helper()
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatalf("admit tool-test source: %v", err)
	}
	projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
	if err != nil {
		t.Fatalf("materialize tool-test source: %v", err)
	}
	t.Cleanup(func() { _ = projection.Release() })
	return projection
}
