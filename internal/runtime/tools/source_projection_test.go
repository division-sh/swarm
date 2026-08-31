package tools

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/workspace"
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

func bindToolTestHostProjection(t testing.TB, manager *workspace.HostManager, projection *sourceartifact.RuntimeProjection) {
	t.Helper()
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatalf("bind tool-test source projection: %v", err)
	}
	t.Cleanup(func() { _ = manager.ReleaseSourceProjection(context.Background()) })
}
