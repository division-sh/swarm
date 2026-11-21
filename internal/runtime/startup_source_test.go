package runtime

import (
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
)

func TestStartupSourceKeepsOriginalRootAndGrant(t *testing.T) {
	rt := newStartupReadinessTestRuntime(t)
	other := newStartupReadinessTestRuntime(t)
	ctx := testAuthorActivityContext(t.Context())
	t.Cleanup(func() {
		if err := errors.Join(rt.Shutdown(), other.Shutdown()); err != nil {
			t.Error(err)
		}
	})
	bundle, present := semanticview.Bundle(rt.Options.WorkflowModule.SemanticSource())
	if !present {
		t.Fatal("fixture has no compiled bundle")
	}
	root, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	foreign, _ := sourceartifactfixture.Rooted(t, "other-root", root.Artifact())
	crossed := correlation.WithRootAdmission(correlation.WithSourceArtifactFact(ctx, rt.Options.SourceArtifactFact), foreign.Admission())
	if _, err := rt.PreflightStartupSource(crossed); err == nil {
		t.Fatal("composed admission accepted a different root with the same content")
	}
	if _, err := rt.PrepareStart(crossed); err == nil {
		t.Fatal("independent preparation accepted a different root with the same content")
	}
	source, err := rt.PreflightStartupSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source.RootAdmission(); err != nil || got.Reference() != root.Admission().Reference() {
		t.Fatalf("checked root = %v, %v", got.Reference(), err)
	}
	if source.runtime != rt || source.grant != rt.startupGrant {
		t.Fatal("checked source lost its original runtime and generation")
	}
	original := rt.startupGrant
	rt.startupGrant = other.startupGrant
	defer func() { rt.startupGrant = original }()
	if _, err := source.PrepareStart(); err == nil {
		t.Fatal("stale checked source prepared a replacement grant")
	}
	if _, err := source.RootAdmission(); err == nil {
		t.Fatal("stale checked source projected a root for the replacement grant")
	}
}
