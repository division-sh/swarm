package storetest

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

func TestManagedTurnFixturePreservesLiveScope(t *testing.T) {
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	const runtimeID = "11111111-1111-4111-8111-111111111112"
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: source.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := owner.RetireAndWait(context.Background()); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	ctx := correlation.WithRuntimeInstanceID(context.Background(), runtimeID)
	ctx = correlation.WithSourceArtifactFact(ctx, source)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, source.BundleHash()))
	ctx = worklifetime.WithOccurrence(ctx, owner)
	got, fact, err := managedTurnFixtureContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := correlation.RuntimeInstanceIDFromContext(got)
	scope, _ := authoractivity.ScopeFromContext(got)
	occurrence, _ := worklifetime.OccurrenceFromContext(got)
	if id != runtimeID || fact != source || scope != authoractivity.BundleScope(runtimeID, source.BundleHash()) || occurrence != owner {
		t.Fatalf("live fixture ownership changed: runtime=%s source=%v scope=%v occurrence=%v", id, fact, scope, occurrence)
	}
	for _, bad := range []context.Context{
		authoractivity.WithScope(ctx, authoractivity.BundleScope("foreign-runtime", source.BundleHash())),
		authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, SemanticFixtureBundleHash)),
	} {
		if _, _, err := managedTurnFixtureContext(bad); err == nil {
			t.Fatal("conflicting live scope accepted")
		}
	}
}

func TestManagedTurnFixtureStandaloneScope(t *testing.T) {
	ctx, source, err := managedTurnFixtureContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := correlation.RuntimeInstanceIDFromContext(ctx)
	if source.BundleHash() != SemanticFixtureBundleHash || id != semanticFixtureRuntimeInstanceID {
		t.Fatalf("standalone fixture scope = %s %s", id, source.BundleHash())
	}
}
