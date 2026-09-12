package runtime

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRuntimeFixtureRequiresExactRunAndCurrentGrant(t *testing.T) {
	ctx := context.Background()
	session := newRuntimeTestRetainedSession(t)
	source := testSourceArtifactFact(t, runtimeTestBundleHash)
	_, grant, err := newRuntimeTestProcessCapabilityWithSession(t, nil, nil, source, authorActivityTestRuntimeInstanceID, session)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	if _, err := session.InspectRunExecutionOwnership(ctx, evidence, runID); err == nil {
		t.Fatal("unknown run was authorized")
	}
	session.admitRun(t, runID, source)
	if got, err := session.InspectRunExecutionOwnership(ctx, evidence, runID); err != nil || got != manager.RunExecutionOwned {
		t.Fatalf("exact fixture owner = %v, %v", got, err)
	}
	foreignRun := uuid.NewString()
	foreign := sourceartifactfixture.FactFor(sourceartifactfixture.New("agents.yaml", []byte("agents: {}\n# foreign fixture\n")))
	session.admitRun(t, foreignRun, foreign)
	if got, err := session.InspectRunExecutionOwnership(ctx, evidence, foreignRun); err != nil || got != manager.RunExecutionOtherNormalSource {
		t.Fatalf("foreign source ownership = %v, %v", got, err)
	}
	crossed := evidence
	crossed.GrantID = uuid.NewString()
	if _, err := session.InspectRunExecutionOwnership(ctx, crossed, runID); err == nil {
		t.Fatal("unrecorded grant was authorized")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := session.InspectRunExecutionOwnership(cancelled, evidence, runID); err == nil {
		t.Fatal("cancelled inspection was authorized")
	}
	if err := grant.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.InspectRunExecutionOwnership(ctx, evidence, runID); err == nil {
		t.Fatal("stale grant evidence was authorized after retirement")
	}
}
