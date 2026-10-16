package runtimepersistence

import (
	"context"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

type activityFixtureJournal struct {
	once  sync.Once
	owner pipeline.ActivityAttemptJournal
}

func activityNativeJournalForFixture(t *testing.T, fixture authorActivityReceiptFixture) pipeline.ActivityAttemptJournal {
	t.Helper()
	if fixture.activityJournal == nil || fixture.activityOwner == nil {
		t.Fatal("native activity fixture has no retained original role slot")
	}
	fixture.activityJournal.once.Do(func() {
		fixture.activityJournal.owner = captureActivityFixtureJournal(fixture.activityOwner, testAuthorActivityContext(), fixture.store)
	})
	return fixture.activityJournal.owner
}

// Reuse the fixture's original process/source owner, never a callback-time
// lookup of a replacement or a role derived from the submitted attempt.
func captureActivityFixtureJournal(t testing.TB, ctx context.Context, selected any) pipeline.ActivityAttemptJournal {
	t.Helper()
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || fact.Validate() != nil {
		t.Fatal("native activity fixture requires its admitted source fact")
	}
	store := selected.(agentfixture.Store)
	process, err := agentfixture.ProcessCapability(t, ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	plan, found, err := process.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		plan, err = agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, store, plan, agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	if evidence.State == startupownership.GrantPrepared {
		evidence, err = grant.MarkProbesSettled(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if evidence.State == startupownership.GrantProbeSettled {
		evidence, err = grant.AdmitExecution(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	journal, err := startupownership.CaptureActivityJournal(ctx, grant, evidence)
	if err != nil {
		t.Fatal(err)
	}
	return journal
}
