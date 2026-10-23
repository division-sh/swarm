package pipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// Native activity proofs capture a real admitted role. Historical result replay
// intentionally continues to use the unbound, read-only journal after reopening.
func installActivityReplayJournal(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, bus interface{ WaitForQuiescence(context.Context) error }, pc *pipeline.PipelineCoordinator) (pipeline.ActivityAttemptJournal, func()) {
	t.Helper()
	journal, release := captureActivityReplayJournal(t, ctx, selected.events.(startupownership.Store), bus)
	if err := pc.InstallActivityJournal(journal); err != nil {
		t.Fatal(err)
	}
	return journal, release
}

func activityBoringDeliveryNativeFixture(t *testing.T, backend string, source semanticview.Source) *pipeline.PipelineDeliveryNativeFixtureForTest {
	t.Helper()
	return bindActivityBoringDeliveryJournal(t, pipelineDeliveryNativeFixture(t, backend, source))
}

func bindActivityBoringDeliveryJournal(t *testing.T, fixture *pipeline.PipelineDeliveryNativeFixtureForTest) *pipeline.PipelineDeliveryNativeFixtureForTest {
	t.Helper()
	construct, reopen := fixture.NewCoordinator, fixture.ReopenExecution
	var release func()
	fixture.NewCoordinator = func(options pipeline.PipelineCoordinatorOptions) *pipeline.PipelineCoordinator {
		pc := construct(options)
		journal, join := captureActivityReplayJournal(t, fixture.Context, fixture.Store.(startupownership.Store), activityFixtureJoin(fixture.JoinExecution))
		release = join
		if err := pc.InstallActivityJournal(journal); err != nil {
			t.Fatal(err)
		}
		return pc
	}
	fixture.ReopenExecution = func() *pipeline.PipelineDeliveryNativeFixtureForTest {
		if release != nil {
			release()
		}
		return bindActivityBoringDeliveryJournal(t, reopen())
	}
	return fixture
}

func captureActivityReplayJournal(t *testing.T, ctx context.Context, selected startupownership.Store, bus interface{ WaitForQuiescence(context.Context) error }) (pipeline.ActivityAttemptJournal, func()) {
	t.Helper()
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	scope, scoped := authoractivity.ScopeFromContext(ctx)
	if !found || fact.Validate() != nil || !scoped || scope.RuntimeInstanceID == "" {
		t.Fatal("activity proof requires its exact admitted source and runtime scope")
	}
	process, err := agentfixture.ProcessCapability(t, ctx, selected.(agentfixture.Store))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		t.Helper()
		once.Do(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := bus.WaitForQuiescence(cleanup); err != nil {
				t.Error(err)
			}
			if err := process.Release(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(release)
	set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fact.BundleHash()}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, installed, err := process.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if installed {
		if current.Revision != set.Revision {
			t.Fatal("activity proof reopened a different complete source set")
		}
	} else if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: set}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: fact.BundleHash(), RuntimeInstanceID: scope.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: set.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := startupownership.CaptureActivityJournal(ctx, grant, evidence)
	if err != nil {
		t.Fatal(err)
	}
	return journal, release
}
