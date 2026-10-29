package storetest

import (
	"context"
	"errors"
	"github.com/division-sh/swarm/internal/runtime/correlation"

	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"

	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type runFixtureProofStore interface {
	RunFixtureStore
	sourceArtifactFixtureOwner
	runtimebus.RunLifecycleReadPersistence
}

func runFixtureProofBackends() []struct {
	name string
	open func(*testing.T) runFixtureProofStore
} {
	return []struct {
		name string
		open func(*testing.T) runFixtureProofStore
	}{
		{"sqlite", func(t *testing.T) runFixtureProofStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) runFixtureProofStore { return StartPostgresRuntimeStore(t) }},
	}
}

func TestRunFixtureRefusalsDoNotWrite(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := context.Background()
			if original, err := agentfixture.ProcessContext(t, ctx, any(selected).(agentfixture.Store)); err != nil {
				t.Fatal(err)
			} else {
				ctx = original
			}
			rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
			sourceartifactfixture.RequireArtifact(t, ctx, selected, rooted.Artifact())
			ctx = correlation.WithRootAdmission(correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.FactFor(rooted.Artifact())), rooted.Admission())
			collector := CollectTransactions(t, selected, TransactionProbeOptions{})
			for _, refusal := range []struct {
				name    string
				fixture RunFixture
			}{
				{"blank_identity", RunFixture{Origin: ScenarioSetupOrigin()}},
				{"retired_fork_state", RunFixture{RunID: uuid.NewString(), State: "forked", Origin: ScenarioSetupOrigin()}},
				{"contradictory_source", RunFixture{RunID: uuid.NewString(), Origin: ScenarioSetupOrigin(), Artifact: sourceartifactfixture.Artifact(), BundleHash: "different"}},
			} {
				t.Run(refusal.name, func(t *testing.T) {
					if err := MaterializeRun(ctx, selected, refusal.fixture); err == nil {
						t.Fatal("invalid fixture was accepted")
					}
				})
			}
			t.Run("cancelled", func(t *testing.T) {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				fixture := RunFixture{RunID: uuid.NewString(), Origin: ScenarioSetupOrigin()}
				if err := MaterializeRun(cancelled, selected, fixture); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled fixture = %v, want cancellation", err)
				}
				if err := selected.RequirePresentRun(ctx, fixture.RunID); err == nil {
					t.Fatal("cancelled fixture created a run")
				}
			})
			if proof := collector.Snapshot(); proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("refused fixture persisted work: %+v", proof)
			}
		})
	}
}
