package storetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
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
		{"postgres", func(t *testing.T) runFixtureProofStore {
			_, db, _ := testutil.StartPostgres(t)
			return AdmitPostgresRuntimeStore(t, db)
		}},
	}
}

func TestRunFixturesUseExactSelectedLifecycleOwner(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := context.Background()
			sourceartifactfixture.Require(t, ctx, selected)
			collector := CollectTransactions(t, selected, TransactionProbeOptions{})
			for _, state := range []runtimerunlifecycle.State{
				"", runtimerunlifecycle.StateRunning, runtimerunlifecycle.StatePaused,
				runtimerunlifecycle.StateFailed, runtimerunlifecycle.StateCancelled,
			} {
				name := string(state)
				if name == "" {
					name = "default_running"
				}
				t.Run(name, func(t *testing.T) {
					at := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
					fixture := RunFixture{RunID: uuid.NewString(), State: state, Origin: ScenarioSetupOrigin(), StartedAt: at, EndedAt: at.Add(time.Second)}
					if state == runtimerunlifecycle.StateFailed {
						failure := runtimefailures.Normalize(errors.New("fixture failure"), "run_fixture_test", "materialize")
						fixture.Failure = &failure
					}
					before := collector.Snapshot()
					RequireRun(t, ctx, selected, fixture)
					after := collector.Snapshot()
					if after.Total.WriteCommits <= before.Total.WriteCommits || after.Active != 0 {
						t.Fatalf("fixture escaped exact selected coordinator or retained work: before=%+v after=%+v", before, after)
					}
					snapshot, err := selected.LoadRunLifecycleSnapshot(ctx, fixture.RunID)
					if err != nil {
						t.Fatal(err)
					}
					wantState := state
					if wantState == "" {
						wantState = runtimerunlifecycle.StateRunning
					}
					if snapshot.RunID != fixture.RunID || snapshot.Status != string(wantState) || !snapshot.StartedAt.Equal(at) || !reflect.DeepEqual(snapshot.Failure, fixture.Failure) {
						t.Fatalf("lifecycle facts = %+v, want fixture %+v", snapshot, fixture)
					}
					terminal := state == runtimerunlifecycle.StateFailed || state == runtimerunlifecycle.StateCancelled
					if terminal != (snapshot.EndedAt != nil) {
						t.Fatalf("terminal timestamp = %v for state %s", snapshot.EndedAt, wantState)
					}
					if (state == runtimerunlifecycle.StateFailed || state == runtimerunlifecycle.StateCancelled) && !snapshot.EndedAt.Equal(fixture.EndedAt) {
						t.Fatalf("terminal timestamp = %v, want %v", snapshot.EndedAt, fixture.EndedAt)
					}
					source, err := selected.RequirePresentRunSource(ctx, fixture.RunID)
					if err != nil || source.BundleHash() != SemanticFixtureBundleHash {
						t.Fatalf("fixture source = %v, err=%v", source, err)
					}
				})
			}
		})
	}
}

func TestRunFixtureCompletionRequiresSelectedCatalog(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := context.Background()
			sourceartifactfixture.Require(t, ctx, selected)
			collector := CollectTransactions(t, selected, TransactionProbeOptions{})
			fixture := RunFixture{RunID: uuid.NewString(), Origin: ScenarioSetupOrigin(), State: runtimerunlifecycle.StateCompleted}
			// The convenience fixture has no compiled catalog. It must not invent
			// terminal evidence or bypass production completion admission.
			if err := MaterializeRun(ctx, selected, fixture); err == nil || err.Error() != "normal run completion requires terminal catalog" {
				t.Fatalf("unconfigured completion fixture = %v, want catalog refusal", err)
			}
			snapshot, err := selected.LoadRunLifecycleSnapshot(ctx, fixture.RunID)
			if err != nil || snapshot.Status != string(runtimerunlifecycle.StateRunning) || snapshot.EndedAt != nil {
				t.Fatalf("missing catalog forged completion: snapshot=%+v err=%v", snapshot, err)
			}
			proof := collector.Snapshot()
			if proof.Total.WriteCommits == 0 || proof.Active != 0 {
				t.Fatalf("completion preparation escaped selected coordinator: %+v", proof)
			}
		})
	}
}

func TestRunFixtureReplayPreservesSelectedLifecycleFacts(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := context.Background()
			fixture := RunFixture{RunID: uuid.NewString(), Origin: ScenarioSetupOrigin(), StartedAt: time.Now().UTC().Truncate(time.Microsecond)}
			RequireRun(t, ctx, selected, fixture)
			before, err := selected.LoadRunLifecycleSnapshot(ctx, fixture.RunID)
			if err != nil {
				t.Fatal(err)
			}
			collector := CollectTransactions(t, selected, TransactionProbeOptions{})
			RequireRun(t, ctx, selected, fixture)
			after, err := selected.LoadRunLifecycleSnapshot(ctx, fixture.RunID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("exact replay changed lifecycle facts: before=%+v after=%+v err=%v", before, after, err)
			}
			if proof := collector.Snapshot(); proof.Total.WriteCommits == 0 || proof.Active != 0 {
				t.Fatalf("exact replay bypassed selected transaction settlement: %+v", proof)
			}
		})
	}
}

func TestRunFixtureRefusalsDoNotWrite(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := context.Background()
			sourceartifactfixture.Require(t, ctx, selected)
			collector := CollectTransactions(t, selected, TransactionProbeOptions{})
			for _, refusal := range []struct {
				name    string
				fixture RunFixture
			}{
				{"blank_identity", RunFixture{Origin: ScenarioSetupOrigin()}},
				{"fork_requires_named_operation", RunFixture{RunID: uuid.NewString(), State: runtimerunlifecycle.StateForked, Origin: ScenarioSetupOrigin()}},
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
