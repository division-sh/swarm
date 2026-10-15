package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestEntityToolEntityCardinalityKeepsExactRunScopeBothStores(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			run, sibling := uuid.NewString(), uuid.NewString()
			setup := selected.(interface {
				SetupScenarioEntities(context.Context, pipeline.ScenarioSetupRequest) (pipeline.ScenarioSetupResult, error)
			})
			for i, id := range []string{run, sibling} {
				RequireRun(t, ctx, selected, RunFixture{Origin: ScenarioSetupOrigin(), RunID: id})
				entities := []pipeline.ScenarioSetupEntityRequest{}
				for j := 0; j < i+1; j++ {
					entity := uuid.NewString()
					entities = append(entities, pipeline.ScenarioSetupEntityRequest{Alias: entity, EntityID: entity, FlowInstance: "fixture/one", EntityType: "fixture", CurrentState: "pending"})
				}
				if _, err := setup.SetupScenarioEntities(ctx, pipeline.ScenarioSetupRequest{RunID: id, CreatedAt: time.Now().UTC(), Entities: entities}); err != nil {
					t.Fatal(err)
				}
			}
			probe := CollectTransactions(t, selected, TransactionProbeOptions{})
			for id, want := range map[string]int{run: 1, sibling: 2, uuid.NewString(): 0} {
				if got, err := CountRunEntityStorage(ctx, selected, id); got != want || err != nil {
					t.Fatalf("exact run count=%d want=%d err=%v", got, want, err)
				}
			}
			if got := probe.Snapshot(); got.Active != 0 || got.Total.ReadCommits != 3 || got.Total.WriteCommits != 0 {
				t.Fatalf("entity count escaped its original read owner: %+v", got)
			}
		})
	}
}

func TestEntityToolEntityCardinalityRefusesForeignAndUnavailableOwners(t *testing.T) {
	for _, backend := range runFixtureProofBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			run := uuid.NewString()
			for _, foreign := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
				if got, err := CountRunEntityStorage(context.Background(), foreign, run); got != 0 || err == nil {
					t.Fatalf("foreign %T supplied a count: %d/%v", foreign, got, err)
				}
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if got, err := CountRunEntityStorage(cancelled, selected, run); got != 0 || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled owner supplied a count: %d/%v", got, err)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if got, err := CountRunEntityStorage(context.Background(), selected, run); got != 0 || err == nil {
				t.Fatalf("closed owner supplied a count: %d/%v", got, err)
			}
		})
	}
}
