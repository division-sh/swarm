package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeFixtureReopenJoinsConsumerBeforeClosingIndependentOwners(t *testing.T) {
	ctx := context.Background()
	var primary, peer *store.SQLiteRuntimeStore
	runID := uuid.NewString()
	consumerJoined := false
	t.Run("consumer", func(t *testing.T) {
		primary, _ = StartSQLiteRuntimeStoreWithReopen(t, ctx)
		peer, _ = StartSQLiteRuntimeStoreWithReopen(t, ctx, primary.Path())
		if primary == peer || primary.Path() == "" || primary.Path() != peer.Path() {
			t.Fatal("pair did not construct independent owners at one native location")
		}
		RequireRun(t, ctx, primary, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin()})
		if _, err := peer.LoadRunLifecycleSnapshot(ctx, runID); err != nil {
			t.Fatalf("independent peer cannot read primary persistence: %v", err)
		}
		release, result := make(chan struct{}), make(chan error, 1)
		go func() {
			<-release
			if err := primary.RequirePresentRun(ctx, runID); err != nil {
				result <- err
				return
			}
			result <- peer.RequirePresentRun(ctx, runID)
		}()
		t.Cleanup(func() {
			close(release)
			select {
			case err := <-result:
				consumerJoined = true
				if err != nil {
					t.Errorf("store closed before consumer joined: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("native consumer did not join")
			}
		})
	})
	if !consumerJoined {
		t.Fatal("consumer cleanup was not joined")
	}
	for _, selected := range []*store.SQLiteRuntimeStore{primary, peer} {
		if err := selected.RequirePresentRun(ctx, runID); err == nil {
			t.Fatal("fixture cleanup left an independent store open")
		}
	}
}
