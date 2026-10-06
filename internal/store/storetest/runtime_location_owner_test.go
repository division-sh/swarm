package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestPostgresRuntimeLocationFixtureRetainsOriginalWriterAndAdmitsIndependentPeer(t *testing.T) {
	dsn := testutil.StartPostgresDSN(t)
	selected, _ := StartPostgresRuntimeStoreWithReopen(t, dsn)
	if dsn == "" {
		t.Fatal("empty sandbox location")
	}
	ctx := context.Background()
	sourceartifactfixture.Require(t, ctx, selected)
	probe := CollectTransactions(t, selected, TransactionProbeOptions{})
	run := uuid.NewString()
	RequireRun(t, ctx, selected, RunFixture{Origin: ScenarioSetupOrigin(), RunID: run})
	if counts := probe.Snapshot(); counts.Total.WriteCommits == 0 || counts.Active != 0 {
		t.Fatalf("fixture bypassed original writer: %+v", counts)
	}
	peer, _ := StartPostgresRuntimeStoreWithReopen(t, dsn)
	if _, err := peer.LoadRunLifecycleSnapshot(ctx, run); err != nil {
		t.Fatalf("location peer lost original committed run: %v", err)
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.LoadRunLifecycleSnapshot(ctx, run); err != nil {
		t.Fatalf("original close revoked independent peer: %v", err)
	}
	if _, err := selected.LoadRunLifecycleSnapshot(ctx, run); err == nil {
		t.Fatal("closed original fixture retained read authority")
	}
}
