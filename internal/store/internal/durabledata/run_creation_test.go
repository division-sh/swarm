package durabledata

import (
	"context"
	"database/sql"
	"testing"

	runtimedata "github.com/division-sh/swarm/internal/durabledata"
	"github.com/google/uuid"
)

type deploymentFeedWriterProbe struct {
	feeds []runtimedata.DeploymentFeed
}

func (p *deploymentFeedWriterProbe) CreateDeploymentFeedTx(_ context.Context, _ *sql.Tx, feed runtimedata.DeploymentFeed) error {
	p.feeds = append(p.feeds, feed)
	return nil
}

func TestRunCreationDeploymentFeedPortPreservesEmptyAndMultiplePins(t *testing.T) {
	first, err := runtimedata.ParseDeclarationRef(".", "first.loaded")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtimedata.ParseDeclarationRef(".", "second.loaded")
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	version := runtimedata.VersionID("resource-version-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	schema := runtimedata.SchemaDigest("resource-schema-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	feeds := []runtimedata.DeploymentFeed{
		{RunID: runID, BundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Declaration: first, SchemaDigest: schema, VersionID: version, RowCount: 0},
		{RunID: runID, BundleHash: "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Declaration: second, SchemaDigest: schema, VersionID: version, RowCount: 3},
	}
	plan := RunCreationPlan{committed: true, feeds: feeds}
	projected := plan.DeploymentFeeds()
	projected[0].RowCount = 10
	if plan.feeds[0].RowCount != 0 {
		t.Fatal("DeploymentFeeds exposed mutable plan storage")
	}
	if err := CommitRunCreationFeedsTx(context.Background(), &sql.Tx{}, &plan, nil); err == nil {
		t.Fatal("pinned run creation accepted a missing feed owner")
	}
	writer := &deploymentFeedWriterProbe{}
	if err := CommitRunCreationFeedsTx(context.Background(), &sql.Tx{}, &plan, writer); err != nil {
		t.Fatal(err)
	}
	if len(writer.feeds) != 2 || writer.feeds[0].RowCount != 0 || writer.feeds[1].RowCount != 3 {
		t.Fatalf("feed handoff = %#v", writer.feeds)
	}
	if err := CommitRunCreationFeedsTx(context.Background(), &sql.Tx{}, &plan, writer); err == nil {
		t.Fatal("one run-creation plan committed feeds twice")
	}
}
