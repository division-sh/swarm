package runforkpersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestForkDeploymentReadbackAllowsProgressButPreservesInheritedPrefixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE fan_out_intents (
					run_id TEXT,origin_kind TEXT,deployment_feed_id TEXT,bundle_hash TEXT,source_kind TEXT,
					source_resource_flow_path TEXT,source_resource_event_name TEXT,source_resource_version_id TEXT,
					deployment_schema_digest TEXT,cardinality INTEGER,cursor INTEGER,status TEXT,blocked_reason TEXT,
					triggering_delivery_id TEXT,flow_path TEXT,declaration_family TEXT,semantic_path TEXT,
					semantic_digest TEXT,capsule TEXT)`,
				`CREATE TABLE fan_out_outcomes (
					run_id TEXT,deployment_feed_id TEXT,ordinal INTEGER,outcome_kind TEXT,event_id TEXT,
					source_event_id TEXT,inherited_disposition TEXT,failure TEXT,created_at TIMESTAMP)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			key := fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()}
			target := durabledata.PinnedSource{
				Declaration:  durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"},
				VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("c", 64)),
				SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64)),
				RowCount:     2,
			}
			bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents (
				run_id,origin_kind,deployment_feed_id,bundle_hash,source_kind,source_resource_flow_path,
				source_resource_event_name,source_resource_version_id,deployment_schema_digest,cardinality,cursor,status
			) VALUES ($1,'deployment',$2,$3,'resource_version',$4,$5,$6,$7,2,2,'closed')`,
				key.RunID, key.DeploymentFeedID, bundle, target.Declaration.FlowPath, target.Declaration.EventName,
				string(target.VersionID), string(target.SchemaDigest)); err != nil {
				t.Fatal(err)
			}
			sourceID, childID := uuid.NewString(), uuid.NewString()
			now := time.Now().UTC()
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes VALUES
				($1,$2,0,'committed',NULL,$3,'no_route',NULL,$5),
				($1,$2,1,'committed',$4,NULL,NULL,NULL,$5)`, key.RunID, key.DeploymentFeedID, sourceID, childID, now); err != nil {
				t.Fatal(err)
			}
			carriage := forkDeploymentCarriage{inherit: true, cursor: 1, status: fanoutobligation.StatusOpen,
				outcomes: []fanoutobligation.Outcome{{Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted,
					SourceEventID: sourceID, InheritedDisposition: fanoutobligation.InheritedNoRoute, CreatedAt: now}},
			}
			check := func(want bool) {
				t.Helper()
				cursor, err := requireRunForkDeploymentFeedRowTx(ctx, tx, key, bundle, target, carriage)
				if err == nil {
					err = requireRunForkDeploymentOutcomesTx(ctx, tx, backend == "postgres", key, target, cursor, carriage)
				}
				if (err == nil) != want {
					t.Fatalf("exact readback success=%t want=%t err=%v", err == nil, want, err)
				}
			}
			check(true)
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_outcomes SET source_event_id=$3 WHERE run_id=$1 AND deployment_feed_id=$2 AND ordinal=0`, key.RunID, key.DeploymentFeedID, uuid.NewString()); err != nil {
				t.Fatal(err)
			}
			check(false)
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_outcomes SET source_event_id=$3 WHERE run_id=$1 AND deployment_feed_id=$2 AND ordinal=0`, key.RunID, key.DeploymentFeedID, sourceID); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET source_resource_version_id=$3 WHERE run_id=$1 AND deployment_feed_id=$2`, key.RunID, key.DeploymentFeedID, "other"); err != nil {
				t.Fatal(err)
			}
			check(false)
		})
	}
}

func TestForkDeploymentReadbackPreservesPreactivationPendingAndInitialStatusBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE fan_out_intents (
					run_id TEXT,origin_kind TEXT,deployment_feed_id TEXT,bundle_hash TEXT,source_kind TEXT,
					source_resource_flow_path TEXT,source_resource_event_name TEXT,source_resource_version_id TEXT,
					deployment_schema_digest TEXT,cardinality INTEGER,cursor INTEGER,status TEXT,blocked_reason TEXT,
					triggering_delivery_id TEXT,flow_path TEXT,declaration_family TEXT,semantic_path TEXT,
					semantic_digest TEXT,capsule TEXT)`,
				`CREATE TABLE fan_out_outcomes (
					run_id TEXT,deployment_feed_id TEXT,ordinal INTEGER,outcome_kind TEXT,event_id TEXT,
					source_event_id TEXT,inherited_disposition TEXT,failure TEXT,created_at TIMESTAMP)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			key := fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()}
			target := durabledata.PinnedSource{
				Declaration:  durabledata.DeclarationRef{FlowPath: ".", EventName: "items.ready"},
				VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("c", 64)),
				SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("b", 64)),
				RowCount:     1,
			}
			bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents (
				run_id,origin_kind,deployment_feed_id,bundle_hash,source_kind,source_resource_flow_path,
				source_resource_event_name,source_resource_version_id,deployment_schema_digest,cardinality,cursor,status
			) VALUES ($1,'deployment',$2,$3,'resource_version',$4,$5,$6,$7,1,0,'closed')`,
				key.RunID, key.DeploymentFeedID, bundle, target.Declaration.FlowPath, target.Declaration.EventName,
				string(target.VersionID), string(target.SchemaDigest)); err != nil {
				t.Fatal(err)
			}
			if _, err := requireRunForkDeploymentFeedRowTx(ctx, tx, key, bundle, target, forkDeploymentCarriage{status: fanoutobligation.StatusOpen}); err == nil {
				t.Fatal("fresh unissued feed accepted contradictory closed status")
			}
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET cursor=1,status='closed' WHERE run_id=$1 AND deployment_feed_id=$2`, key.RunID, key.DeploymentFeedID); err != nil {
				t.Fatal(err)
			}
			carriage := forkDeploymentCarriage{inherit: true, cursor: 1, status: fanoutobligation.StatusClosed,
				pending: []runfork.RunForkFanOutPendingReplay{{Ordinal: 0, SourceEventID: uuid.NewString()}},
			}
			if err := requireRunForkDeploymentOutcomesTx(ctx, tx, backend == "postgres", key, target, 1, carriage); err != nil {
				t.Fatalf("preactivation pending replay cannot yet have an outcome: %v", err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_outcomes
				(run_id,deployment_feed_id,ordinal,outcome_kind,event_id,created_at)
				VALUES ($1,$2,0,'committed',$3,$4)`, key.RunID, key.DeploymentFeedID, uuid.NewString(), time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if err := requireRunForkDeploymentOutcomesTx(ctx, tx, backend == "postgres", key, target, 1, carriage); err == nil {
				t.Fatal("fork exact readback accepted a pending replay bound to the wrong child event")
			}
		})
	}
}
