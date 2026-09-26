package eventpersistence

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type deploymentFactRecorder struct {
	runID string
	refs  []runforkrevision.FactRef
}

func (r *deploymentFactRecorder) AddFacts(runID string, refs ...runforkrevision.FactRef) error {
	r.runID = runID
	r.refs = append(r.refs, refs...)
	return nil
}

func TestDeploymentRunCreationRecordsExactFeedFactsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, opened, cleanup := testutil.StartEmptyPostgres(t)
				db = opened
				t.Cleanup(cleanup)
			} else {
				opened, err := sql.Open("sqlite", ":memory:")
				if err != nil {
					t.Fatal(err)
				}
				db = opened
				db.SetMaxOpenConns(1)
				t.Cleanup(func() { _ = db.Close() })
			}
			if _, err := db.Exec(`CREATE TABLE fan_out_intents (
				run_id TEXT,origin_kind TEXT,deployment_feed_id TEXT,bundle_hash TEXT,
				source_resource_flow_path TEXT,source_resource_event_name TEXT,
				source_resource_version_id TEXT,deployment_schema_digest TEXT,cardinality INTEGER)`); err != nil {
				t.Fatal(err)
			}
			runID := uuid.NewString()
			bundle := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			feeds := []durabledata.DeploymentFeed{
				{RunID: runID, BundleHash: bundle, Declaration: durabledata.DeclarationRef{FlowPath: ".", EventName: "root.ready"},
					VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("b", 64)),
					SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("c", 64)), RowCount: 2},
				{RunID: runID, BundleHash: bundle, Declaration: durabledata.DeclarationRef{FlowPath: "child", EventName: "child.ready"},
					VersionID:    durabledata.VersionID("resource-version-v1:sha256:" + strings.Repeat("d", 64)),
					SchemaDigest: durabledata.SchemaDigest("resource-schema-v1:sha256:" + strings.Repeat("e", 64)), RowCount: 0},
			}
			ids := []string{uuid.NewString(), uuid.NewString()}
			ctx := context.Background()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for i, feed := range feeds {
				if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents VALUES ($1,'deployment',$2,$3,$4,$5,$6,$7,$8)`,
					runID, ids[i], bundle, feed.Declaration.FlowPath, feed.Declaration.EventName,
					string(feed.VersionID), string(feed.SchemaDigest), feed.RowCount); err != nil {
					t.Fatal(err)
				}
			}
			recorder := &deploymentFactRecorder{}
			if err := recordDeploymentRunFeedFactsTx(ctx, tx, recorder, feeds); err != nil {
				t.Fatal(err)
			}
			if recorder.runID != runID || len(recorder.refs) != len(feeds) {
				t.Fatalf("recorded run=%s refs=%d", recorder.runID, len(recorder.refs))
			}
			for i, id := range ids {
				want, err := runforkrevision.FanOutIntentFact(fanoutobligation.IntentKey{RunID: runID, DeploymentFeedID: id})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(recorder.refs[i], want) {
					t.Fatalf("feed %d registered a different historical fact", i)
				}
			}
			foreign := append([]durabledata.DeploymentFeed(nil), feeds...)
			foreign[1].RunID = uuid.NewString()
			if err := recordDeploymentRunFeedFactsTx(ctx, tx, &deploymentFactRecorder{}, foreign); err == nil {
				t.Fatal("feeds from different runs were accepted as one revision")
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fan_out_intents VALUES ($1,'deployment',$2,$3,$4,$5,$6,$7,$8)`,
				runID, uuid.NewString(), bundle, feeds[0].Declaration.FlowPath, feeds[0].Declaration.EventName,
				string(feeds[0].VersionID), string(feeds[0].SchemaDigest), feeds[0].RowCount); err != nil {
				t.Fatal(err)
			}
			if err := recordDeploymentRunFeedFactsTx(ctx, tx, &deploymentFactRecorder{}, feeds); err == nil {
				t.Fatal("ambiguous canonical feed rows were accepted")
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM fan_out_intents WHERE deployment_feed_id<>$1 AND source_resource_flow_path=$2`,
				ids[0], feeds[0].Declaration.FlowPath); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE fan_out_intents SET source_resource_version_id='other' WHERE deployment_feed_id=$1`, ids[0]); err != nil {
				t.Fatal(err)
			}
			if err := recordDeploymentRunFeedFactsTx(ctx, tx, &deploymentFactRecorder{}, feeds); err == nil {
				t.Fatal("contradictory canonical feed row was accepted")
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM fan_out_intents WHERE deployment_feed_id=$1`, ids[0]); err != nil {
				t.Fatal(err)
			}
			if err := recordDeploymentRunFeedFactsTx(ctx, tx, &deploymentFactRecorder{}, feeds); err == nil {
				t.Fatal("missing canonical feed row was accepted")
			}
		})
	}
}
