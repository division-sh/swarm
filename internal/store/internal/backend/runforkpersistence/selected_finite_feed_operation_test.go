package runforkpersistence

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSelectedFiniteFeedRecoveryPinsMatchDurableFeedInventoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE runs (run_id TEXT PRIMARY KEY,status TEXT NOT NULL)`,
				`CREATE TABLE resource_version_pins (run_id TEXT,flow_path TEXT,event_name TEXT,schema_digest TEXT,version_id TEXT,selection TEXT)`,
				`CREATE TABLE fan_out_intents (run_id TEXT,origin_kind TEXT,source_resource_flow_path TEXT,source_resource_event_name TEXT,source_resource_version_id TEXT,deployment_schema_digest TEXT,bundle_hash TEXT,deployment_feed_id TEXT DEFAULT '')`,
				`CREATE TABLE resource_feed_publications (publication_id TEXT,run_id TEXT)`,
				`CREATE TABLE resource_feed_publication_members (publication_id TEXT,deployment_feed_id TEXT,flow_path TEXT,event_name TEXT,version_id TEXT,schema_digest TEXT)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			ctx, runID := context.Background(), uuid.NewString()
			bundle := "bundle-v2:sha256:" + strings.Repeat("c", 64)
			schema := "resource-schema-v1:sha256:" + strings.Repeat("a", 64)
			version := "resource-version-v1:sha256:" + strings.Repeat("b", 64)
			if _, err := db.Exec(`INSERT INTO runs VALUES ($1,'paused')`, runID); err != nil {
				t.Fatal(err)
			}
			read := func(want bool) {
				t.Helper()
				withForkOperationTx(t, db, func(tx *sql.Tx) {
					pins, err := selectedForkRecoveryPinsTx(ctx, tx, runID, bundle)
					if want && (err != nil || len(pins) != 1 || pins[0].RunID != runID || pins[0].RunState != "paused") {
						t.Fatalf("exact selected pins=%+v err=%v", pins, err)
					}
					if !want && err == nil {
						t.Fatalf("contradictory selected pins accepted: %+v", pins)
					}
				})
			}
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				pins, err := selectedForkRecoveryPinsTx(ctx, tx, runID, bundle)
				if err != nil || len(pins) != 0 {
					t.Fatalf("eventless child invented a feed pin: pins=%+v err=%v", pins, err)
				}
			})
			if _, err := db.Exec(`INSERT INTO fan_out_intents (run_id,origin_kind,source_resource_flow_path,source_resource_event_name,source_resource_version_id,deployment_schema_digest,bundle_hash) VALUES ($1,'deployment','.','root.ready',$2,$3,$4)`, runID, version, schema, bundle); err != nil {
				t.Fatal(err)
			}
			read(false)
			if _, err := db.Exec(`INSERT INTO resource_version_pins VALUES ($1,'.','root.ready',$2,$3,'fork_override')`, runID, schema, version); err != nil {
				t.Fatal(err)
			}
			read(true)
			if _, err := db.Exec(`UPDATE fan_out_intents SET deployment_schema_digest=$2 WHERE run_id=$1`, runID, "resource-schema-v1:sha256:"+strings.Repeat("d", 64)); err != nil {
				t.Fatal(err)
			}
			read(false)
			if _, err := db.Exec(`UPDATE fan_out_intents SET deployment_schema_digest=$2 WHERE run_id=$1`, runID, schema); err != nil {
				t.Fatal(err)
			}
			read(true)
			if _, err := db.Exec(`UPDATE resource_version_pins SET version_id='wrong' WHERE run_id=$1`, runID); err != nil {
				t.Fatal(err)
			}
			read(false)
		})
	}
}
