package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

func TestFanOutProducerFairnessIgnoresCallerAuditTimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutReadbackTestDB(t, backend)
			seedFanOutReadbackClaim(t, db)
			intent, err := scanFanOutIntent(db.QueryRow(`SELECT ` + fanOutIntentColumns + ` FROM fan_out_intents`))
			if err != nil {
				t.Fatal(err)
			}
			request := intent.Request
			resourceData := installFanOutFairnessResource(t, db, backend, request)
			request.Source = fanoutobligation.SourceRef{Kind: fanoutobligation.SourceResourceVersion,
				Declaration: durabledata.DeclarationRef{FlowPath: "root", EventName: "items"}, VersionID: resourceData.version}
			for _, offset := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
				request.Key.TriggeringDeliveryID = uuid.NewString()
				before := time.Now().UTC().Add(-time.Millisecond)
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := request.Validate(); err != nil {
					t.Fatal(err)
				}
				if err := insertFanOutIntentSQL(context.Background(), tx, backend == "postgres", resourceData.owner, runforkrevision.NewEffects(), request, nil, request.Capsule.Lineage.ParentEventID, before.Add(offset)); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				after := time.Now().UTC().Add(time.Millisecond)
				actual, err := scanFanOutIntent(db.QueryRow(`SELECT `+fanOutIntentColumns+` FROM fan_out_intents WHERE triggering_delivery_id=$1`, request.Key.TriggeringDeliveryID))
				if err != nil {
					t.Fatal(err)
				}
				if actual.CreatedAt.Before(before) || actual.CreatedAt.After(after) || !actual.LastServedAt.IsZero() || !actual.UpdatedAt.Equal(actual.CreatedAt) {
					t.Fatalf("caller audit offset %v changed finite creation position: created=%v updated=%v served=%v admission=[%v,%v]", offset, actual.CreatedAt, actual.UpdatedAt, actual.LastServedAt, before, after)
				}
			}
		})
	}
}

type fanOutFairnessResource struct {
	owner   *storedurabledata.Owner
	version durabledata.VersionID
}

func installFanOutFairnessResource(t *testing.T, db *sql.DB, backend string, request fanoutobligation.IntentRequest) fanOutFairnessResource {
	t.Helper()
	ref := durabledata.DeclarationRef{FlowPath: "root", EventName: "items"}
	schema := map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "integer"}}, "required": []string{"value"}}
	compiled, defects := durabledata.CompileJSONL(ref, schema, "", []byte("{\"value\":1}\n{\"value\":2}\n"))
	if len(defects) != 0 || len(compiled.Rows) != request.Cardinality {
		t.Fatalf("fairness source rows=%d defects=%+v", len(compiled.Rows), defects)
	}
	manifest, err := json.Marshal(compiled.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	blobType := "BLOB"
	if backend == "postgres" {
		blobType = "BYTEA"
	}
	for _, statement := range []string{
		`CREATE TABLE resource_bundle_declarations (bundle_hash TEXT, flow_path TEXT, event_name TEXT, schema_digest TEXT, canonical_schema_bytes ` + blobType + `, business_key_field TEXT)`,
		`CREATE TABLE resource_version_pins (run_id TEXT, flow_path TEXT, event_name TEXT, schema_digest TEXT, version_id TEXT)`,
		`CREATE TABLE resource_versions (version_id TEXT, flow_path TEXT, event_name TEXT, schema_digest TEXT, canonical_schema_bytes ` + blobType + `, manifest_json ` + blobType + `, row_count INTEGER, business_key_field TEXT, content_digest TEXT, row_codec TEXT, canonical_jsonl ` + blobType + `, pruned_at TIMESTAMP)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO resource_bundle_declarations VALUES ($1,$2,$3,$4,$5,$6)`, request.PlanRef.BundleHash, ref.FlowPath, ref.EventName, compiled.Manifest.SchemaDigest, compiled.CanonicalSchema, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO resource_version_pins VALUES ($1,$2,$3,$4,$5)`, request.Key.RunID, ref.FlowPath, ref.EventName, compiled.Manifest.SchemaDigest, compiled.VersionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO resource_versions VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULL)`, compiled.VersionID, ref.FlowPath, ref.EventName, compiled.Manifest.SchemaDigest, compiled.CanonicalSchema, manifest, len(compiled.Rows), "", compiled.Manifest.ContentDigest, compiled.Manifest.RowCodec, compiled.CanonicalJSONL); err != nil {
		t.Fatal(err)
	}
	var owner *storedurabledata.Owner
	if backend == "postgres" {
		store, err := postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		owner, err = storedurabledata.NewPostgres(store, func() error { return nil })
	} else {
		store, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		owner, err = storedurabledata.NewSQLite(store, func() error { return nil }, time.Now)
	}
	if err != nil {
		t.Fatal(err)
	}
	return fanOutFairnessResource{owner: owner, version: compiled.VersionID}
}
