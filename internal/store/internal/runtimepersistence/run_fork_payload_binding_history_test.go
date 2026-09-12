package runtimepersistence

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkRevisionPreservesPayloadBindingBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, reopen := openPayloadBindingHistoryFixture(t, backend.name)
			store := fixture.store.(eventRecordContractStore)
			ctx := testAuthorActivityContext()
			payload := []byte("{\n  \"integer\":9007199254740993, \"double\":1.0\n}")
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "history.binding", "operator", "", payload, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
			if err := commitSemanticEventFixture(ctx, store, event); err != nil {
				t.Fatal(err)
			}
			record, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
			if err != nil || !found {
				t.Fatalf("canonical record: %v %v", found, err)
			}
			admitted, err := record.Decode()
			if err != nil {
				t.Fatal(err)
			}
			want, ok := admitted.Event().PayloadAdmission()
			if !ok {
				t.Fatal("canonical writer lost payload admission")
			}
			var firstRevision int64
			var firstRaw []byte
			if err := fixture.db.QueryRowContext(ctx, `SELECT revision, fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2 AND present ORDER BY revision LIMIT 1`, event.RunID(), event.ID()).Scan(&firstRevision, &firstRaw); err != nil {
				t.Fatal(err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			fixture = reopen()
			store = fixture.store.(eventRecordContractStore)
			restored, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
			if err != nil || !found || !restored.Equal(record) {
				t.Fatalf("reopened canonical record differs: found=%v err=%v", found, err)
			}
			if _, err := restored.Decode(); err != nil {
				t.Fatalf("reopened strict event decode: %v", err)
			}
			for attempt := 0; attempt < 3; attempt++ {
				var raw []byte
				if err := fixture.db.QueryRowContext(ctx, `SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='events' AND fact_key=$2 AND revision=$3 AND present`, event.RunID(), event.ID(), firstRevision).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(raw, firstRaw) {
					t.Fatal("duplicate publication changed fixed revision")
				}
				var fact struct {
					Payload string                    `json:"payload_base64"`
					Bundle  string                    `json:"payload_schema_bundle_hash"`
					Flow    string                    `json:"payload_schema_flow_id"`
					Event   string                    `json:"payload_schema_event_key"`
					Digest  string                    `json:"payload_schema_digest"`
					Class   events.PayloadSchemaClass `json:"payload_schema_class"`
				}
				if err := json.Unmarshal(raw, &fact); err != nil {
					t.Fatal(err)
				}
				binding, err := events.RestorePayloadSchemaBinding(events.PayloadSchemaBindingInput{BundleHash: fact.Bundle, FlowID: fact.Flow, EventKey: fact.Event, SchemaDigest: fact.Digest, SchemaClass: fact.Class})
				if err != nil {
					t.Fatal(err)
				}
				got, err := base64.StdEncoding.DecodeString(fact.Payload)
				if err != nil {
					t.Fatal(err)
				}
				if !binding.Equal(want.Binding()) || !bytes.Equal(got, want.Payload()) || !bytes.Equal(got, payload) {
					t.Fatal("historical payload/binding differs from canonical record")
				}
				if err := commitSemanticEventFixture(ctx, store, event); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func openPayloadBindingHistoryFixture(t *testing.T, backend string) (authorActivityReceiptFixture, func() authorActivityReceiptFixture) {
	t.Helper()
	if backend == "sqlite" {
		path := filepath.Join(t.TempDir(), "history.db")
		open := func() authorActivityReceiptFixture {
			store := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
			return authorActivityReceiptFixture{store: store, db: store.backend.ConstructionHandle(), dialect: authoractivityfixture.DialectSQLite}
		}
		return open(), open
	}
	if backend != "postgres" {
		t.Fatalf("unsupported history fixture backend %q", backend)
	}
	dsn, db, _ := testutil.StartPostgres(t)
	wrap := func(db *sql.DB) authorActivityReceiptFixture {
		store := admitTestPostgresStore(t, db)
		registerTestAuthorActivityCatalog(t, store)
		return authorActivityReceiptFixture{store: store, db: db, dialect: authoractivityfixture.DialectPostgres}
	}
	return wrap(db), func() authorActivityReceiptFixture {
		reopened, err := sql.Open("postgres", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		return wrap(reopened)
	}
}
