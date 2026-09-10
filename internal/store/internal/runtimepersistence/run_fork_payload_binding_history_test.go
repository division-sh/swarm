package runtimepersistence

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

func TestRunForkRevisionPreservesPayloadBindingBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
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
