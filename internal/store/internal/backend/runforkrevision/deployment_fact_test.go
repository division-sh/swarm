package runforkrevision

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestDeploymentFanOutFactCoordinates(t *testing.T) {
	key := fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()}
	intent, err := FanOutIntentFact(key)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := FanOutOutcomeFact(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if intent.key == outcome.key || !strings.Contains(intent.key, key.DeploymentFeedID) {
		t.Fatalf("deployment fact keys do not distinguish intent and ordinal: %q %q", intent.key, outcome.key)
	}
	for _, test := range []struct {
		name string
		body map[string]any
		want string
	}{
		{"intent", map[string]any{"fact_kind": "intent", "origin_kind": "deployment", "deployment_feed_id": key.DeploymentFeedID}, intent.key},
		{"outcome", map[string]any{"fact_kind": "outcome", "origin_kind": "deployment", "deployment_feed_id": key.DeploymentFeedID, "ordinal": 2}, outcome.key},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(test.body)
			if err != nil {
				t.Fatal(err)
			}
			got, err := FactKey(FamilyFanOutObligations, raw)
			if err != nil || got != test.want {
				t.Fatalf("deployment fact key=%q want=%q err=%v", got, test.want, err)
			}
			if err := intent.validate(key.RunID); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, body := range []map[string]any{
		{"fact_kind": "intent", "origin_kind": "deployment", "deployment_feed_id": key.DeploymentFeedID, "triggering_delivery_id": uuid.NewString()},
		{"fact_kind": "intent", "origin_kind": "handler", "deployment_feed_id": key.DeploymentFeedID},
		{"fact_kind": "outcome", "origin_kind": "deployment", "deployment_feed_id": key.DeploymentFeedID},
		{"fact_kind": "intent", "origin_kind": "deployment", "deployment_feed_id": key.DeploymentFeedID, "flow_path": "."},
	} {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := FactKey(FamilyFanOutObligations, raw); err == nil {
			t.Fatalf("mixed or incomplete deployment fact key accepted: %s", raw)
		}
	}
}

func TestDeploymentFanOutRevisionProjectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := exactSeekDB(t, backend)
			for _, ddl := range []string{
				`CREATE TABLE fan_out_intents (
					run_id TEXT, origin_kind TEXT, deployment_feed_id TEXT, triggering_delivery_id TEXT,
					flow_path TEXT, declaration_family TEXT, semantic_path TEXT, bundle_hash TEXT,
					semantic_digest TEXT, deployment_schema_digest TEXT, source_kind TEXT,
					source_event_id TEXT, source_run_id TEXT, source_entity_id TEXT, source_field TEXT,
					source_mutation_id TEXT, source_resource_flow_path TEXT, source_resource_event_name TEXT,
					source_resource_version_id TEXT, cardinality INTEGER, cursor INTEGER, status TEXT,
					capsule TEXT, blocked_reason TEXT, created_at TIMESTAMP)`,
				`CREATE TABLE fan_out_outcomes (
					run_id TEXT, deployment_feed_id TEXT, triggering_delivery_id TEXT, flow_path TEXT,
					declaration_family TEXT, semantic_path TEXT, ordinal INTEGER, outcome_kind TEXT,
					event_id TEXT, source_event_id TEXT, inherited_disposition TEXT, failure TEXT,
					created_at TIMESTAMP)`,
				`CREATE TABLE fan_out_obligation_barriers (
					run_id TEXT, triggering_delivery_id TEXT, flow_path TEXT, declaration_family TEXT,
					semantic_path TEXT, bundle_hash TEXT, semantic_digest TEXT, created_at TIMESTAMP,
					target_flow_path TEXT, target_node_id TEXT, handler_event TEXT, join_id TEXT,
					route_scope_key TEXT, route_instance_id TEXT, route_instance_path TEXT, entity_id TEXT,
					routing_source TEXT, execution_mode TEXT, timer_handle TEXT, status TEXT,
					summary TEXT, schedule_key TEXT, schedule_activation_id TEXT, updated_at TIMESTAMP)`,
			} {
				if _, err := db.Exec(ddl); err != nil {
					t.Fatal(err)
				}
			}
			key := fanoutobligation.IntentKey{RunID: uuid.NewString(), DeploymentFeedID: uuid.NewString()}
			eventID := uuid.NewString()
			at := time.Now().UTC()
			if _, err := db.Exec(`INSERT INTO fan_out_intents (
				run_id,origin_kind,deployment_feed_id,bundle_hash,deployment_schema_digest,source_kind,
				source_resource_flow_path,source_resource_event_name,source_resource_version_id,
				cardinality,cursor,status,created_at) VALUES ($1,'deployment',$2,$3,$4,'resource_version','.','items.ready',$5,1,1,'closed',$6)`,
				key.RunID, key.DeploymentFeedID, "bundle-v2:sha256:"+strings.Repeat("a", 64),
				"resource-schema-v1:sha256:"+strings.Repeat("b", 64),
				"resource-version-v1:sha256:"+strings.Repeat("c", 64), at); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO fan_out_outcomes (
				run_id,deployment_feed_id,ordinal,outcome_kind,source_event_id,inherited_disposition,created_at
			) VALUES ($1,$2,0,'committed',$3,'no_route',$4)`, key.RunID, key.DeploymentFeedID, eventID, at); err != nil {
				t.Fatal(err)
			}
			intent, err := FanOutIntentFact(key)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := FanOutOutcomeFact(key, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, refs := range [][]FactRef{nil, {intent, outcome}} {
				facts, err := loadSelectedCanonicalProjection(context.Background(), db, key.RunID, FamilyFanOutObligations, refs)
				if err != nil {
					t.Fatal(err)
				}
				if len(facts) != 2 || facts[0].key != intent.key || facts[1].key != outcome.key {
					t.Fatalf("projection lost typed deployment facts: %+v", facts)
				}
			}
		})
	}
}
