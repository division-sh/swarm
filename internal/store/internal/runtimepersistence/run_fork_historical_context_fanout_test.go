package runtimepersistence

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestRunForkHistoricalContextFanOutCoordinatesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newHistoricalContextFixtureWithSeed(t, backend, seedHistoricalContextFanOutKinds)
			kinds := 0
			for _, fact := range f.facts {
				if fact.family != "fan_out_obligations" {
					continue
				}
				kind := historicalContextFields(t, fact.raw)["fact_kind"].(string)
				kinds++
				t.Run(kind, func(t *testing.T) {
					attacks := []string{"wrapper_key", "alias", "unknown_kind", "wrong_known_kind", "triggering_delivery_id", "flow_path", "declaration_family", "semantic_path"}
					if kind == "outcome" {
						attacks = append(attacks, "ordinal")
					}
					for _, attack := range attacks {
						t.Run(attack, func(t *testing.T) {
							bad := fact
							fields := historicalContextFields(t, fact.raw)
							switch attack {
							case "wrapper_key", "alias":
								bad.key = kind + `|` + uuid.NewString() + `|.|fan_out|nodes["fan-out-source"].handlers["items.ready"].fan_out`
								if kind == "outcome" {
									bad.key += "|0"
								}
							case "unknown_kind":
								fields["fact_kind"] = "unknown"
							case "wrong_known_kind":
								fields["fact_kind"] = map[string]string{"intent": "outcome", "outcome": "barrier", "barrier": "intent"}[kind]
							case "triggering_delivery_id":
								fields[attack] = uuid.NewString()
							case "flow_path":
								fields[attack] = "other/nested"
							case "declaration_family":
								fields[attack] = "handler_rule"
							case "semantic_path":
								fields[attack] = `nodes["other"].handlers["items.ready"].fan_out`
							case "ordinal":
								fields[attack] = 1
							}
							bad.raw = historicalContextJSON(t, fields)
							f.reject(t, fact, bad, attack == "alias")
						})
					}
				})
			}
			if kinds != 3 {
				t.Fatalf("executed fan-out kind census=%d, want intent/outcome/barrier", kinds)
			}
		})
	}
}

// H16 deliberately compares actual canonical writer bytes to fixture-authored
// literals. It does not call FactKey to manufacture its own expected result.
func TestRunForkHistoricalContextCanonicalKeyBytesBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newHistoricalContextFixtureWithSeed(t, backend, seedHistoricalContextFanOutKinds)
			s := f.source
			want := map[string]string{
				"events": s.eventID, "entity_mutations": s.mutationID, "entity_metadata": s.entityID,
				"event_deliveries": s.deliveryID, "committed_replay_scopes": s.eventID, "event_receipts": s.receiptID,
				"dead_letters": s.deadLetterID, "timers": s.timerID, "agent_sessions": s.sessionID,
				"agent_turns": s.turnID, "agent_conversation_audits": s.auditID, "reply_contexts": s.replyID,
				"intent":  `intent|` + s.deliveryID + `|.|fan_out|nodes["fan-out-source"].handlers["items.ready"].fan_out`,
				"outcome": `outcome|` + s.deliveryID + `|.|fan_out|nodes["fan-out-source"].handlers["items.ready"].fan_out|0`,
				"barrier": `barrier|` + s.deliveryID + `|.|fan_out|nodes["fan-out-source"].handlers["items.ready"].fan_out`,
			}
			for _, fact := range f.facts {
				key := fact.family
				if key == "fan_out_obligations" {
					key = historicalContextFields(t, fact.raw)["fact_kind"].(string)
				}
				if expected, ok := want[key]; !ok || fact.key != expected {
					t.Fatalf("canonical %s fact key=%q, want independently authored bytes=%q", key, fact.key, expected)
				}
				delete(want, key)
			}
			if len(want) != 0 {
				t.Fatalf("canonical writer omitted key cells: %#v", want)
			}
		})
	}
}

func seedHistoricalContextFanOutKinds(t *testing.T, tx *sql.Tx, f historicalContextFixture) {
	t.Helper()
	ctx, s := testAuthorActivityContext(), f.source
	fixture := fanOutOwnerFixture{
		runID: s.runID, eventID: s.eventID, deliveryID: s.deliveryID, flowPath: ".",
		semanticPath: `nodes["fan-out-source"].handlers["items.ready"].fan_out`,
		bundleHash:   "bundle-v2:sha256:" + strings.Repeat("1", 64), createdAt: s.at,
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `DELETE FROM fan_out_intents WHERE run_id=$1`, s.runID)
	insertFanOutOwnerIntent(t, ctx, tx, fixture, 1, s.at)
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "fan_out_test_item_invalid", "test", "commit_fan_out_chunk", nil))
	if !ok {
		t.Fatal("construct typed fan-out fixture rejection")
	}
	failureJSON, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		t.Fatal(err)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,failure,created_at) VALUES ($1,$2,'.','fan_out',$3,0,'semantic_rejected',$4,$5)`, s.runID, s.deliveryID, fixture.semanticPath, string(failureJSON), s.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE fan_out_intents SET cursor=1,status='closed' WHERE run_id=$1`, s.runID)
	digest := "sha256:" + strings.Repeat("2", 64)
	ref, err := timeridentity.NewFanOutDeliveryJoinRef(mustPersistenceRootNode("fan-out-source"), "items.ready", "all-items-delivered", mustFanOutBarrierDeclaration(t, fixture), fixture.bundleHash, digest)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindFanOutIntent(s.deliveryID, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := events.NewRootRoutingSource(s.entityID)
	if err != nil {
		t.Fatal(err)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_obligation_barriers (
		run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,
		target_flow_path,target_node_id,handler_event,join_id,route_scope_key,route_instance_id,route_instance_path,
		entity_id,routing_source,execution_mode,timer_handle,status,created_at,updated_at
	) VALUES ($1,$2,'.','fan_out',$3,$4,$5,$6,$7,$8,$9,'root','root','root',$10,$11,'live',$12,'armed',$13,$13)`,
		s.runID, s.deliveryID, fixture.semanticPath, fixture.bundleHash, digest, ref.FlowPath(), ref.NodeID(), ref.HandlerEvent(), ref.JoinID(),
		s.entityID, historicalContextJSON(t, producer), historicalContextJSON(t, handle), s.at)
}
