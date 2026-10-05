package serveapp

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

func TestMaterializingSenderReachesExistingRequiredReceiverBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, required := range []bool{false, true} {
			name := "entityless_control"
			if required {
				name = "existing_required"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyMaterializingSenderExistingReceiver(t, required))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "exact"}, "idempotency_key": "seed"})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				var count int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 AND e.event_name='work.ready' AND d.status='delivered'`, seed.RunID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("receiver completions=%d: %s", count, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
				}
			})
		}
	}
}

func TestProspectiveReceiverUpdatesAndCompanionRepairBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, rejectMissingHeader := range []bool{false, true} {
			name := "complete"
			if rejectMissingHeader {
				name = "missing_header_refused"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				rt, _, restart := newRetainedMailboxCompletionRuntime(t, backend, canonicalrouting.CopyMaterializingSenderExistingReceiver(t, true))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "first"}, "idempotency_key": "seed"})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				var entityID, instance string
				if err := rt.DB.QueryRow(`SELECT entity_id,flow_instance FROM entity_state WHERE run_id=$1`, seed.RunID).Scan(&entityID, &instance); err != nil {
					t.Fatal(err)
				}
				if rejectMissingHeader {
					before := mailboxCompletionRunEffects(t, rt, seed.RunID)
					companions := readForkReceiverCompanions(t, rt, seed.RunID, instance)
					_, err := rt.DB.Exec(`DELETE FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, seed.RunID, instance)
					if backend == servedparity.BackendExplicitPostgres {
						var constraint *pq.Error
						if !errors.As(err, &constraint) || constraint.Code != "23503" {
							t.Fatalf("missing constructed header did not fail foreign-key integrity: %v", err)
						}
					} else {
						var constraint *sqlite.Error
						if !errors.As(err, &constraint) || constraint.Code() != 787 {
							t.Fatalf("missing constructed header did not fail foreign-key integrity: %v", err)
						}
					}
					if after := mailboxCompletionRunEffects(t, rt, seed.RunID); !reflect.DeepEqual(before, after) {
						t.Fatal("refused header removal changed state, publications or receiver delivery")
					}
					if after := readForkReceiverCompanions(t, rt, seed.RunID, instance); !reflect.DeepEqual(companions, after) {
						t.Fatal("refused header removal changed construction or attachment evidence")
					}
				}
				params := map[string]any{"event_name": "start", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"case_id": "second"}, "idempotency_key": "update"}
				updated := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != updated.EventID {
					t.Fatal("duplicate publication changed identity")
				}
				var fields string
				if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance=$3`, seed.RunID, entityID, instance).Scan(&fields); err != nil {
					t.Fatal(err)
				}
				var values map[string]any
				if err := json.Unmarshal([]byte(fields), &values); err != nil || values["case_id"] != "second" {
					t.Fatalf("updated state=%s err=%v", fields, err)
				}
				var deliveries, companions int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 AND e.event_name='work.ready' AND d.status='delivered'`, seed.RunID).Scan(&deliveries); err != nil {
					t.Fatal(err)
				}
				if err := rt.DB.QueryRow(`SELECT count(*) FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, seed.RunID, instance).Scan(&companions); err != nil {
					t.Fatal(err)
				}
				if deliveries != 2 || companions != 1 {
					t.Fatalf("delivered=%d companions=%d; %s", deliveries, companions, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
				}
				before := mailboxCompletionRunEffects(t, rt, seed.RunID)
				rt, _ = restart()
				duplicate = requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != updated.EventID || duplicate.RunID != seed.RunID {
					t.Fatal("restart changed the committed prospective-state publication")
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				if after := mailboxCompletionRunEffects(t, rt, seed.RunID); !reflect.DeepEqual(before, after) {
					t.Fatal("restart/duplicate changed state, companion, publication or receiver delivery")
				}
			})
		}
	}
}

func TestProspectiveTerminalReceiverRefusesWithoutMutationOrPublicationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyProspectiveTerminalSender(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "terminal"}, "idempotency_key": "seed"})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			var states, publications, failed int
			if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1`, seed.RunID).Scan(&states); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT count(*) FROM events WHERE run_id=$1 AND event_name='work.ready'`, seed.RunID).Scan(&publications); err != nil {
				t.Fatal(err)
			}
			if err := rt.DB.QueryRow(`SELECT count(*) FROM event_deliveries WHERE event_id=$1 AND status='dead_letter'`, seed.EventID).Scan(&failed); err != nil {
				t.Fatal(err)
			}
			// Construction precedes execution. A rejected handler must preserve
			// its initial header/fields, not erase the successfully constructed tree.
			if states != 1 || publications != 0 || failed != 1 {
				t.Fatalf("terminal planning states=%d publications=%d refused=%d; %s", states, publications, failed, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
			}
			var entityID, stage, fields string
			var headerRevision, fieldRevision, mutations int
			if err := rt.DB.QueryRow(`SELECT fi.entity_id,fi.current_state,fi.revision,CAST(es.fields AS TEXT),es.revision
				FROM flow_instances fi JOIN entity_state es ON es.run_id=fi.run_id AND es.entity_id=fi.entity_id AND es.flow_instance=fi.instance_path
				WHERE fi.run_id=$1 AND fi.instance_path=$2 AND fi.flow_template='.'`, seed.RunID, seed.RunID).Scan(&entityID, &stage, &headerRevision, &fields, &fieldRevision); err != nil {
				t.Fatal(err)
			}
			var values map[string]any
			if err := json.Unmarshal([]byte(fields), &values); err != nil || len(values) != 0 || entityID != seed.RunID || stage != "waiting" || headerRevision != 1 || fieldRevision != 1 {
				t.Fatalf("rejected handler changed construction: entity=%s stage=%s revisions=%d/%d fields=%s err=%v", entityID, stage, headerRevision, fieldRevision, fields, err)
			}
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND domain='authored_field'`, seed.RunID).Scan(&mutations); err != nil || mutations != 0 {
				t.Fatalf("rejected handler journaled business mutations=%d err=%v", mutations, err)
			}
		})
	}
}
