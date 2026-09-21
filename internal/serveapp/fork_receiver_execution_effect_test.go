package serveapp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// Optional-node settlement is execution evidence, not a replacement mailbox
// effect. State-owning nodes additionally prove the supported business write.
func requireForkReceiverExecution(t *testing.T, rt servedControlProofRuntime, runID, sourceEvent, path, label, kind, entityID string, policy canonicalrouting.ForkReceiverPolicy) events.DeliveryRoute {
	t.Helper()
	if entityID != "" && entityID != flowidentity.EntityID(path) {
		t.Fatalf("receiver identity=%s, want exact static owner %s", entityID, flowidentity.EntityID(path))
	}
	route := requireForkReceiverDelivery(t, rt, runID, sourceEvent, path, kind, entityID)
	var mutations int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_count'`, runID, flowidentity.EntityID(path), sourceEvent).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if policy != canonicalrouting.ForkReceiverOptionalAbsent && policy != canonicalrouting.ForkReceiverOptionalExisting {
		var oldRaw, newRaw, writer, writerID, step string
		if err := rt.DB.QueryRow(`SELECT COALESCE(CAST(old_value AS TEXT),'null'),CAST(new_value AS TEXT),writer_type,writer_id,handler_step FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_count' AND writer_id='workflow_engine'`, runID, entityID, sourceEvent).Scan(&oldRaw, &newRaw, &writer, &writerID, &step); err != nil {
			t.Fatal(err)
		}
		var oldValue any
		var newValue float64
		if err := json.Unmarshal([]byte(oldRaw), &oldValue); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(newRaw), &newValue); err != nil {
			t.Fatal(err)
		}
		wantStep, wantMutations := "mutate", 1
		prior, numeric := oldValue.(float64)
		if policy == canonicalrouting.ForkReceiverAutoMaterializing {
			wantStep = "create"
			if oldValue != nil {
				t.Fatalf("automatic creation invented prior authored counter: %s", oldRaw)
			}
		} else if !numeric || prior < 0 || prior != float64(int(prior)) {
			t.Fatalf("existing counter was not an exact nonnegative integer: %s", oldRaw)
		}
		if policy == canonicalrouting.ForkReceiverExplicitCreate {
			wantStep, wantMutations = "create", 2
			var initial int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3 AND domain='authored_field' AND path='processed_count' AND old_value IS NULL AND CAST(new_value AS TEXT)='0' AND writer_type='platform' AND writer_id='entity_initial_value' AND handler_step='create_entity'`, runID, entityID, sourceEvent).Scan(&initial); err != nil || initial != 1 {
				t.Fatalf("explicit creation lost initial counter history: count=%d err=%v", initial, err)
			}
		}
		if mutations != wantMutations || newValue != prior+1 || writer != "platform" || writerID != "workflow_engine" || step != wantStep {
			t.Fatalf("%s business write: count=%d %s -> %s writer=%s/%s/%s", path, mutations, oldRaw, newRaw, writer, writerID, step)
		}
	} else {
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND entity_id=$2 AND caused_by_event=$3`, runID, flowidentity.EntityID(path), sourceEvent).Scan(&mutations); err != nil || mutations != 0 {
			t.Fatalf("optional receiver mutated state: count=%d err=%v", mutations, err)
		}
	}
	if entityID != "" {
		row := readForkReceiverRows(t, rt, runID)[path]
		var entity operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &entity)
		if entity.Entity.RunID != runID || entity.Entity.EntityID != entityID || entity.Entity.FlowInstance != path || entity.Entity.EntityType != "receipt" || entity.Entity.CurrentState != row.State || !reflect.DeepEqual(entity.Fields, row.Fields) {
			t.Fatalf("public receiver state: %+v rows=%+v", entity, row)
		}
	}
	requireForkReceiverNoPublicationOrNotice(t, rt, runID, sourceEvent)
	return route
}

func requireForkReceiverNoPublicationOrNotice(t *testing.T, rt servedControlProofRuntime, runID, sourceEvent string) {
	t.Helper()
	var notices, emitted, deliveries int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM mailbox WHERE source_event_id=$1`, sourceEvent).Scan(&notices); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`, runID, sourceEvent, string(events.EventAdmissionDiagnosticDirect), string(events.EventTypePlatformRuntimeLog)).Scan(&emitted); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.source_event_id=$2`, runID, sourceEvent).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if notices != 0 || emitted != 0 || deliveries != 0 {
		t.Fatalf("retired notice or post-frontier work: notices=%d events=%d deliveries=%d", notices, emitted, deliveries)
	}
}

func requireForkReceiverMutationOwners(t *testing.T, rt servedControlProofRuntime, runID, eventID, prefix string, receivers []canonicalrouting.ForkReceiver) {
	t.Helper()
	allowed := map[string]bool{}
	for _, receiver := range receivers {
		if receiver.Policy != canonicalrouting.ForkReceiverOptionalAbsent && receiver.Policy != canonicalrouting.ForkReceiverOptionalExisting {
			allowed[flowidentity.EntityID(prefix+receiver.Path)] = true
		}
	}
	rows, err := rt.DB.Query(`SELECT DISTINCT entity_id FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2`, runID, eventID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		if !allowed[id] {
			t.Fatalf("receiver execution mutated a non-state-owning receiver, source or sibling entity: %s", id)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
