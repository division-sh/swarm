package serveapp

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/google/uuid"
)

func requireForkReceiverNoticeEffect(t *testing.T, rt servedControlProofRuntime, runID, sourceEvent, path, label, kind, entityID string) events.DeliveryRoute {
	t.Helper()
	if entityID != "" && entityID != flowidentity.EntityID(path) {
		t.Fatalf("receiver identity=%s, want authored static future identity %s", entityID, flowidentity.EntityID(path))
	}
	route := requireForkReceiverDelivery(t, rt, runID, sourceEvent, path, kind, entityID)
	node := identitytest.FlowNode(t, path, "collector")
	itemID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:mailbox_write:"+sourceEvent+":"+node.Key())).String()
	var gotEntity, gotFlow, scope, itemType, from, severity, summary, payload, status string
	if err := rt.DB.QueryRow(`SELECT COALESCE(CAST(entity_id AS TEXT),''),COALESCE(flow_instance,''),scope,item_type,from_agent,severity,summary,CAST(payload AS TEXT),status FROM mailbox WHERE item_id=$1 AND source_event_id=$2`, itemID, sourceEvent).Scan(&gotEntity, &gotFlow, &scope, &itemType, &from, &severity, &summary, &payload, &status); err != nil {
		t.Fatalf("exact receiver business notice: %v", err)
	}
	wantScope := "entity"
	if entityID == "" {
		wantScope = "flow"
	}
	wantPayload := map[string]any{"owner": path, "token": "receiver-proof"}
	var fields map[string]any
	if err := json.Unmarshal([]byte(payload), &fields); err != nil {
		t.Fatal(err)
	}
	if gotEntity != entityID || gotFlow != path || scope != wantScope || itemType != "receiver_processed" || from != "system_node:"+node.Key() || severity != "normal" || summary != path+" processed" || status != "pending" || !reflect.DeepEqual(fields, wantPayload) {
		t.Fatalf("receiver business notice ownership/effect: entity=%s flow=%s scope=%s type=%s from=%s severity=%s summary=%s status=%s payload=%v", gotEntity, gotFlow, scope, itemType, from, severity, summary, status, fields)
	}
	var detail struct {
		Kind   string               `json:"kind"`
		Notice mailbox.V1ItemDetail `json:"notice"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": itemID}, &detail)
	if detail.Kind != "notice" {
		t.Fatalf("public business effect kind=%q", detail.Kind)
	}
	public := detail.Notice
	if public.Item.MailboxID != itemID || public.Item.SourceEventID != sourceEvent || public.Item.SourceFlow != path || public.Item.SourceEntityID != entityID || public.Item.Type != "receiver_processed" || public.Item.Status != "pending" || !reflect.DeepEqual(public.Payload, wantPayload) {
		t.Fatalf("public receiver notice lost exact owner/effect: %+v", public)
	}
	if entityID != "" {
		row := readForkReceiverRows(t, rt, runID)[path]
		var entity operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &entity)
		if entity.Entity.RunID != runID || entity.Entity.EntityID != entityID || entity.Entity.FlowInstance != path || entity.Entity.EntityType != "receipt" || entity.Entity.CurrentState != row.State || !reflect.DeepEqual(entity.Fields, row.Fields) {
			t.Fatalf("public notice receiver state: %+v rows=%+v", entity, row)
		}
	}
	var count, emitted, deliveries int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM mailbox WHERE source_event_id=$1 AND from_agent=$2`, sourceEvent, "system_node:"+node.Key()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND source_event_id=$2 AND NOT (event_class=$3 AND event_name=$4)`, runID, sourceEvent, string(events.EventAdmissionDiagnosticDirect), string(events.EventTypePlatformRuntimeLog)).Scan(&emitted); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND e.source_event_id=$2`, runID, sourceEvent).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if count != 1 || emitted != 0 || deliveries != 0 {
		t.Fatalf("notice-only effect census: notices=%d domain events=%d downstream deliveries=%d", count, emitted, deliveries)
	}
	return route
}

func readForkReceiverNoticeDomain(t *testing.T, rt servedControlProofRuntime, runID string) []string {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT m.* FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		for i, value := range values {
			if raw, ok := value.([]byte); ok {
				values[i] = string(raw)
			}
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(raw))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}
