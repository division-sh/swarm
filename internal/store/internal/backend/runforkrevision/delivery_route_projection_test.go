package runforkrevision

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

// Roundtrip tests exercise the values. This inventory also makes a newly added
// route field require an explicit historical projection decision.
func TestHistoricalDeliveryProjectionCoversEveryRouteField(t *testing.T) {
	spec, ok := canonicalProjectionSpec(FamilyEventDeliveries)
	if !ok {
		t.Fatal("missing delivery projection")
	}
	fields := map[string][]string{
		"Recipient":         {"subscriber_type", "subscriber_id"},
		"AgentIdentity":     {"agent_identity"},
		"Target":            {"delivery_target_ownership"},
		"Context":           {"delivery_context"},
		"PayloadProjection": {"delivery_payload_projection"},
		"ConnectClaim":      {"connect_execution_claim"},
		"Initialization":    {"receiver_materialization_plan"},
	}
	routeType := reflect.TypeOf(events.DeliveryRoute{})
	if routeType.NumField() != len(fields) {
		t.Fatalf("DeliveryRoute changed: audit historical projection of all %d fields (inventory has %d)", routeType.NumField(), len(fields))
	}
	values := make(map[string]any, len(spec.columns))
	kinds := make(map[string]valueKind, len(spec.columns))
	for _, column := range spec.columns {
		values[column.name] = nil
		kinds[column.name] = column.kind
	}
	projected := spec.build(values)
	for i := 0; i < routeType.NumField(); i++ {
		field := routeType.Field(i).Name
		keys, exists := fields[field]
		if !exists {
			t.Fatalf("route field %s has no audited historical projection", field)
		}
		for _, key := range keys {
			if _, exists := projected[key]; !exists {
				t.Errorf("route field %s loses historical coordinate %s", field, key)
			}
		}
	}
	for _, key := range []string{"delivery_target_ownership", "delivery_context", "delivery_payload_projection", "connect_execution_claim", "receiver_materialization_plan"} {
		if kind, exists := kinds[key]; !exists || kind != valueJSON {
			t.Errorf("%s must preserve JSON structure, got kind %d, exists %t", key, kind, exists)
		}
	}
	if !strings.Contains(spec.query, "d.connect_execution_claim") {
		t.Error("historical projection does not read persisted connect evidence")
	}
	if strings.Contains(spec.query, "claim_token") {
		t.Error("historical projection must not copy a live worker claim token")
	}
	if strings.Contains(spec.query, "d.updated_at") {
		t.Error("historical projection must not read the physical delivery update clock")
	}
	if _, exists := projected["updated_at"]; exists {
		t.Error("historical delivery fact contains the physical update clock")
	}
	if _, exists := projected["claim_version"]; exists {
		t.Error("historical delivery fact contains an unconditional live claim generation")
	}
	if !strings.Contains(spec.query, "CASE WHEN d.settled_at IS NOT NULL THEN d.claim_version END") {
		t.Error("historical projection must read claim generation only as exact settlement evidence")
	}
}

func TestHistoricalDeliveryProjectionExcludesLeaseExpiry(t *testing.T) {
	spec, ok := canonicalProjectionSpec(FamilyEventDeliveries)
	if !ok {
		t.Fatal("missing delivery projection")
	}
	if strings.Contains(spec.query, "lease_expires_at") {
		t.Error("historical delivery projection reads the live lease expiry")
	}
	values := make(map[string]any, len(spec.columns))
	for _, column := range spec.columns {
		values[column.name] = nil
	}
	if _, exists := spec.build(values)["claim_expires_at"]; exists {
		t.Error("historical delivery fact contains operational lease expiry")
	}
}

func TestHistoricalDeliveryProjectionExcludesPhysicalHandoff(t *testing.T) {
	spec, ok := canonicalProjectionSpec(FamilyEventDeliveries)
	if !ok {
		t.Fatal("missing delivery projection")
	}
	if strings.Contains(spec.query, "continuation_handoff_at") {
		t.Error("historical delivery projection reads the physical handoff stamp")
	}
	values := make(map[string]any, len(spec.columns))
	for _, column := range spec.columns {
		values[column.name] = nil
	}
	if _, exists := spec.build(values)["continuation_handoff_at"]; exists {
		t.Error("historical delivery fact contains an operational handoff stamp")
	}
}

func TestHistoricalDeliveryProjectionSeparatesRefusalAndRetryEvidence(t *testing.T) {
	spec, ok := canonicalProjectionSpec(FamilyEventDeliveries)
	if !ok {
		t.Fatal("missing delivery projection")
	}
	values := map[string]any{"status": "in_progress", "in_progress": true, "started": true, "session_bound": false,
		"retry_outcome": "retry_scheduled", "retry_claim_version": int64(2), "retry_settled_at": "settled", "retry_eligible_at": "due"}
	projected := spec.build(values)
	for _, field := range []string{"next_eligible_at", "active_session_id", "started_at", "in_progress", "started", "session_bound", "retry_outcome", "retry_claim_version", "retry_settled_at", "retry_eligible_at"} {
		if _, present := projected[field]; present {
			t.Fatalf("historical delivery retains physical/intermediate field %s", field)
		}
	}
	if got := projected["source_execution_at_cut"]; !reflect.DeepEqual(got, map[string]any{"in_progress": true, "started": true, "session_bound": false}) {
		t.Fatalf("captured refusal predicates changed: %#v", got)
	}
	if got := projected["retry_decision"]; !reflect.DeepEqual(got, map[string]any{"claim_version": int64(2), "settled_at": "settled", "eligible_at": "due"}) {
		t.Fatalf("committed retry coordinates changed: %#v", got)
	}
	if !strings.Contains(spec.source, "SELECT MAX(a.claim_version)") || !strings.Contains(spec.source, "a.closure_kind = 'settled'") || strings.Contains(spec.source, "a.outcome") {
		t.Fatal("latest settled selector must compare all outcomes, not only retry rows")
	}
	for _, column := range spec.columns {
		if column.name == "in_progress" || column.name == "started" || column.name == "session_bound" {
			if column.kind != valueBool {
				t.Fatalf("cut predicate %s is not a typed boolean", column.name)
			}
		}
	}
	for _, status := range []string{"pending", "delivered", "dead_letter", "canceled"} {
		values := map[string]any{"status": status, "retry_outcome": "retry_scheduled", "retry_claim_version": int64(2), "retry_settled_at": "settled", "retry_eligible_at": "due"}
		if got := spec.build(values)["retry_decision"]; got != nil {
			t.Fatalf("%s resurrected an older retry decision: %#v", status, got)
		}
	}
}
