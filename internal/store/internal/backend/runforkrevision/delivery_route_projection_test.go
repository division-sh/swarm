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
		"Materialization":   {"receiver_materialization_plan"},
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
}
