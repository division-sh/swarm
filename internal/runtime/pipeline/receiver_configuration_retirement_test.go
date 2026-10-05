package pipeline

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func TestWorkflowHeaderHasNoBusinessConfiguration(t *testing.T) {
	for _, value := range []any{WorkflowInstance{}, FlowInstanceActivationRequest{}, WorkflowInstanceRouteRecoveryProjection{}} {
		if _, present := reflect.TypeOf(value).FieldByName("Config"); present {
			t.Fatalf("business configuration carrier restored on %T", value)
		}
	}
	route := flowidentity.RouteForInstancePath("review/one")
	for _, raw := range []string{
		`{"config":{}}`, `{"config":null}`, `{"config":[]}`,
		`{"instance_id":"one","flow_path":"review/one","config":{"label":"old"}}`,
		`{"instance_id":"one","flow_path":"review/one","unknown":false}`,
	} {
		if _, err := DecodeWorkflowInstanceRecordedHeader(route, []byte(raw)); err == nil {
			t.Fatalf("obsolete or malformed header admitted: %s", raw)
		}
	}
	if _, err := DecodeWorkflowInstanceRecordedHeader(route, []byte(`{"instance_id":"one","flow_path":"review/one"}`)); err != nil {
		t.Fatal(err)
	}
}
