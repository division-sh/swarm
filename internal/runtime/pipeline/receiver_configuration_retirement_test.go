package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func TestWorkflowHeaderHasNoBusinessConfiguration(t *testing.T) {
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
