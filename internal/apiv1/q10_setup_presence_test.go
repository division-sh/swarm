package apiv1

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestQ10DataOnlyLifecycleInputPresenceAdmission(t *testing.T) {
	runID, entityID := uuid.NewString(), uuid.NewString()
	for _, key := range []string{"current_state", "gates", "bookkeeping", "accumulator", "revision", "entered_state_at"} {
		for index, value := range []any{nil, "", "ready", false, 0, []any{}, map[string]any{}, map[string]any{"gate": true}} {
			t.Run(fmt.Sprintf("%s/%d", key, index), func(t *testing.T) {
				input := map[string]any{"alias": "import", "entity_id": entityID, "entity_type": "task", "fields": map[string]any{}, key: value}
				if _, err := testSetupEntityRequestFromParam(input, 1, runID); err == nil {
					t.Fatal("data-only declaration admitted a lifecycle input")
				}
			})
		}
	}
	input := map[string]any{"alias": "import", "entity_id": entityID, "entity_type": "task", "fields": map[string]any{"current_state": "business"}}
	data, err := testSetupEntityRequestFromParam(input, 1, runID)
	if err != nil || data.CurrentState != nil || data.Gates != nil || data.Fields["current_state"] != "business" {
		t.Fatalf("ordinary business data lost or gained lifecycle: %+v %v", data, err)
	}
	input["entity_id"], input["current_state"], input["gates"] = runID, "ready", map[string]any{"approved": true}
	root, err := testSetupEntityRequestFromParam(input, 0, runID)
	if err != nil || root.CurrentState == nil || *root.CurrentState != "ready" || !root.Gates["approved"] {
		t.Fatalf("constructor-bound seed lost explicit initialization: %+v %v", root, err)
	}
}
