package agentpersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

func TestFlowReadinessSuccessorCannotAuthorizeIndividualRestamp(t *testing.T) {
	for _, test := range []struct {
		name, oldID, newID, run, path string
		generation                    uint64
		valid                         bool
	}{
		{name: "successor", oldID: "1", newID: "2", run: "run", path: "flow/one", generation: 2, valid: true},
		{name: "same_attempt", oldID: "1", newID: "1", run: "run", path: "flow/one", generation: 2},
		{name: "stale_attempt", oldID: "2", newID: "1", run: "run", path: "flow/one", generation: 2},
		{name: "foreign_run", oldID: "1", newID: "2", run: "foreign", path: "flow/one", generation: 2},
		{name: "foreign_instance", oldID: "1", newID: "2", run: "run", path: "flow/two", generation: 2},
		{name: "retained_token", oldID: "1", newID: "2", run: "run", path: "flow/one", generation: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			runID := uuid.NewString()
			nextRun := runID
			if test.run != "run" {
				nextRun = uuid.NewString()
			}
			old, err := agenttopology.FlowReadinessAdmission(runID, "flow/one", "old-plan")
			if err != nil {
				t.Fatal(err)
			}
			old, err = old.WithFlowActivationAttempt(test.oldID)
			if err != nil {
				t.Fatal(err)
			}
			next, err := agenttopology.FlowReadinessAdmission(nextRun, test.path, "new-plan")
			if err != nil {
				t.Fatal(err)
			}
			next, err = next.WithFlowActivationAttempt(test.newID)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := canonicaljson.Bytes(old)
			if err != nil {
				t.Fatal(err)
			}
			err = validateFlowReadinessSuccessor(manager.AgentLifecycleTransition{
				OperationKind: "source_set_rebind", Topology: next, ExpectedGeneration: 1, TargetGeneration: test.generation,
			}, lifecycleCell{Topology: raw}, true, false)
			if (err == nil) != test.valid {
				t.Fatalf("successor admission=%v want valid=%v", err, test.valid)
			}
		})
	}
}
