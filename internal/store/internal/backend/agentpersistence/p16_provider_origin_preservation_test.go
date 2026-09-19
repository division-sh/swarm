package agentpersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

// This guards the private origin interpreter shared by both selected backends.
// Authorization-origin tests do not exercise its exact terminal replay branch.
func TestP16ProviderDirectiveTerminalizationRequiresExactOrigin(t *testing.T) {
	origin := agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()}
	failure := agentcontrol.DirectiveExecutionLeaseExpiredFailure()
	for _, terminal := range []agentcontrol.DirectiveOperationState{agentcontrol.DirectiveOperationFailed, agentcontrol.DirectiveOperationIndeterminate} {
		for _, existing := range []agentcontrol.DirectiveOperationState{agentcontrol.DirectiveOperationExecuting, terminal} {
			for _, variant := range []string{"matching", "foreign_owner", "foreign_operation", "conflicting_failure", "conflicting_state"} {
				if existing == agentcontrol.DirectiveOperationExecuting && (variant == "conflicting_failure" || variant == "conflicting_state") {
					continue
				}
				t.Run(string(terminal)+"/"+string(existing)+"/"+variant, func(t *testing.T) {
					op := agentcontrol.DirectiveOperation{OperationID: origin.OperationID, ExecutionOwnerID: origin.ExecutionOwnerID, State: existing}
					if existing == terminal {
						op.Failure = failures.CloneEnvelope(&failure)
					}
					candidate, target, proposed := origin, terminal, *failures.CloneEnvelope(&failure)
					switch variant {
					case "foreign_owner":
						candidate.ExecutionOwnerID = uuid.NewString()
					case "foreign_operation":
						candidate.OperationID = uuid.NewString()
					case "conflicting_failure":
						proposed.Detail.Code = "different_terminal_cause"
					case "conflicting_state":
						target = agentcontrol.DirectiveOperationFailed
						if terminal == target {
							target = agentcontrol.DirectiveOperationIndeterminate
						}
					}
					before := op
					before.Failure = failures.CloneEnvelope(op.Failure)
					err := requireProviderDirectiveOriginOrExactTerminal(op, candidate, target, proposed)
					if (err == nil) != (variant == "matching") {
						t.Fatalf("origin/state/failure admission=%v, matching=%v", err, variant == "matching")
					}
					if !reflect.DeepEqual(op, before) {
						t.Fatal("origin admission rewrote persisted operation evidence")
					}
				})
			}
		}
	}
}
