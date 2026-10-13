package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

func TestForkFanOutReplyCarrierConsumesOnlyCapturedChildRow(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		t.Run(string(state), func(t *testing.T) {
			plan, child, original := runForkReplyPlanFixture(t, state, true, false)
			_, capsule := fanOutOwnershipFixture(t, ".", "existing")
			capsule.Route = flowidentity.StoredRoute(".", plan.SourceRunID, plan.SourceRunID)
			capsule.EntityID, capsule.ProducerSource = plan.SourceRunID, eventtest.RootRoutingSource(plan.SourceRunID)
			capsule.Receiver.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: plan.SourceRunID, EntityID: plan.SourceRunID})
			capsule.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: original.ID}}
			before, _ := json.Marshal(capsule)
			projected, err := projectRunForkFanOutExecutionOwnership(plan, child, capsule)
			if err != nil || projected.Context.ReplyContextID() != deterministicRunForkReplyContextID(child, original.ID) || projected.Context.ReplyContextID() == original.ID {
				t.Fatalf("fork retained the wrong reply owner: %+v error=%v", projected.Context, err)
			}
			rows, err := projectRunForkReplyContexts(plan, child)
			if err != nil || len(rows) != 1 || rows[0].ID != projected.Context.ReplyContextID() || len(rows[0].ReturnJoins) != len(original.ReturnJoins) || len(projected.Context.Joins) != 0 {
				t.Fatalf("carrier must use the child row, not copy its return admissions: %+v error=%v", rows, err)
			}
			for _, fault := range []string{"missing", "duplicate", "foreign", "consumed_join"} {
				t.Run(fault, func(t *testing.T) {
					badPlan, badCapsule := plan, capsule
					badPlan.ReplyContexts = append([]replycontext.Record(nil), plan.ReplyContexts...)
					switch fault {
					case "missing":
						badPlan.ReplyContexts = nil
					case "duplicate":
						badPlan.ReplyContexts = append(badPlan.ReplyContexts, original)
					case "foreign":
						badPlan.ReplyContexts[0].RunID = uuid.NewString()
					case "consumed_join":
						badCapsule.Context.Joins = append([]events.JoinAdmissionReceipt(nil), original.ReturnJoins...)
					}
					if _, err := projectRunForkFanOutExecutionOwnership(badPlan, child, badCapsule); err == nil {
						t.Fatal("uncaptured or consumed continuation was accepted")
					}
				})
			}
			after, _ := json.Marshal(capsule)
			if string(before) != string(after) || !reflect.DeepEqual(plan.ReplyContexts[0], original) {
				t.Fatal("projection changed original evidence")
			}
			// No reply family is required to preserve a genuinely absent input.
			capsule.Context = events.DeliveryContext{}
			plan.ReplyContexts = nil
			if got, err := projectRunForkFanOutExecutionOwnership(plan, child, capsule); err != nil || !got.Context.Empty() {
				t.Fatalf("absence invented a child return: %+v error=%v", got.Context, err)
			}
		})
	}
}
