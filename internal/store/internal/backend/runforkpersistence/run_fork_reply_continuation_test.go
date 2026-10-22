package runforkpersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

func TestRunForkReplyContinuingReadbackUsesOwnImmutableInheritedRows(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		for _, change := range []string{"unchanged", "settled", "wrong_run", "wrong_original", "wrong_enclosing", "wrong_origin", "early_terminal", "rewritten_terminal"} {
			t.Run(string(state)+"/"+change, func(t *testing.T) {
				plan, childRun, _ := runForkReplyPlanFixture(t, state, true, true)
				expected, err := projectRunForkReplyContext(plan, childRun, plan.ReplyContexts[0])
				if err != nil {
					t.Fatal(err)
				}
				actual := expected
				good := change == "unchanged" || state == replycontext.StateOpen && change == "settled"
				switch change {
				case "settled", "early_terminal", "rewritten_terminal":
					at := expected.UpdatedAt.Add(time.Second)
					if change == "early_terminal" {
						at = expected.CreatedAt.Add(-time.Second)
					}
					actual.State, actual.AcceptedReplyEventID = replycontext.StateTerminal, uuid.NewString()
					actual.UpdatedAt, actual.TerminalAt = at, &at
					if change == "rewritten_terminal" && state == replycontext.StateOpen {
						actual.UpdatedAt = at.Add(time.Second)
					}
					good = state == replycontext.StateOpen && change == "settled"
				case "wrong_run":
					actual.RunID, actual.ReturnJoins = uuid.NewString(), nil
				case "wrong_original":
					actual.SourceRequestEventID = uuid.NewString()
				case "wrong_enclosing":
					actual.EnclosingReplyContextID = "newer-parent"
				case "wrong_origin":
					actual.Origin.FlowInstance, actual.ReturnJoins = "sibling", nil
				}
				tx, _ := startSnapshotTransaction(t)
				owner := &runForkReplyOwnerProbe{records: map[string]replycontext.Record{expected.ID: actual}}
				admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: state == replycontext.StateOpen})
				got, err := requireContinuingRunForkReplyContexts(context.Background(), tx, owner, plan, childRun, expected.UpdatedAt, admission)
				if good {
					if err != nil || len(got.UnsupportedBlockers) != 0 {
						t.Fatalf("unchanged/once-settled inherited row refused: %+v %v", got, err)
					}
				} else if err == nil || !reflect.DeepEqual(got, admission) {
					t.Fatalf("contradictory continuation discharged or replaced source admission: %+v %v", got, err)
				}
				if len(owner.written) != 0 {
					t.Fatal("continuation readback recreated or repaired a context")
				}
			})
		}
	}
}

func TestRunForkReplyProspectiveAdmissionDoesNotAuthorizeExecution(t *testing.T) {
	plan, childRun, _ := runForkReplyPlanFixture(t, replycontext.StateOpen, true, true)
	admission := runForkReplayResumeAdmission(runForkAdmissionEvidence{OpenReplyContext: true})
	before := admission
	blockers, err := runForkSelectedMaterializationBlockers(plan, childRun, admission)
	if err != nil || len(blockers) != 0 || !reflect.DeepEqual(before, admission) || len(admission.UnsupportedBlockers) != 1 {
		t.Fatalf("prospective projection became execution authority: %+v %+v %v", blockers, admission, err)
	}
	plan.ReplyContexts[0].EnclosingReplyContextID = "missing-parent"
	if _, err := runForkSelectedMaterializationBlockers(plan, childRun, admission); err == nil {
		t.Fatal("prospective projection admitted missing enclosing evidence")
	}
}
