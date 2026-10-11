package runforkpersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestRunForkReplyCarrierUsesExactCapturedRow(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		t.Run(string(state), func(t *testing.T) {
			plan, child, source := runForkReplyPlanFixture(t, state, true, false)
			before := source
			got, err := projectRunForkReplyReference(plan.SourceRunID, child, plan.ReplyContexts, source.ID)
			if err != nil || got != deterministicRunForkReplyContextID(child, source.ID) || got == source.ID {
				t.Fatalf("project exact captured reference: %q error=%v", got, err)
			}
			if !reflect.DeepEqual(source, before) || !reflect.DeepEqual(plan.ReplyContexts[0], before) {
				t.Fatal("carrier projection mutated captured lifecycle")
			}
			for _, cell := range []struct {
				name string
				run  string
				rows []replycontext.Record
				id   string
			}{
				{"missing", plan.SourceRunID, nil, source.ID},
				{"wrong_id", plan.SourceRunID, plan.ReplyContexts, "other-reply"},
				{"duplicate", plan.SourceRunID, []replycontext.Record{source, source}, source.ID},
				{"foreign_run", uuid.NewString(), plan.ReplyContexts, source.ID},
				{"absent_source", "", plan.ReplyContexts, source.ID},
				{"same_run", child, plan.ReplyContexts, source.ID},
			} {
				t.Run(cell.name, func(t *testing.T) {
					if got, err := projectRunForkReplyReference(cell.run, child, cell.rows, cell.id); err == nil || got != "" {
						t.Fatalf("invalid captured reference returned child authority: %q error=%v", got, err)
					}
				})
			}
			corrupt := source
			corrupt.SourceRequestEventID = "not-an-event-id"
			if got, err := projectRunForkReplyReference(plan.SourceRunID, child, []replycontext.Record{corrupt}, source.ID); err == nil || got != "" {
				t.Fatalf("corrupt original evidence returned child reference: %q error=%v", got, err)
			}
		})
	}
	if got, err := projectRunForkReplyReference("", "", nil, ""); err != nil || got != "" {
		t.Fatalf("absent incoming reply invented a carrier: %q error=%v", got, err)
	}
}

func TestRunForkReplyProspectiveProjectionKeepsUnrelatedBlockers(t *testing.T) {
	for _, state := range []replycontext.State{replycontext.StateOpen, replycontext.StateTerminal} {
		t.Run(string(state), func(t *testing.T) {
			plan, child, _ := runForkReplyPlanFixture(t, state, true, false)
			blockers := []runfork.RunForkUnsupportedBlocker{
				{Code: runfork.RunForkBlockerOpenReplyContextUnsupported},
				{Code: runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported},
				{Code: runfork.RunForkBlockerTimerHistoryUnproven},
			}
			before := append([]runfork.RunForkUnsupportedBlocker(nil), blockers...)
			got, err := runForkReplyProjectionBlockers(plan, child, blockers)
			want := blockers
			if state == replycontext.StateOpen {
				want = blockers[1:]
			}
			if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(blockers, before) {
				t.Fatalf("prospective projection changed unrelated/source blockers: got=%+v want=%+v error=%v", got, want, err)
			}
			plan.ReplyContexts = nil
			got, err = runForkReplyProjectionBlockers(plan, child, blockers)
			if err != nil || !reflect.DeepEqual(got, before) {
				t.Fatalf("uncaptured open reply was admitted prospectively: %+v error=%v", got, err)
			}
		})
	}
}
