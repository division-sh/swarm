package runfork

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

func historicalReplyReturnFixture(t *testing.T) (RunForkPlan, string, replycontext.Record) {
	t.Helper()
	born := time.Unix(100, 0).UTC()
	settled := born.Add(time.Second)
	row := replycontext.Record{
		ID: "owned-reply", RunID: uuid.NewString(), RequestEventID: uuid.NewString(), SourceRunID: uuid.NewString(), SourceRequestEventID: uuid.NewString(),
		RequesterFlowID: "requester", RequestOutputPin: "request", ReplyInputPin: "return",
		ProviderFlowID: "provider", ProviderInputPin: "request", ProviderOutputPin: "return",
		Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/one", EntityID: uuid.NewString()},
		RequestCorrelationID: "business-key", State: replycontext.StateTerminal, AcceptedReplyEventID: uuid.NewString(),
		CreatedAt: born, UpdatedAt: settled, TerminalAt: &settled,
	}
	physical := uuid.NewString()
	plan := (RunForkPlan{SourceRunID: row.RunID, ForkPoint: RunForkPoint{Revision: 5}, ReplyContexts: []replycontext.Record{row.Normalized()}}).
		WithHistoricalEvents(5, []string{physical})
	return plan, physical, row
}

func TestIssue642HistoricalReplyReturnKeepsPhysicalAndLogicalIdentity(t *testing.T) {
	plan, physical, row := historicalReplyReturnFixture(t)
	input := map[string]replycontext.Record{physical: row}
	bound, err := plan.WithHistoricalReplyReturns(5, input)
	if err != nil {
		t.Fatal(err)
	}
	*input[physical].TerminalAt = row.UpdatedAt.Add(time.Second)
	delete(input, physical)
	got, err := bound.HistoricalReplyReturn(physical)
	if err != nil || got == nil || !reflect.DeepEqual(*got, plan.ReplyContexts[0]) || got.AcceptedReplyEventID == physical {
		t.Fatalf("exact physical return lost its own logical row: %+v error=%v", got, err)
	}
	*got.TerminalAt = got.UpdatedAt.Add(time.Second)
	again, err := bound.HistoricalReplyReturn(physical)
	if err != nil || again == nil || !reflect.DeepEqual(*again, plan.ReplyContexts[0]) {
		t.Fatalf("reader mutated retained reply evidence: %+v error=%v", again, err)
	}
	for _, unknown := range []string{"", row.AcceptedReplyEventID, row.RequestEventID, uuid.NewString()} {
		if got, err := bound.HistoricalReplyReturn(unknown); err != nil || got != nil {
			t.Fatalf("logical/future identity predicted a physical return: %+v error=%v", got, err)
		}
	}
	if got, err := plan.HistoricalReplyReturn(physical); err != nil || got != nil {
		t.Fatalf("unbound plan manufactured return evidence: %+v error=%v", got, err)
	}
}

func TestIssue642HistoricalReplyReturnRejectsChangedCutOrOwnRow(t *testing.T) {
	for _, fault := range []string{"run", "revision", "history_revision", "membership", "missing_row", "duplicate_row", "origin", "accepted", "terminal", "updated", "open", "enclosing"} {
		t.Run(fault, func(t *testing.T) {
			plan, physical, row := historicalReplyReturnFixture(t)
			bound, err := plan.WithHistoricalReplyReturns(5, map[string]replycontext.Record{physical: row})
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "run":
				bound.SourceRunID = uuid.NewString()
			case "revision":
				bound.ForkPoint.Revision++
			case "history_revision":
				bound = bound.WithHistoricalEvents(6, []string{physical})
			case "membership":
				bound = bound.WithHistoricalEvents(5, []string{uuid.NewString()})
			case "missing_row":
				bound.ReplyContexts = nil
			case "duplicate_row":
				bound.ReplyContexts = append(bound.ReplyContexts, row.Normalized())
			case "origin":
				bound.ReplyContexts[0].Origin.EntityID = uuid.NewString()
			case "accepted":
				bound.ReplyContexts[0].AcceptedReplyEventID = uuid.NewString()
			case "terminal":
				*bound.ReplyContexts[0].TerminalAt = row.UpdatedAt.Add(time.Second)
			case "updated":
				bound.ReplyContexts[0].UpdatedAt = row.UpdatedAt.Add(time.Second)
			case "open":
				bound.ReplyContexts[0].State = replycontext.StateOpen
				bound.ReplyContexts[0].AcceptedReplyEventID, bound.ReplyContexts[0].TerminalAt = "", nil
			case "enclosing":
				bound.ReplyContexts[0].EnclosingReplyContextID = "another-level"
			}
			if got, err := bound.HistoricalReplyReturn(physical); err == nil || got != nil {
				t.Fatalf("changed %s borrowed fixed return evidence: %+v error=%v", fault, got, err)
			}
		})
	}
}

func TestIssue642HistoricalReplyReturnRejectsInvalidBinding(t *testing.T) {
	for _, fault := range []string{"revision", "unbound_history", "foreign_event", "alias_event", "nil_event", "duplicate_publication", "foreign_row", "missing_row", "duplicate_row", "wrong_origin", "open_row", "clock"} {
		t.Run(fault, func(t *testing.T) {
			plan, physical, row := historicalReplyReturnFixture(t)
			revision := int64(5)
			input := map[string]replycontext.Record{physical: row.Normalized()}
			switch fault {
			case "revision":
				revision++
			case "unbound_history":
				plan.historicalRevision = 0
			case "foreign_event":
				input = map[string]replycontext.Record{uuid.NewString(): row}
			case "alias_event":
				input = map[string]replycontext.Record{strings.ToUpper(physical): row}
			case "nil_event":
				input = map[string]replycontext.Record{uuid.Nil.String(): row}
			case "duplicate_publication":
				other := uuid.NewString()
				plan = plan.WithHistoricalEvents(5, []string{physical, other})
				input[other] = row
			case "foreign_row":
				row.RunID = uuid.NewString()
				input[physical] = row
			case "missing_row":
				plan.ReplyContexts = nil
			case "duplicate_row":
				plan.ReplyContexts = append(plan.ReplyContexts, row)
			case "wrong_origin":
				row.Origin.FlowID = "sibling"
				input[physical] = row
			case "open_row":
				row.State, row.AcceptedReplyEventID, row.TerminalAt = replycontext.StateOpen, "", nil
				input[physical] = row
			case "clock":
				row.UpdatedAt = row.CreatedAt.Add(-time.Second)
				input[physical] = row
			}
			if _, err := plan.WithHistoricalReplyReturns(revision, input); err == nil {
				t.Fatal("invalid native-derived binding admitted")
			}
		})
	}
}
