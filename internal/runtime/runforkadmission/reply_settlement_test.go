package runforkadmission

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestIssue642CapturedReplySettlementRequiresExactAdmittedRow(t *testing.T) {
	born := time.Unix(100, 0).UTC()
	settled := born.Add(time.Second)
	row := replycontext.Record{
		ID: "opaque-reply", RunID: uuid.NewString(), RequestEventID: uuid.NewString(), SourceRequestEventID: uuid.NewString(),
		RequesterFlowID: "requester", RequestOutputPin: "request", ReplyInputPin: "return",
		ProviderFlowID: "provider", ProviderInputPin: "request", ProviderOutputPin: "return",
		Origin:               events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/one", EntityID: uuid.NewString()},
		RequestCorrelationID: "business-key", State: replycontext.StateTerminal, AcceptedReplyEventID: uuid.NewString(),
		CreatedAt: born, UpdatedAt: settled, TerminalAt: &settled,
	}
	row.SourceRunID = row.RunID
	plan := (runfork.RunForkPlan{SourceRunID: row.RunID, ForkPoint: runfork.RunForkPoint{Revision: 1}, ReplyContexts: []replycontext.Record{row}}).
		WithHistoricalEvents(1, []string{row.AcceptedReplyEventID})
	plan, err := plan.WithHistoricalReplyReturns(1, map[string]replycontext.Record{row.AcceptedReplyEventID: row})
	if err != nil {
		t.Fatal(err)
	}
	found, err := plan.HistoricalReplyReturn(row.AcceptedReplyEventID)
	if err != nil || found == nil || !reflect.DeepEqual(*found, row) {
		t.Fatalf("exact captured terminal row not consumed: %+v %v", found, err)
	}
	*found.TerminalAt = settled.Add(time.Second)
	if !plan.ReplyContexts[0].TerminalAt.Equal(settled) {
		t.Fatal("route evaluation borrowed mutable source lifecycle evidence")
	}
	for _, eventID := range []string{"", uuid.NewString(), row.RequestEventID} {
		if found, err := plan.HistoricalReplyReturn(eventID); err != nil || found != nil {
			t.Fatalf("unknown/future request predicted a reply target: %+v %v", found, err)
		}
	}
	for _, change := range []string{"duplicate", "foreign_run", "wrong_origin", "open", "no_terminal_time"} {
		t.Run(change, func(t *testing.T) {
			hostile := plan
			hostile.ReplyContexts = append([]replycontext.Record(nil), plan.ReplyContexts...)
			switch change {
			case "duplicate":
				hostile.ReplyContexts = append(hostile.ReplyContexts, row)
			case "foreign_run":
				hostile.ReplyContexts[0].RunID = uuid.NewString()
			case "wrong_origin":
				hostile.ReplyContexts[0].Origin.FlowID = "sibling"
			case "open":
				hostile.ReplyContexts[0].State = replycontext.StateOpen
			case "no_terminal_time":
				hostile.ReplyContexts[0].TerminalAt = nil
			}
			if found, err := hostile.HistoricalReplyReturn(row.AcceptedReplyEventID); err == nil || found != nil {
				t.Fatalf("contradictory captured row supplied a destination: %+v %v", found, err)
			}
		})
	}
}
