package runforkpersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/google/uuid"
)

func TestIssue642RequestOwnRowRecognitionDoesNotRequireProviderRoute(t *testing.T) {
	for _, root := range []bool{true, false} {
		t.Run(map[bool]string{true: "root", false: "nested"}[root], func(t *testing.T) {
			plan, child, source := runForkReplyPlanFixture(t, replycontext.StateTerminal, root, false)
			row, err := projectRunForkReplyContext(plan, child, source)
			if err != nil {
				t.Fatal(err)
			}
			lineage, err := events.NewSelectedForkLineage(child, plan.SourceRunID, source.RequestEventID, "selected-owner", "", executionmode.Live)
			if err != nil {
				t.Fatal(err)
			}
			name := row.RequestOutputPin
			if !root {
				name = row.Origin.FlowInstance + "/" + name
			}
			event := eventtest.SelectedForkReplay(activityidentity.ForkLineageEventID(child, source.RequestEventID), events.EventType(name),
				eventtest.Producer(events.EventProducerPlatform, "selected-owner"), "", []byte(`{"item":{"id":"item-42"}}`), 0, lineage, events.EventEnvelope{}, row.CreatedAt)
			if !capturedReplyRequestEventMatches(event, row) {
				t.Fatalf("canonical request name lost: %s", name)
			}
			snapshot := &runForkRevisionSnapshot{RunID: child, ReplyContexts: []runForkRevisionReplyContext{{runForkHistoricalReplyFact: runForkHistoricalReplyFact{Record: row}}}}
			before := projectionJSON(t, snapshot)
			rows, err := capturedProjectedRequestRows(snapshot, event, []replycontext.Record{row})
			if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0], row) {
				t.Fatalf("admitted own row lost without a provider delivery: rows=%+v error=%v", rows, err)
			}
			for _, fault := range []string{"missing", "duplicate", "foreign", "request", "accepted", "name"} {
				t.Run(fault, func(t *testing.T) {
					bad := row.Normalized()
					badSnapshot := *snapshot
					badSnapshot.ReplyContexts = append([]runForkRevisionReplyContext(nil), snapshot.ReplyContexts...)
					badEvent := event
					switch fault {
					case "missing":
						badSnapshot.ReplyContexts = nil
					case "duplicate":
						badSnapshot.ReplyContexts = append(badSnapshot.ReplyContexts, badSnapshot.ReplyContexts[0])
					case "foreign":
						bad.RunID = uuid.NewString()
					case "request":
						bad.RequestEventID = uuid.NewString()
					case "accepted":
						bad.AcceptedReplyEventID = uuid.NewString()
					case "name":
						badEvent = eventtest.SelectedForkReplay(event.ID(), events.EventType("other/request"), event.Producer(), "", event.Payload(), 0, lineage, events.EventEnvelope{}, row.CreatedAt)
					}
					if len(badSnapshot.ReplyContexts) == 1 {
						badSnapshot.ReplyContexts[0].Record = bad
					}
					if _, err := capturedProjectedRequestRows(&badSnapshot, badEvent, []replycontext.Record{row}); err == nil {
						t.Fatalf("changed %s evidence admitted", fault)
					}
				})
			}
			if before != projectionJSON(t, snapshot) {
				t.Fatal("recognition repaired source evidence")
			}
		})
	}
}
