package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

func TestIssue642RecordedActivityRetainsChildReplyBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, approved := range []bool{false, true} {
				policy := "recorded_write"
				if approved {
					policy = "approved_effect"
				}
				for _, status := range []string{"succeeded", "failed"} {
					t.Run(policy+"/"+status, func(t *testing.T) {
						child, event, original := seedActivityEvidenceForForkWithReply(t, fixture, backend.name == "postgres", approved, status, 0, false, false, true)
						ctx := testAuthorActivityContext()
						owner := fixture.store.(selectedActivityProjectionStore)
						journal := fixture.store.(activityTimestampJournal)
						replies := fixture.store.(replycontext.Store)
						sourceReply, err := replies.LoadReplyContext(ctx, original.ReplyContextID)
						if err != nil {
							t.Fatal(err)
						}
						inherited, err := ReadReplyContextRecordsForTest(ctx, fixture.store, child.ForkRunID)
						if err != nil || len(inherited) != 1 {
							t.Fatalf("read exact child reply inventory: %+v error=%v", inherited, err)
						}
						if inherited[0].ID == sourceReply.ID || inherited[0].RunID != child.ForkRunID || inherited[0].State != replycontext.StateOpen || inherited[0].SourceRunID != sourceReply.SourceRunID || inherited[0].SourceRequestEventID != sourceReply.SourceRequestEventID {
							t.Fatalf("child context lost original evidence or cut ownership: %+v", inherited[0])
						}
						loaded, err := owner.LoadRunForkSelectedContractSourceEvents(ctx, event.RunID(), child.ForkRunID, []string{event.ID()}, originalCarriageForRun(t, owner, event.RunID()))
						if err != nil || len(loaded) != 1 {
							t.Fatalf("project activity source: count=%d error=%v", len(loaded), err)
						}
						var request runForkActivityRequestPayload
						if err := json.Unmarshal(loaded[0].Payload, &request); err != nil {
							t.Fatal(err)
						}
						fact, err := activityidentity.ParseOwnerKey(request.NodeID)
						if err != nil {
							t.Fatal(err)
						}
						requestID := activityidentity.RequestEventID(activityidentity.Fact{
							RunID: child.ForkRunID, SourceEventID: request.SourceEventID, ParentEventID: request.ParentEventID,
							EntityID: request.EntityID, Owner: fact, ExecutionFlowID: request.FlowID, HandlerEventKey: request.HandlerEventKey,
							ActivityID: request.ActivityID, Tool: request.Tool, Attempt: 1,
						})
						copied, found, err := journal.LoadActivityAttempt(ctx, requestID)
						if err != nil || !found {
							t.Fatalf("read copied journal: found=%t error=%v", found, err)
						}
						if copied.ReplyContextID != inherited[0].ID {
							t.Fatalf("copied activity reply=%q, want exact own child row %q (source %q)", copied.ReplyContextID, inherited[0].ID, sourceReply.ID)
						}
						if copied.Status != original.Status || copied.InputHash != original.InputHash || !reflect.DeepEqual(copied.ResultPayload, original.ResultPayload) || !reflect.DeepEqual(copied.Failure, original.Failure) || !copied.StartedAt.Equal(original.StartedAt) || copied.CompletedAt == nil || original.CompletedAt == nil || !copied.CompletedAt.Equal(*original.CompletedAt) {
							t.Fatalf("reply projection changed frozen recorded business evidence: original=%+v copied=%+v", original, copied)
						}
						after, found, err := journal.LoadActivityAttempt(ctx, original.RequestEventID)
						if err != nil || !found || !reflect.DeepEqual(after, original) {
							t.Fatalf("projection changed original journal: found=%t error=%v", found, err)
						}
						afterReply, err := replies.LoadReplyContext(ctx, sourceReply.ID)
						if err != nil || !reflect.DeepEqual(afterReply, sourceReply) {
							t.Fatalf("projection changed original reply: %+v error=%v", afterReply, err)
						}
					})
				}
			}
		})
	}
}

func TestIssue642OrdinaryActivityReplayRetainsChildReplyBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			child, event, original := seedActivityEvidenceForForkWithReply(t, fixture, backend.name == "postgres", false, "succeeded", 0, true, false, true)
			ctx := testAuthorActivityContext()
			rows, err := ReadReplyContextRecordsForTest(ctx, fixture.store, child.ForkRunID)
			if err != nil || len(rows) != 1 || rows[0].ID == original.ReplyContextID || rows[0].SourceRunID != event.RunID() {
				t.Fatalf("ordinary materialization lost exact own context: %+v error=%v", rows, err)
			}
			owner := fixture.store.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
				ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
			})
			repeat, err := owner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: event.RunID(), At: event.ID()})
			if err != nil || repeat.ForkRunID != child.ForkRunID {
				t.Fatalf("ordinary exact repeated materialization: %+v error=%v", repeat, err)
			}
			activation, err := owner.ActivateRunFork(ctx, runfork.RunForkActivateRequest{
				ForkRunID: child.ForkRunID, AllowSourceFreeze: true,
				OriginalLoopCarriage:              originalCarriageForRun(t, fixture.store, event.RunID()),
				HistoricalReplayExecutionAdmitter: runforkexecution.HistoricalReplayExecutionAdmitter{},
			})
			if err != nil || !activation.Activated || activation.DeliveryEventReplay == nil || activation.DeliveryEventReplay.ReplayedDeliveryCount != 1 {
				t.Fatalf("ordinary admitted replay: %+v error=%v", activation, err)
			}
			executableOwner, err := activityidentity.ParseOwnerKey(original.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			requestID := activityidentity.RequestEventID(activityidentity.Fact{
				RunID: child.ForkRunID, SourceEventID: activityidentity.ForkLineageEventID(child.ForkRunID, event.ID()),
				EntityID: child.ForkRunID, Owner: executableOwner, ExecutionFlowID: ".", HandlerEventKey: original.HandlerEventKey,
				ActivityID: original.ActivityID, Tool: original.Tool, Attempt: 1,
			})
			copied, found, err := fixture.store.(activityTimestampJournal).LoadActivityAttempt(ctx, requestID)
			if err != nil || !found || copied.ReplyContextID != rows[0].ID || copied.InputHash != original.InputHash || !reflect.DeepEqual(copied.ResultPayload, original.ResultPayload) {
				t.Fatalf("ordinary replay failed to retain own reply and frozen result: %+v found=%t error=%v", copied, found, err)
			}
			source, found, err := fixture.store.(activityTimestampJournal).LoadActivityAttempt(ctx, original.RequestEventID)
			if err != nil || !found || !reflect.DeepEqual(source, original) {
				t.Fatalf("ordinary replay changed original activity: %+v found=%t error=%v", source, found, err)
			}
		})
	}
}
