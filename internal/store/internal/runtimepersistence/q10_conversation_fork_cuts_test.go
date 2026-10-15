package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type conversationForkCutTestOwner interface {
	forkChatCompletionTestStore
	LoadOperatorEntity(context.Context, string, string) (operatorread.OperatorEntityFull, error)
	CreateOperatorConversationFork(context.Context, runfork.ConversationForkCreateRequest) (runfork.OperatorConversationForkSession, error)
	PrepareOperatorConversationForkChat(context.Context, runfork.ConversationForkChatPrepareRequest) (runfork.ConversationForkChatPrepared, error)
	RecordOperatorConversationForkChat(context.Context, runfork.ConversationForkChatRecordRequest) (runfork.ConversationForkChatResult, error)
}

func proveQ10ConversationDataOnlyCuts(t *testing.T, ctx context.Context, owner conversationForkCutTestOwner, source conversationForkSourceFixture, stagedID string, now time.Time) {
	t.Helper()
	importedID := uuid.NewString()
	if err := seedConversationForkDataOnlyEntityForTest(ctx, owner, source.runID, importedID, source.turn1At); err != nil {
		t.Fatal(err)
	}
	current, err := owner.LoadOperatorEntity(ctx, importedID, source.runID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Entity.StageDefined != nil || current.Entity.CurrentState != nil || current.Entity.Revision != nil ||
		current.Entity.EnteredStateAt != nil || current.Entity.LifecycleCreatedAt != nil || current.Entity.LifecycleUpdatedAt != nil ||
		current.Gates != nil || current.Bookkeeping != nil || current.Accumulated != nil || current.Loops != nil || current.Fields["name"] != "After" {
		t.Fatalf("current data-only witness acquired lifecycle: %+v", current)
	}
	for index, cut := range []struct {
		name, turn, state, field string
		enteredAt                time.Time
	}{
		{"before", source.turn1ID, "draft", "Before", source.turn1At.Add(-30 * time.Second)},
		{"after", source.turn2ID, "after", "After", source.turn1At.Add(10 * time.Second)},
	} {
		t.Run("data-only-cut-"+cut.name, func(t *testing.T) {
			at := now.Add(time.Duration(index+20) * time.Second)
			fork, err := owner.CreateOperatorConversationFork(ctx, runfork.ConversationForkCreateRequest{
				SourceSessionID: source.sessionID, ForkPoint: runfork.ConversationForkPointSelector{Kind: "turn", TurnID: cut.turn},
				CreatedBy: "actor-token", Now: at,
			})
			if err != nil {
				t.Fatal(err)
			}
			message := "inspect " + cut.name + " data-only cut"
			prepared, err := owner.PrepareOperatorConversationForkChat(ctx, runfork.ConversationForkChatPrepareRequest{
				ExecutionPosture: executionposture.Live, ForkID: fork.ForkID, Message: message, Method: "conversation.fork_chat",
				ActorTokenID: "actor-token", RequestHash: runtimeeffects.Fingerprint([]byte(message)), Now: at,
			})
			if err != nil {
				t.Fatal(err)
			}
			members := make(map[string]runfork.ConversationForkEntitySnapshot)
			for _, entity := range prepared.Snapshot.EntitySnapshot {
				if _, duplicate := members[entity.EntityID]; duplicate {
					t.Fatalf("snapshot repeated entity %s", entity.EntityID)
				}
				members[entity.EntityID] = entity
			}
			if len(members) != 2 {
				t.Fatalf("historical cut changed exact membership: %+v", members)
			}
			staged, ok := members[stagedID]
			if !ok || staged.CurrentState == nil || *staged.CurrentState != cut.state || staged.EnteredStateAt == nil ||
				!staged.EnteredStateAt.Equal(cut.enteredAt) || staged.Fields["name"] != cut.field || staged.Gates["review"] != true ||
				staged.Bookkeeping["activation"] != "standing" || staged.Accumulator["total"] != float64(7) {
				t.Fatalf("constructed cut lost its exact lifecycle/domain evidence: %+v", staged)
			}
			imported, ok := members[importedID]
			if !ok || imported.CurrentState != nil || imported.EnteredStateAt != nil || imported.Gates != nil ||
				imported.Bookkeeping != nil || imported.Accumulator != nil || imported.Fields["name"] != cut.field || imported.Fields["current_state"] != "business-draft" ||
				runtimeProjectionNestedValue(t, imported.Fields, "gates", "review") != "authored-review" ||
				runtimeProjectionNestedValue(t, imported.Fields, "bookkeeping", "activation") != "manual" ||
				runtimeProjectionNestedValue(t, imported.Fields, "accumulator", "total") != "authored-total" {
				t.Fatalf("data-only cut borrowed lifecycle or lost same-spelled business fields: %+v", imported)
			}
			if _, survived := imported.Fields["removed"]; survived {
				t.Fatalf("cleared field survived at %s cut: %+v", cut.name, imported.Fields)
			}
			raw, err := json.Marshal(imported)
			if err != nil {
				t.Fatal(err)
			}
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(raw, &wire); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"current_state", "entered_state_at", "gates", "bookkeeping", "accumulator"} {
				if _, fabricated := wire[key]; fabricated {
					t.Fatalf("optional historical lifecycle property %s fabricated on the wire: %s", key, raw)
				}
			}
			settleForkChatCompletionForTest(t, ctx, owner, prepared, 1, at)
			if _, err := owner.RecordOperatorConversationForkChat(ctx, runfork.ConversationForkChatRecordRequest{
				ForkID: fork.ForkID, Message: message, ActorTokenID: "actor-token", Prepared: prepared, Now: at,
				Execution: runfork.ConversationForkChatExecution{AssistantMessage: "cut verified", AvailableTools: prepared.AvailableTools,
					ExecutionOwner: prepared.ExecutionOwner, FenceGeneration: prepared.FenceGeneration},
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
