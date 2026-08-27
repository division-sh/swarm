package llmpersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimeturnactivity "github.com/division-sh/swarm/internal/runtime/turnactivity"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

type authorActivityTurn struct {
	TurnID           string
	RunID            string
	AgentID          string
	SessionID        string
	EntityID         string
	FlowID           string
	TriggerEventType string
	Blocks           []runtimellm.TurnBlock
	ParseOK          bool
	DurationMS       int
	RetryCount       int
	UsageExactness   string
	InputTokens      *int64
	OutputTokens     *int64
	ExecutionMode    string
	Failure          *runtimefailures.Envelope
	OccurredAt       time.Time
}

func authorActivityTurnDrafts(turn authorActivityTurn) ([]runtimeauthoractivity.Draft, error) {
	activity, _, _, err := runtimeturnactivity.Project(turn.Blocks, turn.ParseOK)
	if err != nil {
		return nil, err
	}
	transition := "completed"
	if turn.Failure != nil {
		transition = "failed"
	}
	parseOK := turn.ParseOK
	duration := turn.DurationMS
	retry := turn.RetryCount
	identity := strings.TrimSpace(turn.TurnID) + ":" + transition
	drafts := []runtimeauthoractivity.Draft{{
		Kind: runtimeauthoractivity.KindTurnLifecycle, Transition: transition,
		SourceOwner: "agent_turns", SourceIdentity: identity, DedupKey: "turn:" + identity,
		OccurredAt: turn.OccurredAt.UTC(), RunID: turn.RunID, EntityID: turn.EntityID, AgentID: turn.AgentID, FlowID: turn.FlowID,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "agent", SubjectID: turn.AgentID, TurnID: turn.TurnID, DurationMS: &duration,
			ParseOK: &parseOK, UsageExactness: turn.UsageExactness, InputTokens: turn.InputTokens,
			OutputTokens: turn.OutputTokens, RetryCount: &retry, EventType: turn.TriggerEventType,
			ExecutionMode: turn.ExecutionMode,
		},
		Failure: turn.Failure,
	}}
	for _, item := range activity {
		if item.Kind != "tool_result" {
			continue
		}
		ordinal := item.BlockOrdinal
		blockIdentity := fmt.Sprintf("%s:block:%d", turn.TurnID, ordinal)
		drafts = append(drafts, runtimeauthoractivity.Draft{
			Kind: runtimeauthoractivity.KindTurnToolCompleted, Transition: "completed",
			SourceOwner: "agent_turns", SourceIdentity: blockIdentity, DedupKey: "turn-tool:" + blockIdentity,
			OccurredAt: turn.OccurredAt.UTC(), RunID: turn.RunID, EntityID: turn.EntityID, AgentID: turn.AgentID, FlowID: turn.FlowID,
			Projection: runtimeauthoractivity.Projection{
				SubjectType: "agent", SubjectID: turn.AgentID, TurnID: turn.TurnID,
				ToolName: item.ToolName, ToolUseID: item.ToolUseID, ExecutionMode: turn.ExecutionMode,
			},
		})
	}
	return drafts, nil
}

func recordCompletionTurnAuthorActivity(ctx context.Context, story *privateauthoractivity.Mutation, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	if settlement.AgentTurn == nil {
		return nil
	}
	if story == nil {
		return fmt.Errorf("completion author activity owner is required")
	}
	var blocks []runtimellm.TurnBlock
	if len(settlement.AgentTurn.TurnBlocks) > 0 {
		if err := json.Unmarshal(settlement.AgentTurn.TurnBlocks, &blocks); err != nil {
			return fmt.Errorf("decode completion author activity turn blocks: %w", err)
		}
	}
	t := settlement.AgentTurn
	drafts, err := authorActivityTurnDrafts(authorActivityTurn{
		TurnID: t.TurnID, RunID: t.RunID, AgentID: t.AgentID, SessionID: t.SessionID, EntityID: t.EntityID,
		FlowID: settlement.Spend.FlowInstance, TriggerEventType: t.TriggerEventType, Blocks: blocks,
		ParseOK: t.ParseOK, DurationMS: t.LatencyMS, RetryCount: t.RetryCount,
		UsageExactness: string(settlement.Usage.Exactness), InputTokens: settlement.Usage.InputTokens,
		OutputTokens: settlement.Usage.OutputTokens, ExecutionMode: string(attempt.Authority.ExecutionMode),
		Failure: t.Failure, OccurredAt: settlement.Now.UTC(),
	})
	if err != nil {
		return err
	}
	for _, draft := range drafts {
		if err := story.Record(ctx, draft); err != nil {
			return err
		}
	}
	return nil
}

func (s *LLMPostgresOwner) RecordCompletionTurnAuthorActivityTx(ctx context.Context, story *privateauthoractivity.Mutation, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	return recordCompletionTurnAuthorActivity(ctx, story, attempt, settlement)
}

func (s *LLMSQLiteOwner) RecordCompletionTurnAuthorActivityTx(ctx context.Context, story *privateauthoractivity.Mutation, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement) error {
	return recordCompletionTurnAuthorActivity(ctx, story, attempt, settlement)
}
