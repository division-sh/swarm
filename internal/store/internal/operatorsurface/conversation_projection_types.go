package operatorsurface

import (
	"context"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type OperatorConversationTurnListOptions struct {
	SessionID string
	Limit     int
	Cursor    string
}

type OperatorConversationTurnListResult struct {
	Conversation OperatorConversationSummary        `json:"conversation"`
	Turns        []OperatorConversationTurnListItem `json:"turns"`
	NextCursor   string                             `json:"next_cursor,omitempty"`
}

type OperatorConversationTokenUsage struct {
	Input     int64  `json:"input"`
	Output    int64  `json:"output"`
	Exactness string `json:"exactness"`
}

type OperatorConversationActivity struct {
	Kind         string `json:"kind"`
	ToolName     string `json:"tool_name,omitempty"`
	ToolUseID    string `json:"tool_use_id,omitempty"`
	EventID      string `json:"event_id,omitempty"`
	EventType    string `json:"event_type,omitempty"`
	Text         string `json:"text,omitempty"`
	OK           *bool  `json:"ok,omitempty"`
	BlockOrdinal int    `json:"-"`
}

type OperatorConversationActivityCounts struct {
	Dispatch   int `json:"dispatch"`
	Tool       int `json:"tool"`
	ToolResult int `json:"tool_result"`
	Publish    int `json:"publish"`
	Output     int `json:"output"`
	Failure    int `json:"failure"`
}

type OperatorConversationTurnListItem struct {
	TurnID           string                             `json:"turn_id"`
	ExecutionMode    string                             `json:"execution_mode"`
	Ordinal          int                                `json:"ordinal"`
	CompletedAt      time.Time                          `json:"completed_at"`
	DurationMS       int                                `json:"duration_ms"`
	TriggerEventID   string                             `json:"trigger_event_id,omitempty"`
	TriggerEventType string                             `json:"trigger_event_type,omitempty"`
	ActivityCounts   OperatorConversationActivityCounts `json:"activity_counts"`
	Tokens           *OperatorConversationTokenUsage    `json:"tokens,omitempty"`
	Outcome          string                             `json:"outcome,omitempty"`
	ParseOK          bool                               `json:"parse_ok"`
	Failure          *runtimefailures.Envelope          `json:"failure,omitempty"`
}

type OperatorPublicConversationTurn struct {
	TurnID                 string                          `json:"turn_id"`
	ExecutionMode          string                          `json:"execution_mode"`
	Ordinal                int                             `json:"ordinal"`
	CompletedAt            time.Time                       `json:"completed_at"`
	DurationMS             int                             `json:"duration_ms"`
	TriggerEventID         string                          `json:"trigger_event_id,omitempty"`
	TriggerEventType       string                          `json:"trigger_event_type,omitempty"`
	EntityID               string                          `json:"entity_id,omitempty"`
	TaskID                 string                          `json:"task_id,omitempty"`
	Activity               []OperatorConversationActivity  `json:"activity"`
	Tokens                 *OperatorConversationTokenUsage `json:"tokens,omitempty"`
	Outcome                string                          `json:"outcome,omitempty"`
	ParseOK                bool                            `json:"parse_ok"`
	Failure                *runtimefailures.Envelope       `json:"failure,omitempty"`
	AssistantVisibleOutput string                          `json:"assistant_visible_output,omitempty"`
	RetryCount             int                             `json:"retry_count,omitempty"`
	AgentID                string                          `json:"-"`
	SessionID              string                          `json:"-"`
	RunID                  string                          `json:"-"`
}

type OperatorPublicConversationTurnDetail struct {
	Session OperatorConversationSummary    `json:"session"`
	Turn    OperatorPublicConversationTurn `json:"turn"`
}

type operatorPublicConversationProjectionSource interface {
	ListOperatorConversationTurns(context.Context, OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error)
	LoadOperatorPublicConversationTurn(context.Context, string, string) (OperatorPublicConversationTurnDetail, error)
}

func loadOperatorLatestConversationTurn(ctx context.Context, source operatorPublicConversationProjectionSource, sessionID string) (*OperatorPublicConversationTurn, error) {
	if source == nil || strings.TrimSpace(sessionID) == "" {
		return nil, nil
	}
	page, err := source.ListOperatorConversationTurns(ctx, OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(page.Turns) == 0 {
		return nil, nil
	}
	detail, err := source.LoadOperatorPublicConversationTurn(ctx, sessionID, page.Turns[0].TurnID)
	if err != nil {
		return nil, err
	}
	turn := detail.Turn
	return &turn, nil
}

func operatorLiveTurnFromPublic(turn *OperatorPublicConversationTurn) *OperatorLiveTurn {
	if turn == nil {
		return nil
	}
	out := &OperatorLiveTurn{
		TurnID: turn.TurnID, TaskID: turn.TaskID, ParseOK: turn.ParseOK,
		AssistantVisibleOutput: turn.AssistantVisibleOutput, Outcome: turn.Outcome,
	}
	for i := len(turn.Activity) - 1; i >= 0; i-- {
		activity := turn.Activity[i]
		if activity.Kind != "tool_result" && activity.Kind != "tool" {
			continue
		}
		ok := turn.ParseOK
		if activity.OK != nil {
			ok = *activity.OK
		}
		out.LastTool = &OperatorAgentTool{Name: activity.ToolName, ToolUseID: activity.ToolUseID, OK: ok}
		break
	}
	return out
}

func enrichOperatorProjectionWithPublicTurn(projection *operatorAgentProjection, turn *OperatorPublicConversationTurn) {
	if projection == nil || turn == nil {
		return
	}
	projection.LiveTurn = operatorLiveTurnFromPublic(turn)
	projection.LastTool = projection.LiveTurn.LastTool
	projection.CurrentTaskID = turn.TaskID
	projection.DiagnosisActive = operatorAgentDiagnosisActiveFromLatestTurn(turn.TurnID, turn.TaskID, turn.EntityID)
	projection.LastTurnRef = &OperatorTurnRef{
		TurnID: turn.TurnID, CompletedAt: turn.CompletedAt, ParseOK: turn.ParseOK,
		Failure: runtimefailures.CloneEnvelope(turn.Failure),
	}
}
