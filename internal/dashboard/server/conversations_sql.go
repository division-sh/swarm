package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store"
)

type SQLConversationReader struct {
	capSource schemaCapabilitySource
	owner     *store.OperatorAgentConversationReadSurface
}

func NewSQLConversationReader(db *sql.DB, capSource schemaCapabilitySource) *SQLConversationReader {
	if db == nil {
		return nil
	}
	return &SQLConversationReader{
		capSource: capSource,
		owner:     store.NewOperatorConversationReadSurface(db, capSource),
	}
}

func (r *SQLConversationReader) ListOperatorConversations(ctx context.Context, opts store.OperatorConversationListOptions) (store.OperatorConversationListResult, error) {
	if r == nil || r.owner == nil {
		if _, err := r.resolveCapabilities(ctx); err != nil {
			return store.OperatorConversationListResult{}, err
		}
		return store.OperatorConversationListResult{}, errors.New("conversation reader is not configured")
	}
	return r.owner.ListOperatorConversations(ctx, opts)
}

func (r *SQLConversationReader) ListOperatorConversationTurns(ctx context.Context, opts store.OperatorConversationTurnListOptions) (store.OperatorConversationTurnListResult, error) {
	if r == nil || r.owner == nil {
		if _, err := r.resolveCapabilities(ctx); err != nil {
			return store.OperatorConversationTurnListResult{}, err
		}
		return store.OperatorConversationTurnListResult{}, errors.New("conversation reader is not configured with the canonical turn owner")
	}
	return r.owner.ListOperatorConversationTurns(ctx, opts)
}

func (r *SQLConversationReader) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (store.OperatorPublicConversationTurnDetail, error) {
	if r == nil || r.owner == nil {
		if _, err := r.resolveCapabilities(ctx); err != nil {
			return store.OperatorPublicConversationTurnDetail{}, err
		}
		return store.OperatorPublicConversationTurnDetail{}, errors.New("conversation reader is not configured with the canonical exact-turn owner")
	}
	return r.owner.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}

func (r *SQLConversationReader) resolveCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error) {
	if r == nil || r.capSource == nil {
		return store.StoreSchemaCapabilities{}, missingDashboardCapabilityOwner("conversation reader")
	}
	return r.capSource.ResolveSchemaCapabilities(ctx)
}

func conversationSummaryFromOperator(item store.OperatorConversationSummary) ConversationSummary {
	return ConversationSummary{
		SessionID:    strings.TrimSpace(item.SessionID),
		AgentID:      strings.TrimSpace(item.AgentID),
		Kind:         strings.TrimSpace(item.Kind),
		FlowInstance: strings.TrimSpace(item.FlowInstance),
		Memory:       item.Memory,
		MemorySource: strings.TrimSpace(item.MemorySource),
		Status:       strings.TrimSpace(item.Status),
		TurnCount:    item.TurnCount,
		Summary:      strings.TrimSpace(item.Summary),
		UpdatedAt:    formatTime(item.UpdatedAt),
		Metadata:     conversationMetadataFromOperator(item.Metadata),
	}
}

func conversationDetailFromOperator(item store.OperatorConversationTurnListResult) ConversationDetail {
	out := ConversationDetail{
		Conversation: conversationSummaryFromOperator(item.Conversation),
		NextCursor:   strings.TrimSpace(item.NextCursor),
	}
	out.Turns = make([]ConversationTurnListItem, 0, len(item.Turns))
	for _, turn := range item.Turns {
		out.Turns = append(out.Turns, conversationTurnListItemFromOperator(turn))
	}
	return out
}

func conversationMetadataFromOperator(item store.OperatorConversationSummaryMetadata) ConversationSummaryMetadata {
	return ConversationSummaryMetadata{
		ProviderSessionID:    strings.TrimSpace(item.ProviderSessionID),
		RetryReason:          strings.TrimSpace(item.RetryReason),
		RetriesFromSessionID: strings.TrimSpace(item.RetriesFromSessionID),
		Watchdog:             conversationWatchdogFromOperator(item.Watchdog),
		LiveTurn:             dashboardLiveTurn(item.LiveTurn),
	}
}

func conversationWatchdogFromOperator(item *store.OperatorConversationWatchdog) *ConversationRuntimeWatchdog {
	if item == nil {
		return nil
	}
	return &ConversationRuntimeWatchdog{
		State:         strings.TrimSpace(item.State),
		BlockingLayer: strings.TrimSpace(item.BlockingLayer),
		Action:        strings.TrimSpace(item.Action),
		Outcome:       strings.TrimSpace(item.Outcome),
		LastOutputAt:  strings.TrimSpace(item.LastOutputAt),
		RecordedAt:    strings.TrimSpace(item.RecordedAt),
	}
}

func conversationTurnListItemFromOperator(item store.OperatorConversationTurnListItem) ConversationTurnListItem {
	var tokens *ConversationTokenUsage
	if item.Tokens != nil {
		tokens = &ConversationTokenUsage{Input: item.Tokens.Input, Output: item.Tokens.Output, Exactness: item.Tokens.Exactness}
	}
	return ConversationTurnListItem{
		TurnID: item.TurnID, Ordinal: item.Ordinal, CompletedAt: formatTime(item.CompletedAt),
		DurationMS: item.DurationMS, TriggerEventID: item.TriggerEventID,
		TriggerEventType: item.TriggerEventType,
		ActivityCounts: ConversationActivityCounts{
			Dispatch: item.ActivityCounts.Dispatch, Tool: item.ActivityCounts.Tool,
			ToolResult: item.ActivityCounts.ToolResult, Publish: item.ActivityCounts.Publish,
			Output: item.ActivityCounts.Output, Failure: item.ActivityCounts.Failure,
		},
		Tokens: tokens, Outcome: item.Outcome, ParseOK: item.ParseOK,
		Failure: runtimefailures.CloneEnvelope(item.Failure),
	}
}

func readString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}
