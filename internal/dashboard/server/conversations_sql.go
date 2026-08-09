package server

import (
	"strings"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func conversationSummaryFromOperator(item operatorread.OperatorConversationSummary) ConversationSummary {
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

func conversationDetailFromOperator(item operatorread.OperatorConversationTurnListResult) ConversationDetail {
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

func conversationMetadataFromOperator(item operatorread.OperatorConversationSummaryMetadata) ConversationSummaryMetadata {
	return ConversationSummaryMetadata{
		ProviderSessionID:    strings.TrimSpace(item.ProviderSessionID),
		RetryReason:          strings.TrimSpace(item.RetryReason),
		RetriesFromSessionID: strings.TrimSpace(item.RetriesFromSessionID),
		Watchdog:             conversationWatchdogFromOperator(item.Watchdog),
		LiveTurn:             dashboardLiveTurn(item.LiveTurn),
	}
}

func conversationWatchdogFromOperator(item *operatorread.OperatorConversationWatchdog) *ConversationRuntimeWatchdog {
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

func conversationTurnListItemFromOperator(item operatorread.OperatorConversationTurnListItem) ConversationTurnListItem {
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
