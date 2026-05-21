package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"swarm/internal/store"
)

const (
	conversationKindLiveSession = "live_session"
	conversationKindTurnAudit   = "turn_audit"
)

type SQLConversationReader struct {
	db        *sql.DB
	capSource schemaCapabilitySource
	owner     *store.OperatorAgentConversationReadSurface
}

func NewSQLConversationReader(db *sql.DB, capSource schemaCapabilitySource) *SQLConversationReader {
	if db == nil {
		return nil
	}
	return &SQLConversationReader{
		db:        db,
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

func (r *SQLConversationReader) LoadOperatorConversation(ctx context.Context, sessionID string) (store.OperatorConversationDetail, error) {
	if r == nil || r.owner == nil {
		if _, err := r.resolveCapabilities(ctx); err != nil {
			return store.OperatorConversationDetail{}, err
		}
		return store.OperatorConversationDetail{}, store.ErrSessionNotFound
	}
	return r.owner.LoadOperatorConversation(ctx, sessionID)
}

func (r *SQLConversationReader) List(ctx context.Context, limit int) ([]ConversationSummary, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	caps, err := r.resolveCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if err := requireConversationSurfaceCapabilities(caps); err != nil {
		return nil, err
	}
	if err := requireConversationTurnCapabilities(caps); err != nil {
		return nil, err
	}
	rows, err := r.owner.ListDashboardOperatorConversations(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]ConversationSummary, 0, len(rows))
	for _, item := range rows {
		out = append(out, conversationSummaryFromOperator(item))
	}
	return out, nil
}

func (r *SQLConversationReader) Get(ctx context.Context, sessionID string) (ConversationDetail, bool, error) {
	if r == nil || r.db == nil {
		return ConversationDetail{}, false, nil
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ConversationDetail{}, false, nil
	}
	caps, err := r.resolveCapabilities(ctx)
	if err != nil {
		return ConversationDetail{}, false, err
	}
	if err := requireConversationSurfaceCapabilities(caps); err != nil {
		return ConversationDetail{}, false, err
	}
	item, ok, err := r.owner.LoadDashboardOperatorConversation(ctx, sessionID)
	if err != nil {
		if turnErr := requireConversationTurnCapabilities(caps); turnErr != nil {
			return ConversationDetail{}, false, turnErr
		}
		return ConversationDetail{}, false, err
	}
	return conversationDetailFromOperator(item), ok, nil
}

func (r *SQLConversationReader) resolveCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error) {
	if r == nil || r.capSource == nil {
		return store.StoreSchemaCapabilities{}, missingDashboardCapabilityOwner("conversation reader")
	}
	return r.capSource.ResolveSchemaCapabilities(ctx)
}

func conversationSummaryFromOperator(item store.OperatorConversationSummary) ConversationSummary {
	return ConversationSummary{
		SessionID:   strings.TrimSpace(item.SessionID),
		AgentID:     strings.TrimSpace(item.AgentID),
		Kind:        strings.TrimSpace(item.Kind),
		ScopeKey:    strings.TrimSpace(item.ScopeKey),
		Scope:       strings.TrimSpace(item.Scope),
		RuntimeMode: strings.TrimSpace(item.RuntimeMode),
		Status:      strings.TrimSpace(item.Status),
		TurnCount:   item.TurnCount,
		Summary:     strings.TrimSpace(item.Summary),
		UpdatedAt:   formatTime(item.UpdatedAt),
		Metadata:    conversationMetadataFromOperator(item.Metadata),
	}
}

func conversationDetailFromOperator(item store.OperatorConversationDetail) ConversationDetail {
	out := ConversationDetail{
		AgentID:      strings.TrimSpace(item.Conversation.AgentID),
		SessionID:    strings.TrimSpace(item.Conversation.SessionID),
		Kind:         strings.TrimSpace(item.Conversation.Kind),
		ScopeKey:     strings.TrimSpace(item.Conversation.ScopeKey),
		Scope:        strings.TrimSpace(item.Conversation.Scope),
		RuntimeMode:  strings.TrimSpace(item.Conversation.RuntimeMode),
		Status:       strings.TrimSpace(item.Conversation.Status),
		TurnCount:    item.Conversation.TurnCount,
		Summary:      strings.TrimSpace(item.Conversation.Summary),
		UpdatedAt:    formatTime(item.Conversation.UpdatedAt),
		Messages:     conversationMessagesFromOperator(item.Messages),
		RuntimeState: conversationStateFromOperator(item.RuntimeState),
	}
	out.Turns = make([]ConversationTurn, 0, len(item.Turns))
	for _, turn := range item.Turns {
		out.Turns = append(out.Turns, conversationTurnFromOperator(turn))
	}
	if out.Messages == nil {
		out.Messages = []ConversationMessage{}
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

func conversationStateFromOperator(item store.OperatorConversationState) ConversationRuntimeState {
	out := ConversationRuntimeState{
		Summary:              strings.TrimSpace(item.Summary),
		ProviderSessionID:    strings.TrimSpace(item.ProviderSessionID),
		RetryReason:          strings.TrimSpace(item.RetryReason),
		RetriesFromSessionID: strings.TrimSpace(item.RetriesFromSessionID),
		Watchdog:             conversationWatchdogFromOperator(item.Watchdog),
	}
	if item.LastTurn != nil {
		out.LastTurn = &ConversationRuntimeLastTurn{
			TaskID:  strings.TrimSpace(item.LastTurn.TaskID),
			ParseOK: item.LastTurn.ParseOK,
		}
	}
	return out
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

func conversationMessagesFromOperator(items []store.OperatorConversationMessage) []ConversationMessage {
	if len(items) == 0 {
		return []ConversationMessage{}
	}
	out := make([]ConversationMessage, 0, len(items))
	for _, item := range items {
		out = append(out, ConversationMessage{Role: strings.TrimSpace(item.Role), Content: item.Content})
	}
	return out
}

func conversationTurnFromOperator(item store.OperatorConversationTurn) ConversationTurn {
	return ConversationTurn{
		TurnIndex:              item.TurnIndex,
		TurnID:                 strings.TrimSpace(item.TurnID),
		AgentID:                strings.TrimSpace(item.AgentID),
		SessionID:              strings.TrimSpace(item.SessionID),
		RuntimeMode:            strings.TrimSpace(item.RuntimeMode),
		ScopeKey:               strings.TrimSpace(item.ScopeKey),
		EntityID:               strings.TrimSpace(item.EntityID),
		TriggerEventID:         strings.TrimSpace(item.TriggerEventID),
		TriggerEventType:       strings.TrimSpace(item.TriggerEventType),
		TaskID:                 strings.TrimSpace(item.TaskID),
		AvailableTools:         append([]string(nil), item.AvailableTools...),
		ToolCalls:              conversationToolCallsFromOperator(item.ToolCalls),
		ToolResults:            conversationToolResultsFromOperator(item.ToolResults),
		TurnBlocks:             conversationTurnBlocksFromOperator(item.TurnBlocks),
		EmittedEvents:          append([]string(nil), item.EmittedEvents...),
		MCPServers:             cloneStringMap(item.MCPServers),
		MCPToolsListed:         append([]string(nil), item.MCPToolsListed...),
		MCPToolsVisible:        append([]string(nil), item.MCPToolsVisible...),
		RequestPayload:         appendJSONRaw(item.RequestPayload),
		ResponsePayload:        appendJSONRaw(item.ResponsePayload),
		AssistantVisibleOutput: strings.TrimSpace(item.AssistantVisibleOutput),
		ReasoningBlocks:        append([]string(nil), item.ReasoningBlocks...),
		ProgressUpdates:        append([]string(nil), item.ProgressUpdates...),
		Outcome:                strings.TrimSpace(item.Outcome),
		ParseOK:                item.ParseOK,
		LatencyMS:              item.LatencyMS,
		RetryCount:             item.RetryCount,
		Error:                  strings.TrimSpace(item.Error),
		CreatedAt:              formatTime(item.CreatedAt),
	}
}

func conversationToolCallsFromOperator(items []store.OperatorConversationToolCall) []ConversationToolCall {
	if len(items) == 0 {
		return nil
	}
	out := make([]ConversationToolCall, 0, len(items))
	for _, item := range items {
		out = append(out, ConversationToolCall{Name: strings.TrimSpace(item.Name), Arguments: appendJSONRaw(item.Arguments)})
	}
	return out
}

func conversationToolResultsFromOperator(items []store.OperatorConversationToolResult) []ConversationToolResult {
	if len(items) == 0 {
		return nil
	}
	out := make([]ConversationToolResult, 0, len(items))
	for _, item := range items {
		out = append(out, ConversationToolResult{
			ToolName:  strings.TrimSpace(item.ToolName),
			ToolUseID: strings.TrimSpace(item.ToolUseID),
			Output:    appendJSONRaw(item.Output),
		})
	}
	return out
}

func conversationTurnBlocksFromOperator(items []store.OperatorConversationTurnBlock) []ConversationTurnBlock {
	if len(items) == 0 {
		return nil
	}
	out := make([]ConversationTurnBlock, 0, len(items))
	for _, item := range items {
		out = append(out, ConversationTurnBlock{
			Kind:     strings.TrimSpace(item.Kind),
			Title:    strings.TrimSpace(item.Title),
			Text:     item.Text,
			ToolName: strings.TrimSpace(item.ToolName),
			Input:    appendJSONRaw(item.Input),
			Output:   appendJSONRaw(item.Output),
			Data:     appendJSONRaw(item.Data),
		})
	}
	return out
}

func appendJSONRaw(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	return append([]byte(nil), in...)
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func readString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}
