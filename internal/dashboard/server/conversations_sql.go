package server

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store"
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

func (r *SQLConversationReader) ListOperatorConversationTurns(ctx context.Context, opts store.OperatorConversationTurnListOptions) (store.OperatorConversationTurnListResult, error) {
	if r == nil {
		if _, err := r.resolveCapabilities(ctx); err != nil {
			return store.OperatorConversationTurnListResult{}, err
		}
		return store.OperatorConversationTurnListResult{}, store.ErrSessionNotFound
	}
	owner, ok := r.capSource.(interface {
		ListOperatorConversationTurns(context.Context, store.OperatorConversationTurnListOptions) (store.OperatorConversationTurnListResult, error)
	})
	if !ok {
		return store.OperatorConversationTurnListResult{}, errors.New("conversation reader is not configured with the canonical turn owner")
	}
	return owner.ListOperatorConversationTurns(ctx, opts)
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
	page, err := r.owner.ListOperatorConversations(ctx, store.OperatorConversationListOptions{Limit: limit})
	if err != nil {
		return nil, err
	}
	out := make([]ConversationSummary, 0, len(page.Conversations))
	for _, item := range page.Conversations {
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
	page, err := r.ListOperatorConversationTurns(ctx, store.OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 500})
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			return ConversationDetail{}, false, nil
		}
		return ConversationDetail{}, false, err
	}
	return conversationDetailFromOperator(page), true, nil
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

func conversationDetailFromOperator(item store.OperatorConversationTurnListResult) ConversationDetail {
	out := ConversationDetail{
		Conversation: conversationSummaryFromOperator(item.Conversation),
		NextCursor:   strings.TrimSpace(item.NextCursor),
	}
	out.Turns = make([]ConversationTurn, 0, len(item.Turns))
	for _, turn := range item.Turns {
		out.Turns = append(out.Turns, conversationTurnFromOperator(turn))
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

func conversationTurnFromOperator(item store.OperatorPublicConversationTurn) ConversationTurn {
	activity := make([]ConversationActivity, 0, len(item.Activity))
	for _, fact := range item.Activity {
		activity = append(activity, ConversationActivity{
			Kind: fact.Kind, ToolName: fact.ToolName, ToolUseID: fact.ToolUseID,
			EventID: fact.EventID, EventType: fact.EventType, Text: fact.Text, OK: fact.OK,
		})
	}
	var tokens *ConversationTokenUsage
	if item.Tokens != nil {
		tokens = &ConversationTokenUsage{Input: item.Tokens.Input, Output: item.Tokens.Output, Exactness: item.Tokens.Exactness}
	}
	return ConversationTurn{
		TurnID: item.TurnID, Ordinal: item.Ordinal, CompletedAt: formatTime(item.CompletedAt),
		DurationMS: item.DurationMS, TriggerEventID: item.TriggerEventID,
		TriggerEventType: item.TriggerEventType, EntityID: item.EntityID, TaskID: item.TaskID,
		Activity: activity, Tokens: tokens, Outcome: item.Outcome, ParseOK: item.ParseOK,
		Failure: runtimefailures.CloneEnvelope(item.Failure), AssistantVisibleOutput: item.AssistantVisibleOutput,
		RetryCount: item.RetryCount,
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
