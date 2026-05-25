package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimesessions "swarm/internal/runtime/sessions"
)

type OperatorAgentConversationReadSource interface {
	OperatorConversationReadSource
	LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error)
	ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]PendingAgentDeliveryFacts, error)
	ListPendingAgentDeliveryDetails(ctx context.Context, opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error)
	ListAgentLifecycleFacts(ctx context.Context, agentIDs []string) (map[string]AgentLifecycleFacts, error)
}

type OperatorConversationReadSource interface {
	ResolveSchemaCapabilities(ctx context.Context) (StoreSchemaCapabilities, error)
}

type OperatorAgentConversationReadSurface struct {
	db        *sql.DB
	source    OperatorAgentConversationReadSource
	capSource OperatorConversationReadSource
	turnLimit int
}

func NewOperatorAgentConversationReadSurface(db *sql.DB, source OperatorAgentConversationReadSource, turnLimit int) *OperatorAgentConversationReadSurface {
	if db == nil || source == nil {
		return nil
	}
	return &OperatorAgentConversationReadSurface{db: db, source: source, capSource: source, turnLimit: maxStoreInt(turnLimit, 0)}
}

func NewOperatorConversationReadSurface(db *sql.DB, source OperatorConversationReadSource) *OperatorAgentConversationReadSurface {
	if db == nil || source == nil {
		return nil
	}
	return &OperatorAgentConversationReadSurface{db: db, capSource: source}
}

type OperatorAgentListOptions struct {
	Flow string
	Role string
}

type OperatorAgentListResult struct {
	Agents []OperatorAgentSummary `json:"agents"`
}

type OperatorAgentSummary struct {
	AgentID          string `json:"agent_id"`
	Role             string `json:"role"`
	Type             string `json:"type"`
	ModelTier        string `json:"model_tier"`
	ConversationMode string `json:"conversation_mode"`
	SessionScope     string `json:"session_scope"`
	Status           string `json:"status"`

	Mode                  string                              `json:"-"`
	FlowInstance          string                              `json:"-"`
	EntityID              string                              `json:"-"`
	ParentAgentID         string                              `json:"-"`
	CoordinatorID         string                              `json:"-"`
	HiredBy               string                              `json:"-"`
	TemplateVersion       string                              `json:"-"`
	BudgetEnvelope        float64                             `json:"-"`
	Subscriptions         []string                            `json:"-"`
	Permissions           []string                            `json:"-"`
	PendingEvents         int                                 `json:"-"`
	OldestPendingAgeSec   int                                 `json:"-"`
	LockOwner             string                              `json:"-"`
	LockExpiresAt         time.Time                           `json:"-"`
	Failures24h           int                                 `json:"-"`
	DeadLetters24h        int                                 `json:"-"`
	TurnCount             int                                 `json:"-"`
	TurnLimit             int                                 `json:"-"`
	NearBreaker           bool                                `json:"-"`
	SessionID             string                              `json:"-"`
	ProviderSessionID     string                              `json:"-"`
	CurrentTaskID         string                              `json:"-"`
	LastTool              *OperatorAgentTool                  `json:"-"`
	LiveTurn              *OperatorLiveTurn                   `json:"-"`
	DiagnosisActive       *OperatorAgentDiagnosisActive       `json:"-"`
	StartedAt             time.Time                           `json:"-"`
	DashboardStatus       string                              `json:"-"`
	DashboardState        string                              `json:"-"`
	DeliveryLifecycle     string                              `json:"-"`
	BlockingLayer         string                              `json:"-"`
	CurrentSessionRef     *OperatorSessionRef                 `json:"-"`
	LastTurnRef           *OperatorTurnRef                    `json:"-"`
	DiagnosisRuntimeState *OperatorAgentDiagnosisRuntimeState `json:"-"`
}

type OperatorSessionRef struct {
	SessionID string    `json:"session_id"`
	StartedAt time.Time `json:"started_at"`
}

type OperatorTurnRef struct {
	TurnID      string    `json:"turn_id"`
	CompletedAt time.Time `json:"completed_at"`
	ParseOK     bool      `json:"parse_ok"`
	Error       string    `json:"error,omitempty"`
}

type OperatorAgentDetail struct {
	Agent             OperatorAgentSummary `json:"agent"`
	CurrentSessionRef *OperatorSessionRef  `json:"current_session_ref,omitempty"`
	LastTurnRef       *OperatorTurnRef     `json:"last_turn_ref,omitempty"`
}

type OperatorAgentDiagnosis struct {
	AgentID           string                              `json:"agent_id"`
	Status            string                              `json:"status"`
	CurrentSessionRef *OperatorSessionRef                 `json:"current_session_ref,omitempty"`
	LastTurnRef       *OperatorTurnRef                    `json:"last_turn_ref,omitempty"`
	Queue             OperatorAgentDiagnosisQueue         `json:"queue"`
	DeliveryLifecycle *OperatorAgentDeliveryLifecycle     `json:"delivery_lifecycle,omitempty"`
	RuntimeState      *OperatorAgentDiagnosisRuntimeState `json:"runtime_state,omitempty"`
	Active            *OperatorAgentDiagnosisActive       `json:"active,omitempty"`
	LastToolOutcome   *OperatorAgentLastToolOutcome       `json:"last_tool_outcome,omitempty"`
}

type OperatorAgentDiagnosisActive struct {
	TurnID   string `json:"turn_id"`
	TaskID   string `json:"task_id,omitempty"`
	EntityID string `json:"entity_id,omitempty"`
}

type OperatorAgentLastToolOutcome struct {
	TurnID    string          `json:"turn_id"`
	ToolName  string          `json:"tool_name"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	OK        bool            `json:"ok"`
	Result    json.RawMessage `json:"result,omitempty"`
}

type OperatorAgentDiagnosisRuntimeState struct {
	Watchdog *OperatorAgentDiagnosisWatchdog `json:"watchdog"`
}

type OperatorAgentDiagnosisWatchdog struct {
	State         string `json:"state"`
	BlockingLayer string `json:"blocking_layer"`
	Action        string `json:"action"`
	Outcome       string `json:"outcome"`
	LastOutputAt  string `json:"last_output_at,omitempty"`
	RecordedAt    string `json:"recorded_at"`
}

type OperatorAgentDiagnosisQueue struct {
	PendingCount            int                            `json:"pending_count"`
	OldestPendingAgeSeconds int                            `json:"oldest_pending_age_seconds"`
	PendingDeliveries       []OperatorAgentPendingDelivery `json:"pending_deliveries"`
	NextCursor              string                         `json:"next_cursor,omitempty"`
}

type OperatorAgentPendingDelivery struct {
	EventID    string    `json:"event_id"`
	EventName  string    `json:"event_name"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Attempts   int       `json:"attempts"`
}

type OperatorAgentDeliveryLifecycle struct {
	State         string `json:"state"`
	BlockingLayer string `json:"blocking_layer"`
}

type OperatorAgentDiagnosisOptions struct {
	QueueLimit  int
	QueueCursor string
}

type OperatorAgentTool struct {
	Name      string          `json:"name"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	OK        bool            `json:"ok"`
	Result    json.RawMessage `json:"result,omitempty"`
}

type OperatorLiveTurn struct {
	TurnID                 string             `json:"turn_id,omitempty"`
	TaskID                 string             `json:"task_id,omitempty"`
	ParseOK                bool               `json:"parse_ok"`
	AssistantVisibleOutput string             `json:"assistant_visible_output,omitempty"`
	Outcome                string             `json:"outcome,omitempty"`
	ProgressUpdates        []string           `json:"progress_updates,omitempty"`
	LastTool               *OperatorAgentTool `json:"last_tool,omitempty"`
}

type OperatorConversationListOptions struct {
	AgentID string
	RunID   string
	Limit   int
	Cursor  string
}

type OperatorConversationListResult struct {
	Conversations []OperatorConversationSummary `json:"conversations"`
	NextCursor    string                        `json:"next_cursor,omitempty"`
}

type OperatorConversationSummary struct {
	SessionID    string     `json:"session_id"`
	AgentID      string     `json:"agent_id"`
	RunID        string     `json:"run_id,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
	TurnCount    int        `json:"turn_count"`
	MessageCount int        `json:"message_count"`
	Status       string     `json:"status"`

	Kind        string                              `json:"-"`
	ScopeKey    string                              `json:"-"`
	Scope       string                              `json:"-"`
	RuntimeMode string                              `json:"-"`
	Summary     string                              `json:"-"`
	UpdatedAt   time.Time                           `json:"-"`
	Metadata    OperatorConversationSummaryMetadata `json:"-"`
}

type OperatorConversationSummaryMetadata struct {
	ProviderSessionID    string                        `json:"provider_session_id,omitempty"`
	RetryReason          string                        `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                        `json:"retries_from_session_id,omitempty"`
	Watchdog             *OperatorConversationWatchdog `json:"watchdog,omitempty"`
	LiveTurn             *OperatorLiveTurn             `json:"live_turn,omitempty"`
}

type OperatorConversationDetail struct {
	Conversation OperatorConversationSummary `json:"conversation"`
	Turns        []OperatorConversationTurn  `json:"turns"`

	Messages     []OperatorConversationMessage `json:"-"`
	RuntimeState OperatorConversationState     `json:"-"`
}

type OperatorConversationTurnDetail struct {
	Session               OperatorConversationSummary     `json:"session"`
	Turn                  OperatorConversationDeepTurn    `json:"turn"`
	TurnBlocksRaw         []OperatorConversationTurnBlock `json:"turn_blocks_raw"`
	RuntimeLogWindowStart time.Time                       `json:"-"`
	RuntimeLogWindowEnd   *time.Time                      `json:"-"`
}

type OperatorConversationDeepTurn struct {
	TurnIndex                   int                                  `json:"turn_index"`
	TurnID                      string                               `json:"turn_id"`
	Scope                       string                               `json:"scope,omitempty"`
	StartedAt                   time.Time                            `json:"started_at"`
	CompletedAt                 time.Time                            `json:"completed_at"`
	DurationMS                  int                                  `json:"duration_ms"`
	Outcome                     string                               `json:"outcome,omitempty"`
	ParseOK                     bool                                 `json:"parse_ok"`
	Error                       string                               `json:"error,omitempty"`
	RetryCount                  int                                  `json:"retry_count,omitempty"`
	DispatchMetadata            OperatorConversationDispatchMetadata `json:"dispatch_metadata"`
	AdvertisedTools             []string                             `json:"advertised_tools"`
	MCPToolsListed              []string                             `json:"mcp_tools_listed,omitempty"`
	MCPToolsVisible             []string                             `json:"mcp_tools_visible,omitempty"`
	ReasoningBlocks             []string                             `json:"reasoning_blocks,omitempty"`
	ProgressUpdates             []string                             `json:"progress_updates,omitempty"`
	ToolCalls                   []OperatorConversationToolCall       `json:"tool_calls,omitempty"`
	ToolResults                 []OperatorConversationToolResult     `json:"tool_results,omitempty"`
	EmittedEvents               []string                             `json:"emitted_events,omitempty"`
	RuntimeLogEntries           []OperatorRuntimeLogEntry            `json:"runtime_log_entries"`
	ProviderMetadata            OperatorConversationProviderMetadata `json:"provider_metadata"`
	RequestPayload              json.RawMessage                      `json:"request_payload,omitempty"`
	ResponsePayload             json.RawMessage                      `json:"response_payload,omitempty"`
	FullPromptContext           any                                  `json:"full_prompt_context"`
	FullPromptContextV2Reserved bool                                 `json:"full_prompt_context_v2_reserved"`
	RawLLMResponse              any                                  `json:"raw_llm_response"`
	RawLLMResponseV2Reserved    bool                                 `json:"raw_llm_response_v2_reserved"`
	AssistantVisibleOutput      string                               `json:"assistant_visible_output,omitempty"`
}

type OperatorConversationDispatchMetadata struct {
	TriggerEventID   string `json:"trigger_event_id,omitempty"`
	TriggerEventType string `json:"trigger_event_type,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	TaskID           string `json:"task_id,omitempty"`
	RunID            string `json:"run_id,omitempty"`
}

type OperatorConversationProviderMetadata struct {
	LatencyMS int `json:"latency_ms"`
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneRawMessage(in json.RawMessage) json.RawMessage {
	if in == nil {
		return nil
	}
	out := make(json.RawMessage, len(in))
	copy(out, in)
	return out
}

func cloneConversationToolCalls(in []OperatorConversationToolCall) []OperatorConversationToolCall {
	if in == nil {
		return nil
	}
	out := make([]OperatorConversationToolCall, len(in))
	for i, item := range in {
		out[i] = item
		out[i].Arguments = cloneRawMessage(item.Arguments)
		out[i].Result = cloneRawMessage(item.Result)
	}
	return out
}

func cloneConversationToolResults(in []OperatorConversationToolResult) []OperatorConversationToolResult {
	if in == nil {
		return nil
	}
	out := make([]OperatorConversationToolResult, len(in))
	for i, item := range in {
		out[i] = item
		out[i].Output = cloneRawMessage(item.Output)
	}
	return out
}

func cloneConversationTurnBlocks(in []OperatorConversationTurnBlock) []OperatorConversationTurnBlock {
	if in == nil {
		return nil
	}
	out := make([]OperatorConversationTurnBlock, len(in))
	for i, item := range in {
		out[i] = item
		out[i].Input = cloneRawMessage(item.Input)
		out[i].Output = cloneRawMessage(item.Output)
		out[i].Data = cloneRawMessage(item.Data)
	}
	return out
}

type OperatorConversationMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type OperatorConversationState struct {
	Summary              string                        `json:"summary,omitempty"`
	LastTurn             *OperatorConversationLastTurn `json:"last_turn,omitempty"`
	ProviderSessionID    string                        `json:"provider_session_id,omitempty"`
	RetryReason          string                        `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                        `json:"retries_from_session_id,omitempty"`
	Watchdog             *OperatorConversationWatchdog `json:"watchdog,omitempty"`
}

type OperatorConversationLastTurn struct {
	TaskID  string `json:"task_id,omitempty"`
	ParseOK bool   `json:"parse_ok"`
}

type OperatorConversationWatchdog struct {
	State         string `json:"state,omitempty"`
	BlockingLayer string `json:"blocking_layer,omitempty"`
	Action        string `json:"action,omitempty"`
	Outcome       string `json:"outcome,omitempty"`
	LastOutputAt  string `json:"last_output_at,omitempty"`
	RecordedAt    string `json:"recorded_at,omitempty"`
}

type OperatorConversationTurn struct {
	TurnIndex        int                             `json:"turn_index"`
	TurnID           string                          `json:"turn_id"`
	TriggerEventID   string                          `json:"trigger_event_id"`
	TriggerEventType string                          `json:"trigger_event_type"`
	RequestPayload   json.RawMessage                 `json:"request_payload,omitempty"`
	ResponsePayload  json.RawMessage                 `json:"response_payload,omitempty"`
	ToolCalls        []OperatorConversationToolCall  `json:"tool_calls,omitempty"`
	TurnBlocks       []OperatorConversationTurnBlock `json:"turn_blocks,omitempty"`
	ParseOK          bool                            `json:"parse_ok"`
	LatencyMS        int                             `json:"latency_ms"`
	Error            string                          `json:"error,omitempty"`

	AgentID                string                           `json:"-"`
	SessionID              string                           `json:"-"`
	RuntimeMode            string                           `json:"-"`
	ScopeKey               string                           `json:"-"`
	EntityID               string                           `json:"-"`
	TaskID                 string                           `json:"-"`
	AvailableTools         []string                         `json:"-"`
	EmittedEvents          []string                         `json:"-"`
	MCPServers             map[string]string                `json:"-"`
	MCPToolsListed         []string                         `json:"-"`
	MCPToolsVisible        []string                         `json:"-"`
	AssistantVisibleOutput string                           `json:"-"`
	ReasoningBlocks        []string                         `json:"-"`
	ProgressUpdates        []string                         `json:"-"`
	Outcome                string                           `json:"-"`
	ToolResults            []OperatorConversationToolResult `json:"-"`
	RetryCount             int                              `json:"-"`
	CreatedAt              time.Time                        `json:"-"`
}

type OperatorConversationToolCall struct {
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
}

type OperatorConversationToolResult struct {
	ToolName  string          `json:"tool_name,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
}

type OperatorConversationTurnBlock struct {
	Kind     string          `json:"kind"`
	Title    string          `json:"title,omitempty"`
	Text     string          `json:"text,omitempty"`
	ToolName string          `json:"tool_name,omitempty"`
	Input    json.RawMessage `json:"input,omitempty"`
	Output   json.RawMessage `json:"output,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
}

type operatorAgentProjection struct {
	Status              string
	LifecycleState      string
	BlockingLayer       string
	PendingEvents       int
	OldestPendingAgeSec int
	LockOwner           string
	LockExpiresAt       time.Time
	Failures24h         int
	DeadLetters24h      int
	TurnCount           int
	SessionID           string
	SessionStartedAt    time.Time
	ProviderSessionID   string
	CurrentTaskID       string
	LastTool            *OperatorAgentTool
	LiveTurn            *OperatorLiveTurn
	DiagnosisActive     *OperatorAgentDiagnosisActive
	LastTurnRef         *OperatorTurnRef
	Watchdog            *OperatorConversationWatchdog
}

type conversationPositionCursor struct {
	Kind      string `json:"kind"`
	UpdatedAt string `json:"updated_at"`
	SessionID string `json:"session_id"`
}

type operatorRowScanner interface {
	Scan(dest ...any) error
}

func (s *PostgresStore) ListOperatorAgents(ctx context.Context, opts OperatorAgentListOptions) (OperatorAgentListResult, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).ListOperatorAgents(ctx, opts)
}

func (s *PostgresStore) LoadOperatorAgent(ctx context.Context, agentID string) (OperatorAgentDetail, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorAgent(ctx, agentID)
}

func (s *PostgresStore) LoadOperatorAgentDiagnosis(ctx context.Context, agentID string, opts OperatorAgentDiagnosisOptions) (OperatorAgentDiagnosis, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorAgentDiagnosis(ctx, agentID, opts)
}

func (s *PostgresStore) ListOperatorConversations(ctx context.Context, opts OperatorConversationListOptions) (OperatorConversationListResult, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).ListOperatorConversations(ctx, opts)
}

func (s *PostgresStore) LoadOperatorConversation(ctx context.Context, sessionID string) (OperatorConversationDetail, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorConversation(ctx, sessionID)
}

func (s *PostgresStore) LoadOperatorConversationTurn(ctx context.Context, sessionID string, turnIndex int) (OperatorConversationTurnDetail, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorConversationTurn(ctx, sessionID, turnIndex)
}

func (s *PostgresStore) LoadCurrentOperatorConversationForAgent(ctx context.Context, agentID string) (*OperatorConversationDetail, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadCurrentOperatorConversationForAgent(ctx, agentID)
}

func (r *OperatorAgentConversationReadSurface) ListOperatorAgents(ctx context.Context, opts OperatorAgentListOptions) (OperatorAgentListResult, error) {
	if err := r.requireAgentCapabilities(ctx); err != nil {
		return OperatorAgentListResult{}, err
	}
	opts.Flow = strings.Trim(strings.TrimSpace(opts.Flow), "/")
	opts.Role = strings.TrimSpace(opts.Role)
	baseRows, err := r.source.LoadAgents(ctx)
	if err != nil {
		return OperatorAgentListResult{}, err
	}
	projections, err := r.loadAgentOperatorProjections(ctx)
	if err != nil {
		return OperatorAgentListResult{}, err
	}
	agents := make([]OperatorAgentSummary, 0, len(baseRows))
	for _, row := range baseRows {
		if opts.Role != "" && strings.TrimSpace(row.Config.Role) != opts.Role {
			continue
		}
		if opts.Flow != "" && !operatorAgentFlowMatches(row.Config.CanonicalFlowPath(), opts.Flow) {
			continue
		}
		id := strings.TrimSpace(row.Config.ID)
		projection, ok := projections[id]
		if !ok {
			return OperatorAgentListResult{}, fmt.Errorf("missing agent operator projection: %s", id)
		}
		agents = append(agents, operatorAgentSummaryFromPersisted(row, projection, r.turnLimit))
	}
	if agents == nil {
		agents = []OperatorAgentSummary{}
	}
	return OperatorAgentListResult{Agents: agents}, nil
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorAgent(ctx context.Context, agentID string) (OperatorAgentDetail, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDetail{}, ErrAgentNotFound
	}
	result, err := r.ListOperatorAgents(ctx, OperatorAgentListOptions{})
	if err != nil {
		return OperatorAgentDetail{}, err
	}
	for _, agent := range result.Agents {
		if strings.TrimSpace(agent.AgentID) == agentID {
			return OperatorAgentDetail{
				Agent:             agent,
				CurrentSessionRef: agent.CurrentSessionRef,
				LastTurnRef:       agent.LastTurnRef,
			}, nil
		}
	}
	return OperatorAgentDetail{}, ErrAgentNotFound
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorAgentDiagnosis(ctx context.Context, agentID string, opts OperatorAgentDiagnosisOptions) (OperatorAgentDiagnosis, error) {
	detail, err := r.LoadOperatorAgent(ctx, agentID)
	if err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	diagnosis, err := operatorAgentDiagnosisFromDetail(detail)
	if err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	queue, err := r.source.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{
		AgentID: strings.TrimSpace(agentID),
		Limit:   opts.QueueLimit,
		Cursor:  opts.QueueCursor,
	})
	if err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	diagnosis.Queue = operatorAgentDiagnosisQueueFromPendingPage(queue)
	if err := validateOperatorAgentDiagnosis(diagnosis); err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	return diagnosis, nil
}

func (r *OperatorAgentConversationReadSurface) ListOperatorConversations(ctx context.Context, opts OperatorConversationListOptions) (OperatorConversationListResult, error) {
	if err := r.requireConversationCapabilities(ctx); err != nil {
		return OperatorConversationListResult{}, err
	}
	opts, err := defaultOperatorConversationListOptions(opts)
	if err != nil {
		return OperatorConversationListResult{}, err
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return OperatorConversationListResult{}, err
	}
	if opts.RunID != "" && !caps.Conversations.SessionRunID && !caps.Conversations.AuditRunID {
		return OperatorConversationListResult{}, operatorConversationRunIDCapabilityError("run_id filtering requires agent_sessions.run_id or agent_conversation_audits.run_id")
	}
	sources := operatorConversationQuerySources(caps)
	if len(sources) == 0 {
		return OperatorConversationListResult{Conversations: []OperatorConversationSummary{}}, nil
	}
	args := make([]any, 0, 8)
	where := []string{"TRUE"}
	add := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	if opts.AgentID != "" {
		n := add(opts.AgentID)
		where = append(where, fmt.Sprintf("conversations.agent_id = $%d", n))
	}
	if opts.RunID != "" {
		n := add(opts.RunID)
		where = append(where, fmt.Sprintf("conversations.run_id = $%d", n))
	}
	if opts.Cursor != "" {
		cursor, err := decodeConversationPositionCursor(opts.Cursor, "conversation.list")
		if err != nil {
			return OperatorConversationListResult{}, err
		}
		updatedAt, err := time.Parse(time.RFC3339Nano, cursor.UpdatedAt)
		if err != nil || strings.TrimSpace(cursor.SessionID) == "" {
			return OperatorConversationListResult{}, ErrInvalidConversationCursor
		}
		nTime := add(updatedAt.UTC())
		nSession := add(cursor.SessionID)
		where = append(where, fmt.Sprintf(`(
			conversations.updated_at < $%d
			OR (conversations.updated_at = $%d AND conversations.session_id > $%d)
		)`, nTime, nTime, nSession))
	}
	limitArg := add(opts.Limit + 1)
	latestTurnBlocksExpr := `'[]'::jsonb`
	if caps.Conversations.TurnBlocks {
		latestTurnBlocksExpr = `COALESCE(turn_blocks, '[]'::jsonb)`
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			conversations.session_id,
			conversations.agent_id,
			conversations.run_id,
			conversations.kind,
			COALESCE(conversations.scope_key, ''),
			COALESCE(conversations.scope, ''),
			COALESCE(conversations.runtime_mode, ''),
			COALESCE(conversations.status, ''),
			COALESCE(conversations.turn_count, 0),
			COALESCE(conversations.message_count, 0),
			COALESCE(conversations.runtime_state, '{}'::jsonb),
			COALESCE(latest_turn.turn_id, ''),
			COALESCE(latest_turn.task_id, ''),
			COALESCE(latest_turn.parse_ok, false),
			COALESCE(latest_turn.turn_blocks, '[]'::jsonb),
			conversations.started_at,
			conversations.ended_at,
			conversations.updated_at
		FROM (
			%s
		) conversations
		LEFT JOIN LATERAL (
			SELECT
				turn_id::text AS turn_id,
				COALESCE(task_id, '') AS task_id,
				parse_ok,
				%s AS turn_blocks
			FROM agent_turns
			WHERE agent_id = conversations.agent_id
			  AND session_id::text = conversations.session_id
			ORDER BY created_at DESC, turn_id DESC
			LIMIT 1
		) latest_turn ON true
		WHERE %s
		ORDER BY conversations.updated_at DESC, conversations.session_id ASC
		LIMIT $%d
	`, strings.Join(sources, "\nUNION ALL\n"), latestTurnBlocksExpr, strings.Join(where, " AND "), limitArg), args...)
	if err != nil {
		return OperatorConversationListResult{}, operatorConversationReadQueryError("list operator conversations", err)
	}
	defer rows.Close()
	conversations := []OperatorConversationSummary{}
	for rows.Next() {
		item, err := scanOperatorConversationSummary(rows)
		if err != nil {
			return OperatorConversationListResult{}, err
		}
		conversations = append(conversations, item)
	}
	if err := rows.Err(); err != nil {
		return OperatorConversationListResult{}, operatorConversationReadQueryError("read operator conversations", err)
	}
	nextCursor := ""
	if len(conversations) > opts.Limit {
		conversations = conversations[:opts.Limit]
		last := conversations[len(conversations)-1]
		nextCursor = encodeConversationPositionCursor(conversationPositionCursor{
			Kind:      "conversation.list",
			UpdatedAt: last.UpdatedAt.UTC().Format(time.RFC3339Nano),
			SessionID: last.SessionID,
		})
	}
	if conversations == nil {
		conversations = []OperatorConversationSummary{}
	}
	return OperatorConversationListResult{Conversations: conversations, NextCursor: nextCursor}, nil
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorConversation(ctx context.Context, sessionID string) (OperatorConversationDetail, error) {
	if err := r.requireConversationCapabilities(ctx); err != nil {
		return OperatorConversationDetail{}, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return OperatorConversationDetail{}, ErrSessionNotFound
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return OperatorConversationDetail{}, err
	}
	sources := operatorConversationQuerySources(caps)
	if len(sources) == 0 {
		return OperatorConversationDetail{}, ErrSessionNotFound
	}
	row := r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			session_id,
			agent_id,
			run_id,
			kind,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			COALESCE(message_count, 0),
			COALESCE(runtime_state, '{}'::jsonb),
			COALESCE(conversation, '[]'::jsonb),
			started_at,
			ended_at,
			updated_at
		FROM (
			%s
		) conversations
		WHERE session_id = $1
		LIMIT 1
	`, strings.Join(sources, "\nUNION ALL\n")), sessionID)
	item, err := scanOperatorConversationDetail(row)
	if err == sql.ErrNoRows {
		return OperatorConversationDetail{}, ErrSessionNotFound
	}
	if err != nil {
		return OperatorConversationDetail{}, operatorConversationReadQueryError("load operator conversation", err)
	}
	item.Turns, err = r.loadConversationTurns(ctx, item.Conversation.AgentID, item.Conversation.SessionID)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("load operator conversation turns: %w", err)
	}
	if item.Turns == nil {
		item.Turns = []OperatorConversationTurn{}
	}
	return item, nil
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorConversationTurn(ctx context.Context, sessionID string, turnIndex int) (OperatorConversationTurnDetail, error) {
	if turnIndex < 1 {
		return OperatorConversationTurnDetail{}, ErrTurnNotFound
	}
	detail, err := r.LoadOperatorConversation(ctx, sessionID)
	if err != nil {
		return OperatorConversationTurnDetail{}, err
	}
	if turnIndex > len(detail.Turns) {
		return OperatorConversationTurnDetail{}, ErrTurnNotFound
	}
	selected := detail.Turns[turnIndex-1]
	completedAt := selected.CreatedAt.UTC()
	startedAt := completedAt
	if selected.LatencyMS > 0 {
		startedAt = completedAt.Add(-time.Duration(selected.LatencyMS) * time.Millisecond)
	}
	windowStart := startedAt.Add(-time.Nanosecond)
	windowEnd := completedAt
	out := OperatorConversationTurnDetail{
		Session:               detail.Conversation,
		TurnBlocksRaw:         cloneConversationTurnBlocks(selected.TurnBlocks),
		RuntimeLogWindowStart: windowStart,
		RuntimeLogWindowEnd:   &windowEnd,
		Turn: OperatorConversationDeepTurn{
			TurnIndex:   turnIndex,
			TurnID:      selected.TurnID,
			Scope:       selected.ScopeKey,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			DurationMS:  selected.LatencyMS,
			Outcome:     selected.Outcome,
			ParseOK:     selected.ParseOK,
			Error:       selected.Error,
			RetryCount:  selected.RetryCount,
			DispatchMetadata: OperatorConversationDispatchMetadata{
				TriggerEventID:   selected.TriggerEventID,
				TriggerEventType: selected.TriggerEventType,
				EntityID:         selected.EntityID,
				TaskID:           selected.TaskID,
				RunID:            detail.Conversation.RunID,
			},
			AdvertisedTools:             cloneStrings(selected.AvailableTools),
			MCPToolsListed:              cloneStrings(selected.MCPToolsListed),
			MCPToolsVisible:             cloneStrings(selected.MCPToolsVisible),
			ReasoningBlocks:             cloneStrings(selected.ReasoningBlocks),
			ProgressUpdates:             cloneStrings(selected.ProgressUpdates),
			ToolCalls:                   cloneConversationToolCalls(selected.ToolCalls),
			ToolResults:                 cloneConversationToolResults(selected.ToolResults),
			EmittedEvents:               cloneStrings(selected.EmittedEvents),
			RuntimeLogEntries:           []OperatorRuntimeLogEntry{},
			ProviderMetadata:            OperatorConversationProviderMetadata{LatencyMS: selected.LatencyMS},
			RequestPayload:              cloneRawMessage(selected.RequestPayload),
			ResponsePayload:             cloneRawMessage(selected.ResponsePayload),
			FullPromptContext:           nil,
			FullPromptContextV2Reserved: true,
			RawLLMResponse:              nil,
			RawLLMResponseV2Reserved:    true,
			AssistantVisibleOutput:      selected.AssistantVisibleOutput,
		},
	}
	if out.Turn.AdvertisedTools == nil {
		out.Turn.AdvertisedTools = []string{}
	}
	return out, nil
}

func (r *OperatorAgentConversationReadSurface) ListDashboardOperatorConversations(ctx context.Context, limit int) ([]OperatorConversationSummary, error) {
	if err := r.requireConversationCapabilities(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	sources := dashboardOperatorConversationQuerySources(caps)
	latestTurnBlocksExpr := `'[]'::jsonb`
	if caps.Conversations.TurnBlocks {
		latestTurnBlocksExpr = `COALESCE(turn_blocks, '[]'::jsonb)`
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			conversations.session_id,
			conversations.agent_id,
			conversations.kind,
			COALESCE(conversations.scope_key, ''),
			COALESCE(conversations.scope, ''),
			COALESCE(conversations.runtime_mode, ''),
			COALESCE(conversations.status, ''),
			COALESCE(conversations.turn_count, 0),
			COALESCE(conversations.runtime_state, '{}'::jsonb),
			COALESCE(latest_turn.turn_id, ''),
			COALESCE(latest_turn.task_id, ''),
			COALESCE(latest_turn.parse_ok, false),
			COALESCE(latest_turn.turn_blocks, '[]'::jsonb),
			conversations.updated_at
		FROM (
			%s
		) conversations
		LEFT JOIN LATERAL (
			SELECT
				turn_id::text AS turn_id,
				COALESCE(task_id, '') AS task_id,
				parse_ok,
				%s AS turn_blocks
			FROM agent_turns
			WHERE agent_id = conversations.agent_id
			  AND session_id::text = conversations.session_id
			ORDER BY created_at DESC, turn_id DESC
			LIMIT 1
		) latest_turn ON true
		ORDER BY updated_at DESC, agent_id ASC
		LIMIT $1
	`, strings.Join(sources, "\nUNION ALL\n"), latestTurnBlocksExpr), limit)
	if err != nil {
		return nil, fmt.Errorf("list dashboard operator conversations: %w", err)
	}
	defer rows.Close()

	out := []OperatorConversationSummary{}
	for rows.Next() {
		item, err := scanDashboardOperatorConversationSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list dashboard operator conversations rows: %w", err)
	}
	return out, nil
}

func (r *OperatorAgentConversationReadSurface) LoadDashboardOperatorConversation(ctx context.Context, sessionID string) (OperatorConversationDetail, bool, error) {
	if err := r.requireDashboardConversationSurfaceCapabilities(ctx); err != nil {
		return OperatorConversationDetail{}, false, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return OperatorConversationDetail{}, false, nil
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return OperatorConversationDetail{}, false, err
	}
	sources := dashboardOperatorConversationQuerySources(caps)
	row := r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			session_id,
			agent_id,
			kind,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			COALESCE(runtime_state, '{}'::jsonb),
			COALESCE(conversation, '[]'::jsonb),
			updated_at
		FROM (
			%s
		) conversations
		WHERE session_id = $1
		LIMIT 1
	`, strings.Join(sources, "\nUNION ALL\n")), sessionID)

	item, err := scanDashboardOperatorConversationDetail(row)
	if err == sql.ErrNoRows {
		return OperatorConversationDetail{}, false, nil
	}
	if err != nil {
		return OperatorConversationDetail{}, false, fmt.Errorf("get dashboard operator conversation: %w", err)
	}
	item.Turns, err = r.loadConversationTurns(ctx, item.Conversation.AgentID, item.Conversation.SessionID)
	if err != nil {
		return OperatorConversationDetail{}, false, fmt.Errorf("load dashboard operator conversation turns: %w", err)
	}
	return item, true, nil
}

func (r *OperatorAgentConversationReadSurface) requireDashboardConversationSurfaceCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil || r.capSource == nil {
		return fmt.Errorf("operator conversation read surface requires postgres store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical && caps.Conversations.Audits != SchemaFlavorCanonical {
		if caps.Conversations.Audits != SchemaFlavorUnavailable {
			return unsupportedSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
		}
		return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	return nil
}

func (r *OperatorAgentConversationReadSurface) LoadCurrentOperatorConversationForAgent(ctx context.Context, agentID string) (*OperatorConversationDetail, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, ErrAgentNotFound
	}
	if _, err := r.LoadOperatorAgent(ctx, agentID); err != nil {
		return nil, err
	}
	return r.loadCurrentActiveOperatorConversationForAgent(ctx, agentID)
}

func (r *OperatorAgentConversationReadSurface) requireAgentCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil || r.source == nil {
		return fmt.Errorf("operator agent read surface requires postgres store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	return RequireCanonicalPendingAgentDeliveryCapabilities(caps)
}

func (r *OperatorAgentConversationReadSurface) requireConversationCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil || r.capSource == nil {
		return fmt.Errorf("operator conversation read surface requires postgres store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical && caps.Conversations.Audits != SchemaFlavorCanonical {
		if caps.Conversations.Audits != SchemaFlavorUnavailable {
			return unsupportedSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
		}
		return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	return nil
}

func (r *OperatorAgentConversationReadSurface) loadCurrentActiveOperatorConversationForAgent(ctx context.Context, agentID string) (*OperatorConversationDetail, error) {
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	sessionRunID := "''"
	if caps.Conversations.SessionRunID {
		sessionRunID = "COALESCE(run_id::text, '')"
	}
	row := r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			session_id::text,
			agent_id,
			%s AS run_id,
			'live_session' AS kind,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			jsonb_array_length(COALESCE(conversation, '[]'::jsonb)) AS message_count,
			COALESCE(runtime_state, '{}'::jsonb),
			COALESCE(conversation, '[]'::jsonb),
			created_at AS started_at,
			NULL::timestamptz AS ended_at,
			updated_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND status = 'active'
		  AND runtime_mode IN ($2, $3)
		ORDER BY updated_at DESC, created_at DESC, session_id ASC
		LIMIT 1
	`, sessionRunID), agentID, runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity)
	item, err := scanOperatorConversationDetail(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load current operator conversation: %w", err)
	}
	item.Turns, err = r.loadConversationTurns(ctx, item.Conversation.AgentID, item.Conversation.SessionID)
	if err != nil {
		return nil, fmt.Errorf("load current operator conversation turns: %w", err)
	}
	if item.Turns == nil {
		item.Turns = []OperatorConversationTurn{}
	}
	return &item, nil
}

func (r *OperatorAgentConversationReadSurface) resolveConversationCapabilities(ctx context.Context) (StoreSchemaCapabilities, error) {
	if r == nil || r.capSource == nil {
		return StoreSchemaCapabilities{}, fmt.Errorf("operator conversation read surface requires schema capabilities")
	}
	return r.capSource.ResolveSchemaCapabilities(ctx)
}

func (r *OperatorAgentConversationReadSurface) loadAgentOperatorProjections(ctx context.Context) (map[string]operatorAgentProjection, error) {
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	latestTurnBlocksExpr := `'[]'::jsonb`
	if caps.Conversations.TurnBlocks {
		latestTurnBlocksExpr = `COALESCE(turn_blocks, '[]'::jsonb)`
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			a.agent_id,
			COALESCE(a.status, 'active'),
			COALESCE(sess.session_id::text, ''),
			sess.created_at,
			COALESCE(sess.turn_count, 0),
			COALESCE(sess.lease_holder, ''),
			sess.lease_expires_at,
			COALESCE(sess.runtime_state, '{}'::jsonb),
			0,
			0,
			COALESCE(f.failures_24h, 0),
			COALESCE(f.dead_letters_24h, 0),
			COALESCE(latest_turn.turn_id, ''),
			COALESCE(latest_turn.task_id, ''),
			COALESCE(latest_turn.entity_id::text, ''),
			COALESCE(latest_turn.parse_ok, false),
			COALESCE(latest_turn.error, ''),
			latest_turn.created_at,
			COALESCE(latest_turn.turn_blocks, '[]'::jsonb)
		FROM agents a
		LEFT JOIN LATERAL (
			SELECT
				session_id,
				created_at,
				turn_count,
				lease_holder,
				lease_expires_at,
				runtime_state
			FROM agent_sessions
			WHERE agent_id = a.agent_id
			  AND status = 'active'
			  AND runtime_mode IN ($1, $2)
			ORDER BY updated_at DESC, created_at DESC, session_id ASC
			LIMIT 1
		) sess ON true
		LEFT JOIN LATERAL (
			SELECT
				turn_id::text AS turn_id,
				COALESCE(task_id, '') AS task_id,
				entity_id,
				parse_ok,
				COALESCE(error, '') AS error,
				created_at,
				%s AS turn_blocks
			FROM agent_turns
			WHERE agent_id = a.agent_id
			  AND sess.session_id IS NOT NULL
			  AND session_id = sess.session_id
			ORDER BY created_at DESC, turn_id DESC
			LIMIT 1
		) latest_turn ON true
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*) FILTER (WHERE status = 'failed')::int AS failures_24h,
				COUNT(*) FILTER (WHERE status = 'dead_letter')::int AS dead_letters_24h
			FROM event_deliveries
			WHERE subscriber_type = 'agent'
			  AND subscriber_id = a.agent_id
			  AND COALESCE(delivered_at, created_at) >= now() - interval '24 hours'
		) f ON true
		WHERE a.status NOT IN ('terminated', 'ephemeral')
		ORDER BY a.created_at ASC, a.agent_id ASC
	`, latestTurnBlocksExpr), runtimesessions.RuntimeModeSession, runtimesessions.RuntimeModeSessionPerEntity)
	if err != nil {
		return nil, fmt.Errorf("query agent operator projections: %w", err)
	}
	defer rows.Close()

	out := map[string]operatorAgentProjection{}
	agentIDs := make([]string, 0)
	for rows.Next() {
		var (
			id               string
			projection       operatorAgentProjection
			lockExpiresAt    sql.NullTime
			sessionStartedAt sql.NullTime
			runtimeStateRaw  []byte
			latestTurnID     string
			latestTaskID     string
			latestEntityID   string
			latestParseOK    bool
			latestError      string
			latestCompleted  sql.NullTime
			latestTurnRaw    []byte
		)
		if err := rows.Scan(
			&id,
			&projection.Status,
			&projection.SessionID,
			&sessionStartedAt,
			&projection.TurnCount,
			&projection.LockOwner,
			&lockExpiresAt,
			&runtimeStateRaw,
			&projection.PendingEvents,
			&projection.OldestPendingAgeSec,
			&projection.Failures24h,
			&projection.DeadLetters24h,
			&latestTurnID,
			&latestTaskID,
			&latestEntityID,
			&latestParseOK,
			&latestError,
			&latestCompleted,
			&latestTurnRaw,
		); err != nil {
			return nil, fmt.Errorf("scan agent operator projection: %w", err)
		}
		if sessionStartedAt.Valid {
			projection.SessionStartedAt = sessionStartedAt.Time
		}
		if lockExpiresAt.Valid {
			projection.LockExpiresAt = lockExpiresAt.Time
		}
		if latestCompleted.Valid && strings.TrimSpace(latestTurnID) != "" {
			projection.LastTurnRef = &OperatorTurnRef{
				TurnID:      strings.TrimSpace(latestTurnID),
				CompletedAt: latestCompleted.Time,
				ParseOK:     latestParseOK,
				Error:       strings.TrimSpace(latestError),
			}
		}
		if err := enrichOperatorAgentProjectionFromLatestTurn(&projection, runtimeStateRaw, latestTurnID, latestTaskID, latestParseOK, latestTurnRaw); err != nil {
			return nil, err
		}
		projection.DiagnosisActive = operatorAgentDiagnosisActiveFromLatestTurn(latestTurnID, latestTaskID, latestEntityID)
		id = strings.TrimSpace(id)
		out[id] = projection
		agentIDs = append(agentIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent operator projection rows: %w", err)
	}
	factsByAgent, err := r.source.ListPendingAgentDeliveryFacts(ctx, agentIDs, time.Time{})
	if err != nil {
		return nil, err
	}
	lifecycleByAgent, err := r.source.ListAgentLifecycleFacts(ctx, agentIDs)
	if err != nil {
		return nil, err
	}
	for agentID, facts := range factsByAgent {
		projection := out[strings.TrimSpace(agentID)]
		projection.PendingEvents = facts.PendingCount
		projection.OldestPendingAgeSec = facts.OldestPendingAgeSec
		out[strings.TrimSpace(agentID)] = projection
	}
	for agentID, facts := range lifecycleByAgent {
		projection := out[strings.TrimSpace(agentID)]
		projection.LifecycleState = strings.TrimSpace(facts.CurrentState)
		projection.BlockingLayer = strings.TrimSpace(facts.BlockingLayer)
		out[strings.TrimSpace(agentID)] = projection
	}
	return out, nil
}

func operatorAgentSummaryFromPersisted(row runtimemanager.PersistedAgent, projection operatorAgentProjection, turnLimit int) OperatorAgentSummary {
	mode := strings.TrimSpace(row.Config.ConversationMode)
	if mode == "" {
		mode = runtimesessions.RuntimeModeTask.String()
	}
	scope := strings.TrimSpace(row.Config.SessionScope)
	if scope == "" {
		scope = runtimesessions.SessionScopeGlobal.String()
	}
	out := OperatorAgentSummary{
		AgentID:               strings.TrimSpace(row.Config.ID),
		Role:                  strings.TrimSpace(row.Config.Role),
		Type:                  agentPersistedType(row.Config, agentModelTier(row.Config)),
		ModelTier:             agentModelTier(row.Config),
		ConversationMode:      mode,
		SessionScope:          scope,
		Status:                projection.v1Status(),
		Mode:                  strings.TrimSpace(row.Config.Mode),
		FlowInstance:          strings.TrimSpace(row.Config.CanonicalFlowPath()),
		EntityID:              strings.TrimSpace(row.Config.EffectiveEntityID()),
		ParentAgentID:         strings.TrimSpace(row.ParentAgentID),
		CoordinatorID:         strings.TrimSpace(row.CoordinatorID),
		HiredBy:               strings.TrimSpace(row.HiredBy),
		TemplateVersion:       strings.TrimSpace(row.TemplateVersion),
		BudgetEnvelope:        row.Config.BudgetEnvelope,
		Subscriptions:         append([]string(nil), row.Config.Subscriptions...),
		Permissions:           append([]string(nil), row.Config.Permissions...),
		PendingEvents:         projection.PendingEvents,
		OldestPendingAgeSec:   projection.OldestPendingAgeSec,
		LockOwner:             strings.TrimSpace(projection.LockOwner),
		LockExpiresAt:         projection.LockExpiresAt,
		Failures24h:           projection.Failures24h,
		DeadLetters24h:        projection.DeadLetters24h,
		TurnCount:             projection.TurnCount,
		TurnLimit:             maxStoreInt(turnLimit, 0),
		SessionID:             strings.TrimSpace(projection.SessionID),
		ProviderSessionID:     strings.TrimSpace(projection.ProviderSessionID),
		CurrentTaskID:         strings.TrimSpace(projection.CurrentTaskID),
		LastTool:              projection.LastTool,
		LiveTurn:              projection.LiveTurn,
		DiagnosisActive:       cloneOperatorAgentDiagnosisActive(projection.DiagnosisActive),
		StartedAt:             row.StartedAt,
		DashboardStatus:       strings.TrimSpace(projection.Status),
		DashboardState:        projection.dashboardState(),
		DeliveryLifecycle:     strings.TrimSpace(projection.LifecycleState),
		BlockingLayer:         projection.dashboardBlockingLayer(),
		CurrentSessionRef:     projection.currentSessionRef(),
		LastTurnRef:           projection.LastTurnRef,
		DiagnosisRuntimeState: operatorAgentDiagnosisRuntimeStateFromConversationWatchdog(projection.Watchdog),
	}
	if out.TurnLimit > 0 {
		out.NearBreaker = out.TurnCount*100 >= out.TurnLimit*85
	}
	return out
}

func operatorAgentDiagnosisFromDetail(detail OperatorAgentDetail) (OperatorAgentDiagnosis, error) {
	agent := detail.Agent
	out := OperatorAgentDiagnosis{
		AgentID:           strings.TrimSpace(agent.AgentID),
		Status:            strings.TrimSpace(agent.Status),
		CurrentSessionRef: detail.CurrentSessionRef,
		LastTurnRef:       detail.LastTurnRef,
		Queue: OperatorAgentDiagnosisQueue{
			PendingCount:            agent.PendingEvents,
			OldestPendingAgeSeconds: agent.OldestPendingAgeSec,
			PendingDeliveries:       []OperatorAgentPendingDelivery{},
		},
		RuntimeState: agent.DiagnosisRuntimeState,
		Active:       cloneOperatorAgentDiagnosisActive(agent.DiagnosisActive),
	}
	lastToolOutcome, err := operatorAgentDiagnosisLastToolOutcomeFromAgent(agent)
	if err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	out.LastToolOutcome = lastToolOutcome
	if state := strings.TrimSpace(agent.DeliveryLifecycle); state != "" {
		out.DeliveryLifecycle = &OperatorAgentDeliveryLifecycle{
			State:         state,
			BlockingLayer: strings.TrimSpace(agent.BlockingLayer),
		}
	}
	if err := validateOperatorAgentDiagnosis(out); err != nil {
		return OperatorAgentDiagnosis{}, err
	}
	return out, nil
}

func operatorAgentDiagnosisQueueFromPendingPage(page PendingAgentDeliveryPage) OperatorAgentDiagnosisQueue {
	queue := OperatorAgentDiagnosisQueue{
		PendingCount:            page.PendingCount,
		OldestPendingAgeSeconds: page.OldestPendingAgeSec,
		PendingDeliveries:       make([]OperatorAgentPendingDelivery, 0, len(page.PendingDeliveries)),
		NextCursor:              strings.TrimSpace(page.NextCursor),
	}
	for _, detail := range page.PendingDeliveries {
		queue.PendingDeliveries = append(queue.PendingDeliveries, OperatorAgentPendingDelivery{
			EventID:    strings.TrimSpace(detail.EventID),
			EventName:  strings.TrimSpace(detail.EventName),
			EnqueuedAt: detail.EnqueuedAt.UTC(),
			Attempts:   detail.Attempts,
		})
	}
	return queue
}

func validateOperatorAgentDiagnosis(item OperatorAgentDiagnosis) error {
	if strings.TrimSpace(item.AgentID) == "" {
		return fmt.Errorf("agent diagnosis agent_id is required")
	}
	if !validOperatorAgentDiagnosisStatus(item.Status) {
		return fmt.Errorf("agent diagnosis status %q is not valid", item.Status)
	}
	if item.Queue.PendingCount < 0 {
		return fmt.Errorf("agent diagnosis queue.pending_count must be non-negative")
	}
	if item.Queue.OldestPendingAgeSeconds < 0 {
		return fmt.Errorf("agent diagnosis queue.oldest_pending_age_seconds must be non-negative")
	}
	if item.Queue.PendingDeliveries == nil {
		return fmt.Errorf("agent diagnosis queue.pending_deliveries must be an array")
	}
	for i, detail := range item.Queue.PendingDeliveries {
		if strings.TrimSpace(detail.EventID) == "" {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].event_id is required", i)
		}
		if strings.TrimSpace(detail.EventName) == "" {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].event_name is required", i)
		}
		if detail.EnqueuedAt.IsZero() {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].enqueued_at is required", i)
		}
		if detail.Attempts < 0 {
			return fmt.Errorf("agent diagnosis queue.pending_deliveries[%d].attempts must be non-negative", i)
		}
	}
	if item.DeliveryLifecycle != nil {
		if !validOperatorAgentDeliveryLifecycleState(item.DeliveryLifecycle.State) {
			return fmt.Errorf("agent diagnosis delivery_lifecycle.state %q is not valid", item.DeliveryLifecycle.State)
		}
		if strings.TrimSpace(item.DeliveryLifecycle.BlockingLayer) == "" {
			return fmt.Errorf("agent diagnosis delivery_lifecycle.blocking_layer is required")
		}
	}
	if err := validateOperatorAgentDiagnosisActive(item.Active); err != nil {
		return err
	}
	if err := validateOperatorAgentDiagnosisRuntimeState(item.RuntimeState); err != nil {
		return err
	}
	if err := validateOperatorAgentDiagnosisLastToolOutcome(item.LastToolOutcome); err != nil {
		return err
	}
	if item.LastToolOutcome != nil {
		if item.Active == nil {
			return fmt.Errorf("agent diagnosis last_tool_outcome requires active selected-turn evidence")
		}
		activeTurnID := strings.TrimSpace(item.Active.TurnID)
		lastToolTurnID := strings.TrimSpace(item.LastToolOutcome.TurnID)
		if activeTurnID != lastToolTurnID {
			return fmt.Errorf("agent diagnosis last_tool_outcome.turn_id %q must match active.turn_id %q", lastToolTurnID, activeTurnID)
		}
	}
	return nil
}

func validateOperatorAgentDiagnosisActive(item *OperatorAgentDiagnosisActive) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent diagnosis active.turn_id is required")
	}
	return nil
}

func validateOperatorAgentDiagnosisRuntimeState(item *OperatorAgentDiagnosisRuntimeState) error {
	if item == nil {
		return nil
	}
	if item.Watchdog == nil {
		return fmt.Errorf("agent diagnosis runtime_state.watchdog is required")
	}
	if err := ValidateConversationRuntimeWatchdogDescriptor(conversationWatchdogDescriptorFromAgentDiagnosis(*item.Watchdog)); err != nil {
		return fmt.Errorf("agent diagnosis runtime_state.watchdog is invalid: %w", err)
	}
	return nil
}

func operatorAgentDiagnosisActiveFromLatestTurn(turnID, taskID, entityID string) *OperatorAgentDiagnosisActive {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return nil
	}
	return &OperatorAgentDiagnosisActive{
		TurnID:   turnID,
		TaskID:   strings.TrimSpace(taskID),
		EntityID: strings.TrimSpace(entityID),
	}
}

func cloneOperatorAgentDiagnosisActive(in *OperatorAgentDiagnosisActive) *OperatorAgentDiagnosisActive {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func operatorAgentDiagnosisLastToolOutcomeFromAgent(agent OperatorAgentSummary) (*OperatorAgentLastToolOutcome, error) {
	if agent.DiagnosisActive == nil {
		return nil, nil
	}
	turnID := strings.TrimSpace(agent.DiagnosisActive.TurnID)
	if turnID == "" {
		return nil, nil
	}
	if agent.LiveTurn == nil || agent.LiveTurn.LastTool == nil {
		return nil, nil
	}
	if liveTurnID := strings.TrimSpace(agent.LiveTurn.TurnID); liveTurnID != "" && liveTurnID != turnID {
		return nil, fmt.Errorf("agent diagnosis last_tool_outcome turn_id %q does not match active turn_id %q", liveTurnID, turnID)
	}
	last := agent.LiveTurn.LastTool
	out := &OperatorAgentLastToolOutcome{
		TurnID:    turnID,
		ToolName:  strings.TrimSpace(last.Name),
		ToolUseID: strings.TrimSpace(last.ToolUseID),
		OK:        last.OK,
	}
	if last.Result != nil {
		trimmed := bytes.TrimSpace(last.Result)
		if len(trimmed) == 0 {
			return nil, fmt.Errorf("agent diagnosis last_tool_outcome.result is empty")
		}
		var obj map[string]any
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return nil, fmt.Errorf("agent diagnosis last_tool_outcome.result must be a JSON object: %w", err)
		}
		if obj == nil {
			return nil, fmt.Errorf("agent diagnosis last_tool_outcome.result must be a JSON object")
		}
		out.Result = append(json.RawMessage(nil), trimmed...)
	}
	if err := validateOperatorAgentDiagnosisLastToolOutcome(out); err != nil {
		return nil, err
	}
	return out, nil
}

func validateOperatorAgentDiagnosisLastToolOutcome(item *OperatorAgentLastToolOutcome) error {
	if item == nil {
		return nil
	}
	if strings.TrimSpace(item.TurnID) == "" {
		return fmt.Errorf("agent diagnosis last_tool_outcome.turn_id is required")
	}
	if strings.TrimSpace(item.ToolName) == "" {
		return fmt.Errorf("agent diagnosis last_tool_outcome.tool_name is required")
	}
	if item.Result != nil {
		trimmed := bytes.TrimSpace(item.Result)
		if len(trimmed) == 0 {
			return fmt.Errorf("agent diagnosis last_tool_outcome.result is empty")
		}
		var obj map[string]any
		if err := json.Unmarshal(trimmed, &obj); err != nil {
			return fmt.Errorf("agent diagnosis last_tool_outcome.result must be a JSON object: %w", err)
		}
		if obj == nil {
			return fmt.Errorf("agent diagnosis last_tool_outcome.result must be a JSON object")
		}
	}
	return nil
}

func validOperatorAgentDiagnosisStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "idle", "running", "paused", "failed", "terminated":
		return true
	default:
		return false
	}
}

func validOperatorAgentDeliveryLifecycleState(state string) bool {
	switch strings.TrimSpace(state) {
	case "queued", "launching", "active", "retrying", "exhausted":
		return true
	default:
		return false
	}
}

func (p operatorAgentProjection) currentSessionRef() *OperatorSessionRef {
	if strings.TrimSpace(p.SessionID) == "" || p.SessionStartedAt.IsZero() {
		return nil
	}
	return &OperatorSessionRef{SessionID: strings.TrimSpace(p.SessionID), StartedAt: p.SessionStartedAt}
}

func (p operatorAgentProjection) dashboardState() string {
	status := strings.ToLower(strings.TrimSpace(p.Status))
	if status == "terminated" {
		return "terminated"
	}
	if state := strings.TrimSpace(p.LifecycleState); state != "" {
		return state
	}
	return "idle"
}

func (p operatorAgentProjection) dashboardBlockingLayer() string {
	if layer := strings.TrimSpace(p.BlockingLayer); layer != "" {
		return layer
	}
	return ""
}

func (p operatorAgentProjection) v1Status() string {
	switch strings.TrimSpace(p.Status) {
	case "terminated":
		return "terminated"
	case "paused":
		return "paused"
	}
	switch strings.TrimSpace(p.LifecycleState) {
	case "active", "launching", "retrying":
		return "running"
	case "exhausted":
		return "failed"
	}
	return "idle"
}

func enrichOperatorAgentProjectionFromLatestTurn(projection *operatorAgentProjection, runtimeStateRaw []byte, turnID, taskID string, parseOK bool, turnBlocksRaw []byte) error {
	if projection == nil {
		return nil
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return fmt.Errorf("decode latest agent session runtime_state: %w", err)
	}
	projection.ProviderSessionID = strings.TrimSpace(runtimeState.ProviderSessionID)
	projection.Watchdog = operatorConversationWatchdogFromDescriptor(runtimeState.Watchdog)
	projection.LiveTurn, err = projectOperatorLatestTurn(taskID, parseOK, turnID, turnBlocksRaw)
	if err != nil {
		return fmt.Errorf("decode latest agent turn live_turn: %w", err)
	}
	if projection.LiveTurn != nil {
		projection.CurrentTaskID = strings.TrimSpace(projection.LiveTurn.TaskID)
		projection.LastTool = projection.LiveTurn.LastTool
		return nil
	}
	projection.CurrentTaskID = strings.TrimSpace(taskID)
	return nil
}

func operatorAgentFlowMatches(agentFlow, filter string) bool {
	agentFlow = strings.Trim(strings.TrimSpace(agentFlow), "/")
	filter = strings.Trim(strings.TrimSpace(filter), "/")
	return filter == "" || agentFlow == filter || strings.HasPrefix(agentFlow, filter+"/")
}

func defaultOperatorConversationListOptions(opts OperatorConversationListOptions) (OperatorConversationListOptions, error) {
	opts.AgentID = strings.TrimSpace(opts.AgentID)
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.RunID != "" {
		if _, err := uuid.Parse(opts.RunID); err != nil {
			return OperatorConversationListOptions{}, fmt.Errorf("%w: run_id must be a UUID", ErrInvalidEntityReadParam)
		}
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts, nil
}

func operatorConversationRunIDCapabilityError(reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return ErrOperatorConversationRunIDCapability
	}
	return fmt.Errorf("%w: %s", ErrOperatorConversationRunIDCapability, reason)
}

func operatorConversationReadQueryError(operation string, err error) error {
	if err == nil {
		return nil
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		operation = "operator conversation read"
	}
	if operatorConversationRunIDProjectionError(err) {
		return fmt.Errorf("%s: %w", operation, operatorConversationRunIDCapabilityError("selected conversation source cannot project run_id"))
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func operatorConversationRunIDProjectionError(err error) bool {
	if err == nil {
		return false
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr != nil {
		if string(pqErr.Code) == "42703" && (strings.EqualFold(pqErr.Column, "run_id") || strings.Contains(strings.ToLower(pqErr.Message), "run_id")) {
			return true
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "run_id") &&
		(strings.Contains(message, `column "run_id" does not exist`) || strings.Contains(message, "42703"))
}

func operatorConversationQuerySources(caps StoreSchemaCapabilities) []string {
	sources := []string{}
	if caps.Conversations.Sessions == SchemaFlavorCanonical {
		sessionRunID := "''"
		if caps.Conversations.SessionRunID {
			sessionRunID = "COALESCE(run_id::text, '')"
		}
		sources = append(sources, fmt.Sprintf(`
			SELECT
				session_id::text AS session_id,
				agent_id,
				%s AS run_id,
				'live_session' AS kind,
				scope_key,
				scope,
				runtime_mode,
				CASE WHEN status = 'terminated' THEN 'terminated' ELSE 'active' END AS status,
				turn_count,
				jsonb_array_length(COALESCE(conversation, '[]'::jsonb)) AS message_count,
				runtime_state,
				conversation,
				created_at AS started_at,
				CASE WHEN status = 'terminated' THEN terminated_at ELSE NULL END AS ended_at,
				updated_at,
				created_at
			FROM agent_sessions
			WHERE status IN ('active', 'terminated')
			  AND runtime_mode IN ('session', 'session_per_entity')
		`, sessionRunID))
	}
	if caps.Conversations.Audits == SchemaFlavorCanonical {
		auditRunID := "''"
		if caps.Conversations.AuditRunID {
			auditRunID = "COALESCE(run_id::text, '')"
		}
		sources = append(sources, fmt.Sprintf(`
			SELECT
				session_id::text AS session_id,
				agent_id,
				%s AS run_id,
				'turn_audit' AS kind,
				COALESCE(scope_key, '') AS scope_key,
				COALESCE(scope, '') AS scope,
				COALESCE(runtime_mode, '') AS runtime_mode,
				CASE WHEN status = 'terminated' THEN 'terminated' ELSE 'active' END AS status,
				COALESCE(turn_count, 0) AS turn_count,
				jsonb_array_length(COALESCE(conversation, '[]'::jsonb)) AS message_count,
				COALESCE(runtime_state, '{}'::jsonb) AS runtime_state,
				COALESCE(conversation, '[]'::jsonb) AS conversation,
				created_at AS started_at,
				NULL::timestamptz AS ended_at,
				updated_at,
				created_at
			FROM (
				%s
			) task_conversations
		`, auditRunID, CanonicalTaskConversationVisibilitySourceSQL(caps.Conversations)))
	}
	return sources
}

func dashboardOperatorConversationQuerySources(caps StoreSchemaCapabilities) []string {
	sources := []string{}
	if caps.Conversations.Sessions == SchemaFlavorCanonical {
		sources = append(sources, `
			SELECT
				session_id::text AS session_id,
				agent_id,
				'live_session' AS kind,
				scope_key,
				scope,
				runtime_mode,
				status,
				turn_count,
				runtime_state,
				conversation,
				updated_at,
				created_at
			FROM agent_sessions
			WHERE status = 'active'
			  AND runtime_mode IN ('session', 'session_per_entity')
		`)
	}
	if taskSource := CanonicalTaskConversationVisibilitySourceSQL(caps.Conversations); taskSource != "" {
		sources = append(sources, fmt.Sprintf(`
			SELECT
				session_id,
				agent_id,
				'turn_audit' AS kind,
				scope_key,
				scope,
				runtime_mode,
				status,
				turn_count,
				runtime_state,
				conversation,
				updated_at,
				created_at
			FROM (
				%s
			) task_conversations
		`, taskSource))
	}
	return sources
}

func scanDashboardOperatorConversationSummary(scanner operatorRowScanner) (OperatorConversationSummary, error) {
	var (
		item            OperatorConversationSummary
		runtimeStateRaw []byte
		turnID          string
		taskID          string
		parseOK         bool
		turnBlocksRaw   []byte
	)
	if err := scanner.Scan(
		&item.SessionID,
		&item.AgentID,
		&item.Kind,
		&item.ScopeKey,
		&item.Scope,
		&item.RuntimeMode,
		&item.Status,
		&item.TurnCount,
		&runtimeStateRaw,
		&turnID,
		&taskID,
		&parseOK,
		&turnBlocksRaw,
		&item.UpdatedAt,
	); err != nil {
		return OperatorConversationSummary{}, err
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
	item.Metadata.LiveTurn, err = projectOperatorLatestTurn(taskID, parseOK, turnID, turnBlocksRaw)
	if err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation live_turn: %w", err)
	}
	return item, nil
}

func scanDashboardOperatorConversationDetail(scanner operatorRowScanner) (OperatorConversationDetail, error) {
	var (
		item            OperatorConversationDetail
		runtimeStateRaw []byte
		messagesRaw     []byte
	)
	if err := scanner.Scan(
		&item.Conversation.SessionID,
		&item.Conversation.AgentID,
		&item.Conversation.Kind,
		&item.Conversation.ScopeKey,
		&item.Conversation.Scope,
		&item.Conversation.RuntimeMode,
		&item.Conversation.Status,
		&item.Conversation.TurnCount,
		&runtimeStateRaw,
		&messagesRaw,
		&item.Conversation.UpdatedAt,
	); err != nil {
		return OperatorConversationDetail{}, err
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Conversation.Summary = runtimeState.Summary
	item.RuntimeState = projectOperatorConversationState(runtimeState)
	item.Messages, err = decodeStoreJSONArray[OperatorConversationMessage](messagesRaw)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("decode conversation messages: %w", err)
	}
	if item.Messages == nil {
		item.Messages = []OperatorConversationMessage{}
	}
	return item, nil
}

func scanOperatorConversationSummary(scanner operatorRowScanner) (OperatorConversationSummary, error) {
	var (
		item            OperatorConversationSummary
		runtimeStateRaw []byte
		turnID          string
		taskID          string
		parseOK         bool
		turnBlocksRaw   []byte
		endedAt         sql.NullTime
	)
	if err := scanner.Scan(
		&item.SessionID,
		&item.AgentID,
		&item.RunID,
		&item.Kind,
		&item.ScopeKey,
		&item.Scope,
		&item.RuntimeMode,
		&item.Status,
		&item.TurnCount,
		&item.MessageCount,
		&runtimeStateRaw,
		&turnID,
		&taskID,
		&parseOK,
		&turnBlocksRaw,
		&item.StartedAt,
		&endedAt,
		&item.UpdatedAt,
	); err != nil {
		return OperatorConversationSummary{}, err
	}
	if endedAt.Valid {
		ended := endedAt.Time
		item.EndedAt = &ended
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
	item.Metadata.LiveTurn, err = projectOperatorLatestTurn(taskID, parseOK, turnID, turnBlocksRaw)
	if err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation live_turn: %w", err)
	}
	return item, nil
}

func scanOperatorConversationDetail(scanner operatorRowScanner) (OperatorConversationDetail, error) {
	var (
		item            OperatorConversationDetail
		runtimeStateRaw []byte
		messagesRaw     []byte
		endedAt         sql.NullTime
	)
	if err := scanner.Scan(
		&item.Conversation.SessionID,
		&item.Conversation.AgentID,
		&item.Conversation.RunID,
		&item.Conversation.Kind,
		&item.Conversation.ScopeKey,
		&item.Conversation.Scope,
		&item.Conversation.RuntimeMode,
		&item.Conversation.Status,
		&item.Conversation.TurnCount,
		&item.Conversation.MessageCount,
		&runtimeStateRaw,
		&messagesRaw,
		&item.Conversation.StartedAt,
		&endedAt,
		&item.Conversation.UpdatedAt,
	); err != nil {
		return OperatorConversationDetail{}, err
	}
	if endedAt.Valid {
		ended := endedAt.Time
		item.Conversation.EndedAt = &ended
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Conversation.Summary = runtimeState.Summary
	item.Conversation.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
	item.RuntimeState = projectOperatorConversationState(runtimeState)
	item.Messages, err = decodeStoreJSONArray[OperatorConversationMessage](messagesRaw)
	if err != nil {
		return OperatorConversationDetail{}, fmt.Errorf("decode conversation messages: %w", err)
	}
	if item.Messages == nil {
		item.Messages = []OperatorConversationMessage{}
	}
	return item, nil
}

func (r *OperatorAgentConversationReadSurface) loadConversationTurns(ctx context.Context, agentID, sessionID string) ([]OperatorConversationTurn, error) {
	agentID = strings.TrimSpace(agentID)
	sessionID = strings.TrimSpace(sessionID)
	if agentID == "" || sessionID == "" {
		return []OperatorConversationTurn{}, nil
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	query := `
		SELECT
			turn_id::text,
			agent_id,
			session_id::text,
			COALESCE(runtime_mode, ''),
			COALESCE(scope_key, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(trigger_event_id::text, ''),
			COALESCE(trigger_event_type, ''),
			COALESCE(task_id, ''),
			COALESCE(available_tools, '[]'::jsonb),
			COALESCE(tool_calls, '[]'::jsonb),
			COALESCE(emitted_events, '[]'::jsonb),
			COALESCE(mcp_servers, '{}'::jsonb),
			COALESCE(mcp_tools_listed, '[]'::jsonb),
			COALESCE(mcp_tools_visible, '[]'::jsonb),
			COALESCE(request_payload, '{}'::jsonb),
			COALESCE(response_payload, '{}'::jsonb),
			COALESCE(turn_blocks, '[]'::jsonb),
			parse_ok,
			COALESCE(latency_ms, 0),
			COALESCE(retry_count, 0),
			COALESCE(error, ''),
			created_at
		FROM agent_turns
		WHERE agent_id = $1
		  AND session_id = $2::uuid
		ORDER BY created_at ASC, turn_id ASC
	`
	if !caps.Conversations.TurnBlocks {
		query = strings.Replace(query, "COALESCE(turn_blocks, '[]'::jsonb),", "'[]'::jsonb AS turn_blocks,", 1)
	}
	rows, err := r.db.QueryContext(ctx, query, agentID, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []OperatorConversationTurn{}
	for rows.Next() {
		item, err := scanOperatorConversationTurn(rows)
		if err != nil {
			return nil, err
		}
		item.TurnIndex = len(out) + 1
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func scanOperatorConversationTurn(scanner operatorRowScanner) (OperatorConversationTurn, error) {
	var (
		item                                  OperatorConversationTurn
		availableToolsRaw, toolCallsRaw       []byte
		emittedEventsRaw, mcpServersRaw       []byte
		mcpToolsListedRaw, mcpToolsVisibleRaw []byte
		requestPayloadRaw, responsePayloadRaw []byte
		turnBlocksRaw                         []byte
	)
	if err := scanner.Scan(
		&item.TurnID,
		&item.AgentID,
		&item.SessionID,
		&item.RuntimeMode,
		&item.ScopeKey,
		&item.EntityID,
		&item.TriggerEventID,
		&item.TriggerEventType,
		&item.TaskID,
		&availableToolsRaw,
		&toolCallsRaw,
		&emittedEventsRaw,
		&mcpServersRaw,
		&mcpToolsListedRaw,
		&mcpToolsVisibleRaw,
		&requestPayloadRaw,
		&responsePayloadRaw,
		&turnBlocksRaw,
		&item.ParseOK,
		&item.LatencyMS,
		&item.RetryCount,
		&item.Error,
		&item.CreatedAt,
	); err != nil {
		return OperatorConversationTurn{}, err
	}
	summary, hasSummary, err := decodeOperatorTurnSummaryProjection(turnBlocksRaw)
	if err != nil {
		return OperatorConversationTurn{}, err
	}
	if item.AvailableTools, err = decodeStoreJSONArray[string](availableToolsRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn available_tools: %w", err)
	}
	if item.ToolCalls, err = decodeStoreJSONArray[OperatorConversationToolCall](toolCallsRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn tool_calls: %w", err)
	}
	if item.EmittedEvents, err = decodeStoreJSONArray[string](emittedEventsRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn emitted_events: %w", err)
	}
	if item.MCPToolsListed, err = decodeStoreJSONArray[string](mcpToolsListedRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn mcp_tools_listed: %w", err)
	}
	if item.MCPToolsVisible, err = decodeStoreJSONArray[string](mcpToolsVisibleRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn mcp_tools_visible: %w", err)
	}
	if item.MCPServers, err = decodeStoreJSONStringMap(mcpServersRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn mcp_servers: %w", err)
	}
	if item.RequestPayload, err = decodeStoreJSONObjectRaw(requestPayloadRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn request_payload: %w", err)
	}
	if item.ResponsePayload, err = decodeStoreJSONObjectRaw(responsePayloadRaw); err != nil {
		return OperatorConversationTurn{}, fmt.Errorf("decode turn response_payload: %w", err)
	}
	if len(turnBlocksRaw) > 0 {
		if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocksJSON(turnBlocksRaw); err != nil {
			return OperatorConversationTurn{}, fmt.Errorf("decode canonical runtime_log turn_blocks: %w", err)
		}
		if item.TurnBlocks, err = decodeStoreJSONArray[OperatorConversationTurnBlock](turnBlocksRaw); err != nil {
			return OperatorConversationTurn{}, fmt.Errorf("decode turn turn_blocks: %w", err)
		}
	}
	if hasSummary {
		item.AssistantVisibleOutput, item.Outcome, item.ReasoningBlocks, item.ProgressUpdates, item.ToolResults = projectedOperatorTurnSummaryConversationFields(summary)
	}
	return item, nil
}

func projectOperatorConversationSummaryMetadata(p ConversationRuntimeStateDescriptor) OperatorConversationSummaryMetadata {
	meta := OperatorConversationSummaryMetadata{
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
	meta.Watchdog = operatorConversationWatchdogFromDescriptor(p.Watchdog)
	return meta
}

func projectOperatorConversationState(p ConversationRuntimeStateDescriptor) OperatorConversationState {
	state := OperatorConversationState{
		Summary:              p.Summary,
		ProviderSessionID:    p.ProviderSessionID,
		RetryReason:          p.RetryReason,
		RetriesFromSessionID: p.RetriesFromSessionID,
	}
	if p.LastTurn != nil {
		state.LastTurn = &OperatorConversationLastTurn{TaskID: p.LastTurn.TaskID, ParseOK: p.LastTurn.ParseOK}
	}
	state.Watchdog = operatorConversationWatchdogFromDescriptor(p.Watchdog)
	return state
}

func operatorConversationWatchdogFromDescriptor(p *ConversationRuntimeWatchdogDescriptor) *OperatorConversationWatchdog {
	if p == nil {
		return nil
	}
	return &OperatorConversationWatchdog{
		State:         p.State,
		BlockingLayer: p.BlockingLayer,
		Action:        p.Action,
		Outcome:       p.Outcome,
		LastOutputAt:  p.LastOutputAt,
		RecordedAt:    p.RecordedAt,
	}
}

func operatorAgentDiagnosisRuntimeStateFromConversationWatchdog(w *OperatorConversationWatchdog) *OperatorAgentDiagnosisRuntimeState {
	if w == nil {
		return nil
	}
	return &OperatorAgentDiagnosisRuntimeState{
		Watchdog: &OperatorAgentDiagnosisWatchdog{
			State:         strings.TrimSpace(w.State),
			BlockingLayer: strings.TrimSpace(w.BlockingLayer),
			Action:        strings.TrimSpace(w.Action),
			Outcome:       strings.TrimSpace(w.Outcome),
			LastOutputAt:  strings.TrimSpace(w.LastOutputAt),
			RecordedAt:    strings.TrimSpace(w.RecordedAt),
		},
	}
}

func conversationWatchdogDescriptorFromAgentDiagnosis(w OperatorAgentDiagnosisWatchdog) ConversationRuntimeWatchdogDescriptor {
	return ConversationRuntimeWatchdogDescriptor{
		State:         strings.TrimSpace(w.State),
		BlockingLayer: strings.TrimSpace(w.BlockingLayer),
		Action:        strings.TrimSpace(w.Action),
		Outcome:       strings.TrimSpace(w.Outcome),
		LastOutputAt:  strings.TrimSpace(w.LastOutputAt),
		RecordedAt:    strings.TrimSpace(w.RecordedAt),
	}
}

func projectOperatorLatestTurn(taskID string, parseOK bool, turnID string, turnBlocksRaw []byte) (*OperatorLiveTurn, error) {
	taskID = strings.TrimSpace(taskID)
	turnID = strings.TrimSpace(turnID)
	summary, ok, err := decodeOperatorTurnSummaryProjection(turnBlocksRaw)
	if err != nil {
		return nil, err
	}
	if !ok && taskID == "" && turnID == "" {
		return nil, nil
	}
	out := &OperatorLiveTurn{TurnID: turnID, TaskID: taskID, ParseOK: parseOK}
	if ok {
		out.AssistantVisibleOutput = summary.AssistantVisibleOutput
		out.Outcome = summary.Outcome
		out.ProgressUpdates = cloneStoreStringSlice(summary.ProgressUpdates)
		out.LastTool, err = projectedOperatorTurnSummaryLastTool(summary, parseOK)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decodeOperatorTurnSummaryProjection(raw []byte) (runtimellm.TurnSummaryTurnBlockData, bool, error) {
	summary, ok, err := runtimellm.DecodeCanonicalTurnSummaryJSON(raw)
	if err != nil {
		return runtimellm.TurnSummaryTurnBlockData{}, false, err
	}
	return summary, ok, nil
}

func projectedOperatorTurnSummaryConversationFields(p runtimellm.TurnSummaryTurnBlockData) (string, string, []string, []string, []OperatorConversationToolResult) {
	return p.AssistantVisibleOutput, p.Outcome, cloneStoreStringSlice(p.ReasoningBlocks), cloneStoreStringSlice(p.ProgressUpdates), projectedOperatorTurnSummaryToolResults(p)
}

func projectedOperatorTurnSummaryToolResults(p runtimellm.TurnSummaryTurnBlockData) []OperatorConversationToolResult {
	if len(p.ToolResults) == 0 {
		return nil
	}
	out := make([]OperatorConversationToolResult, 0, len(p.ToolResults))
	for _, item := range p.ToolResults {
		row := OperatorConversationToolResult{ToolName: item.ToolName}
		if item.ToolUseID != "" {
			row.ToolUseID = item.ToolUseID
		}
		if item.Output != nil {
			row.Output = append(json.RawMessage(nil), item.Output...)
		}
		out = append(out, row)
	}
	return out
}

func projectedOperatorTurnSummaryLastTool(p runtimellm.TurnSummaryTurnBlockData, parseOK bool) (*OperatorAgentTool, error) {
	if len(p.ToolResults) == 0 {
		return nil, nil
	}
	last := p.ToolResults[len(p.ToolResults)-1]
	if last.ToolName == "" {
		return nil, fmt.Errorf("latest canonical tool_result is missing tool_name")
	}
	out := &OperatorAgentTool{Name: last.ToolName, OK: parseOK}
	if last.ToolUseID != "" {
		out.ToolUseID = last.ToolUseID
	}
	if last.Output != nil {
		trimmed := bytes.TrimSpace(last.Output)
		if len(trimmed) == 0 {
			return nil, fmt.Errorf("latest canonical tool_result output is empty")
		}
		if !json.Valid(trimmed) {
			return nil, fmt.Errorf("latest canonical tool_result output is invalid JSON")
		}
		out.Result = append(json.RawMessage(nil), trimmed...)
	}
	return out, nil
}

func encodeConversationPositionCursor(cursor conversationPositionCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeConversationPositionCursor(raw string, kind string) (conversationPositionCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return conversationPositionCursor{}, ErrInvalidConversationCursor
	}
	var cursor conversationPositionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return conversationPositionCursor{}, ErrInvalidConversationCursor
	}
	if strings.TrimSpace(cursor.Kind) != kind {
		return conversationPositionCursor{}, ErrInvalidConversationCursor
	}
	return cursor, nil
}

func decodeStoreJSONObjectRaw(raw []byte) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), raw...), nil
}

func decodeStoreJSONArray[T any](raw []byte) ([]T, error) {
	if len(raw) == 0 {
		return []T{}, nil
	}
	out := []T{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeStoreJSONStringMap(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func cloneStoreStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	return append([]string(nil), in...)
}

func maxStoreInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}
