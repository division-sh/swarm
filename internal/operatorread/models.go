package operatorread

import (
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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

type OperatorEntityListOptions struct {
	RunID        string
	EntityID     string
	Flow         string
	Type         string
	CurrentState string
	Limit        int
	Cursor       string
}

type OperatorEntityListResult struct {
	Entities   []OperatorEntitySummary `json:"entities"`
	NextCursor string                  `json:"next_cursor,omitempty"`
}

type OperatorEntitySummary struct {
	EntityID     string    `json:"entity_id"`
	RunID        string    `json:"run_id"`
	FlowInstance string    `json:"flow_instance"`
	EntityType   string    `json:"entity_type"`
	CurrentState string    `json:"current_state"`
	Revision     int       `json:"revision"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	Slug         string    `json:"slug,omitempty"`
	Name         string    `json:"name,omitempty"`
}

type OperatorEntityFull struct {
	Entity      OperatorEntitySummary          `json:"entity"`
	Fields      map[string]any                 `json:"fields"`
	Bookkeeping map[string]any                 `json:"bookkeeping"`
	Gates       map[string]bool                `json:"gates"`
	Accumulated map[string]any                 `json:"accumulated"`
	Loops       []loopruntime.PublicActivation `json:"loops,omitempty"`
}

type OperatorEntityAggregateOptions struct {
	RunID   string
	GroupBy string
	Type    string
}

type OperatorEntityAggregateResult struct {
	Counts map[string]int `json:"counts"`
}

type OperatorEventListFilter struct {
	RunID          string
	EntityID       string
	EventName      string
	DeliveryStatus string
	SubscriberID   string
	SubscriberType string
	ReasonCode     string
	HasDeadLetter  *bool
}

type OperatorEventListOptions struct {
	Filter             OperatorEventListFilter
	Source             string
	Since              *time.Time
	Until              *time.Time
	Limit              int
	Cursor             string
	Order              string
	ExcludeRuntimeLogs bool
}

type OperatorEventListResult struct {
	Events     []OperatorEventFull `json:"events"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type OperatorEventFull struct {
	EventID                  string                     `json:"event_id"`
	EventName                string                     `json:"event_name"`
	ExecutionMode            executionmode.Mode         `json:"execution_mode"`
	EntityID                 string                     `json:"entity_id,omitempty"`
	RunID                    string                     `json:"run_id,omitempty"`
	SourceEventID            string                     `json:"source_event_id,omitempty"`
	OperatorReferenceEventID string                     `json:"operator_reference_event_id,omitempty"`
	CreatedAt                time.Time                  `json:"created_at"`
	Source                   string                     `json:"source"`
	ProducerType             events.EventProducerType   `json:"producer_type"`
	Payload                  map[string]any             `json:"payload"`
	Deliveries               []OperatorEventDelivery    `json:"deliveries"`
	NoDelivery               *OperatorNoDelivery        `json:"no_delivery,omitempty"`
	DeadLetters              []OperatorDeadLetterRecord `json:"dead_letters"`
	event                    events.Event
}

type OperatorEventDelivery struct {
	DeliveryID     string                     `json:"delivery_id"`
	SubscriberType string                     `json:"subscriber_type"`
	SubscriberID   string                     `json:"subscriber_id"`
	Route          events.DeliveryRoute       `json:"-"`
	Target         OperatorDeliveryTarget     `json:"target"`
	SessionID      string                     `json:"session_id,omitempty"`
	Status         string                     `json:"status"`
	ReasonCode     string                     `json:"reason_code,omitempty"`
	Failure        *runtimefailures.Envelope  `json:"failure,omitempty"`
	RetryCount     int                        `json:"retry_count"`
	RetryScheduled bool                       `json:"retry_scheduled"`
	Terminal       bool                       `json:"terminal"`
	CreatedAt      *time.Time                 `json:"created_at,omitempty"`
	StartedAt      *time.Time                 `json:"started_at,omitempty"`
	FinishedAt     *time.Time                 `json:"finished_at,omitempty"`
	DeadLetters    []OperatorDeadLetterRecord `json:"dead_letters,omitempty"`
	ClaimVersion   int64                      `json:"-"`
}

type OperatorDeliveryTarget struct {
	Kind         string `json:"kind,omitempty"`
	FlowID       string `json:"flow_id,omitempty"`
	FlowInstance string `json:"flow_instance,omitempty"`
	EntityID     string `json:"entity_id,omitempty"`
}

type OperatorNoDelivery struct {
	Reason string                          `json:"reason"`
	Plans  []OperatorConnectPlanEvaluation `json:"plans"`
}

type OperatorConnectPlanEvaluation struct {
	PlanSHA256 string                             `json:"plan_sha256"`
	Resolution string                             `json:"resolution"`
	Targets    []OperatorConnectPlanTarget        `json:"targets"`
	Candidates []OperatorConnectCandidateEvidence `json:"candidates"`
}

// OperatorConnectPlanTarget is route-selection evidence, not admitted
// delivery ownership, so it cannot carry an ownership kind.
type OperatorConnectPlanTarget struct {
	FlowID       string `json:"flow_id,omitempty"`
	FlowInstance string `json:"flow_instance,omitempty"`
	EntityID     string `json:"entity_id,omitempty"`
}

type OperatorConnectCandidateEvidence struct {
	ReceiverSHA256 string `json:"receiver_sha256"`
	RecipientKind  string `json:"recipient_kind"`
	RecipientID    string `json:"recipient_id"`
	Path           string `json:"path,omitempty"`
	AgentIdentity  string `json:"agent_identity,omitempty"`
	Outcome        string `json:"outcome"`
}

type OperatorDeadLetterRecord struct {
	DeadLetterID string                   `json:"dead_letter_id"`
	DeliveryID   string                   `json:"delivery_id,omitempty"`
	ClaimVersion int64                    `json:"claim_version,omitempty"`
	Failure      runtimefailures.Envelope `json:"failure"`
	RetryCount   int                      `json:"retry_count"`
	ChainDepth   int                      `json:"chain_depth"`
	HandlerNode  string                   `json:"handler_node,omitempty"`
	CreatedAt    time.Time                `json:"created_at"`
}

type OperatorRuntimeLogListOptions struct {
	RunID             string
	BundleHash        string
	EntityID          string
	SessionID         string
	Component         string
	Level             string
	ErrorCode         string
	Source            string
	ActionOrEventType string
	Since             *time.Time
	Until             *time.Time
	Limit             int
	Cursor            string
	Order             string
}

type OperatorRuntimeLogListResult struct {
	Logs       []OperatorRuntimeLogEntry `json:"logs"`
	NextCursor string                    `json:"next_cursor,omitempty"`
}

type OperatorRuntimeLogEntry struct {
	LogID           string                    `json:"log_id"`
	TS              time.Time                 `json:"ts"`
	Level           string                    `json:"level"`
	Component       string                    `json:"component"`
	Source          string                    `json:"source"`
	RunID           string                    `json:"run_id,omitempty"`
	EntityID        string                    `json:"entity_id,omitempty"`
	SessionID       string                    `json:"session_id,omitempty"`
	ErrorCode       string                    `json:"error_code,omitempty"`
	Failure         *runtimefailures.Envelope `json:"failure,omitempty"`
	Message         string                    `json:"message"`
	EventID         string                    `json:"event_id,omitempty"`
	Action          string                    `json:"action,omitempty"`
	EventType       string                    `json:"event_type,omitempty"`
	ParentEventID   string                    `json:"parent_event_id,omitempty"`
	HandlerID       string                    `json:"handler_id,omitempty"`
	AgentID         string                    `json:"agent_id,omitempty"`
	DurationUS      int                       `json:"duration_us,omitempty"`
	DeliveryState   string                    `json:"delivery_state,omitempty"`
	PreviousState   string                    `json:"delivery_previous_state,omitempty"`
	Transition      string                    `json:"delivery_transition,omitempty"`
	Reason          string                    `json:"delivery_reason,omitempty"`
	Terminal        string                    `json:"delivery_terminal_outcome,omitempty"`
	RetryCount      int                       `json:"delivery_retry_count,omitempty"`
	Correlation     map[string]string         `json:"correlation,omitempty"`
	CanonicalDetail map[string]any            `json:"details,omitempty"`
}

type OperatorRuntimeIncidentListOptions struct {
	SinceHours int
	BundleHash string
	Component  string
	Level      string
	MCPOnly    bool
	Limit      int
	Cursor     string
}

type OperatorRuntimeIncidentListResult struct {
	Incidents  []OperatorRuntimeIncident `json:"incidents"`
	NextCursor string                    `json:"next_cursor,omitempty"`
}

type OperatorRuntimeIncident struct {
	IncidentID    string    `json:"incident_id"`
	FirstSeen     time.Time `json:"first_seen"`
	LastSeen      time.Time `json:"last_seen"`
	Count         int       `json:"count"`
	Level         string    `json:"level"`
	Component     string    `json:"component"`
	ErrorCode     string    `json:"error_code,omitempty"`
	SampleMessage string    `json:"sample_message"`
	SampleLogIDs  []string  `json:"sample_log_ids"`

	Agents     []string `json:"-"`
	Actions    []string `json:"-"`
	Components []string `json:"-"`
}

type OperatorAgentListOptions struct {
	Flow      string
	Role      string
	TurnLimit int
}

type OperatorAgentListResult struct {
	Agents []OperatorAgentSummary `json:"agents"`
}

type OperatorAgentSummary struct {
	AgentID       string `json:"agent_id"`
	Role          string `json:"role"`
	Type          string `json:"type"`
	Model         string `json:"model"`
	ExecutionMode string `json:"execution_mode"`
	Memory        bool   `json:"memory"`
	MemorySource  string `json:"memory_source"`
	Status        string `json:"status"`

	Identity              agentidentity.Identity              `json:"-"`
	RuntimeFlowID         string                              `json:"-"`
	FlowInstance          string                              `json:"flow_instance,omitempty"`
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
	TurnID      string                    `json:"turn_id"`
	CompletedAt time.Time                 `json:"completed_at"`
	ParseOK     bool                      `json:"parse_ok"`
	Failure     *runtimefailures.Envelope `json:"failure,omitempty"`
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
	TurnID    string `json:"turn_id"`
	ToolName  string `json:"tool_name"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	OK        bool   `json:"ok"`
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
	DeliveryID string    `json:"delivery_id"`
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
	Name      string `json:"name"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	OK        bool   `json:"ok"`
}

type OperatorLiveTurn struct {
	TurnID                 string             `json:"turn_id,omitempty"`
	TaskID                 string             `json:"task_id,omitempty"`
	ParseOK                bool               `json:"parse_ok"`
	AssistantVisibleOutput string             `json:"assistant_visible_output,omitempty"`
	Outcome                string             `json:"outcome,omitempty"`
	LastTool               *OperatorAgentTool `json:"last_tool,omitempty"`
}

type OperatorConversationListOptions struct {
	AgentID      string
	FlowInstance string
	RunID        string
	Limit        int
	Cursor       string
}

type OperatorConversationListResult struct {
	Conversations []OperatorConversationSummary `json:"conversations"`
	NextCursor    string                        `json:"next_cursor,omitempty"`
}

type OperatorConversationSummary struct {
	SessionID     string     `json:"session_id"`
	AgentID       string     `json:"agent_id"`
	ExecutionMode string     `json:"execution_mode,omitempty"`
	RunID         string     `json:"run_id,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
	TurnCount     int        `json:"turn_count"`
	MessageCount  int        `json:"message_count"`
	Status        string     `json:"status"`

	Kind         string                              `json:"-"`
	FlowInstance string                              `json:"-"`
	Memory       bool                                `json:"-"`
	MemorySource string                              `json:"-"`
	Summary      string                              `json:"-"`
	UpdatedAt    time.Time                           `json:"-"`
	Metadata     OperatorConversationSummaryMetadata `json:"-"`
}

type OperatorConversationSummaryMetadata struct {
	ProviderSessionID    string                        `json:"provider_session_id,omitempty"`
	RetryReason          string                        `json:"retry_reason,omitempty"`
	RetriesFromSessionID string                        `json:"retries_from_session_id,omitempty"`
	Watchdog             *OperatorConversationWatchdog `json:"watchdog,omitempty"`
	LiveTurn             *OperatorLiveTurn             `json:"live_turn,omitempty"`
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
	ExecutionMode    string                          `json:"execution_mode"`
	TriggerEventID   string                          `json:"trigger_event_id"`
	TriggerEventType string                          `json:"trigger_event_type"`
	RequestPayload   json.RawMessage                 `json:"request_payload,omitempty"`
	ResponsePayload  json.RawMessage                 `json:"response_payload,omitempty"`
	ToolCalls        []OperatorConversationToolCall  `json:"tool_calls,omitempty"`
	TurnBlocks       []OperatorConversationTurnBlock `json:"turn_blocks,omitempty"`
	ParseOK          bool                            `json:"parse_ok"`
	LatencyMS        int                             `json:"latency_ms"`
	Failure          *runtimefailures.Envelope       `json:"failure,omitempty"`

	AgentID                string                           `json:"-"`
	SessionID              string                           `json:"-"`
	FlowInstance           string                           `json:"-"`
	Memory                 bool                             `json:"-"`
	MemorySource           string                           `json:"-"`
	EntityID               string                           `json:"-"`
	TaskID                 string                           `json:"-"`
	EmittedEvents          []string                         `json:"-"`
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

type AgentDeliveryDiagnosticsCursorError struct {
	Field string
}

type OperatorAgentDeliveryDiagnosticsOptions struct {
	FailureLimit     int
	FailureCursor    string
	DeadLetterLimit  int
	DeadLetterCursor string
}

type OperatorAgentDeliveryDiagnostics struct {
	AgentID               string                                  `json:"agent_id"`
	Summary               OperatorAgentDeliveryDiagnosticsSummary `json:"summary"`
	Failures              []OperatorAgentDeliveryFailure          `json:"failures"`
	FailuresNextCursor    string                                  `json:"failures_next_cursor,omitempty"`
	DeadLetters           []OperatorAgentDeadLetterDelivery       `json:"dead_letters"`
	DeadLettersNextCursor string                                  `json:"dead_letters_next_cursor,omitempty"`
}

type OperatorAgentDeliveryDiagnosticsSummary struct {
	Failures24h    int `json:"failures_24h"`
	DeadLetters24h int `json:"dead_letters_24h"`
}

type OperatorAgentDeliveryFailure struct {
	DeliveryID string                    `json:"delivery_id"`
	EventID    string                    `json:"event_id"`
	EventName  string                    `json:"event_name"`
	RunID      string                    `json:"run_id,omitempty"`
	EntityID   string                    `json:"entity_id,omitempty"`
	Status     string                    `json:"status"`
	ReasonCode string                    `json:"reason_code,omitempty"`
	Failure    *runtimefailures.Envelope `json:"failure,omitempty"`
	RetryCount int                       `json:"retry_count"`
	OccurredAt time.Time                 `json:"occurred_at"`
}

type OperatorAgentDeadLetterDelivery struct {
	DeliveryID        string                     `json:"delivery_id"`
	EventID           string                     `json:"event_id"`
	EventName         string                     `json:"event_name"`
	RunID             string                     `json:"run_id,omitempty"`
	EntityID          string                     `json:"entity_id,omitempty"`
	Status            string                     `json:"status"`
	ReasonCode        string                     `json:"reason_code,omitempty"`
	Failure           *runtimefailures.Envelope  `json:"failure,omitempty"`
	RetryCount        int                        `json:"retry_count"`
	OccurredAt        time.Time                  `json:"occurred_at"`
	DeadLetterRecords []OperatorDeadLetterRecord `json:"dead_letter_records"`
}

type AgentDeliveryLifecycleCursorError struct{}

type AgentDeliveryLifecycleStatusError struct {
	Status string
}

type OperatorAgentDeliveryLifecycleOptions struct {
	RunID    string
	Statuses []string
	Limit    int
	Cursor   string
}

type OperatorAgentDeliveryLifecycleList struct {
	AgentID    string                              `json:"agent_id"`
	Deliveries []OperatorAgentDeliveryLifecycleRow `json:"deliveries"`
	NextCursor string                              `json:"next_cursor,omitempty"`
}

type OperatorAgentDeliveryLifecycleRow struct {
	DeliveryID          string                    `json:"delivery_id"`
	EventID             string                    `json:"event_id"`
	EventName           string                    `json:"event_name"`
	RunID               string                    `json:"run_id,omitempty"`
	EntityID            string                    `json:"entity_id,omitempty"`
	Status              string                    `json:"status"`
	RetryCount          int                       `json:"retry_count"`
	ReasonCode          string                    `json:"reason_code,omitempty"`
	Failure             *runtimefailures.Envelope `json:"failure,omitempty"`
	DeliveryCreatedAt   time.Time                 `json:"delivery_created_at"`
	DeliveryStartedAt   *time.Time                `json:"delivery_started_at,omitempty"`
	DeliveryDeliveredAt *time.Time                `json:"delivery_delivered_at,omitempty"`
}

type OperatorAgentUsageOptions struct {
	Since *time.Time
	Until *time.Time
}

type OperatorAgentUsage struct {
	AgentID   string                         `json:"agent_id"`
	Window    OperatorAgentUsageWindow       `json:"window"`
	Usage     OperatorAgentUsageByAccounting `json:"usage"`
	Breakdown []OperatorAgentUsageBreakdown  `json:"breakdown"`
}

type OperatorAgentUsageWindow struct {
	Since *time.Time `json:"since,omitempty"`
	Until *time.Time `json:"until,omitempty"`
}

type OperatorAgentUsageByAccounting struct {
	Exact     OperatorAgentUsageTotals `json:"exact"`
	Estimated OperatorAgentUsageTotals `json:"estimated"`
}

type OperatorAgentUsageTotals struct {
	LedgerEntries    int     `json:"ledger_entries"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd"`
}

type OperatorAgentUsageBreakdown struct {
	ExecutionMode   string                   `json:"execution_mode"`
	UsageAccounting string                   `json:"usage_accounting"`
	InvocationType  string                   `json:"invocation_type"`
	Model           string                   `json:"model"`
	ModelAlias      string                   `json:"model_alias"`
	BackendProfile  string                   `json:"backend_profile"`
	Provider        string                   `json:"provider"`
	Transport       string                   `json:"transport"`
	ResolvedModel   string                   `json:"resolved_model"`
	CostDisplay     string                   `json:"cost_display"`
	Totals          OperatorAgentUsageTotals `json:"totals"`
}

type PendingAgentDeliveryFacts struct {
	PendingCount        int
	OldestPendingAgeSec int
}

type PendingAgentDeliveryListOptions struct {
	AgentIdentity agentidentity.Identity
	Since         time.Time
	Limit         int
	Cursor        string
}

type PendingAgentDeliveryPage struct {
	PendingCount        int
	OldestPendingAgeSec int
	PendingDeliveries   []PendingAgentDeliveryDetail
	NextCursor          string
}

type PendingAgentDeliveryDetail struct {
	DeliveryID string
	EventID    string
	EventName  string
	EnqueuedAt time.Time
	Attempts   int
	Event      events.Event `json:"-"`
}

type RunHeader struct {
	RunID            string                        `json:"run_id"`
	Status           string                        `json:"status"`
	Origin           runtimerunlifecycle.RunOrigin `json:"origin"`
	EntityCount      int                           `json:"entity_count"`
	EventCount       int                           `json:"event_count"`
	StartedAt        time.Time                     `json:"started_at"`
	EndedAt          *time.Time                    `json:"ended_at,omitempty"`
	ContinuedAsRunID string                        `json:"continued_as_run_id,omitempty"`
	Failure          *runtimefailures.Envelope     `json:"failure,omitempty"`
	ControlReason    string                        `json:"control_reason,omitempty"`
}

type RunHeaderListOptions struct {
	Status     string
	BundleHash string
	Since      *time.Time
	Until      *time.Time
	Limit      int
	Cursor     string
}

type RunDebugQueryOptions struct {
	LogsAllLevels   bool
	Component       string
	EventLimit      int
	MutationLimit   int
	RuntimeLogLimit int
	DeadLetterLimit int
}

type RunDebugRunSummary struct {
	RunID          string     `json:"run_id"`
	RunTableStatus string     `json:"run_table_status,omitempty"`
	RootEventID    string     `json:"root_event_id,omitempty"`
	RootEventType  string     `json:"root_event_type,omitempty"`
	StartedAt      time.Time  `json:"started_at,omitempty"`
	LastEventAt    time.Time  `json:"last_event_at,omitempty"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
	EventCount     int        `json:"event_count"`
	EntityCount    int        `json:"entity_count"`
}

type RunDebugEventCount struct {
	EventName string `json:"event_name"`
	Count     int    `json:"count"`
}

type RunDebugDeliveryCount struct {
	SubscriberID string `json:"subscriber_id"`
	Status       string `json:"status"`
	Count        int    `json:"count"`
}

type RunDebugEvent struct {
	EventID    string          `json:"event_id,omitempty"`
	EventName  string          `json:"event_name"`
	EntityID   string          `json:"entity_id,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	Source     string          `json:"source,omitempty"`
	SourceType string          `json:"source_type,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type RunDebugMutation struct {
	MutationID    string          `json:"mutation_id,omitempty"`
	EntityID      string          `json:"entity_id,omitempty"`
	Domain        string          `json:"domain"`
	Path          string          `json:"path"`
	OldValue      json.RawMessage `json:"old_value,omitempty"`
	NewValue      json.RawMessage `json:"new_value,omitempty"`
	WriterType    string          `json:"writer_type,omitempty"`
	WriterID      string          `json:"writer_id,omitempty"`
	HandlerStep   string          `json:"handler_step,omitempty"`
	CausedByEvent string          `json:"caused_by_event,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

type RunDebugDeadLetter struct {
	OriginalEvent string                   `json:"original_event"`
	EntityID      string                   `json:"entity_id,omitempty"`
	Failure       runtimefailures.Envelope `json:"failure"`
	HandlerNode   string                   `json:"handler_node,omitempty"`
	CreatedAt     time.Time                `json:"created_at"`
}

type RunDebugFailureDelivery struct {
	EventID        string                     `json:"event_id"`
	EventName      string                     `json:"event_name"`
	EntityID       string                     `json:"entity_id,omitempty"`
	DeliveryID     string                     `json:"delivery_id"`
	SubscriberType string                     `json:"subscriber_type"`
	SubscriberID   string                     `json:"subscriber_id"`
	SessionID      string                     `json:"session_id,omitempty"`
	Status         string                     `json:"status"`
	ReasonCode     string                     `json:"reason_code,omitempty"`
	Failure        *runtimefailures.Envelope  `json:"failure,omitempty"`
	RetryCount     int                        `json:"retry_count"`
	RetryScheduled bool                       `json:"retry_scheduled"`
	Terminal       bool                       `json:"terminal"`
	CreatedAt      *time.Time                 `json:"created_at,omitempty"`
	StartedAt      *time.Time                 `json:"started_at,omitempty"`
	FinishedAt     *time.Time                 `json:"finished_at,omitempty"`
	DeadLetters    []OperatorDeadLetterRecord `json:"dead_letters,omitempty"`
}

type RunDebugAgentTurn struct {
	AgentID    string    `json:"agent_id"`
	Turns      int       `json:"turns"`
	ErrorCount int       `json:"error_count"`
	LastAt     time.Time `json:"last_at"`
}

type RunDebugRuntimeLog struct {
	EventID   string                    `json:"event_id,omitempty"`
	Level     string                    `json:"level"`
	Message   string                    `json:"message,omitempty"`
	Component string                    `json:"component"`
	Action    string                    `json:"action"`
	EventType string                    `json:"event_type,omitempty"`
	AgentID   string                    `json:"agent_id,omitempty"`
	EntityID  string                    `json:"entity_id,omitempty"`
	Failure   *runtimefailures.Envelope `json:"failure,omitempty"`
	Detail    json.RawMessage           `json:"detail,omitempty"`
	CreatedAt time.Time                 `json:"created_at"`
}

type RunDebugRuntimeSummary struct {
	Level     string `json:"level"`
	Component string `json:"component"`
	Action    string `json:"action"`
	Count     int    `json:"count"`
}

type RunTestQuiescence struct {
	Ready                   bool `json:"ready"`
	ActiveDeliveries        int  `json:"active_deliveries"`
	UnsettledPipelineEvents int  `json:"unsettled_pipeline_events"`
	DueTimers               int  `json:"due_timers"`
	ActiveSessionLeases     int  `json:"active_session_leases"`
}

type RunDebugReport struct {
	RunID             string                    `json:"run_id"`
	RunTableStatus    string                    `json:"run_table_status,omitempty"`
	RootEventID       string                    `json:"root_event_id,omitempty"`
	RootEventType     string                    `json:"root_event_type,omitempty"`
	Failure           *runtimefailures.Envelope `json:"failure,omitempty"`
	ControlReason     string                    `json:"control_reason,omitempty"`
	StartedAt         time.Time                 `json:"started_at,omitempty"`
	LastEventAt       time.Time                 `json:"last_event_at,omitempty"`
	EndedAt           *time.Time                `json:"ended_at,omitempty"`
	EventCount        int                       `json:"event_count"`
	EntityCount       int                       `json:"entity_count"`
	WarnErrorLogCount int                       `json:"warn_error_log_count"`
	EventCounts       []RunDebugEventCount      `json:"event_counts,omitempty"`
	Deliveries        []RunDebugDeliveryCount   `json:"deliveries,omitempty"`
	Events            []RunDebugEvent           `json:"events,omitempty"`
	FailedDeliveries  []RunDebugFailureDelivery `json:"failed_deliveries,omitempty"`
	DeadLetters       []RunDebugDeadLetter      `json:"dead_letters,omitempty"`
	AgentTurns        []RunDebugAgentTurn       `json:"agent_turns,omitempty"`
	Mutations         []RunDebugMutation        `json:"mutations,omitempty"`
	RuntimeLogSummary []RunDebugRuntimeSummary  `json:"runtime_log_summary,omitempty"`
	RuntimeLogs       []RunDebugRuntimeLog      `json:"runtime_logs,omitempty"`
	TestQuiescence    RunTestQuiescence         `json:"test_quiescence"`
}

type RunDebugTraceQueryOptions struct {
	Limit              int
	Cursor             string
	Since              *time.Time
	Until              *time.Time
	Filter             RunDebugTraceFilter
	ExcludeRuntimeLogs bool
}

type RunDebugTraceFilter struct {
	EventNames       []string
	EntityIDs        []string
	DeliveryStatuses []string
	SubscriberIDs    []string
	SubscriberTypes  []string
}

type RunDebugTraceRow struct {
	EventID                   string                            `json:"event_id,omitempty"`
	EventName                 string                            `json:"event_name,omitempty"`
	SourceEventID             string                            `json:"source_event_id,omitempty"`
	EntityID                  string                            `json:"entity_id,omitempty"`
	EventSource               string                            `json:"event_source,omitempty"`
	EventSourceType           string                            `json:"event_source_type,omitempty"`
	EventCreatedAt            time.Time                         `json:"event_created_at"`
	DeliveryID                string                            `json:"delivery_id,omitempty"`
	SubscriberType            string                            `json:"subscriber_type,omitempty"`
	SubscriberID              string                            `json:"subscriber_id,omitempty"`
	DeliveryStatus            string                            `json:"delivery_status,omitempty"`
	DeliveryReasonCode        string                            `json:"delivery_reason_code,omitempty"`
	ReplyContextID            string                            `json:"reply_context_id,omitempty"`
	DeliveryPayloadProjection *events.DeliveryPayloadProjection `json:"delivery_payload_projection,omitempty"`
	DeliveryFailure           *runtimefailures.Envelope         `json:"delivery_failure,omitempty"`
	DeliveryRetryCount        int                               `json:"delivery_retry_count,omitempty"`
	DeliveryRetryScheduled    bool                              `json:"delivery_retry_scheduled,omitempty"`
	DeliveryTerminal          bool                              `json:"delivery_terminal,omitempty"`
	ActiveSessionID           string                            `json:"active_session_id,omitempty"`
	DeliveryCreatedAt         *time.Time                        `json:"delivery_created_at,omitempty"`
	DeliveryStartedAt         *time.Time                        `json:"delivery_started_at,omitempty"`
	DeliveryDeliveredAt       *time.Time                        `json:"delivery_delivered_at,omitempty"`
	SessionID                 string                            `json:"session_id,omitempty"`
	SessionKind               string                            `json:"session_kind,omitempty"`
	SessionMemory             bool                              `json:"session_memory"`
	SessionMemorySource       string                            `json:"session_memory_source,omitempty"`
	SessionStatus             string                            `json:"session_status,omitempty"`
	SessionUpdatedAt          *time.Time                        `json:"session_updated_at,omitempty"`
	TurnID                    string                            `json:"turn_id,omitempty"`
	TurnTriggerEventID        string                            `json:"turn_trigger_event_id,omitempty"`
	TurnTriggerEventType      string                            `json:"turn_trigger_event_type,omitempty"`
	TurnFlowInstance          string                            `json:"turn_flow_instance,omitempty"`
	TurnMemory                bool                              `json:"turn_memory"`
	TurnMemorySource          string                            `json:"turn_memory_source,omitempty"`
	TurnEntityID              string                            `json:"turn_entity_id,omitempty"`
	TurnTaskID                string                            `json:"turn_task_id,omitempty"`
	TurnParseOK               bool                              `json:"turn_parse_ok,omitempty"`
	TurnRetryCount            int                               `json:"turn_retry_count,omitempty"`
	TurnFailure               *runtimefailures.Envelope         `json:"turn_failure,omitempty"`
	TurnCreatedAt             *time.Time                        `json:"turn_created_at,omitempty"`
}

type RunOperationalStatus struct {
	State          string
	BlockingLayer  string
	BlockingReason string
	Heuristics     []string
}

type AgentDeliveryLifecycleFacts struct {
	CurrentState  string
	BlockingLayer string
}
