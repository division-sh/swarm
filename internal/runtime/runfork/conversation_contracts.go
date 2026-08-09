package runfork

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

const (
	ConversationForkLifecycleTTL      = 24 * time.Hour
	ConversationForkChatSnapshotOwner = "conversation.fork_chat.snapshot.v1"
	ConversationForkChatSandboxOwner  = "conversation.fork_chat.sandbox.v1"
)

var (
	ErrConversationForkNotFound      = errors.New("conversation fork not found")
	ErrInvalidConversationForkCursor = errors.New("invalid conversation fork cursor")
)

type ConversationForkPointSelector struct {
	Kind    string
	TurnID  string
	EventID string
	At      *time.Time
}

type ConversationForkPointDescriptor struct {
	Kind       string     `json:"kind"`
	TurnIndex  int        `json:"turn_index"`
	TurnID     string     `json:"turn_id"`
	EventID    string     `json:"event_id,omitempty"`
	At         *time.Time `json:"at,omitempty"`
	SelectedAt time.Time  `json:"selected_at"`
}

type OperatorConversationForkSession struct {
	ForkID          string                                  `json:"fork_id"`
	SourceSessionID string                                  `json:"source_session_id"`
	SourceRunID     string                                  `json:"source_run_id,omitempty"`
	SourceAgentID   string                                  `json:"source_agent_id"`
	SourceIdentity  runtimeagentidentity.Identity           `json:"-"`
	ForkPoint       ConversationForkPointDescriptor         `json:"fork_point"`
	CreatedBy       string                                  `json:"created_by"`
	CreatedAt       time.Time                               `json:"created_at"`
	ExpiresAt       time.Time                               `json:"expires_at"`
	DeletedAt       *time.Time                              `json:"deleted_at,omitempty"`
	State           string                                  `json:"state"`
	Turns           []operatorread.OperatorConversationTurn `json:"turns"`
}

type ConversationForkCreateRequest struct {
	SourceSessionID string
	ForkPoint       ConversationForkPointSelector
	CreatedBy       string
	Now             time.Time
}

type ConversationForkListOptions struct {
	SourceSessionID string
	Limit           int
	Cursor          string
	Now             time.Time
}

type ConversationForkListResult struct {
	Forks      []OperatorConversationForkSession `json:"forks"`
	NextCursor string                            `json:"next_cursor,omitempty"`
}

type ConversationForkDeleteResult struct {
	ForkID         string `json:"fork_id"`
	Deleted        bool   `json:"deleted"`
	AlreadyDeleted bool   `json:"already_deleted"`
}

type ConversationForkChatPrepareRequest struct {
	ForkID         string
	Message        string
	Method         string
	ActorTokenID   string
	RequestHash    string
	IdempotencyKey string
	Now            time.Time
}

type ConversationForkChatRecordRequest struct {
	ForkID       string
	Message      string
	ActorTokenID string
	Prepared     ConversationForkChatPrepared
	Execution    ConversationForkChatExecution
	Now          time.Time
}

type ConversationForkChatFailureRequest struct {
	Prepared         ConversationForkChatPrepared
	Cause            error
	OutcomeUncertain bool
	Now              time.Time
}

type ConversationForkChatPrepared struct {
	Fork                OperatorConversationForkSession
	Snapshot            ConversationForkSnapshot
	SandboxPolicy       ConversationForkSandboxPolicy
	AvailableTools      []string
	ForkTurnID          string
	SourceBundleHash    string
	TurnIndex           int
	RequestOccurrenceID string
	RequestHash         string
	IdempotencyKey      string
	ActorTokenID        string
	ExecutionOwner      string
	LeaseExpiresAt      time.Time
	FenceGeneration     uint64
}

type ConversationForkChatResult struct {
	ForkID              string                                `json:"fork_id"`
	Turn                operatorread.OperatorConversationTurn `json:"turn"`
	Snapshot            ConversationForkSnapshot              `json:"snapshot"`
	SandboxPolicy       ConversationForkSandboxPolicy         `json:"sandbox_policy"`
	IdempotencyReplayed bool                                  `json:"idempotency_replayed"`
}

type ConversationForkSnapshot struct {
	ForkID          string                           `json:"fork_id"`
	SourceSessionID string                           `json:"source_session_id"`
	SourceRunID     string                           `json:"source_run_id,omitempty"`
	SourceAgentID   string                           `json:"source_agent_id"`
	SourceIdentity  runtimeagentidentity.Identity    `json:"-"`
	SourceTurn      ConversationForkSourceTurn       `json:"source_turn"`
	EntitySnapshot  []ConversationForkEntitySnapshot `json:"entity_snapshot"`
	SnapshotOwner   string                           `json:"snapshot_owner"`
	CreatedAt       time.Time                        `json:"created_at"`
	SourceAgent     runtimeactors.AgentConfig        `json:"-"`
}

type ConversationForkSourceTurn struct {
	TurnID          string          `json:"turn_id"`
	TurnIndex       int             `json:"turn_index"`
	SelectedAt      time.Time       `json:"selected_at"`
	CreatedAt       time.Time       `json:"created_at"`
	RequestPayload  json.RawMessage `json:"request_payload,omitempty"`
	ResponsePayload json.RawMessage `json:"response_payload,omitempty"`
	ToolCalls       json.RawMessage `json:"tool_calls,omitempty"`
	AvailableTools  json.RawMessage `json:"available_tools,omitempty"`
}

type ConversationForkEntitySnapshot struct {
	EntityID       string         `json:"entity_id"`
	CurrentState   string         `json:"current_state,omitempty"`
	EnteredStateAt *time.Time     `json:"entered_state_at,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
	Gates          map[string]any `json:"gates,omitempty"`
	Accumulator    map[string]any `json:"accumulator,omitempty"`
}

type ConversationForkSandboxPolicy struct {
	Owner              string   `json:"owner"`
	ReadPolicy         string   `json:"read_policy"`
	WritePolicy        string   `json:"write_policy"`
	SideEffectingTools []string `json:"side_effecting_tools"`
	StubbedTools       []string `json:"stubbed_tools"`
}

func CanonicalConversationForkSandboxPolicy() ConversationForkSandboxPolicy {
	sideEffecting := []string{
		"save_entity_field", "create_entity", "emit_event", "mailbox.decide", "mailbox.defer",
		"mailbox.begin_input", "mailbox.cancel_input", "mailbox.acknowledge", "run.start",
		"run.continue", "run.pause", "run.stop",
	}
	return ConversationForkSandboxPolicy{
		Owner:              ConversationForkChatSandboxOwner,
		ReadPolicy:         "fork_snapshot_only",
		WritePolicy:        "stub_record_only_no_live_mutation",
		SideEffectingTools: append([]string(nil), sideEffecting...),
		StubbedTools:       append([]string(nil), sideEffecting...),
	}
}

func (p ConversationForkSandboxPolicy) Validate() error {
	canonical := CanonicalConversationForkSandboxPolicy()
	if p.Owner != canonical.Owner || p.ReadPolicy != canonical.ReadPolicy || p.WritePolicy != canonical.WritePolicy ||
		!slices.Equal(p.SideEffectingTools, canonical.SideEffectingTools) || !slices.Equal(p.StubbedTools, canonical.StubbedTools) {
		return fmt.Errorf("conversation fork sandbox policy is not canonical")
	}
	return nil
}

func (p ConversationForkSandboxPolicy) AvailableToolNames() []string {
	out := []string{"fork_snapshot_read_entities"}
	for _, name := range p.StubbedTools {
		out = append(out, strings.NewReplacer(".", "_", "-", "_").Replace(strings.TrimSpace(name)))
	}
	return out
}

func (p ConversationForkChatPrepared) ValidateSandboxPolicy() error {
	if err := p.SandboxPolicy.Validate(); err != nil {
		return err
	}
	if !slices.Equal(p.AvailableTools, p.SandboxPolicy.AvailableToolNames()) {
		return fmt.Errorf("conversation fork sandbox available tools do not match policy")
	}
	return nil
}

type ConversationForkChatExecution struct {
	AssistantMessage string
	ToolCalls        []operatorread.OperatorConversationToolCall
	ToolResults      []operatorread.OperatorConversationToolResult
	AvailableTools   []string
	ExecutionOwner   string
	FenceGeneration  uint64
}

type ConversationForkChatReplayStateError struct {
	ForkTurnID string
	State      string
}

func (e *ConversationForkChatReplayStateError) Error() string {
	return fmt.Sprintf("conversation fork chat request already exists in state %s", strings.TrimSpace(e.State))
}
