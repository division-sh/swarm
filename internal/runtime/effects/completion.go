package effects

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type CompletionUsageExactness string

const (
	CompletionUsageExact       CompletionUsageExactness = "exact"
	CompletionUsageEstimated   CompletionUsageExactness = "estimated"
	CompletionUsageUnavailable CompletionUsageExactness = "unavailable"
)

type CompletionUsage struct {
	ResolvedModel              string
	Exactness                  CompletionUsageExactness
	InputTokens                *int64
	OutputTokens               *int64
	CacheReadInputTokens       *int64
	CacheCreationInputTokens   *int64
	CacheCreation5mInputTokens *int64
	CacheCreation1hInputTokens *int64
	ProviderReportedCostUSD    *float64
}

func (u CompletionUsage) Validate() error {
	if strings.TrimSpace(u.ResolvedModel) == "" {
		return fmt.Errorf("completion usage resolved model is required")
	}
	switch u.Exactness {
	case CompletionUsageExact, CompletionUsageEstimated:
		if u.InputTokens == nil || u.OutputTokens == nil || *u.InputTokens < 0 || *u.OutputTokens < 0 {
			return fmt.Errorf("completion %s usage requires non-negative input and output tokens", u.Exactness)
		}
	case CompletionUsageUnavailable:
		if u.InputTokens != nil || u.OutputTokens != nil {
			return fmt.Errorf("unavailable completion usage cannot carry input or output tokens")
		}
	default:
		return fmt.Errorf("completion usage exactness %q is invalid", u.Exactness)
	}
	for name, value := range map[string]*int64{
		"cache_read_input_tokens":        u.CacheReadInputTokens,
		"cache_creation_input_tokens":    u.CacheCreationInputTokens,
		"cache_creation_5m_input_tokens": u.CacheCreation5mInputTokens,
		"cache_creation_1h_input_tokens": u.CacheCreation1hInputTokens,
	} {
		if value != nil && *value < 0 {
			return fmt.Errorf("completion usage %s cannot be negative", name)
		}
	}
	if u.ProviderReportedCostUSD != nil && *u.ProviderReportedCostUSD < 0 {
		return fmt.Errorf("completion provider-reported cost cannot be negative")
	}
	if u.CacheCreation5mInputTokens != nil || u.CacheCreation1hInputTokens != nil {
		if u.CacheCreationInputTokens == nil {
			return fmt.Errorf("completion cache-creation subtotals require the total")
		}
		var subtotal int64
		if u.CacheCreation5mInputTokens != nil {
			subtotal += *u.CacheCreation5mInputTokens
		}
		if u.CacheCreation1hInputTokens != nil {
			subtotal += *u.CacheCreation1hInputTokens
		}
		if subtotal > *u.CacheCreationInputTokens {
			return fmt.Errorf("completion cache-creation subtotals exceed the total")
		}
	}
	if u.InputTokens != nil && (u.CacheReadInputTokens != nil || u.CacheCreationInputTokens != nil) {
		var processedInput int64
		if u.CacheReadInputTokens != nil {
			processedInput += *u.CacheReadInputTokens
		}
		if u.CacheCreationInputTokens != nil {
			processedInput += *u.CacheCreationInputTokens
		}
		if processedInput > *u.InputTokens {
			return fmt.Errorf("completion cache input exceeds total input tokens")
		}
	}
	return nil
}

type CompletionAgentTurn struct {
	TurnID              string
	RunID               string
	AgentID             string
	SessionID           string
	Memory              agentmemory.Plan
	FlowInstance        string
	EntityID            string
	TriggerEventID      string
	TriggerEventType    string
	TaskID              string
	CapabilitySurfaceID string
	CapabilitySurface   json.RawMessage
	ToolCalls           json.RawMessage
	EmittedEvents       json.RawMessage
	RequestPayload      json.RawMessage
	ResponsePayload     json.RawMessage
	TurnBlocks          json.RawMessage
	ParseOK             bool
	LatencyMS           int
	RetryCount          int
	Failure             *runtimefailures.Envelope
}

func (t CompletionAgentTurn) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(t.TurnID)); err != nil {
		return fmt.Errorf("completion agent turn id is invalid: %w", err)
	}
	if strings.TrimSpace(t.AgentID) == "" || strings.TrimSpace(t.SessionID) == "" || strings.TrimSpace(t.RunID) == "" {
		return fmt.Errorf("completion agent turn requires run, agent, and session identity")
	}
	memory, err := t.Memory.Normalize()
	if err != nil {
		return fmt.Errorf("completion agent turn memory plan: %w", err)
	}
	if memory.Enabled && strings.TrimSpace(t.FlowInstance) == "" {
		return fmt.Errorf("memory-enabled completion agent turn requires flow instance identity")
	}
	if _, err := uuid.Parse(strings.TrimSpace(t.CapabilitySurfaceID)); err != nil || len(t.CapabilitySurface) == 0 {
		return fmt.Errorf("completion agent turn requires exact managed capability surface")
	}
	if _, err := uuid.Parse(strings.TrimSpace(t.SessionID)); err != nil {
		return fmt.Errorf("completion agent turn session id is invalid: %w", err)
	}
	if t.LatencyMS < 0 || t.RetryCount < 0 {
		return fmt.Errorf("completion agent turn latency and retry count cannot be negative")
	}
	return nil
}

type CompletionSpend struct {
	EntityID       string
	FlowInstance   string
	AgentID        string
	Model          string
	ModelAlias     string
	BackendProfile string
	Provider       string
	Transport      string
	ResolvedModel  string
	CostUSD        float64
	InvocationType string
}

func (s CompletionSpend) Validate() error {
	if strings.TrimSpace(s.FlowInstance) == "" || strings.TrimSpace(s.AgentID) == "" || strings.TrimSpace(s.Model) == "" ||
		strings.TrimSpace(s.BackendProfile) == "" || strings.TrimSpace(s.Provider) == "" || strings.TrimSpace(s.Transport) == "" ||
		strings.TrimSpace(s.ResolvedModel) == "" || strings.TrimSpace(s.InvocationType) == "" {
		return fmt.Errorf("completion spend requires complete provider and invocation identity")
	}
	if s.CostUSD < 0 {
		return fmt.Errorf("completion spend cost cannot be negative")
	}
	return nil
}

type CompletionSettlement struct {
	Settlement   Settlement
	Usage        CompletionUsage
	AgentTurn    *CompletionAgentTurn
	Spend        CompletionSpend
	ProviderHead *CompletionProviderHead
	Now          time.Time
}

// CompletionSettlementResult is selected-store truth about a terminal
// settlement. Committed may be true with a non-nil error when the transaction
// deliberately committed an outcome-uncertain provider-head conflict.
type CompletionSettlementResult struct {
	Committed     bool
	SpendRecorded bool
	AttemptID     string
	EntityID      string
}

type CompletionSpendProjection struct {
	AttemptID string
	EntityID  string
}

// CompletionSpendProjector refreshes runtime guardrail state from committed
// spend. It is a projection consumer and must never write accounting facts.
type CompletionSpendProjector interface {
	ProjectCommittedCompletionSpend(context.Context, CompletionSpendProjection)
}

type CompletionProviderHead struct {
	Identity             agentmemory.Identity
	SessionID            string
	LockOwner            string
	ExpectedProviderHead string
	NewProviderHead      string
}

func (s CompletionSettlement) Validate(attempt Attempt) error {
	if attempt.AttemptID == "" || attempt.Authority.ValidateCompletionAdapter(attempt.Adapter) != nil {
		return fmt.Errorf("completion settlement requires a valid completion attempt")
	}
	if err := s.Usage.Validate(); err != nil {
		return err
	}
	if err := s.Spend.Validate(); err != nil {
		return err
	}
	switch attempt.Authority.Target.Kind {
	case UsageTargetAgentTurn:
		if s.AgentTurn == nil {
			return fmt.Errorf("agent-turn completion settlement requires turn evidence")
		}
		if err := s.AgentTurn.Validate(); err != nil {
			return err
		}
		target := attempt.Authority.Target
		if !nonEmpty(target.AgentID, target.SessionID, target.RunID) {
			return fmt.Errorf("agent-turn completion target requires exact run, actor, and session coordinates")
		}
		targetMemory, err := target.Memory.Normalize()
		if err != nil {
			return fmt.Errorf("completion target memory plan: %w", err)
		}
		turnMemory, err := s.AgentTurn.Memory.Normalize()
		if err != nil {
			return fmt.Errorf("completion agent turn memory plan: %w", err)
		}
		if targetMemory != turnMemory {
			return fmt.Errorf("completion target memory does not match agent turn evidence")
		}
		for _, coordinate := range []struct {
			name       string
			target     string
			turnRecord string
		}{
			{"turn_id", target.ID, s.AgentTurn.TurnID},
			{"agent_id", target.AgentID, s.AgentTurn.AgentID},
			{"session_id", target.SessionID, s.AgentTurn.SessionID},
			{"run_id", target.RunID, s.AgentTurn.RunID},
			{"flow_instance", target.FlowInstance, s.AgentTurn.FlowInstance},
			{"entity_id", target.EntityID, s.AgentTurn.EntityID},
		} {
			if strings.TrimSpace(coordinate.turnRecord) != strings.TrimSpace(coordinate.target) {
				return fmt.Errorf("completion target %s does not match agent turn evidence", coordinate.name)
			}
		}
	case UsageTargetConversationForkCompletion:
		if s.AgentTurn != nil {
			return fmt.Errorf("forkchat completion settlement cannot carry an agent turn")
		}
	default:
		return fmt.Errorf("completion settlement target kind %q is invalid", attempt.Authority.Target.Kind)
	}
	if s.Settlement.State != StateSettled && s.Settlement.State != StateTerminalFailure && s.Settlement.State != StateOutcomeUncertain {
		return fmt.Errorf("completion settlement state %q is invalid", s.Settlement.State)
	}
	if s.ProviderHead != nil {
		if attempt.Authority.Kind != AuthorityNormalAgent || s.Settlement.State != StateSettled ||
			!nonEmpty(s.ProviderHead.Identity.RunID, s.ProviderHead.Identity.AgentID, s.ProviderHead.Identity.FlowInstance, s.ProviderHead.SessionID, s.ProviderHead.LockOwner, s.ProviderHead.NewProviderHead) {
			return fmt.Errorf("completion provider-head promotion requires a successful normal-agent settlement and complete lease identity")
		}
		if err := s.ProviderHead.Identity.Validate(); err != nil {
			return fmt.Errorf("completion provider-head identity: %w", err)
		}
	}
	if s.Settlement.State == StateSettled && s.Settlement.Failure != nil {
		return fmt.Errorf("successful completion settlement cannot carry failure")
	}
	if s.Settlement.State != StateSettled && s.Settlement.Failure == nil {
		return fmt.Errorf("failed completion settlement requires failure evidence")
	}
	return nil
}
