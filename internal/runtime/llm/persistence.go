package llm

import (
	"context"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

type AgentTurnRecord struct {
	AgentID          string
	RuntimeMode      string
	SessionID        string
	ScopeKey         string
	RunID            string
	EntityID         string
	TriggerEventID   string
	TriggerEventType string
	AvailableTools   []string
	ToolCalls        []ToolCall
	EmittedEvents    []string
	FlightRecorder   []runtimebus.FlightRecorderEntry
	MCPServers       map[string]string
	MCPToolsListed   []string
	MCPToolsVisible  []string
	TaskID           string
	RequestPayload   []byte
	ResponseRaw      []byte
	TurnBlocks       []TurnBlock
	ParseOK          bool
	Latency          time.Duration
	RetryCount       int
	Error            string
}

type TurnPersistence interface {
	AppendAgentTurn(ctx context.Context, rec AgentTurnRecord) error
}

func enrichTurnRecord(ctx context.Context, s *Session, rec AgentTurnRecord, resp *Response) AgentTurnRecord {
	actor, _ := runtimeactors.ActorFromContext(ctx)
	if s != nil {
		if strings.TrimSpace(rec.SessionID) == "" {
			rec.SessionID = strings.TrimSpace(s.ID)
		}
		if strings.TrimSpace(rec.ScopeKey) == "" {
			rec.ScopeKey = strings.TrimSpace(s.ScopeKey)
		}
		if len(rec.MCPToolsListed) == 0 {
			rec.MCPToolsListed = mcpListedToolsForSession(actor, s.Tools)
		}
	}
	if strings.TrimSpace(rec.RunID) == "" {
		rec.RunID = strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	}
	if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
		if strings.TrimSpace(rec.TriggerEventID) == "" {
			rec.TriggerEventID = strings.TrimSpace(inbound.ID)
		}
		if strings.TrimSpace(rec.TriggerEventType) == "" {
			rec.TriggerEventType = strings.TrimSpace(string(inbound.Type))
		}
		if strings.TrimSpace(rec.EntityID) == "" {
			rec.EntityID = strings.TrimSpace(inbound.EntityID())
		}
	}
	if resp != nil && len(rec.ToolCalls) == 0 {
		if len(resp.ObservedToolCalls) > 0 {
			rec.ToolCalls = append([]ToolCall(nil), resp.ObservedToolCalls...)
		} else {
			rec.ToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
		}
	}
	if resp != nil && len(rec.AvailableTools) == 0 {
		rec.AvailableTools = append([]string(nil), resolvedCLIUsableToolsForTurn(actor, sessionTools(s), resp)...)
	}
	if resp != nil && len(rec.MCPToolsVisible) == 0 {
		rec.MCPToolsVisible = append([]string(nil), resp.MCPVisibleTools...)
	}
	if resp != nil && len(rec.MCPServers) == 0 && len(resp.MCPServers) > 0 {
		rec.MCPServers = map[string]string{}
		for name, status := range resp.MCPServers {
			name = strings.TrimSpace(name)
			status = strings.TrimSpace(status)
			if name != "" && status != "" {
				rec.MCPServers[name] = status
			}
		}
	}
	if len(rec.EmittedEvents) == 0 {
		if eventRec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && eventRec != nil {
			seen := map[string]struct{}{}
			for _, evt := range eventRec.Snapshot() {
				name := strings.TrimSpace(string(evt.Type))
				if name == "" {
					continue
				}
				if _, ok := seen[name]; ok {
					continue
				}
				seen[name] = struct{}{}
				rec.EmittedEvents = append(rec.EmittedEvents, name)
			}
		}
	}
	if len(rec.FlightRecorder) == 0 {
		if eventRec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && eventRec != nil {
			rec.FlightRecorder = append([]runtimebus.FlightRecorderEntry(nil), eventRec.SnapshotFlightRecorder()...)
		}
	}
	if len(rec.AvailableTools) == 0 && s != nil {
		rec.AvailableTools = append([]string(nil), plannedCanonicalVisibleToolsForActor(actor, s.Tools)...)
	}
	return rec
}

func sessionTools(s *Session) []ToolDefinition {
	if s == nil {
		return nil
	}
	return s.Tools
}

func mcpListedToolsForSession(actor runtimeactors.AgentConfig, tools []ToolDefinition) []string {
	if len(tools) == 0 {
		return nil
	}
	surface := cliExecutionToolSurfaceForActor(actor, tools)
	return append([]string(nil), surface.ProviderMCPTools...)
}

const runtimeToolsMCPPrefix = "mcp__runtime-tools__"

type ConversationRecord struct {
	SessionID            string
	AgentID              string
	SessionScope         string
	ScopeKey             string
	RetryReason          string
	RetriesFromSessionID string
	Watchdog             *ConversationWatchdog
	RunID                string
	TaskID               string
	Mode                 string
	Messages             []Message
	Summary              string
	TurnCount            int
	Status               string
}

type ConversationWatchdog struct {
	State         string
	BlockingLayer string
	Action        string
	Outcome       string
	LastOutputAt  string
	RecordedAt    string
}

type ConversationWatchdogUpdate struct {
	SessionID    string
	AgentID      string
	SessionScope string
	ScopeKey     string
	Mode         string
	Watchdog     *ConversationWatchdog
}

type ConversationPersistence interface {
	UpsertConversation(ctx context.Context, rec ConversationRecord) error
	LoadActiveConversation(ctx context.Context, agentID, mode, sessionScope, scopeKey string) (ConversationRecord, bool, error)
	UpdateLiveSessionWatchdog(ctx context.Context, update ConversationWatchdogUpdate) error
}
