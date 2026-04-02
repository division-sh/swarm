package llm

import (
	"context"
	"strings"
	"time"

	runtimebus "swarm/internal/runtime/bus"
	runtimecorrelation "swarm/internal/runtime/correlation"
)

type AgentTurnRecord struct {
	AgentID          string
	RuntimeMode      string
	SessionID        string
	ScopeKey         string
	RunID            string
	TraceID          string
	EntityID         string
	TriggerEventID   string
	TriggerEventType string
	AvailableTools   []string
	ToolCalls        []ToolCall
	EmittedEvents    []string
	MCPServers       map[string]string
	MCPToolsListed   []string
	MCPToolsVisible  []string
	TaskID           string
	RequestPayload   []byte
	ResponseRaw      []byte
	ParseOK          bool
	Latency          time.Duration
	RetryCount       int
	Error            string
}

type TurnPersistence interface {
	AppendAgentTurn(ctx context.Context, rec AgentTurnRecord) error
}

func enrichTurnRecord(ctx context.Context, s *Session, rec AgentTurnRecord, resp *Response) AgentTurnRecord {
	if s != nil {
		if strings.TrimSpace(rec.SessionID) == "" {
			rec.SessionID = strings.TrimSpace(s.ID)
		}
		if strings.TrimSpace(rec.ScopeKey) == "" {
			rec.ScopeKey = strings.TrimSpace(s.ScopeKey)
		}
		if len(rec.AvailableTools) == 0 && len(s.Tools) > 0 {
			rec.AvailableTools = make([]string, 0, len(s.Tools))
			for _, tool := range s.Tools {
				name := strings.TrimSpace(tool.Name)
				if name != "" {
					rec.AvailableTools = append(rec.AvailableTools, name)
				}
			}
		}
		if len(rec.MCPToolsListed) == 0 {
			rec.MCPToolsListed = mcpListedToolsForSession(s.Tools)
		}
	}
	if strings.TrimSpace(rec.TraceID) == "" {
		rec.TraceID = strings.TrimSpace(runtimecorrelation.TraceIDFromContext(ctx))
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
		rec.ToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
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
	return rec
}

func mcpListedToolsForSession(tools []ToolDefinition) []string {
	if len(tools) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		prefixed := runtimeToolsMCPPrefix + name
		if _, ok := seen[prefixed]; ok {
			continue
		}
		seen[prefixed] = struct{}{}
		out = append(out, prefixed)
	}
	return out
}

const runtimeToolsMCPPrefix = "mcp__runtime-tools__"

type ConversationRecord struct {
	SessionID string
	AgentID   string
	ScopeKey  string
	RunID     string
	TaskID    string
	Mode      string
	Messages  []Message
	Summary   string
	TurnCount int
	Status    string
}

type ConversationPersistence interface {
	UpsertConversation(ctx context.Context, rec ConversationRecord) error
	LoadActiveConversation(ctx context.Context, agentID, mode, scopeKey string) (ConversationRecord, bool, error)
}
