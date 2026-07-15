package llm

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

type AgentTurnRecord struct {
	AgentID           string
	Memory            agentmemory.Plan
	SessionID         string
	RunID             string
	EntityID          string
	FlowInstance      string
	TriggerEventID    string
	TriggerEventType  string
	CapabilitySurface *managedcapabilities.Surface
	ToolCalls         []ToolCall
	EmittedEvents     []string
	FlightRecorder    []runtimebus.FlightRecorderEntry
	TaskID            string
	RequestPayload    []byte
	ResponseRaw       []byte
	TurnBlocks        []TurnBlock
	ParseOK           bool
	Latency           time.Duration
	RetryCount        int
	Failure           *runtimefailures.Envelope
}

func enrichTurnRecord(ctx context.Context, s *Session, rec AgentTurnRecord, resp *Response) AgentTurnRecord {
	if s != nil {
		if strings.TrimSpace(rec.SessionID) == "" {
			rec.SessionID = strings.TrimSpace(s.ID)
		}
		rec.Memory = s.Memory
		if strings.TrimSpace(rec.RunID) == "" {
			rec.RunID = s.MemoryIdentity.RunID
		}
		if strings.TrimSpace(rec.FlowInstance) == "" {
			rec.FlowInstance = s.MemoryIdentity.FlowInstance
		}
	}
	if strings.TrimSpace(rec.RunID) == "" {
		rec.RunID = strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	}
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok && strings.TrimSpace(rec.EntityID) == "" {
		// A managed turn belongs to the executing actor. The inbound event entity
		// can identify a cross-flow sender and is only a fallback for unmanaged
		// persistence paths without actor context.
		rec.EntityID = strings.TrimSpace(actor.EffectiveEntityID())
	}
	if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
		if strings.TrimSpace(rec.TriggerEventID) == "" {
			rec.TriggerEventID = strings.TrimSpace(inbound.ID())
		}
		if strings.TrimSpace(rec.TriggerEventType) == "" {
			rec.TriggerEventType = strings.TrimSpace(string(inbound.Type()))
		}
		if strings.TrimSpace(rec.EntityID) == "" {
			rec.EntityID = strings.TrimSpace(inbound.EntityID())
		}
		if strings.TrimSpace(rec.FlowInstance) == "" {
			rec.FlowInstance = strings.TrimSpace(inbound.FlowInstance())
		}
	}
	if actor, ok := runtimeactors.ActorFromContext(ctx); ok {
		if strings.TrimSpace(rec.FlowInstance) == "" {
			rec.FlowInstance = strings.TrimSpace(actor.CanonicalFlowPath())
		}
	}
	if resp != nil && len(rec.ToolCalls) == 0 {
		if len(resp.ObservedToolCalls) > 0 {
			rec.ToolCalls = append([]ToolCall(nil), resp.ObservedToolCalls...)
		} else {
			rec.ToolCalls = append([]ToolCall(nil), resp.ToolCalls...)
		}
	}
	if rec.CapabilitySurface == nil {
		if surface, ok := capabilitySurfaceForResponse(resp); ok {
			rec.CapabilitySurface = &surface
		} else if surface, ok := managedcapabilities.FromContext(ctx); ok {
			rec.CapabilitySurface = &surface
		}
	}
	if len(rec.EmittedEvents) == 0 {
		if eventRec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && eventRec != nil {
			seen := map[string]struct{}{}
			for _, evt := range eventRec.Snapshot() {
				name := strings.TrimSpace(string(evt.Type()))
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
	return rec
}

const runtimeToolsMCPPrefix = "mcp__runtime-tools__"

type ConversationRecord struct {
	SessionID            string
	AgentID              string
	Identity             agentmemory.Identity
	Memory               agentmemory.Plan
	RetryReason          string
	RetriesFromSessionID string
	Watchdog             *ConversationWatchdog
	RunID                string
	TaskID               string
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
	SessionID string
	AgentID   string
	Identity  agentmemory.Identity
	Watchdog  *ConversationWatchdog
}

type ConversationPersistence interface {
	UpsertConversation(ctx context.Context, rec ConversationRecord) error
	UpdateLiveSessionWatchdog(ctx context.Context, update ConversationWatchdogUpdate) error
}

type LiveSessionAcquirer interface {
	AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, ConversationRecord, error)
}
