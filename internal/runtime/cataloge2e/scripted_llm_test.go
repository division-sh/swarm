package cataloge2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

type scriptedLLMRuntime struct {
	mu             sync.Mutex
	responses      map[string]llm.Response
	agentEventFlow map[string][]scriptedAgentFixtureStep
	runBarriers    map[string]*scriptedManagedRunBarrier
}

func newScriptedLLMRuntime() *scriptedLLMRuntime {
	return &scriptedLLMRuntime{
		responses:      map[string]llm.Response{},
		agentEventFlow: map[string][]scriptedAgentFixtureStep{},
		runBarriers:    map[string]*scriptedManagedRunBarrier{},
	}
}

type scriptedManagedRunBarrier struct {
	started chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (r *scriptedLLMRuntime) SetManagedRunBarrier(runID string, started chan<- struct{}, release <-chan struct{}) {
	if r == nil {
		return
	}
	runID = strings.TrimSpace(runID)
	if runID == "" || started == nil || release == nil {
		return
	}
	r.mu.Lock()
	r.runBarriers[runID] = &scriptedManagedRunBarrier{started: started, release: release}
	r.mu.Unlock()
}

type scriptedAgentFixtureStep struct {
	On    string
	Emits []agentFixtureEmit
}

func (r *scriptedLLMRuntime) SetResponse(agentID, key string, response llm.Response) {
	if r == nil {
		return
	}
	agentID = strings.TrimSpace(agentID)
	key = strings.TrimSpace(key)
	if agentID == "" || key == "" {
		return
	}
	r.mu.Lock()
	r.responses[agentID+"::"+key] = response
	r.mu.Unlock()
}

func (r *scriptedLLMRuntime) SetAgentFixture(agentID string, step scriptedAgentFixtureStep) {
	if r == nil {
		return
	}
	agentID = strings.TrimSpace(agentID)
	step.On = strings.TrimSpace(step.On)
	if agentID == "" || step.On == "" {
		return
	}
	r.mu.Lock()
	r.agentEventFlow[agentID] = append(r.agentEventFlow[agentID], step)
	r.mu.Unlock()
}

func (r *scriptedLLMRuntime) StartSession(ctx context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	if r == nil {
		return nil, fmt.Errorf("scripted llm runtime is nil")
	}
	session := &llm.Session{
		ID:           uuid.NewString(),
		AgentID:      strings.TrimSpace(agentID),
		SystemPrompt: systemPrompt,
		Tools:        append([]llm.ToolDefinition(nil), tools...),
	}
	if execution, ok := agentmemory.FromContext(ctx); ok {
		plan, err := execution.Plan.Normalize()
		if err != nil {
			return nil, err
		}
		session.Memory = plan
		session.MemoryIdentity = execution.Identity.Normalize()
	}
	return session, nil
}

func (*scriptedLLMRuntime) ProviderContract() llm.ProviderContract {
	return llm.AnthropicAPIProviderContract()
}

func (*scriptedLLMRuntime) PersistConversationSnapshot(context.Context, *llm.Session) error {
	return nil
}

func (*scriptedLLMRuntime) PrepareManagedSession(context.Context, *llm.Session) error {
	return nil
}

func (r *scriptedLLMRuntime) ContinueManagedSession(ctx context.Context, session *llm.Session, call llm.ManagedCall) (*llm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	frame := call.Frame()
	eventType := strings.TrimSpace(frame.Turn.Event.Type)
	entityID := strings.TrimSpace(frame.Turn.Event.EntityID)
	if r == nil {
		return nil, fmt.Errorf("scripted llm runtime is nil")
	}
	agentID := ""
	if session != nil {
		agentID = strings.TrimSpace(session.AgentID)
		session.Messages = append(session.Messages, message)
		session.TurnCount++
	}
	key := strings.TrimSpace(message.Content)
	r.mu.Lock()
	response, ok := r.responses[agentID+"::"+key]
	steps := append([]scriptedAgentFixtureStep(nil), r.agentEventFlow[agentID]...)
	barrier := r.runBarriers[strings.TrimSpace(frame.Turn.Event.RunID)]
	r.mu.Unlock()
	if barrier != nil {
		barrier.once.Do(func() { barrier.started <- struct{}{} })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-barrier.release:
		}
	}
	if !ok {
		if response, ok = scriptedResponseForEvent(steps, eventType, entityID); !ok {
			if response, ok = defaultScriptedResponseForTools(session, eventType); !ok {
				response = llm.Response{
					Message: llm.Message{Role: "assistant", Content: ""},
				}
			}
		}
	}
	if session != nil {
		session.Messages = append(session.Messages, response.Message)
	}
	if surface, ok := managedcapabilities.FromContext(ctx); ok {
		var deliveredTools []llm.ToolDefinition
		if session != nil {
			deliveredTools = session.Tools
		}
		observed, err := llm.ObserveAPIRequestCapabilitySurface(surface, deliveredTools)
		if err != nil {
			return nil, err
		}
		response.CapabilitySurface = &observed
	}
	if len(response.ToolCalls) > 0 {
		response.ToolOutputAuthority = &llm.ToolOutputAuthority{
			ProviderOperationID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("catalog-scripted-provider-operation:"+frame.FrameID)).String(),
			SettledAt:           time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC),
		}
	}
	return &response, nil
}

func (r *scriptedLLMRuntime) ContinueForkChatSession(ctx context.Context, session *llm.Session, call llm.ForkChatCall) (*llm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: "noop: " + message.Content}}, nil
}

func scriptedResponseForEvent(steps []scriptedAgentFixtureStep, eventType, entityID string) (llm.Response, bool) {
	if eventType == "" {
		return llm.Response{}, false
	}
	for _, step := range steps {
		if strings.TrimSpace(step.On) != eventType {
			continue
		}
		calls := make([]llm.ToolCall, 0, len(step.Emits))
		for _, emit := range step.Emits {
			payload := substituteFixturePayload(emit.Payload, entityID)
			calls = append(calls, llm.ToolCall{
				Name:      runtimetools.EmitToolName(emit.Event),
				Arguments: payload,
			})
		}
		return llm.Response{
			Message:   llm.Message{Role: "assistant", Content: ""},
			ToolCalls: calls,
		}, true
	}
	return llm.Response{}, false
}

func defaultScriptedResponseForTools(session *llm.Session, eventType string) (llm.Response, bool) {
	if session == nil {
		return llm.Response{}, false
	}
	if strings.TrimSpace(eventType) == "" {
		return llm.Response{}, false
	}
	emitTools := make([]string, 0, len(session.Tools))
	for _, tool := range session.Tools {
		name := strings.TrimSpace(tool.Name)
		if strings.HasPrefix(name, "emit_") {
			emitTools = append(emitTools, name)
		}
	}
	if len(emitTools) != 1 {
		return llm.Response{}, false
	}
	return llm.Response{
		Message: llm.Message{Role: "assistant", Content: ""},
		ToolCalls: []llm.ToolCall{{
			Name:      emitTools[0],
			Arguments: map[string]any{},
		}},
	}, true
}

func substituteFixturePayload(payload map[string]any, entityID string) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return cloneFixtureMap(payload)
	}
	replaced := strings.ReplaceAll(string(raw), "{{entity_id}}", strings.TrimSpace(entityID))
	var out map[string]any
	if err := json.Unmarshal([]byte(replaced), &out); err != nil {
		return cloneFixtureMap(payload)
	}
	return out
}

func cloneFixtureMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
