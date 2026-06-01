package cataloge2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

type scriptedLLMRuntime struct {
	mu             sync.Mutex
	nextID         int
	responses      map[string]llm.Response
	agentEventFlow map[string][]scriptedAgentFixtureStep
}

func newScriptedLLMRuntime() *scriptedLLMRuntime {
	return &scriptedLLMRuntime{
		responses:      map[string]llm.Response{},
		agentEventFlow: map[string][]scriptedAgentFixtureStep{},
	}
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

func (r *scriptedLLMRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	if r == nil {
		return nil, fmt.Errorf("scripted llm runtime is nil")
	}
	r.mu.Lock()
	r.nextID++
	sessionID := fmt.Sprintf("scripted-%s-%d", strings.TrimSpace(agentID), r.nextID)
	r.mu.Unlock()
	return &llm.Session{
		ID:           sessionID,
		AgentID:      strings.TrimSpace(agentID),
		RuntimeMode:  "scripted",
		SystemPrompt: systemPrompt,
		Tools:        append([]llm.ToolDefinition(nil), tools...),
	}, nil
}

func (r *scriptedLLMRuntime) ContinueSession(_ context.Context, session *llm.Session, message llm.Message) (*llm.Response, error) {
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
	r.mu.Unlock()
	if !ok {
		if response, ok = scriptedResponseForMessage(steps, message.Content); !ok {
			if response, ok = defaultScriptedResponseForTools(session, message.Content); !ok {
				response = llm.Response{
					Message: llm.Message{Role: "assistant", Content: ""},
				}
			}
		}
	}
	if session != nil {
		session.Messages = append(session.Messages, response.Message)
	}
	return &response, nil
}

func scriptedResponseForMessage(steps []scriptedAgentFixtureStep, content string) (llm.Response, bool) {
	eventType, entityID := parseAgentEventMessage(content)
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

func defaultScriptedResponseForTools(session *llm.Session, content string) (llm.Response, bool) {
	if session == nil {
		return llm.Response{}, false
	}
	eventType, _ := parseAgentEventMessage(content)
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

func parseAgentEventMessage(content string) (eventType, entityID string) {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "- type:"):
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "- type:"))
		case strings.HasPrefix(line, "- entity_id:"):
			entityID = strings.TrimSpace(strings.TrimPrefix(line, "- entity_id:"))
		}
	}
	return strings.TrimSpace(eventType), strings.TrimSpace(entityID)
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
