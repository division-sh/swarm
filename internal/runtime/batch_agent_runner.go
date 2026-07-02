package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type llmBatchAgentRunner struct {
	modelRuntime   runtimellm.Runtime
	source         semanticview.Source
	promptResolver runtimecontracts.PromptResolver
}

func newLLMBatchAgentRunner(modelRuntime runtimellm.Runtime, source semanticview.Source, promptResolver runtimecontracts.PromptResolver) runtimeengine.BatchAgentRunner {
	return llmBatchAgentRunner{
		modelRuntime:   modelRuntime,
		source:         source,
		promptResolver: promptResolver,
	}
}

func (r llmBatchAgentRunner) InvokeBatchAgent(ctx context.Context, req runtimeengine.BatchAgentRequest) (runtimeengine.BatchAgentResponse, error) {
	if r.modelRuntime == nil {
		return runtimeengine.BatchAgentResponse{}, fmt.Errorf("batch_agent runner requires an llm runtime")
	}
	agentID := strings.TrimSpace(req.Agent)
	if agentID == "" {
		return runtimeengine.BatchAgentResponse{}, fmt.Errorf("batch_agent.agent is required")
	}
	entry, ok := semanticview.FindAgentEntry(r.source, agentID, agentID)
	if !ok {
		return runtimeengine.BatchAgentResponse{}, fmt.Errorf("batch_agent agent %q not found", agentID)
	}
	cfg := batchAgentConfig(agentID, entry)
	prompt, found, err := batchAgentPrompt(r.promptResolver, cfg)
	if err != nil {
		return runtimeengine.BatchAgentResponse{}, err
	}
	if !found {
		prompt = defaultBatchAgentPrompt()
	}
	maxTurns := cfg.MaxTurnsPerTask
	if maxTurns <= 0 {
		maxTurns = 1
	}
	taskID := strings.Join(nonEmptyBatchAgentStrings("batch_agent", req.FlowID, req.NodeID, agentID), ":")
	conv := runtimellm.NewConversation(cfg.ID, taskID, prompt, nil, runtimellm.TaskScoped, maxTurns, r.modelRuntime)
	requestPayload := map[string]any{
		"operation": "batch_agent",
		"flow_id":   strings.TrimSpace(req.FlowID),
		"node_id":   strings.TrimSpace(req.NodeID),
		"agent":     agentID,
		"items":     req.Items,
		"input":     req.Input,
		"result_contract": map[string]any{
			"items_from":      req.ResultItemsFrom,
			"correlation_key": req.CorrelationKey,
			"required_fields": req.RequiredFields,
		},
	}
	encoded, err := json.Marshal(requestPayload)
	if err != nil {
		return runtimeengine.BatchAgentResponse{}, err
	}
	resp, err := conv.Step(ctx, string(encoded))
	if err != nil {
		return runtimeengine.BatchAgentResponse{}, err
	}
	var decoded any
	if err := json.Unmarshal([]byte(strings.TrimSpace(resp.Message.Content)), &decoded); err != nil {
		return runtimeengine.BatchAgentResponse{}, fmt.Errorf("batch_agent agent %q returned non-json output: %w", agentID, err)
	}
	if _, ok := decoded.(map[string]any); !ok {
		return runtimeengine.BatchAgentResponse{}, fmt.Errorf("batch_agent agent %q must return a JSON object", agentID)
	}
	return runtimeengine.BatchAgentResponse{Output: decoded}, nil
}

func batchAgentConfig(agentID string, entry runtimecontracts.AgentRegistryEntry) runtimeactors.AgentConfig {
	cfg := runtimeactors.AgentConfig{
		ID:              firstBatchAgentNonEmptyString(strings.TrimSpace(entry.ID), agentID),
		Type:            strings.TrimSpace(entry.Type),
		Role:            firstBatchAgentNonEmptyString(strings.TrimSpace(entry.Role), agentID),
		Mode:            strings.TrimSpace(entry.Mode),
		Model:           strings.TrimSpace(entry.Model),
		MaxTurnsPerTask: entry.MaxTurnsPerTask,
		Tools:           append([]string(nil), entry.Tools...),
		Permissions:     append([]string(nil), entry.Permissions...),
		FlowDataAccess:  append([]string(nil), entry.FlowDataAccess...),
		EmitEvents:      append([]string(nil), entry.EmitEvents...),
	}
	cfg.NormalizeRuntimeDescriptor()
	return cfg
}

func batchAgentPrompt(resolver runtimecontracts.PromptResolver, cfg runtimeactors.AgentConfig) (string, bool, error) {
	if resolver == nil {
		return "", false, nil
	}
	return resolver.LoadPromptForAgent(cfg, "")
}

func defaultBatchAgentPrompt() string {
	return strings.TrimSpace(`You are executing a swarm batch_agent operation.
Return one strict JSON object and no markdown.
The object must contain the result list at the requested result_contract.items_from path.
Each result row must include the requested result_contract.correlation_key and every requested required field.`)
}

func nonEmptyBatchAgentStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func firstBatchAgentNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
