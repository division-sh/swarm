package llm

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

var providerParticipantName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func renderManagedMessage(frame agentframe.Frame, seed agentframe.SessionSeed, event events.Event) (Message, error) {
	if err := frame.ValidateCausalEvent(event); err != nil {
		return Message{}, err
	}
	role, content, err := frame.ProviderInput()
	if err != nil {
		return Message{}, err
	}
	message := Message{Role: role, Content: content}
	if frame.Turn.Kind != agentframe.TurnInitial {
		return message, nil
	}
	if event.ProducerType() == events.EventProducerNode {
		node, err := identity.ParseExecutableNodeKey(event.Producer().ID())
		if err != nil || (seed.Provider == llmselection.ProviderOpenAICompatible && !providerParticipantName.MatchString(node.NodeID())) {
			return Message{}, fmt.Errorf("provider participant requires its exact supported producing node name")
		}
		message.Name = node.NodeID()
	}
	if seed.Content == "" {
		return message, nil
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return Message{}, fmt.Errorf("agent content payload: %w", err)
	}
	context := event.ContextMap("")
	if !event.CreatedAt().IsZero() {
		context["at"] = event.CreatedAt().UTC().Truncate(time.Second).Format(time.RFC3339)
	}
	keys := make([]string, 0, len(seed.ContentPrograms))
	for key := range seed.ContentPrograms {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	local := eventidentity.LocalizeForFlow(seed.AgentIdentity.FlowInstance(), keys, string(event.Type()))
	program := seed.ContentPrograms[local]
	if program == nil {
		return Message{}, fmt.Errorf("agent content requires the exact admitted input program for %s", event.Type())
	}
	value, err := workflowexpr.EvalAgentContentExpression(program, workflowexpr.ValueContext{Payload: payload, Event: context})
	if err != nil {
		return Message{}, fmt.Errorf("agent content projection: %w", err)
	}
	if text, ok := value.(string); ok {
		message.Content = text
	} else {
		raw, err := json.Marshal(value)
		if err != nil {
			return Message{}, err
		}
		message.Content = string(raw)
	}
	return message, nil
}
