package engine

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/identity"
	"empireai/internal/runtime/semanticview"
)

type BaseContext struct {
	Entity     map[string]any
	Policy     map[string]any
	Metadata   map[string]any
	Payload    map[string]any
	Accumulated map[string]any
	FanOut     map[string]any
}

type ContextOverlay struct {
	Payload     map[string]any
	Accumulated map[string]any
	FanOut      map[string]any
}

type ContextBuilderInput struct {
	Source   semanticview.Source
	EntityID identity.EntityID
	FlowID   identity.FlowID
	State    StateSnapshot
	Payload  map[string]any
}

func BuildBaseContext(input ContextBuilderInput) BaseContext {
	entity := cloneStringAnyMap(input.State.Metadata)
	base := BaseContext{
		Entity:      entity,
		Policy:      map[string]any{},
		Metadata:    cloneStringAnyMap(input.State.Metadata),
		Payload:     cloneStringAnyMap(input.Payload),
		Accumulated: map[string]any{},
		FanOut:      map[string]any{},
	}
	if input.Source != nil {
		base.Policy = policyDocumentToMap(input.Source.ResolvedPolicyForFlow(input.FlowID.String()))
	}
	base.Entity["entity_id"] = input.EntityID.String()
	base.Entity["current_state"] = strings.TrimSpace(input.State.CurrentState)
	base.Entity["workflow_name"] = strings.TrimSpace(input.State.WorkflowName)
	base.Entity["workflow_version"] = strings.TrimSpace(input.State.WorkflowVersion)
	return base
}

func WithPayload(base BaseContext, payload map[string]any) BaseContext {
	base.Payload = cloneStringAnyMap(payload)
	return base
}

func WithAccumulated(base BaseContext, accumulated map[string]any) BaseContext {
	base.Accumulated = cloneStringAnyMap(accumulated)
	return base
}

func WithFanOutItem(base BaseContext, fanOut map[string]any) BaseContext {
	base.FanOut = cloneStringAnyMap(fanOut)
	return base
}

func policyDocumentToMap(doc runtimecontracts.PolicyDocument) map[string]any {
	if len(doc.Values) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(doc.Values))
	for key, value := range doc.Values {
		out[key] = value.Value
	}
	return out
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
