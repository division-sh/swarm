package engine

import (
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type BaseContext = values.Context

type ContextOverlay struct {
	Payload     map[string]any
	Accumulated map[string]any
	FanOut      map[string]any
}

type ContextBuilderInput struct {
	Source  semanticview.Source
	FlowID  string
	State   StateSnapshot
	Event   events.Event
	Payload map[string]any
}

func BuildBaseContext(input ContextBuilderInput) BaseContext {
	base := values.NewContext()
	materializedMetadata := entityruntime.MaterializeMetadataForFlow(input.Source, input.FlowID, input.State.StateCarrier.Metadata)
	materializedState := input.State
	materializedState.StateCarrier.Metadata = materializedMetadata
	base.Entity = values.Wrap(materializedState.EntityContext())
	base.PlatformEntity = values.Wrap(materializedState.PlatformEntityContext(contextFlowInstance(input.State, input.Event, input.FlowID)))
	base.FlowID = firstNonEmpty(strings.TrimSpace(input.State.WorkflowName), strings.TrimSpace(input.FlowID))
	base.Metadata = values.Wrap(cloneStringAnyMap(input.State.StateCarrier.Metadata))
	base.Gates = values.Wrap(boolMapToAnyMap(input.State.StateCarrier.Gates))
	base.Event = values.Wrap(input.Event.ContextMap(input.State.CurrentState))
	base.Payload = values.Wrap(cloneStringAnyMap(input.Payload))
	if input.Source != nil {
		base.Policy = values.Wrap(policyDocumentToMap(input.Source.ResolvedPolicyForFlow(input.FlowID)))
	}
	return base
}

func contextFlowInstance(state StateSnapshot, evt events.Event, fallbackFlowID string) string {
	return firstNonEmpty(
		normalizedContextFlowInstance(asString(state.StateCarrier.Metadata["flow_path"])),
		normalizedContextFlowInstance(evt.FlowInstance()),
		normalizedContextFlowInstance(fallbackFlowID),
	)
}

func normalizedContextFlowInstance(value string) string {
	return strings.Trim(strings.TrimSpace(value), "/")
}

func WithPayload(base BaseContext, payload map[string]any) BaseContext {
	base.Payload = values.Wrap(cloneStringAnyMap(payload))
	return base
}

func WithEvent(base BaseContext, event map[string]any) BaseContext {
	base.Event = values.Wrap(cloneStringAnyMap(event))
	return base
}

func WithAccumulated(base BaseContext, accumulated map[string]any) BaseContext {
	base.Accumulated = values.Wrap(cloneStringAnyMap(accumulated))
	return base
}

func WithFanOutItem(base BaseContext, fanOut map[string]any) BaseContext {
	base.FanOut = values.Wrap(cloneStringAnyMap(fanOut))
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
