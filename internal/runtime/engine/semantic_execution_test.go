package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

const semanticExecutionFixtureRunID = "00000000-0000-0000-0000-000000000001"

// ExecuteSemanticFixture completes the durable facts that engine unit tests
// intentionally omit while keeping the production executor fail-closed.
func (e *Executor) ExecuteSemanticFixture(ctx context.Context, req ExecutionRequest) (ExecutionResult, error) {
	if req.Event.AdmissionClass() == events.EventAdmissionRootIngress && strings.TrimSpace(req.Event.RunID()) == "" {
		if req.Event.ProducerType() == "" {
			return ExecutionResult{}, fmt.Errorf("complete engine root fixture: producer type is required")
		}
		req.Event = eventtest.RunCreatingRootIngressWithMode(
			req.Event.ID(), req.Event.Type(), req.Event.Producer().ID(), req.Event.TaskID(),
			req.Event.Payload(), req.Event.ChainDepth(), semanticExecutionFixtureRunID, "",
			req.Event.NormalizedEnvelope(), req.Event.CreatedAt(), req.Event.ExecutionMode(),
		)
	}
	if req.ProducerRoute.Normalized().Empty() {
		flowID := strings.TrimSpace(req.FlowID.String())
		entityID := strings.TrimSpace(req.EntityID.String())
		if entityID == "" {
			entityID = strings.TrimSpace(req.State.EntityID.String())
		}
		flowInstance := strings.Trim(strings.TrimSpace(asString(req.State.StateCarrier.Metadata["flow_path"])), "/")
		if flowInstance == "" {
			flowInstance = flowID
		}
		if flowID != "" && flowInstance != "" && entityID != "" {
			req.ProducerRoute = events.RouteIdentity{
				FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID,
			}.Normalized()
		}
	}
	return e.Execute(ctx, req)
}
