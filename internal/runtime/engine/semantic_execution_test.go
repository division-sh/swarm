package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/contractelementidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const semanticExecutionFixtureRunID = "00000000-0000-0000-0000-000000000001"

// ExecuteSemanticFixture completes the durable facts that engine unit tests
// intentionally omit while keeping the production executor fail-closed.
func (e *Executor) ExecuteSemanticFixture(ctx context.Context, req ExecutionRequest) (ExecutionResult, error) {
	var err error
	req.Handler, err = completeSemanticFixtureHandlerRuleIdentity(req.Node, req.Handler)
	if err != nil {
		return ExecutionResult{}, err
	}
	hasFanOut := len(runtimecontracts.HandlerFanOutSites(req.Handler)) > 0
	if strings.TrimSpace(req.ExecutionFlowID.String()) == "" {
		flowID := strings.TrimSpace(req.Node.FlowID())
		if flowID == "" && e != nil && e.deps.Source != nil {
			flowID = strings.TrimSpace(e.deps.Source.WorkflowName())
		}
		if flowID == "" && hasFanOut {
			flowID = "root"
		}
		req.ExecutionFlowID = identity.NormalizeFlowID(flowID)
	}
	if strings.TrimSpace(req.HandlerEventKey) == "" {
		req.HandlerEventKey = string(req.Event.Type())
	}
	if hasFanOut {
		bundle, ok := semanticview.Bundle(e.deps.Source)
		if !ok || bundle == nil {
			return ExecutionResult{}, fmt.Errorf("complete fan-out fixture: contract bundle is required")
		}
		if err := bundle.CompileFanOutHandlerPlans(req.Node, req.HandlerEventKey, req.Handler); err != nil {
			return ExecutionResult{}, fmt.Errorf("%w: complete fan-out fixture plan: %v", ErrInvalidConfig, err)
		}
		req.FanOutPlans = bundle.FanOutPlansForHandler(req.Node, req.HandlerEventKey)
	}
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
	if req.ProducerSource.Empty() {
		flowID := strings.TrimSpace(req.Node.FlowID())
		entityID := strings.TrimSpace(req.EntityID.String())
		if entityID == "" {
			entityID = strings.TrimSpace(req.State.EntityID.String())
		}
		if entityID == "" {
			entityID = eventtest.UUID("engine-semantic-producer")
		}
		flowInstance := strings.Trim(strings.TrimSpace(asString(req.State.StateCarrier.Fields["flow_path"])), "/")
		if flowID == "" {
			source, err := events.NewRootRoutingSource(entityID)
			if err != nil {
				return ExecutionResult{}, fmt.Errorf("complete engine root producer source fixture: %w", err)
			}
			req.ProducerSource = source
		} else {
			if flowInstance == "" {
				flowInstance = flowID
			}
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
				FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID,
			})
			if err != nil {
				return ExecutionResult{}, fmt.Errorf("complete engine producer source fixture: %w", err)
			}
			req.ProducerSource = source
		}
	}
	if hasFanOut && !req.Route.Valid() {
		scope := strings.Trim(strings.TrimSpace(req.ExecutionFlowID.String()), "/")
		if scope == "" {
			scope = "root"
		}
		req.Route = runtimeflowidentity.DeriveRoute(scope, req.Event.RunID())
	}
	if hasFanOut {
		if _, claimed := runtimedelivery.ClaimFromContext(ctx); !claimed {
			deliveryID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("engine-fan-out-delivery\x00"+req.Event.ID()+"\x00"+req.Node.Key())).String()
			token := uuid.NewSHA1(uuid.NameSpaceOID, []byte("engine-fan-out-claim\x00"+deliveryID)).String()
			claim, claimErr := runtimedelivery.AdmitPersistedClaim(
				deliveryID,
				req.Event.RunID(),
				"engine-semantic-fixture:"+req.Node.Key(),
				token,
				1,
				runtimedelivery.SubscriberNode,
				req.Node.Key(),
			)
			if claimErr != nil {
				return ExecutionResult{}, claimErr
			}
			ctx = runtimedelivery.WithClaim(ctx, claim)
		}
	}
	return e.Execute(ctx, req)
}

func completeSemanticFixtureHandlerRuleIdentity(node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler) (runtimecontracts.SystemNodeEventHandler, error) {
	admitFanOut := func(context string, index int, fanOut *runtimecontracts.FanOutSpec) (*runtimecontracts.FanOutSpec, error) {
		if fanOut == nil {
			return nil, nil
		}
		admitted := *fanOut
		if !admitted.ElementID.Valid() {
			value := uuid.NewSHA1(uuid.NameSpaceOID, []byte(node.Key()+"\x00fan_out\x00"+context+"\x00"+fmt.Sprint(index))).String()
			var err error
			admitted.ElementID, err = contractelementidentity.ParseContractElementID(value)
			if err != nil {
				return nil, err
			}
		}
		return &admitted, nil
	}
	admit := func(context string, index int, rule runtimecontracts.HandlerRuleEntry) (runtimecontracts.HandlerRuleEntry, error) {
		if !rule.ElementID.Valid() {
			value := uuid.NewSHA1(uuid.NameSpaceOID, []byte(node.Key()+"\x00"+context+"\x00"+fmt.Sprint(index))).String()
			var err error
			rule.ElementID, err = contractelementidentity.ParseContractElementID(value)
			if err != nil {
				return runtimecontracts.HandlerRuleEntry{}, err
			}
		}
		var admitted runtimecontracts.HandlerRuleEntry
		if err := yaml.Unmarshal([]byte("element_id: "+rule.ElementID.String()+"\n"), &admitted); err != nil {
			return runtimecontracts.HandlerRuleEntry{}, err
		}
		admitted.ID = rule.ID
		admitted.Description = rule.Description
		admitted.Condition = rule.Condition
		admitted.PolicyRow = rule.PolicyRow
		admitted.AdvancesTo = rule.AdvancesTo
		admitted.Emit = rule.Emit
		admitted.Action = rule.Action
		admitted.Activity = rule.Activity
		admitted.DataAccumulation = rule.DataAccumulation
		admitted.Compute = rule.Compute
		admittedFanOut, fanOutErr := admitFanOut(context, index, rule.FanOut)
		if fanOutErr != nil {
			return runtimecontracts.HandlerRuleEntry{}, fanOutErr
		}
		admitted.FanOut = admittedFanOut
		return admitted, nil
	}
	admitMany := func(context string, rules []runtimecontracts.HandlerRuleEntry) ([]runtimecontracts.HandlerRuleEntry, error) {
		out := append([]runtimecontracts.HandlerRuleEntry(nil), rules...)
		for index := range out {
			var err error
			out[index], err = admit(context, index, out[index])
			if err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	var err error
	handler.Rules, err = admitMany("rules", handler.Rules)
	if err != nil {
		return runtimecontracts.SystemNodeEventHandler{}, err
	}
	handler.FanOut, err = admitFanOut("handler", 0, handler.FanOut)
	if err != nil {
		return runtimecontracts.SystemNodeEventHandler{}, err
	}
	handler.OnComplete, err = admitMany("on_complete", handler.OnComplete)
	if err != nil {
		return runtimecontracts.SystemNodeEventHandler{}, err
	}
	if handler.Join != nil {
		join := *handler.Join
		if join.OnCompleteFound {
			join.OnComplete, err = admit("join_on_complete", 0, join.OnComplete)
			if err != nil {
				return runtimecontracts.SystemNodeEventHandler{}, err
			}
		}
		if join.TimeoutFound {
			join.Timeout.Outcome, err = admit("join_timeout", 0, join.Timeout.Outcome)
			if err != nil {
				return runtimecontracts.SystemNodeEventHandler{}, err
			}
		}
		handler.Join = &join
	}
	return runtimecontracts.QualifySystemNodeHandlerRuleRefs(node, handler)
}
