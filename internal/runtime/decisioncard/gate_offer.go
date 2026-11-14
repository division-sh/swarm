package decisioncard

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// GateOffer compares issued choices and their complete continuation, not source
// identity or mutable presentation. It grants no execution or mutation authority.
type GateOffer struct {
	value semanticvalue.Value
}

func (o GateOffer) Equal(other GateOffer) bool {
	return o.value.Kind() == semanticvalue.KindObject && other.value.Kind() == semanticvalue.KindObject && o.value.Equal(other.value)
}

func (o GateOffer) RoutesJSON() (string, error) {
	routes, found := o.value.Lookup("routes")
	if !found || routes.Kind() != semanticvalue.KindObject {
		return "", fmt.Errorf("gate offer has no validated continuation")
	}
	raw, err := canonicaljson.Encode(routes)
	return string(raw), err
}

func CompileGateOffer(source semanticview.Source, flowID, stage string) (GateOffer, error) {
	if source == nil {
		return GateOffer{}, fmt.Errorf("gate offer requires its compiled source")
	}
	plan, found := source.WorkflowGateForStage(flowID, stage)
	if !found || plan.FlowID != flowID || plan.Stage != stage {
		return GateOffer{}, fmt.Errorf("gate offer requires its exact declared wait")
	}
	outcomes, err := ResolveGateOutcomes(source, plan)
	if err != nil {
		return GateOffer{}, err
	}
	graph, found := semanticview.WorkflowStageTopology(source, flowID)
	if !found {
		return GateOffer{}, fmt.Errorf("gate offer requires compiled flow topology")
	}
	transitions := make(map[string]contracts.CompiledTransition, len(outcomes))
	for verdict, outcome := range outcomes {
		transitions[verdict], err = graph.AdmitTransition(contracts.WorkflowTransitionSite{DecisionID: plan.Decision, Verdict: verdict}, stage, outcome.AdvancesTo)
		if err != nil {
			return GateOffer{}, err
		}
	}
	routes, err := gateruntime.FreezeRoutes(outcomes, transitions)
	if err != nil {
		return GateOffer{}, err
	}
	snapshot, err := FreezeSnapshot(plan.Decision, "", nil, outcomes)
	if err != nil {
		return GateOffer{}, err
	}
	return gateOffer(flowID, stage, snapshot, routes)
}

func IssuedGateOffer(card Card, activation gateruntime.Activation) (GateOffer, error) {
	if err := card.Validate(); err != nil {
		return GateOffer{}, err
	}
	if err := activation.Validate(); err != nil {
		return GateOffer{}, err
	}
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		return GateOffer{}, err
	}
	if anchor.FlowID != activation.FlowID || anchor.Stage != activation.Stage || anchor.StageActivationID != activation.ActivationID || card.CardID != activation.CardID || card.BundleHash != activation.BundleHash || card.Snapshot.Decision != activation.DecisionID {
		return GateOffer{}, fmt.Errorf("issued gate offer contradicts its occurrence")
	}
	for verdict, outcome := range card.Snapshot.Outcomes {
		route, err := gateruntime.RouteFor(activation.RoutesJSON, verdict)
		if err != nil {
			return GateOffer{}, err
		}
		edge := route.Transition.Edge()
		if route.Transition.FlowID() != anchor.FlowID || edge.From != anchor.Stage || edge.DecisionID != card.Snapshot.Decision || edge.Verdict != outcome.Verdict {
			return GateOffer{}, fmt.Errorf("issued gate continuation contradicts its wait")
		}
		if err := gateruntime.ValidateRoute(verdict, route, outcome.Input); err != nil {
			return GateOffer{}, err
		}
	}
	return gateOffer(anchor.FlowID, anchor.Stage, card.Snapshot, activation.RoutesJSON)
}

func gateOffer(flowID, stage string, snapshot Snapshot, routesJSON string) (GateOffer, error) {
	// Title and context are frozen issuance presentation, not the answer contract.
	snapshot.Title, snapshot.Context = "", semanticvalue.EmptyObject()
	inputs, err := snapshot.SemanticValue()
	if err != nil {
		return GateOffer{}, err
	}
	routes, err := canonicaljson.Decode([]byte(routesJSON))
	if err != nil {
		return GateOffer{}, err
	}
	if members, ok := routes.ObjectMap(); !ok || len(members) != len(snapshot.Outcomes) {
		return GateOffer{}, fmt.Errorf("issued gate choices and continuations differ")
	}
	value, err := semanticObjectWithText(map[string]string{"flow": flowID, "stage": stage}, map[string]semanticvalue.Value{"inputs": inputs, "routes": routes})
	return GateOffer{value: value}, err
}

func ResolveGateOutcomes(source semanticview.Source, plan contracts.WorkflowGatePlan) (map[string]contracts.WorkflowGateOutcomePlan, error) {
	outcomes := make(map[string]contracts.WorkflowGateOutcomePlan, len(plan.Outcomes))
	for verdict, outcome := range plan.Outcomes {
		if eventName := strings.TrimSpace(outcome.Emit.Event); eventName != "" {
			if source == nil {
				return nil, fmt.Errorf("freeze gate %s outcome %s event schema: semantic source is unavailable", plan.Decision, verdict)
			}
			resolution := semanticview.ResolveEventSchema(source, plan.FlowID, eventName)
			if !resolution.HasSchema {
				return nil, fmt.Errorf("freeze gate %s outcome %s event schema: event %s has no resolvable payload schema", plan.Decision, verdict, eventName)
			}
			if err := resolution.UnresolvedTypeError(); err != nil {
				return nil, fmt.Errorf("freeze gate %s outcome %s event schema: %w", plan.Decision, verdict, err)
			}
			outcome.Emit.Event = source.ResolveFlowEventReference(plan.FlowID, eventName)
			schema, err := canonicaljson.FromGo(resolution.Schema.Schema)
			if err != nil {
				return nil, fmt.Errorf("freeze gate %s outcome %s resolved schema: %w", plan.Decision, verdict, err)
			}
			if schema.Kind() != semanticvalue.KindObject {
				return nil, fmt.Errorf("freeze gate %s outcome %s resolved schema is not an object", plan.Decision, verdict)
			}
			outcome.EmitSchema = schema.Interface().(map[string]any)
		}
		outcomes[verdict] = outcome
	}
	return outcomes, nil
}
