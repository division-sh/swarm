package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type connectPlanningPreviewKey struct{}

func withConnectPlanningPreview(ctx context.Context) context.Context {
	return context.WithValue(ctx, connectPlanningPreviewKey{}, true)
}

func connectPlanningPreview(ctx context.Context) bool {
	preview, _ := ctx.Value(connectPlanningPreviewKey{}).(bool)
	return preview
}

type connectInstanceSelector struct {
	source      semanticview.Source
	plan        pipeline.FlowInstanceActivationPlanner
	index       pipeline.FlowInstanceIndexReader
	runProposal pipeline.FlowInstanceRunProposal
}

type connectInstanceSelection struct {
	pipeline.FlowInstanceSelection
	KeyDigest                string
	KeyMaterial              []contracts.TemplateInstanceKeyValue
	SourceEventID            string
	receiver                 pinrouting.ConnectRoutePlanEndpoint
	mode                     contracts.FlowInputResolutionMode
	unavailableReceiverError error
}

func (s connectInstanceSelection) Empty() bool { return s.receiver.Readback().FlowID == "" }
func (s connectInstanceSelection) ActivationVariables() map[string]string {
	if s.Activation == nil {
		return nil
	}
	return cloneRouteActivationVariables(s.Activation.ActivationVariables)
}
func (s connectInstanceSelection) ActionCode() string {
	if s.Empty() {
		return ""
	}
	if s.Activation != nil {
		return "would_create"
	}
	if s.mode == contracts.FlowInputResolutionModeSelectOrCreate {
		return "reused"
	}
	return "selected_existing"
}
func (s connectInstanceSelection) Detail() map[string]any {
	if s.Empty() {
		return nil
	}
	keys := make([]map[string]string, 0, len(s.KeyMaterial))
	for _, key := range s.KeyMaterial {
		keys = append(keys, map[string]string{"field": key.Field.Path(), "value": key.Value})
	}
	instance := s.Identity()
	return map[string]any{
		"action": s.ActionCode(), "receiver_flow": s.receiver.Readback().FlowID,
		"instance_id": instance.InstanceID, "instance_path": instance.InstancePath, "entity_id": instance.EntityID,
		"key_digest": s.KeyDigest, "key_material": keys, "source_event_id": s.SourceEventID,
	}
}

func (o connectInstanceSelector) Materialize(ctx context.Context, event events.Event, plan pinrouting.ConnectRoutePlan, values map[string]string) (pinrouting.ConnectRoutePlanMaterialization, connectInstanceSelection, bool, error) {
	if plan.ResolutionKind() != pinrouting.ConnectResolutionInstanceKey || plan.InstanceKey() == nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, false, nil
	}
	_, failure := instanceKeyMaterialForConnectSelection(event, plan, values)
	if !failure.Empty() {
		return pinrouting.ConnectRoutePlanMaterialization{Failure: failure}, connectInstanceSelection{}, true, nil
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, err
	}
	resolved, err := plan.ResolvedInstanceKey(payload, event.ID())
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, err
	}
	keys, err := pipeline.AdmitFlowInstanceKeyMaterial(o.source, plan.ReceiverEndpoint().Readback().FlowID, resolved)
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, err
	}
	parent, err := o.constructionParent(ctx, event, plan)
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, err
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, fmt.Errorf("connect selection requires its admitted source fact")
	}
	lookup, err := pipeline.NewDeclaredFlowInstanceLookup(o.source, fact, event.RunID(), plan.ReceiverEndpoint().Readback().FlowID, parent, keys)
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, err
	}
	selection := connectInstanceSelection{KeyDigest: plan.ReceiverKeyDigest(keys), KeyMaterial: keys, SourceEventID: event.ID(), receiver: plan.ReceiverEndpoint(), mode: plan.InstanceKey().Mode()}
	projection, err := syntheticDeliveryPayloadProjection(plan, selection)
	if err != nil {
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, err
	}
	var prepared []pipeline.FlowInstanceActivationPlan
	if preview, _ := ctx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes); preview != nil {
		prepared = preview.plans
	}
	selected, err := pipeline.PrepareFlowInstanceSelection(ctx, o.index, o.plan, pipeline.FlowInstanceSelectionRequest{
		Lookup: lookup, Mode: selection.mode, MissingInstanceID: connectMissingInstanceID(selection.KeyDigest), Prepared: prepared, RunProposal: o.runProposal,
		Constructor: pipeline.FlowInstanceActivationRequest{ContractBundle: o.source, ConstructorInput: string(plan.ReceiverLocalEvent()), ResolvedKey: resolved,
			PayloadProjection: projection, Bookkeeping: map[string]any{"last_source_event": event.ID()}, TriggerEvent: event},
	})
	if err != nil {
		if _, unavailable := selected.UnavailableReceiver(); unavailable {
			return pinrouting.ConnectRoutePlanMaterialization{Failure: pinrouting.ConnectFailureLifecycleUnavailable}, connectInstanceSelection{
				FlowInstanceSelection: selected, SourceEventID: event.ID(), unavailableReceiverError: err,
			}, true, nil
		}
		if isolatedInstanceLookupMiss(err) {
			return pinrouting.ConnectRoutePlanMaterialization{Failure: pinrouting.ConnectFailureTargetUnresolved}, connectInstanceSelection{}, true, nil
		}
		if _, conflict := pipeline.IsolatedFlowInstanceActivationConflict(err); conflict {
			return pinrouting.ConnectRoutePlanMaterialization{Failure: pinrouting.ConnectFailureInstanceConflict}, connectInstanceSelection{}, true, nil
		}
		return pinrouting.ConnectRoutePlanMaterialization{}, connectInstanceSelection{}, true, err
	}
	selection.FlowInstanceSelection = selected
	instance := selected.Identity()
	route := plan.ReceiverRoute(instance.InstancePath, instance.EntityID)
	switch plan.TargetKind() {
	case pinrouting.ConnectTargetKindTarget:
		return pinrouting.ConnectRoutePlanMaterialization{Target: route}, selection, true, nil
	case pinrouting.ConnectTargetKindTargetSet:
		return pinrouting.ConnectRoutePlanMaterialization{TargetSet: []events.RouteIdentity{route}}, selection, true, nil
	default:
		return pinrouting.ConnectRoutePlanMaterialization{Failure: pinrouting.ConnectFailureDeliveryTopologyInvalid}, connectInstanceSelection{}, true, nil
	}
}

func isolatedInstanceLookupMiss(err error) bool {
	for err != nil {
		if _, missing := err.(*pipeline.WorkflowInstanceLookupMiss); missing {
			return true
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			if len(children) != 1 {
				return false
			}
			err = children[0]
		case interface{ Unwrap() error }:
			err = wrapped.Unwrap()
		default:
			return false
		}
	}
	return false
}

func instanceKeyMaterialForConnectSelection(event events.Event, plan pinrouting.ConnectRoutePlan, values map[string]string) (pinrouting.ConnectRoutePlanInstanceKeyMaterial, pinrouting.ConnectRoutePlanFailure) {
	if plan.InstanceKey() != nil && plan.InstanceKey().RequiresDeliveryProjection() {
		return pinrouting.EventSourcedInstanceKeyMaterialForConnectRoutePlan(plan, event.ID())
	}
	return pinrouting.InstanceKeyMaterialForConnectRoutePlan(plan, pinrouting.AdmitConnectRouteMatchValues(values))
}

func connectMissingInstanceID(digest string) string {
	if digest == "" {
		return ""
	}
	return "ti-" + digest[:min(24, len(digest))]
}
