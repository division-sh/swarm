package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ActivityRequestLineage is decoded by the activity owner, not by a second
// payload interpreter in a store's recursive lineage query.
type ActivityRequestLineage struct {
	request events.Event
	intent  runtimeengine.ActivityIntent
}

type ActivityParentExecution struct {
	Delivery      runtimedelivery.Snapshot
	RuleSelection handlerselection.HandlerRuleSelectionFact
}

// SelectedActivityRequestLineage consumes a root already admitted by the
// selected-source replay authority. Its persisted ID is deliberately not the
// identity of a newly constructed request.
func SelectedActivityRequestLineage(request events.Event) (ActivityRequestLineage, error) {
	if request.Type() != activityRequestEventType {
		return ActivityRequestLineage{}, fmt.Errorf("expected activity request")
	}
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return ActivityRequestLineage{}, err
	}
	if intent.SourceRunID != request.RunID() || intent.SourceEventID != request.ID() {
		return ActivityRequestLineage{}, fmt.Errorf("selected activity request has foreign execution identity")
	}
	return ActivityRequestLineage{request: request, intent: intent}, nil
}

// FreshActivityRequestLineage verifies construction and the exact committed
// parent delivery. This does not authorize replay or execution of an effect.
func FreshActivityRequestLineage(request, parent events.Event, source semanticview.Source, executions []ActivityParentExecution, activations []loopruntime.Activation) (ActivityRequestLineage, error) {
	if request.Type() != activityRequestEventType || source == nil {
		return ActivityRequestLineage{}, fmt.Errorf("fresh activity requires selected execution source")
	}
	intent, err := activityIntentFromRequestEvent(request)
	if err != nil {
		return ActivityRequestLineage{}, err
	}
	if intent.Attempt != 1 {
		return ActivityRequestLineage{}, fmt.Errorf("fresh activity request must begin at attempt 1")
	}
	if parent.ID() == "" || request.RunID() != parent.RunID() || intent.SourceRunID != parent.RunID() ||
		intent.SourceEventID != parent.ID() || intent.ParentEventID != parent.ParentEventID() ||
		intent.SourceTaskID != parent.TaskID() || intent.ChainDepth != parent.ChainDepth() || request.ExecutionMode() != parent.ExecutionMode() {
		return ActivityRequestLineage{}, fmt.Errorf("fresh activity has foreign causal identity")
	}
	expected, err := activityRequestEmitIntentFromAdmittedSource(intent)
	if err != nil {
		return ActivityRequestLineage{}, err
	}
	raw, err := canonicaljson.Decode(request.Payload())
	if err != nil {
		return ActivityRequestLineage{}, err
	}
	canonical, err := canonicaljson.Encode(raw)
	if err != nil {
		return ActivityRequestLineage{}, err
	}
	if request.ID() != expected.Event.ID() || request.ParentEventID() != expected.Event.ParentEventID() ||
		!request.Producer().Equal(expected.Event.Producer()) || request.TaskID() != expected.Event.TaskID() ||
		request.ChainDepth() != expected.Event.ChainDepth() || !bytes.Equal(canonical, expected.Event.Payload()) {
		return ActivityRequestLineage{}, fmt.Errorf("fresh activity does not match canonical request construction")
	}
	node, ok := intent.Owner.Node()
	if !ok {
		return ActivityRequestLineage{}, fmt.Errorf("fresh selected activity requires an admitted node activity site")
	}
	route := request.RoutingSource().Route()
	flowID := node.FlowPath()
	if flowID == "" {
		flowID = semanticview.RootExecutionFlowID(source)
	}
	if route.FlowID != flowID || route.FlowID != intent.ExecutionFlowID.String() || route.FlowInstance != intent.FlowInstance || route.EntityID != intent.EntityID.String() {
		return ActivityRequestLineage{}, fmt.Errorf("fresh activity has conflicting execution route")
	}
	delivered := false
	var selection handlerselection.HandlerRuleSelectionFact
	for _, execution := range executions {
		delivery := execution.Delivery
		if delivery.EventID == parent.ID() && delivery.RunID == parent.RunID() && delivery.Status == runtimedelivery.StatusDelivered &&
			delivery.Route.Recipient == events.MustNodeDeliveryRecipient(node) && delivery.Route.Target.Route() == route {
			resolved := workflowNodeEventHandlerResolutionForDeliveryContext(withWorkflowNodeDeliveryRoute(context.Background(), delivery.Route), source, node, parent)
			delivered = resolved.Matched && resolved.HandlerEventKey == intent.HandlerEventKey
			if delivered {
				selection = execution.RuleSelection
				if err := runtimeengine.ValidateActivityLoopLineage(intent, parent, source, resolved.Handler, execution.RuleSelection, activations); err != nil {
					return ActivityRequestLineage{}, err
				}
				break
			}
		}
	}
	if !delivered {
		return ActivityRequestLineage{}, fmt.Errorf("fresh activity has no completed exact parent delivery")
	}
	tool, ok := source.ToolEntries()[intent.Tool]
	if !ok || tool.Effect() != intent.EffectClass || !runtimecontracts.SupportedActivityEffectClass(intent.EffectClass) {
		return ActivityRequestLineage{}, fmt.Errorf("fresh selected activity effect is not admitted")
	}
	if intent.BundleHash != "" {
		bundle, _ := semanticview.Bundle(source)
		hash, err := runtimecontracts.BundleHash(bundle)
		if err != nil || hash != intent.BundleHash || source.WorkflowVersion() != intent.WorkflowVersion {
			return ActivityRequestLineage{}, fmt.Errorf("fresh activity has a foreign contract pin")
		}
	}
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(intent.EffectClass)
	matched := false
	for _, site := range runtimecontracts.ActivitySitesForNode(node, source.ExecutableNodeEventHandlers(node)) {
		if site.RuleRef.Valid() && (selection.Disposition() != handlerselection.DispositionSelected || site.RuleRef != selection.Ref()) {
			continue
		}
		result := runtimecontracts.ActivityResultEventsForSite(site)
		if site.HandlerEventKey == intent.HandlerEventKey && site.Spec.Tool == intent.Tool && site.Spec.Approval == nil &&
			result.ActivityID == intent.ActivityID && result.SuccessEvent == intent.SuccessEvent && result.FailureEvent == intent.FailureEvent &&
			result.RevisionRequested == intent.RevisionEvent && result.Rejected == intent.RejectedEvent &&
			intent.ForkPolicy == runtimecontracts.ActivityForkPolicyForEffectClass(intent.EffectClass) &&
			intent.RetryMaxAttempts == defaults.MaxAttempts && intent.RetryBackoff == defaults.Backoff {
			matched = true
		}
	}
	if !matched {
		return ActivityRequestLineage{}, fmt.Errorf("fresh activity does not match selected activity contract")
	}
	return ActivityRequestLineage{request: request, intent: intent}, nil
}

func (l ActivityRequestLineage) RequestID() string { return l.request.ID() }

func (l ActivityRequestLineage) OwnsResultType(eventType events.EventType) bool {
	return string(eventType) == l.intent.SuccessEvent || string(eventType) == l.intent.FailureEvent
}

func (l ActivityRequestLineage) ValidateResult(result events.Event) error {
	intent := l.intent
	producer := activityEventProducer(intent.Owner)
	if !l.OwnsResultType(result.Type()) || result.ID() != activityResultEventID(intent, string(result.Type())) ||
		result.RunID() != l.request.RunID() || result.ParentEventID() != firstNonEmptyString(intent.SourceEventID, intent.ParentEventID) ||
		result.RoutingSource() != l.request.RoutingSource() || result.ExecutionMode() != l.request.ExecutionMode() ||
		result.TaskID() != intent.SourceTaskID || result.ChainDepth() != intent.ChainDepth+1 ||
		result.ProducerType() != producer.Type || result.Producer().ID() != producer.ID {
		return fmt.Errorf("activity result has foreign request lineage")
	}
	var payload struct {
		ActivityID  string                    `json:"activity_id"`
		Tool        string                    `json:"tool"`
		EffectClass string                    `json:"effect_class"`
		Attempt     int                       `json:"attempt"`
		Result      json.RawMessage           `json:"result"`
		Failure     *runtimefailures.Envelope `json:"failure"`
	}
	if err := json.Unmarshal(result.Payload(), &payload); err != nil {
		return err
	}
	if payload.ActivityID != intent.ActivityID || payload.Tool != intent.Tool || payload.EffectClass != string(intent.EffectClass) ||
		payload.Attempt < 1 || payload.Attempt > activityRetryMaxAttempts(intent, intent.EffectClass) {
		return fmt.Errorf("activity result has invalid request facts")
	}
	if intent.Generation.Valid() {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(result.Payload(), &fields); err != nil {
			return err
		}
		var revision string
		if json.Unmarshal(fields[intent.Generation.RevisionField], &revision) != nil || revision != intent.Generation.RevisionID {
			return fmt.Errorf("activity result has foreign loop revision")
		}
	}
	if string(result.Type()) == intent.SuccessEvent {
		if len(payload.Result) == 0 || payload.Failure != nil {
			return fmt.Errorf("activity success payload is invalid")
		}
	} else if payload.Failure == nil || runtimefailures.ValidateEnvelope(*payload.Failure) != nil || len(payload.Result) != 0 {
		return fmt.Errorf("activity failure payload is invalid")
	}
	return nil
}

// ActivityDiagnosticSubject recognizes the existing activity logger's carrier.
func ActivityDiagnosticSubject(event events.Event) (string, bool, error) {
	if event.Type() != "platform.runtime_log" {
		return "", false, nil
	}
	var payload struct {
		Details map[string]json.RawMessage `json:"details"`
	}
	if err := json.Unmarshal(event.Payload(), &payload); err != nil {
		return "", false, err
	}
	var component string
	if err := json.Unmarshal(payload.Details["component"], &component); err != nil {
		if _, present := payload.Details["request_event_id"]; present {
			return "", true, fmt.Errorf("activity diagnostic has invalid component")
		}
		return "", false, nil
	}
	if component != "activity" {
		if _, present := payload.Details["request_event_id"]; !present {
			return "", false, nil
		}
		return "", true, fmt.Errorf("activity diagnostic has conflicting component")
	}
	var subject string
	if err := json.Unmarshal(payload.Details["event_id"], &subject); err != nil || subject == "" {
		return "", true, fmt.Errorf("activity diagnostic requires persisted request subject")
	}
	for _, key := range []string{"request_event_id", "runtime_lineage_subject_event_id", "runtime_lineage_parent_event_id", "parent_event_id"} {
		if raw, ok := payload.Details[key]; ok {
			var value string
			if json.Unmarshal(raw, &value) != nil || value != subject {
				return "", true, fmt.Errorf("activity diagnostic has conflicting %s", key)
			}
		}
	}
	var requestID string
	if json.Unmarshal(payload.Details["request_event_id"], &requestID) != nil || requestID != subject {
		return "", true, fmt.Errorf("activity diagnostic has no exact request identity")
	}
	return subject, true, nil
}

func (l ActivityRequestLineage) ValidateDiagnostic(event events.Event) error {
	subject, activity, err := ActivityDiagnosticSubject(event)
	if err != nil {
		return err
	}
	if !activity || subject != l.request.ID() || event.ParentEventID() != l.request.ID() || event.RunID() != l.request.RunID() {
		return fmt.Errorf("activity diagnostic has foreign request lineage")
	}
	var payload struct {
		Details struct {
			Generation attemptgeneration.Generation `json:"loop_generation"`
			Stage      string                       `json:"loop_stage"`
		} `json:"details"`
	}
	if err := json.Unmarshal(event.Payload(), &payload); err != nil {
		return err
	}
	if payload.Details.Generation != l.intent.Generation || payload.Details.Stage != l.intent.LoopStage {
		return fmt.Errorf("activity diagnostic has foreign loop lineage")
	}
	return nil
}
