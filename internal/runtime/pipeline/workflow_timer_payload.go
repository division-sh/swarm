package pipeline

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// This prepares payload authority before persistence, not accepted-event execution.
func ResolveWorkflowStageTimerPayloadAdmission(source semanticview.Source, event events.Event, bundleHash string) (events.PayloadAdmission, bool, error) {
	if event.Type() != runtimecontracts.WorkflowStageTimerInternalEvent {
		return events.PayloadAdmission{}, false, nil
	}
	refuse := func(err error) (events.PayloadAdmission, bool, error) {
		return events.PayloadAdmission{}, true, err
	}
	producer := events.PlatformPayloadProducer(event)
	if err := producer.ValidateForEvent(event); err != nil {
		return refuse(err)
	}
	occurrence, found := timeridentity.ParseWorkflowTimerOccurrenceTaskID(event.TaskID())
	if source == nil || !found || event.ID() != timeridentity.WorkflowTimerOccurrenceEventID(occurrence) || event.RunID() == "" ||
		!workflowTimerJSONEqual(event.Payload(), []byte("{}")) {
		return refuse(fmt.Errorf("stage timer payload requires its exact typed occurrence and empty object"))
	}
	for _, declaration := range source.WorkflowTimers() {
		if !declaration.StageOwned || declaration.AdvancesTo == "" || declaration.Event != runtimecontracts.WorkflowStageTimerInternalEvent ||
			declaration.SemanticKey() != occurrence.Activation.DeclarationKey {
			continue
		}
		revision, err := workflowTimerDeclarationRevision(source, declaration)
		if err != nil {
			return refuse(err)
		}
		routing, eventType, err := workflowTimerDeclarationSourceEvent(source, event.EntityID(), event.FlowInstance(), declaration)
		if err != nil {
			return refuse(err)
		}
		if revision != occurrence.Activation.DeclarationRevision || routing != event.RoutingSource() || eventType != event.Type() {
			return refuse(fmt.Errorf("stage timer payload contradicts its declaration revision or route"))
		}
		if declaration.OwningFlowID() == "." && event.FlowInstance() != "" && event.FlowInstance() != event.RunID() {
			return refuse(fmt.Errorf("stage timer payload crosses its root instance"))
		}
		digest, err := events.StageTimerPayloadDigest(event)
		if err != nil {
			return refuse(err)
		}
		binding, err := events.NewPlatformPayloadSchemaBinding(events.PayloadSchemaBindingInput{
			BundleHash: bundleHash, EventKey: string(event.Type()), SchemaClass: events.PayloadSchemaPlatform, SchemaDigest: digest,
		}, producer, events.StageTimerPayloadReference)
		if err != nil {
			return refuse(err)
		}
		admission, err := events.NewPayloadAdmission(event.Payload(), binding)
		return admission, true, err
	}
	return refuse(fmt.Errorf("stage timer payload has no matching compiled declaration"))
}
