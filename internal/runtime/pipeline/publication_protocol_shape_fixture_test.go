package pipeline

import (
	"context"
	"fmt"
	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"strings"
)

// Shape-only adversarial collaborators. Native storage and execution proofs
// use the original selected-store planner, receipts and dispatcher.
type pipelineTestPublicationPlan struct {
	intent     runtimeengine.EmitIntent
	commitHook func(context.Context, events.Event) error
}

func (p pipelineTestPublicationPlan) DurablePublicationEventID() string {
	return strings.TrimSpace(p.intent.Event.ID())
}

func (p pipelineTestPublicationPlan) ValidateDurablePublicationPlan() error {
	if p.DurablePublicationEventID() == "" || strings.TrimSpace(string(p.intent.Event.Type())) == "" {
		return fmt.Errorf("pipeline test publication requires exact event identity")
	}
	return nil
}

type pipelineTestCommittedPublication struct {
	eventID string
	intent  runtimeengine.EmitIntent
}

func (p pipelineTestCommittedPublication) CommittedDurablePublicationEventID() string {
	return strings.TrimSpace(p.eventID)
}

func (p pipelineTestCommittedPublication) CommittedDurablePublicationIntent() runtimeengine.EmitIntent {
	return p.intent
}

func (p pipelineTestCommittedPublication) ValidateCommittedDurablePublication() error {
	if p.CommittedDurablePublicationEventID() == "" {
		return fmt.Errorf("pipeline test committed publication requires event identity")
	}
	return nil
}

func (b *recordingPipelineBus) PrepareEngineMutationPublications(ctx context.Context, intents []runtimeengine.EmitIntent, prospective PreparedWorkflowPublicationState) ([]runtimeengine.DurablePublicationPlan, error) {
	if prospective.Empty() {
		return nil, fmt.Errorf("test mutation publication requires prepared state")
	}
	return b.PrepareEnginePublications(ctx, intents)
}

func (b *recordingPipelineBus) PrepareEnginePublications(_ context.Context, intents []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	if b != nil && b.publishErr != nil {
		return nil, b.publishErr
	}
	if b != nil && b.outboxErr != nil {
		return nil, b.outboxErr
	}
	plans := make([]runtimeengine.DurablePublicationPlan, 0, len(intents))
	for _, intent := range intents {
		if strings.TrimSpace(string(intent.Event.Type())) == "" {
			continue
		}
		admitted, err := events.AdmitForPublish(intent.Event, events.AdmissionOptions{})
		if err != nil {
			return nil, err
		}
		intent.Event = admitted.Event()
		plan := pipelineTestPublicationPlan{intent: intent}
		if b != nil {
			plan.commitHook = b.publishInMutationHook
		}
		if err := plan.ValidateDurablePublicationPlan(); err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (*recordingPipelineBus) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	return nil
}

func (b *recordingPipelineBus) FinalizeEnginePublications(_ context.Context, evidence []runtimeengine.CommittedDurablePublication) error {
	intents := make([]runtimeengine.EmitIntent, 0, len(evidence))
	for _, committed := range evidence {
		if committed == nil {
			return fmt.Errorf("pipeline test publication evidence is required")
		}
		if err := committed.ValidateCommittedDurablePublication(); err != nil {
			return err
		}
		value, ok := committed.(pipelineTestCommittedPublication)
		if !ok {
			return fmt.Errorf("pipeline test committed publication has unexpected type %T", committed)
		}
		intents = append(intents, value.intent)
	}
	if b != nil {
		b.mu.Lock()
		b.outboxIntents = append(b.outboxIntents, cloneEmitIntents(intents)...)
		b.mu.Unlock()
		return b.finalizeErr
	}
	return nil
}
