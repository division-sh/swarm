package genericschedule

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
)

type preparedPublicationOwnerKey struct{}

// This process-local handoff conveys ownership, not publication authority.
// Only the acknowledged occurrence-preparation path attaches it.
type preparedPublicationOwner struct {
	prepared PreparedOccurrence
	source   correlation.SourceArtifactFact
}

func withPreparedPublicationOwner(ctx context.Context, prepared PreparedOccurrence) context.Context {
	source, _ := correlation.SourceArtifactFactFromContext(ctx)
	return context.WithValue(ctx, preparedPublicationOwnerKey{}, preparedPublicationOwner{prepared: prepared, source: source})
}

// PublicationContext carries only the exact acknowledged occurrence's owner.
// It does not admit publication or turn a run-owned schedule into an instance.
func (c PreparationCommit) PublicationContext(ctx context.Context) (context.Context, error) {
	if !c.Acknowledged || c.Result.Outcome != PrepareReady {
		return nil, fmt.Errorf("schedule publication requires acknowledged ready preparation")
	}
	if err := c.Result.Validate(); err != nil {
		return nil, err
	}
	return withPreparedPublicationOwner(ctx, c.Result), nil
}

// PreparedPublicationAgent returns the native-prepared actor for this exact
// occurrence. It never selects an actor from the event's platform producer.
func PreparedPublicationAgent(ctx context.Context, event events.Event) (agentidentity.Identity, bool, error) {
	command, found, err := PreparedPublicationCommand(ctx, event)
	if err != nil || !found {
		return agentidentity.Identity{}, found, err
	}
	if command.OwnerKind != OwnerAgent {
		return agentidentity.Identity{}, false, nil
	}
	return command.AgentIdentity, true, nil
}

// PreparedPublicationCommand reads the acknowledged native occurrence, not the
// event's generic platform producer. Its identity never grants publication.
func PreparedPublicationCommand(ctx context.Context, event events.Event) (AdmissionCommand, bool, error) {
	if ctx == nil {
		return AdmissionCommand{}, false, nil
	}
	owner, found := ctx.Value(preparedPublicationOwnerKey{}).(preparedPublicationOwner)
	if !found {
		return AdmissionCommand{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return AdmissionCommand{}, true, err
	}
	source, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present || !owner.source.Matches(source) {
		return AdmissionCommand{}, true, fmt.Errorf("prepared schedule publication crosses its admitted source")
	}
	if err := owner.prepared.Validate(); err != nil {
		return AdmissionCommand{}, true, err
	}
	activation, occurrence := owner.prepared.Activation, owner.prepared.Occurrence
	if occurrence.EventID != activation.CurrentEventID || !occurrence.AdmittedAt.Equal(activation.CurrentEventAdmittedAt) {
		return AdmissionCommand{}, true, fmt.Errorf("prepared schedule publication crosses its acknowledged occurrence")
	}
	expected, err := occurrencePublicationEvent(activation, occurrence)
	if err != nil {
		return AdmissionCommand{}, true, err
	}
	if err := validateOccurrencePublicationEvent(event, expected); err != nil {
		return AdmissionCommand{}, true, err
	}
	return activation.Command, true, nil
}
