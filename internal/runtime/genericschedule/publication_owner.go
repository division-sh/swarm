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
	if prepared.Activation.Command.OwnerKind != OwnerAgent {
		return ctx
	}
	source, _ := correlation.SourceArtifactFactFromContext(ctx)
	return context.WithValue(ctx, preparedPublicationOwnerKey{}, preparedPublicationOwner{prepared: prepared, source: source})
}

// PreparedPublicationAgent returns the native-prepared actor for this exact
// occurrence. It never selects an actor from the event's platform producer.
func PreparedPublicationAgent(ctx context.Context, event events.Event) (agentidentity.Identity, bool, error) {
	if ctx == nil {
		return agentidentity.Identity{}, false, nil
	}
	owner, found := ctx.Value(preparedPublicationOwnerKey{}).(preparedPublicationOwner)
	if !found {
		return agentidentity.Identity{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return agentidentity.Identity{}, true, err
	}
	source, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present || !owner.source.Matches(source) {
		return agentidentity.Identity{}, true, fmt.Errorf("prepared schedule publication crosses its admitted source")
	}
	if err := owner.prepared.Validate(); err != nil {
		return agentidentity.Identity{}, true, err
	}
	activation, occurrence := owner.prepared.Activation, owner.prepared.Occurrence
	if activation.Command.OwnerKind != OwnerAgent || occurrence.EventID != activation.CurrentEventID || !occurrence.AdmittedAt.Equal(activation.CurrentEventAdmittedAt) {
		return agentidentity.Identity{}, true, fmt.Errorf("prepared schedule publication crosses its acknowledged occurrence")
	}
	expected, err := occurrencePublicationEvent(activation, occurrence)
	if err != nil {
		return agentidentity.Identity{}, true, err
	}
	if err := validateOccurrencePublicationEvent(event, expected); err != nil {
		return agentidentity.Identity{}, true, err
	}
	return owner.prepared.Activation.Command.AgentIdentity, true, nil
}
