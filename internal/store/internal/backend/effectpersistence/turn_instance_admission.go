package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func prepareProviderTurnOwnership(ctx context.Context, tx *sql.Tx, authority effects.Authority, req effects.AuthorizeRequest, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner) (events.InstanceOwnership, error) {
	if req.Kind != effects.KindProviderTurn || !authority.HasBusinessTurnOrigin() {
		return events.InstanceOwnership{}, nil
	}
	if directives == nil || req.InstanceSource == nil {
		return events.InstanceOwnership{}, fmt.Errorf("provider turn requires its native instance ownership admission")
	}
	token := authority.Normal
	if authority.Kind == effects.AuthoritySelectedContractFork {
		token, _ = effects.LifecycleTokenFromContext(ctx)
	}
	if token.Identity != authority.Target.AgentIdentity {
		return events.InstanceOwnership{}, fmt.Errorf("provider turn crosses its acquired actor")
	}
	current, err := directives.PrepareAgentTurnOwnershipTx(ctx, tx, req.InstanceSource, token)
	if err != nil {
		return events.InstanceOwnership{}, err
	}
	if req.Origin.Kind == effects.CompletionOriginDirective {
		if !req.InstanceOwnership.Equal(current) {
			return events.InstanceOwnership{}, turnInstanceOwnershipChanged(authority, req.InstanceOwnership, current)
		}
		return req.InstanceOwnership.Clone(), nil
	}
	if req.Origin.Kind != effects.CompletionOriginDelivery || delivery == nil {
		return events.InstanceOwnership{}, fmt.Errorf("provider turn requires its exact delivery or directive origin")
	}
	snapshot, err := delivery.ProviderOriginSnapshotTx(ctx, tx, req.Origin.Delivery)
	if err != nil {
		return events.InstanceOwnership{}, err
	}
	if snapshot.Route.AgentIdentity != authority.Target.AgentIdentity {
		return events.InstanceOwnership{}, fmt.Errorf("provider turn ownership crosses its admitted recipient")
	}
	if current.RunOwned && snapshot.Route.Target.EntitylessReceiver() && snapshot.Route.Context.Life == nil {
		// This target is the separately admitted flow dependency, not ownership
		// of the entityless actor. Its currentness remains the origin owner's.
		return current, nil
	}
	captured := snapshot.Route.Context.Life
	if !snapshot.Route.Target.Empty() {
		captured = &events.InstanceLife{RunID: snapshot.RunID, FlowInstance: snapshot.Route.Target.Route().FlowInstance, Life: snapshot.Route.Target.Life()}
	}
	if current.RunOwned && captured == nil {
		return current, nil // Nonmembership was established by the native reader.
	}
	origin := events.InstanceOwnership{Life: captured}
	if !origin.Equal(current) {
		return events.InstanceOwnership{}, turnInstanceOwnershipChanged(authority, origin, current)
	}
	return origin.Clone(), nil
}

func turnInstanceOwnershipChanged(authority effects.Authority, captured, current events.InstanceOwnership) error {
	if captured.Validate() == nil && current.Validate() == nil && captured.Life != nil && current.Life != nil &&
		captured.Life.RunID == current.Life.RunID && captured.Life.FlowInstance == current.Life.FlowInstance && captured.Life.Life != current.Life.Life {
		scope, instance, path, err := authority.BusinessTurnCoordinates()
		if err == nil {
			return pipeline.InstanceRestarted(flowidentity.RunScopedFlowInstance{RunID: authority.Target.RunID, Route: flowidentity.StoredRoute(scope, instance, path)}, captured.Life.Life, current.Life.Life)
		}
	}
	return fmt.Errorf("provider turn differs from its exact originating ownership observation")
}
