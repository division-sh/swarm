package bus

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// A group supplies its own finite reader. Ordinary preview and final planning
// each open a fresh canonical read cut, outside publication claim/settlement.
func (eb *EventBus) withPublicationInstancePlanning(ctx context.Context, event events.Event, planner deliveryPlanner, prepare func(context.Context, deliveryPlanner) error) error {
	// Reply planning owns an independent reply-context read. Keep that original
	// path outside this index-only transaction, including on one-connection pools.
	if events.DeliveryContextFromContext(ctx).ReplyContextID() != "" {
		return prepare(ctx, planner)
	}
	if planner.connectPlanner.lifecycle.runProposal.Present() || eb.semanticSource == nil || event.RunID() == "" {
		return prepare(ctx, planner)
	}
	owner, native := planner.connectPlanner.lifecycle.index.(pipeline.FlowInstancePlanningSnapshotOwner)
	if !native {
		// Group readers are already scoped; pure index fixtures own no SQL cut.
		return prepare(ctx, planner)
	}
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		return fmt.Errorf("publication instance planning requires its admitted source")
	}
	scope, err := pipeline.NewFlowInstanceLookupScope(eb.semanticSource, fact, event.RunID(), nil, nil)
	if err != nil {
		return err
	}
	return owner.WithFlowInstancePlanningSnapshot(ctx, scope, func(ctx context.Context, reader pipeline.FlowInstanceIndexReader) error {
		bounded := planner
		bounded.connectPlanner.lifecycle.index = reader
		bounded.routeResolver.resolveRoutedSubscribers = bounded.connectPlanner.resolvePubsubSubscribers
		return prepare(ctx, bounded)
	})
}
