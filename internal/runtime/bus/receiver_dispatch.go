package bus

import (
	"context"
	"errors"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// receiverDispatchProjection is the complete process-local handoff retained by
// PreparedPublish. Event, route, bundle, and execution facts remain owned by
// their typed owners and are reconstructed when receiver work starts.
type receiverDispatchProjection struct {
	occurrence        worklifetime.Occurrence
	runtimeInstanceID string
	bundleSourceFact  runtimecorrelation.BundleSourceFact
	deliveryContext   events.DeliveryContext
	channelExecution  *runtimechannelactivation.Lease
	completion        *localDeliveryCompletionGroup
}

func (p receiverDispatchProjection) validate() error {
	if p.occurrence == nil {
		return errors.New("receiver dispatch occurrence is required")
	}
	return nil
}

func (p receiverDispatchProjection) withCompletion(group *localDeliveryCompletionGroup) receiverDispatchProjection {
	p.completion = group
	return p
}

type receiverDispatchContext struct {
	context.Context
	projection receiverDispatchProjection
}

func (eb *EventBus) receiverProjection(ctx context.Context, deliveryContext events.DeliveryContext) (receiverDispatchProjection, error) {
	if eb == nil {
		return receiverDispatchProjection{}, errors.New("event bus is required")
	}
	runtimeInstanceID, _ := runtimecorrelation.RuntimeInstanceIDFromContext(ctx)
	bundleSourceFact, _ := runtimecorrelation.BundleSourceFactFromContext(ctx)
	channelExecution, _ := runtimechannelactivation.ExecutionLeaseFromContext(ctx)
	projection := receiverDispatchProjection{
		occurrence:        eb.workOwner,
		runtimeInstanceID: runtimeInstanceID,
		bundleSourceFact:  bundleSourceFact,
		deliveryContext:   deliveryContext.Normalized(),
		channelExecution:  channelExecution,
		completion:        localDeliveryCompletionGroupFromContext(ctx),
	}
	return projection, projection.validate()
}

func (eb *EventBus) beginReceiverDispatch(parent context.Context, projection receiverDispatchProjection, evt events.Event) (receiverDispatchContext, func() error, error) {
	if err := projection.validate(); err != nil {
		return receiverDispatchContext{}, nil, err
	}
	var (
		ctx           context.Context
		closeReceiver func() error
		err           error
	)
	if admission, ok := runtimeWorkAdmissionFromContext(parent); ok && admission.owner == projection.occurrence {
		var closeContext func()
		ctx, closeContext = eventreceiver.NewContext(admission.context)
		closeReceiver = func() error {
			closeContext()
			return nil
		}
	} else {
		lease, err := projection.occurrence.Begin(context.Background())
		if err != nil {
			return receiverDispatchContext{}, nil, err
		}
		var closeContext func()
		ctx, closeContext = eventreceiver.NewContext(lease.Context())
		closeReceiver = func() error {
			closeContext()
			return lease.Done()
		}
	}
	ctx = worklifetime.WithOccurrence(ctx, projection.occurrence)
	ctx = runtimechannelactivation.WithExecutionLease(ctx, projection.channelExecution)
	ctx = withReceiverBundleSource(ctx, projection.runtimeInstanceID, projection.bundleSourceFact)
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		_ = closeReceiver()
		return receiverDispatchContext{}, nil, err
	}
	ctx, err = eb.receiverExecution.Bind(ctx, evt.ExecutionMode())
	if err != nil {
		_ = closeReceiver()
		return receiverDispatchContext{}, nil, err
	}
	if err := eb.receiverExecution.ValidateBound(ctx, evt.ExecutionMode()); err != nil {
		_ = closeReceiver()
		return receiverDispatchContext{}, nil, err
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	deliveryContext := projection.deliveryContext
	if deliveryContext.Empty() {
		deliveryContext = evt.DeliveryContext()
	}
	ctx = events.WithDeliveryContext(ctx, deliveryContext)
	if projection.completion != nil {
		ctx = withLocalDeliveryCompletionGroup(ctx, projection.completion)
	}
	ctx, err = eb.withAuthorActivityEventDescriptor(ctx, evt)
	if err != nil {
		_ = closeReceiver()
		return receiverDispatchContext{}, nil, err
	}
	return receiverDispatchContext{Context: ctx, projection: projection}, closeReceiver, nil
}

func (eb *EventBus) receiverRouteContext(parent context.Context, evt events.Event, route events.DeliveryRoute) (context.Context, func(), error) {
	owner, hasOwner := worklifetime.OccurrenceFromContext(parent)
	completion := localDeliveryCompletionGroupFromContext(parent)
	runtimeInstanceID, _ := runtimecorrelation.RuntimeInstanceIDFromContext(parent)
	bundleSourceFact, _ := runtimecorrelation.BundleSourceFactFromContext(parent)
	ctx, cleanup := eventreceiver.NewContext(context.Background())
	if hasOwner {
		ctx = worklifetime.WithOccurrence(ctx, owner)
	}
	ctx = withReceiverBundleSource(ctx, runtimeInstanceID, bundleSourceFact)
	var err error
	ctx, err = eb.admitBundleSourceFact(ctx)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	ctx, err = eb.receiverExecution.Bind(ctx, evt.ExecutionMode())
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if err := eb.receiverExecution.ValidateBound(ctx, evt.ExecutionMode()); err != nil {
		cleanup()
		return nil, nil, err
	}
	ctx = runtimecorrelation.WithInboundEvent(ctx, evt)
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		ctx = runtimecorrelation.WithRunID(ctx, runID)
	}
	deliveryContext := route.Context.Normalized()
	if deliveryContext.Empty() {
		deliveryContext = evt.DeliveryContext()
	}
	ctx = events.WithDeliveryContext(ctx, deliveryContext)
	ctx = runtimedelivery.WithRoute(ctx, route)
	if completion != nil {
		ctx = withLocalDeliveryCompletionGroup(ctx, completion)
	}
	ctx, err = eb.withAuthorActivityEventDescriptor(ctx, evt)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return ctx, cleanup, nil
}

func withReceiverBundleSource(ctx context.Context, runtimeInstanceID string, sourceFact runtimecorrelation.BundleSourceFact) context.Context {
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	return runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
}
