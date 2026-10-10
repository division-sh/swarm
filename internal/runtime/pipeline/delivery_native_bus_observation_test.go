package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/engine"
)

// This observer delegates planning, finalization and dispatch to the original
// native bus. Its counters cannot manufacture plans, receipts or handoffs.
type nativePipelineDeliveryBusObservationForTest struct {
	Bus
	EngineMutationPublicationPlanner
	mu                    sync.Mutex
	published             []events.Event
	runtimeLogs           []RuntimeLogEntry
	committedPublications int
	committedEventIDs     []string
	finalizeFailure       error
	prepareFailure        error
	beforePrepare         func(context.Context) error
	beforeDispatch        func(context.Context, []engine.EmitIntent) error
}

func (b *nativePipelineDeliveryBusObservationForTest) PrepareEnginePublications(ctx context.Context, intents []engine.EmitIntent) ([]engine.DurablePublicationPlan, error) {
	if err := b.preparationRefusal(ctx); err != nil {
		return nil, err
	}
	return b.EngineMutationPublicationPlanner.PrepareEnginePublications(ctx, intents)
}

func (b *nativePipelineDeliveryBusObservationForTest) PrepareEngineMutationPublications(ctx context.Context, intents []engine.EmitIntent, state PreparedWorkflowPublicationState) ([]engine.DurablePublicationPlan, error) {
	if err := b.preparationRefusal(ctx); err != nil {
		return nil, err
	}
	return b.EngineMutationPublicationPlanner.PrepareEngineMutationPublications(ctx, intents, state)
}

func (b *nativePipelineDeliveryBusObservationForTest) preparationRefusal(ctx context.Context) error {
	b.mu.Lock()
	failure := b.prepareFailure
	before := b.beforePrepare
	b.mu.Unlock()
	if before != nil {
		if err := before(ctx); err != nil {
			return err
		}
	}
	if failure != nil {
		return failure
	}
	return nil
}

// This temporal cut delays, but never replaces, the original native dispatch.
type nativePipelineDeliveryDispatchGateForTest struct {
	Bus
	EngineMutationPublicationPlanner
	entered, release chan struct{}
	once             sync.Once
	flowScope        string
}

func (g *nativePipelineDeliveryDispatchGateForTest) EngineDispatcher() engine.PostCommitDispatcher {
	return g
}

func (g *nativePipelineDeliveryDispatchGateForTest) DispatchPostCommit(ctx context.Context, intents []engine.EmitIntent) error {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.release:
	}
	return g.Bus.EngineDispatcher().DispatchPostCommit(withPipelineFlowScope(ctx, g.flowScope), intents)
}

func observeNativePipelineDeliveryBusForTest(t *testing.T, pc *PipelineCoordinator) *nativePipelineDeliveryBusObservationForTest {
	t.Helper()
	planner, ok := pc.bus.(EngineMutationPublicationPlanner)
	if !ok {
		t.Fatal("native pipeline delivery observation requires its original publication planner")
	}
	observer := &nativePipelineDeliveryBusObservationForTest{Bus: pc.bus, EngineMutationPublicationPlanner: planner}
	pc.bus = observer
	return observer
}

func (b *nativePipelineDeliveryBusObservationForTest) EngineDispatcher() engine.PostCommitDispatcher {
	return b
}

func (b *nativePipelineDeliveryBusObservationForTest) DispatchPostCommit(ctx context.Context, intents []engine.EmitIntent) error {
	if b.beforeDispatch != nil {
		if err := b.beforeDispatch(ctx, intents); err != nil {
			return err
		}
	}
	if err := b.Bus.EngineDispatcher().DispatchPostCommit(ctx, intents); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, intent := range intents {
		b.published = append(b.published, cloneEvent(intent.Event))
	}
	return nil
}

func (b *nativePipelineDeliveryBusObservationForTest) FinalizeEnginePublications(ctx context.Context, evidence []engine.CommittedDurablePublication) error {
	for _, receipt := range evidence {
		if receipt == nil {
			return errors.New("native pipeline publication receipt is required")
		}
		if err := receipt.ValidateCommittedDurablePublication(); err != nil {
			return err
		}
	}
	err := b.EngineMutationPublicationPlanner.FinalizeEnginePublications(ctx, evidence)
	b.mu.Lock()
	b.committedPublications += len(evidence)
	for _, receipt := range evidence {
		b.committedEventIDs = append(b.committedEventIDs, receipt.CommittedDurablePublicationEventID())
	}
	failure := b.finalizeFailure
	b.mu.Unlock()
	return errors.Join(err, failure)
}

func (b *nativePipelineDeliveryBusObservationForTest) publishedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.published)
}

func (b *nativePipelineDeliveryBusObservationForTest) publishedEvent(index int) events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneEvent(b.published[index])
}

func (b *nativePipelineDeliveryBusObservationForTest) LogRuntime(ctx context.Context, entry RuntimeLogEntry) error {
	err := b.Bus.LogRuntime(ctx, entry)
	b.mu.Lock()
	b.runtimeLogs = append(b.runtimeLogs, entry)
	b.mu.Unlock()
	return err
}

func (b *nativePipelineDeliveryBusObservationForTest) runtimeLogEntries() []RuntimeLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]RuntimeLogEntry(nil), b.runtimeLogs...)
}

func (b *nativePipelineDeliveryBusObservationForTest) committedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committedPublications
}

func (b *nativePipelineDeliveryBusObservationForTest) persistedPublishedEvent(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, index int) events.Event {
	t.Helper()
	b.mu.Lock()
	id := b.committedEventIDs[index]
	b.mu.Unlock()
	event, err := fixture.PublishedEvent(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return event
}
