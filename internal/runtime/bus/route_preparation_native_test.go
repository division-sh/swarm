package bus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// The native read and exact-attempt installer are unchanged. On the pre-fix
// tree the callback installs once per evaluation, exhausting all three tries;
// with preparation ownership the same installer runs after evaluation releases.
func VerifySubscribedRoutePreparationNativeForTest(t *testing.T, fixture ExactHandoffNativeFixtureForTest) {
	t.Helper()
	reader, ok := fixture.Store.(SelectedRunTargetOwnerLister)
	if !ok {
		t.Fatal("native selected-store target reader is required")
	}
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	eb, err := newScopedTestEventBus(&InMemoryEventStore{}, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(fixture.Context, 5*time.Second)
	defer cancel()
	runID := uuid.NewString()
	event := fixture.Seed(t, uuid.NewString(), runID, events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient("preparation-proof"),
		AgentIdentity: agentidentitytest.RootRuntimeForRun(t, runID, "preparation-proof", "preparation-proof"),
	})
	readCount := 0
	installCount := 0
	installed := make(chan error, 1)
	install := func(ordinal int) error {
		_, err := publishRoutePreparationProof(ctx, eb, runID, ordinal)
		return err
	}
	planner := eb.newEventBusDeliveryPlanner()
	planner.recipientPolicy.loadActiveAgentDescriptors = func(context.Context) (map[agentidentity.Identity]ActiveAgentDescriptor, bool, error) {
		return nil, false, nil
	}
	planner.recipientPolicy.loadActiveTargetDescriptors = func(ctx context.Context) ([]ActiveTargetDescriptor, bool, error) {
		rows, err := reader.ListSelectedRunTargetOwners(ctx, runID)
		if err != nil {
			return nil, false, err
		}
		readCount++
		before := eb.routeTable.snapshotGeneration()
		installCount++
		ordinal := installCount
		if eb.routeTable.preparationMu.TryLock() {
			eb.routeTable.preparationMu.Unlock()
			// Deterministic old-tree schedule: a genuine canonical install
			// invalidates each descriptor evaluation, not a fabricated counter.
			return rows, true, install(ordinal)
		}
		go func() { installed <- install(ordinal) }()
		if !eb.routeTable.snapshotGenerationCurrent(before) {
			return nil, false, fmt.Errorf("live installation crossed descriptor preparation")
		}
		return rows, true, nil
	}
	// Runtime control avoids constructing a second workflow: the native fixture
	// owns this event, and the test isolates subscribed descriptor preparation.
	before := eb.routeTable.snapshotGeneration()
	if _, err := planner.planForRecipientMaterialization(ctx, event); err != nil {
		t.Fatalf("native descriptor preparation rejected same-source installation: %v (reads=%d installs=%d)", err, readCount, installCount)
	}
	select {
	case err := <-installed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("canonical installation did not finish after preparation released")
	}
	if readCount != 1 || installCount != 1 || eb.routeTable.snapshotGenerationCurrent(before) {
		t.Fatalf("preparation/install disposition: reads=%d installs=%d generation_unchanged=%t", readCount, installCount, eb.routeTable.snapshotGenerationCurrent(before))
	}
	if got := fixture.Load(t, event.ID()); !reflect.DeepEqual(got, event) {
		t.Fatal("preparation changed the exact native publication")
	}
}

func publishRoutePreparationProof(ctx context.Context, eb *EventBus, runID string, ordinal int) (FlowRoutePublicationHandle, error) {
	identity, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.DeriveRoute("consumer", fmt.Sprintf("prepare-%d", ordinal)))
	if err != nil {
		return nil, err
	}
	binding := processbinding.Binding{
		ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "preparation-proof", ProcessBootID: uuid.NewString(),
		GenerationGrantID: uuid.NewString(), BundleHash: eb.sourceArtifactFact.BundleHash(),
		RuntimeInstanceID: eb.runtimeInstanceID, RuntimeGeneration: 1,
	}
	attempt, err := pipeline.NewDynamicFlowRuntimeActivationAttempt("1", runID, identity.Route.InstancePath, binding)
	if err != nil {
		return nil, err
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(eb.runtimeInstanceID, eb.sourceArtifactFact.BundleHash()))
	ctx = correlation.WithRuntimeInstanceID(correlation.WithSourceArtifactFact(ctx, eb.sourceArtifactFact), eb.runtimeInstanceID)
	return eb.PublishPersistedFlowInstanceRouteForAttempt(ctx, FlowInstanceRouteMaterializationRequest{
		Identity: identity, Instance: ConstructedFlowInstanceIdentityFixture(eb.semanticSource, "consumer", identity.Route.InstanceID, runID),
	}, attempt)
}

func routePreparationProofBus(t *testing.T) *EventBus {
	t.Helper()
	source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
	eb, err := newScopedTestEventBus(&InMemoryEventStore{}, EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	return eb
}

func TestRoutePreparationResetLockOrderAndReplacedTableRefusal(t *testing.T) {
	eb := routePreparationProofBus(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	table := eb.routeTable
	before := table.snapshotGeneration()
	table.preparationMu.RLock()
	released := false
	defer func() {
		if !released {
			table.preparationMu.RUnlock()
		}
	}()
	installed := make(chan error, 1)
	go func() {
		_, err := publishRoutePreparationProof(ctx, eb, uuid.NewString(), 1)
		installed <- err
	}()
	// TryRLock refusal identifies the queued installer, not a timing guess about
	// whether its goroutine has reached the lock. Keep the finite test deadline.
	for table.preparationMu.TryRLock() {
		table.preparationMu.RUnlock()
		if ctx.Err() != nil {
			t.Fatal("installer did not queue at preparation ownership")
		}
		runtime.Gosched()
	}
	reset := make(chan error, 1)
	go func() { reset <- eb.ResetInMemoryState() }()
	select {
	case err := <-reset:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("reset inverted EventBus/preparation/generation ownership")
	}
	table.preparationMu.RUnlock()
	released = true
	select {
	case err := <-installed:
		failure, ok := failures.As(err)
		if !ok || failure.Failure.Detail.Code != "connect_route_snapshot_stale" {
			t.Fatalf("replaced-table installation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("replaced-table installer did not settle")
	}
	if eb.routeTable == table || !table.snapshotGenerationCurrent(before) {
		t.Fatal("refused installer changed its replaced table")
	}
}

func TestRoutePreparationPreservesRetirementAndEpochRefusal(t *testing.T) {
	eb := routePreparationProofBus(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	handle, err := publishRoutePreparationProof(ctx, eb, uuid.NewString(), 1)
	if err != nil {
		t.Fatal(err)
	}
	table := eb.routeTable
	before := table.snapshotGeneration()
	table.preparationMu.RLock()
	err = handle.Retire()
	table.preparationMu.RUnlock()
	if err != nil || table.snapshotGenerationCurrent(before) {
		t.Fatalf("preparation suppressed real retirement: %v", err)
	}
	ctx = WithCurrentRuntimeEpoch(ctx)
	EnterRuntimeResetMode()
	defer ExitRuntimeResetMode()
	before = table.snapshotGeneration()
	if _, err := publishRoutePreparationProof(ctx, eb, uuid.NewString(), 2); !errors.Is(err, ErrStaleRuntimeEpoch) {
		t.Fatalf("stale-epoch installation: %v", err)
	}
	if !table.snapshotGenerationCurrent(before) {
		t.Fatal("stale-epoch installation changed routing")
	}
}
