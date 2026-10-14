package cataloge2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func requireHistoricalPublicationFreshBusReplay(t *testing.T, ctx context.Context, h *runtimeHarness, publication bus.PreparedPublishEvent, node, agent deliverylifecycle.Snapshot, calls []scriptedDeliveryCall) {
	t.Helper()
	var selected interface {
		bus.EventStore
		pipeline.WorkflowPersistenceOwner
		CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
		ListCompletionCandidates(context.Context, runlifecycle.CandidateScope, runlifecycle.CandidateCursor, int) (runlifecycle.CandidatePage, error)
		deliverylifecycle.Store
	} = h.pg
	durable := bus.DurableDependencies{PreparedEvents: h.pg, ActiveAgents: h.pg, Instances: h.pg, ConstructionPublications: h.pg, RunLifecycle: h.pg, RunOrigins: h.pg, DeliveryLifecycle: h.pg}
	if h.sqlite != nil {
		selected = h.sqlite
		durable = bus.DurableDependencies{PreparedEvents: h.sqlite, ActiveAgents: h.sqlite, Instances: h.sqlite, ConstructionPublications: h.sqlite, RunLifecycle: h.sqlite, RunOrigins: h.sqlite, DeliveryLifecycle: h.sqlite}
	}
	// These are fresh consumers of the actual stored publication and immutable
	// instance reader, not a copied manifest or the executing Bus's local cache.
	reader := pipeline.NewWorkflowPersistence(selected)
	durable.Instances, durable.ConstructionPublications = reader, reader
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		t.Fatal("historical replay requires the original admitted source")
	}
	fresh, err := bus.NewEphemeralEventBusWithOptions(selected, bus.EventBusOptions{
		ContractBundle: semanticview.Wrap(h.bundle), SourceArtifactFact: fact, RuntimeInstanceID: h.rt.Options.RuntimeInstanceID,
		WorkOwner: h.rt.WorkOccurrence(), DeliveryAuthority: agent.Authority, Durable: durable,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fresh.ResetInMemoryState(); err != nil {
			t.Error(err)
		}
	}()
	waitHistoricalPublicationCompletionObservation(t, ctx, selected, fact, publication.Event.Event().RunID())
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	original := publication.Event.Event()
	plans, err := fresh.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: original}})
	if err != nil || len(plans) != 1 {
		t.Fatalf("fresh Bus refused original historical selector: %v", err)
	}
	defer func() {
		if err := fresh.ReleaseEnginePublications(context.WithoutCancel(ctx), plans); err != nil {
			t.Error(err)
		}
	}()
	command := plans[0].(bus.EnginePublicationPlan).PublicationCommand()
	if len(command.Activations) != 0 || !reflect.DeepEqual(command.Commit.DeliveryRoutes, publication.DeliveryRoutes) {
		t.Fatal("fresh replay changed historical dependency, exact claims or activation")
	}
	duplicate, err := selected.CommitPublication(ctx, command)
	if err != nil || !duplicate.Acknowledged || duplicate.AppendOutcome != bus.EventAppendExactDuplicate || len(duplicate.DeliveryHandoffs) != 0 || len(duplicate.Activations) != 0 {
		t.Fatalf("historical replay created new publication or work: %+v %v", duplicate, err)
	}
	if err := fresh.ReleaseEnginePublications(ctx, plans); err != nil {
		t.Fatal(err)
	}
	for _, original := range []deliverylifecycle.Snapshot{node, agent} {
		claim, err := selected.ClaimDelivery(ctx, original.Authority, publication.Event.Event(), original.Route)
		// The selected execution is already closed. Replay cannot reacquire it;
		// refusal must still observe the identical settled obligation.
		if err != nil || claim.Disposition != deliverylifecycle.ClaimWrongAuthority || !reflect.DeepEqual(claim.Snapshot, original) {
			t.Fatalf("closed historical execution changed a settled claim: %+v %v", claim, err)
		}
	}
	changedTarget := agent.Route.Initialization.ConstructionTarget().Route()
	changedTarget.FlowID = "foreign"
	changed := eventtest.TargetRouted(original, changedTarget)
	if hostile, err := fresh.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: changed}}); err == nil {
		_ = fresh.ReleaseEnginePublications(ctx, hostile)
		t.Fatal("changed historical selector reused the publication")
	}
	for _, variant := range []string{"source_run", "revision"} {
		route := agent.Route
		dependency, _ := route.Initialization.HistoricalConstruction()
		if variant == "source_run" {
			dependency.SourceRunID = uuid.NewString()
		} else {
			dependency.SourceRevision++
		}
		route.Initialization, err = events.AdmitHistoricalFlowReceiverInitialization(publication.Event.Event().RunID(), route.Initialization.ConstructionTarget(), route.Initialization.ReceiverRoute(), dependency)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := selected.ClaimDelivery(ctx, agent.Authority, publication.Event.Event(), route)
		if err != nil || claim.Disposition != deliverylifecycle.ClaimAbsent {
			t.Fatalf("changed historical dependency acquired a claim: %+v %v", claim, err)
		}
	}
	for _, original := range []deliverylifecycle.Snapshot{node, agent} {
		after, err := selected.Snapshot(ctx, original.DeliveryID)
		if err != nil || !reflect.DeepEqual(original, after) {
			t.Fatalf("fresh replay changed exact delivery: %+v %v", after, err)
		}
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	for table, original := range before {
		if !reflect.DeepEqual(original, after[table]) {
			t.Errorf("historical replay/refusal changed %s: before=%+v after=%+v", table, original, after[table])
		}
	}
	h.llm.mu.Lock()
	afterCalls := append([]scriptedDeliveryCall(nil), h.llm.deliveryCalls...)
	h.llm.mu.Unlock()
	if !reflect.DeepEqual(calls, afterCalls) {
		t.Fatal("historical replay/refusal executed another provider turn")
	}
}

func waitHistoricalPublicationCompletionObservation(t *testing.T, ctx context.Context, selected interface {
	ListCompletionCandidates(context.Context, runlifecycle.CandidateScope, runlifecycle.CandidateCursor, int) (runlifecycle.CandidatePage, error)
}, source correlation.SourceArtifactFact, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, catalogRuntimePublishTimeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		page, err := selected.ListCompletionCandidates(ctx, runlifecycle.CandidateScope{BundleHash: source.BundleHash()}, runlifecycle.CandidateCursor{}, 128)
		if err != nil || !page.Exhausted {
			t.Fatalf("read exact catalog completion frontier: %+v %v", page, err)
		}
		pending := false
		for _, candidate := range page.Candidates {
			pending = pending || candidate.RunID == runID
		}
		if !pending {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for exact historical publication completion observation: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
