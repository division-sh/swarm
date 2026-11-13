package runforkexecution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type selectedDrainControlStore struct{ deliverylifecycle.Store }

func (selectedDrainControlStore) ScanDeliveryContinuations(context.Context, deliverylifecycle.ExecutionAuthority, deliverylifecycle.ContinuationCursor, int) (deliverylifecycle.ContinuationPage, error) {
	return deliverylifecycle.ContinuationPage{Exhausted: true}, nil
}

func (selectedDrainControlStore) ObserveDeliveryContinuations(context.Context, deliverylifecycle.ExecutionAuthority, []string) ([]deliverylifecycle.ContinuationObservation, error) {
	return nil, nil
}

type selectedDrainControlDispatcher struct{}

func (selectedDrainControlDispatcher) DispatchDeliveryContinuation(context.Context, events.Event, events.DeliveryRoute) deliverycontinuation.DispatchResult {
	return deliverycontinuation.Transferred()
}

func TestSelectedGracefulDrainRetainsAcceptedPublicationHandoff(t *testing.T) {
	owner, _, ctx := contextLifetimeOwner(t)
	p := contextLifetimePreparation(t, owner, ctx)
	bindContextLifetimePreparation(t, owner, p)
	finish, err := owner.beginSelectedOrchestration(p)
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	selected := p.operation.selected
	held, err := selected.Begin(p.operation.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Done()
	normal, err := deliverylifecycle.NewNormalExecutionAuthority(sourceartifactfixture.Fact(), "selected-stop-control", 1)
	if err != nil {
		t.Fatal(err)
	}
	identity := selected.Identity()
	authority, err := deliverylifecycle.NewSelectedExecutionAuthority(normal.SourceArtifact(), identity.ExecutionID, identity.RunID, identity.Generation)
	if err != nil {
		t.Fatal(err)
	}
	c, err := deliverycontinuation.NewSelected(selectedDrainControlStore{}, authority, selected, selectedDrainControlDispatcher{}, nil, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Retire(context.Background()) })
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- owner.DrainSelectedContexts(drainCtx) }()
	for {
		probe, err := selected.Begin(ctx)
		if probe != nil {
			_ = probe.Done()
		}
		if errors.Is(err, worklifetime.ErrAdmissionFenced) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-drainCtx.Done():
			t.Fatal("selected shutdown did not fence new work")
		case <-time.After(time.Millisecond):
		}
	}
	// Synchronize observes the production selected fence while the accepted
	// producer and its orchestration still hold their original leases.
	_ = c.Synchronize(drainCtx)
	proof, err := deliverylifecycle.AdmitDurableHandoffProof(uuid.NewString(), uuid.NewString(), "selected-stop-control-route", authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptCommitted([]deliverylifecycle.DurableHandoffProof{proof}); err != nil {
		t.Errorf("selected graceful drain refused its already-accepted producer handoff: %v", err)
	}
	if held.Context().Err() != nil || drainCtx.Err() != nil {
		t.Error("control did not retain the accepted producer within its drain budget")
	}
	_ = held.Done()
	finish()
	// The preparation lease is not an accepted business producer.
	_ = p.operation.releaseOrchestration()
	select {
	case err := <-drained:
		if err != nil {
			t.Error(err)
		}
	case <-drainCtx.Done():
		t.Fatal("selected drain failed to join the released accepted producer")
	}
}
