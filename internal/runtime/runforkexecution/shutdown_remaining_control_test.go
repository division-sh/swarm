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

func TestSelectedGracefulDrainRetainsNativePublicationHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newActivityForkFixture(t, backend)
			ctx, request, _ := q1GateComponentSource(t, fixture.owner, false, true, true)
			attachment := q1StageBranchPointComponent(t, ctx, request)
			p, container := attachment.prepared, attachment.container
			if err := p.retainedRuntime.startExecution(container.attachment.ctx, container.attachment.pipeline, container.diagnostics, false); err != nil {
				t.Fatal(err)
			}
			reconcile, err := container.selectedBarrierReconciler()
			if err != nil {
				t.Fatal(err)
			}
			selected := p.operation.selected
			coordinator, err := deliverycontinuation.NewSelected(p.owner.ports.busDurable.DeliveryLifecycle, container.attachment.deliveryAuthority, selected, container.attachment.runtime.bus, nil, reconcile)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = coordinator.Retire(context.Background()) })
			if err := container.attachment.runtime.bus.SetDeliveryContinuationOwner(coordinator); err != nil {
				t.Fatal(err)
			}
			if err := coordinator.Start(container.attachment.ctx); err != nil {
				t.Fatal(err)
			}
			finish, err := p.owner.beginSelectedOrchestration(p)
			if err != nil {
				t.Fatal(err)
			}
			defer finish()
			held, err := selected.Begin(p.operation.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer held.Done()
			inputs, err := container.commitSelectedInputs()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				for _, input := range inputs {
					if err := container.attachment.runtime.bus.AbandonPreparedPublish(context.WithoutCancel(input.ctx), input.prepared); err != nil {
						t.Error(err)
					}
				}
			}()
			var proofs []deliverylifecycle.DurableHandoffProof
			for _, input := range inputs {
				committed, err := input.prepared.CommittedDeliveryHandoffs()
				if err != nil {
					t.Fatal(err)
				}
				proofs = append(proofs, committed...)
			}
			if len(proofs) == 0 {
				t.Fatal("selected graceful handoff lacks a real native publication")
			}
			drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			drained := make(chan error, 1)
			go func() { drained <- p.owner.DrainSelectedContexts(drainCtx) }()
			for {
				probe, err := selected.Begin(ctx)
				if probe != nil {
					_ = probe.Done()
				}
				if err == worklifetime.ErrAdmissionFenced {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-drainCtx.Done():
					t.Fatal("selected native drain did not seal fresh admission")
				case <-time.After(time.Millisecond):
				}
			}
			_ = coordinator.Synchronize(drainCtx)
			if err := coordinator.AcceptCommitted(proofs); err != nil {
				t.Errorf("selected drain refused native committed handoffs: %v", err)
			}
			for _, proof := range proofs {
				row, err := p.owner.ports.busDurable.DeliveryLifecycle.Snapshot(ctx, proof.DeliveryID())
				if err != nil || row.EventID != proof.EventID() {
					t.Fatalf("native handoff lost its exact publication: %+v %v", row, err)
				}
			}
			if held.Context().Err() != nil || drainCtx.Err() != nil {
				t.Fatal("native handoff outlived its original accepted context or drain")
			}
			_ = held.Done()
			finish()
			_ = p.operation.releaseOrchestration()
			select {
			case err := <-drained:
				if err != nil {
					t.Error(err)
				}
			case <-drainCtx.Done():
				t.Fatal("native handoff drain did not join accepted work")
			}
		})
	}
}
