package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestFinalSelectionTerminalizationVersusEngineCommitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"engine_first", "terminal_first", "concurrent"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				selected, db, parent, runID := openStateOnlyAcquisitionStore(t, backend)
				ctx, cancel := context.WithTimeout(parent, 10*time.Second)
				defer cancel()
				engine := selected.(runtimepipeline.WorkflowEngineMutationOwner)
				lifecycle := selected.(runlifecycle.OperationOwner)
				flow, instance, entity := "selection-race", "selection-race/receiver", uuid.NewString()
				created := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				seedWorkflowTargetStateForTransition(t, backend, db, runID, entity, instance, "active", 1, created)
				route := events.DeliveryRoute{
					Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("engine-settlement")),
					Target:    events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flow, FlowInstance: instance, EntityID: entity}),
				}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "engine.delivery.requested", "fixture", "", []byte(`{}`), 0, runID,
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entity), instance), created)
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				fact, err := handlerselection.NoMatch(handlerselection.ContextRules)
				if err != nil {
					t.Fatal(err)
				}
				command := runtimepipeline.WorkflowEngineMutationCommand{
					State:           stateOnlyWorkflowEngineMutationRecord(t, runID, flow, instance, entity, "active", 1, created),
					DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, RuleSelection: fact, SideEffects: []string{"handler_completed"}},
				}
				commit := func() error { _, err := engine.CommitWorkflowEngineMutation(ctx, command); return err }
				terminal := func() error {
					_, _, err := lifecycle.MarkTerminalRun(ctx, runlifecycle.TerminalRequest{RunID: runID, State: runlifecycle.StateCancelled, EndedAt: time.Now().UTC()})
					return err
				}
				var commitErr, terminalErr error
				switch order {
				case "engine_first":
					commitErr, terminalErr = commit(), terminal()
				case "terminal_first":
					terminalErr, commitErr = terminal(), commit()
				case "concurrent":
					start := make(chan struct{})
					engineDone, terminalDone := make(chan error, 1), make(chan error, 1)
					go func() { <-start; engineDone <- commit() }()
					go func() { <-start; terminalDone <- terminal() }()
					close(start)
					commitErr, terminalErr = <-engineDone, <-terminalDone
				}
				if terminalErr != nil || (order == "engine_first" && commitErr != nil) || (order == "terminal_first" && commitErr == nil) {
					t.Fatalf("wrong linearization: engine=%v terminal=%v", commitErr, terminalErr)
				}
				snapshot, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				final, err := snapshot.FinalSelection.Fact()
				if err != nil {
					t.Fatal(err)
				}
				if commitErr == nil {
					if snapshot.Status != runtimedelivery.StatusDelivered || !final.Equal(fact) {
						t.Fatalf("engine winner lost final truth: %+v", snapshot)
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entity, instance, flow, "done", 2, 1)
				} else {
					if snapshot.Status != runtimedelivery.StatusDeadLetter || !final.Equal(handlerselection.NotApplicable()) {
						t.Fatalf("terminal winner lost nonexecution truth: %+v", snapshot)
					}
					assertWorkflowTargetTransitionRows(t, backend, db, runID, entity, instance, "", "active", 1, 0)
				}
				var count, open int
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_handler_rule_selections WHERE delivery_id=$1`, snapshot.DeliveryID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("final facts=%d err=%v", count, err)
				}
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1 AND open_marker=TRUE`, snapshot.DeliveryID).Scan(&open); err != nil || open != 0 {
					t.Fatalf("open attempts=%d err=%v", open, err)
				}
				before := snapshot.FinalSelection
				if err := commit(); err == nil {
					t.Fatal("terminal delivery permitted another engine commit")
				}
				after, err := selected.Snapshot(ctx, snapshot.DeliveryID)
				if err != nil || after.FinalSelection != before {
					t.Fatalf("late engine attempt changed final fact: %v", err)
				}
			})
		}
	}
}
