package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestServedForkLoopGenerationStateEffectBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyForkLoopGenerationState(t)
			bundle := loadWorkflowValidationBundleAt(t, root)
			carriage, err := semanticview.CompileOriginalLoopCarriage(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			if err := carriage.RequireSource(bundle.SourceArtifact.BundleHash()); err != nil {
				t.Fatal(err)
			}
			if err := carriage.RequireSource("different-selected-source"); err == nil {
				t.Fatal("selected artifact supplied original meaning")
			}
			role, found, err := carriage.Resolve(semanticview.LoopEventScope{FlowID: "review", EventType: "review/review.requested"})
			if err != nil || !found || role.LoopID() != "revision" || role.RevisionField() != "revision_id" {
				t.Fatalf("original input declaration: %+v %v %v", role, found, err)
			}
			publication := newExactOriginPublicationProbe(t)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			started := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"token": "loop-notice-proof"}, "idempotency_key": "loop-notice-start"})
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, started.RunID)
			waitForkReceiverSourceCompletion(t, rt.ReceiverStateReader, started.RunID)
			var frontier string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='review/review.requested'`, started.RunID).Scan(&frontier); err != nil {
				t.Fatal(err)
			}
			source := readForkLoopActivation(t, rt.ReceiverStateReader, started.RunID)
			if source.Attempt != 1 || source.MaxAttempts != 3 || source.FlowID != "review" || source.CurrentStage != "reviewing" {
				t.Fatalf("ordinary source loop: %+v", source)
			}
			sourceRoute := requireForkLoopStateEffect(t, rt.Endpoint, rt.ReceiverStateReader, started.RunID, frontier, source.RevisionID)
			if err := waitExactOriginPublications(t.Context(), publication, started.EventID); err != nil {
				t.Fatal(err)
			}
			sourceDomain := readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), started.RunID)
			sourceNotices := readForkReceiverNoticeDomain(t, rt.ReceiverStateReader, started.RunID)
			params := map[string]any{"source_run_id": started.RunID, "fork_event_id": frontier, "allow_source_freeze": true, "idempotency_key": "loop-notice-fork"}
			var fork apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			if fork.SourceRunID != started.RunID || fork.ForkEventID != frontier || fork.ForkRunID == "" || fork.ForkRunID == started.RunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("fork: %+v", fork)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, fork.ForkRunID)
			child := readForkLoopActivation(t, rt.ReceiverStateReader, fork.ForkRunID)
			expected, err := loopruntime.Fork(source, fork.ForkRunID, flowidentity.EntityID("review"))
			if err != nil {
				t.Fatal(err)
			}
			if child.Generation() != expected.Generation() || child.MaxAttempts != source.MaxAttempts || child.CurrentStage != "reviewing" {
				t.Fatalf("child generation or executed stage: %+v, expected %+v", child, expected)
			}
			childEvent := activityidentity.ForkLineageEventID(fork.ForkRunID, frontier)
			childRoute := requireForkLoopStateEffect(t, rt.Endpoint, rt.ReceiverStateReader, fork.ForkRunID, childEvent, expected.RevisionID)
			if !reflect.DeepEqual(sourceRoute, childRoute) {
				t.Fatalf("loop fork changed exact recipient/target/connect identity: source=%+v child=%+v", sourceRoute, childRoute)
			}
			beforeRepeat := readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), fork.ForkRunID)
			noticesBeforeRepeat := readForkReceiverNoticeDomain(t, rt.ReceiverStateReader, fork.ForkRunID)
			var replay apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &replay)
			if !reflect.DeepEqual(fork, replay) || !reflect.DeepEqual(beforeRepeat, readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), fork.ForkRunID)) || !reflect.DeepEqual(noticesBeforeRepeat, readForkReceiverNoticeDomain(t, rt.ReceiverStateReader, fork.ForkRunID)) {
				t.Fatal("replayed fork changed child state or repeated the business write")
			}
			if !reflect.DeepEqual(sourceDomain, readServedForkRecipientSourceDomain(t, rt.Runtime.Bus.Store(), started.RunID)) || !reflect.DeepEqual(sourceNotices, readForkReceiverNoticeDomain(t, rt.ReceiverStateReader, started.RunID)) {
				t.Fatal("fork changed source loop or business history")
			}
		})
	}
}

func readForkLoopActivation(t *testing.T, selectedProofReceiverStateReader receiverProofStateReader, runID string) loopruntime.Activation {
	t.Helper()
	var raw string
	if err := func() error {
		observed, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (string, error) {
			return storetest.ReadEntityAccumulatorTextStorage(ctx, selectedProofReceiverStateReader, runID, flowidentity.EntityID("review"))
		})
		raw = observed
		return err
	}(); err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		t.Fatal(err)
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, nil, nil, state)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil || len(activations) != 1 {
		t.Fatalf("loop inventory: %+v %v", activations, err)
	}
	return activations[0]
}

func requireForkLoopStateEffect(t *testing.T, selectedProofEndpoint string, selectedProofReceiverStateReader receiverProofStateReader, runID, eventID, revision string) events.DeliveryRoute {
	t.Helper()
	var public operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, selectedProofEndpoint, "event.get", map[string]any{"event_id": eventID}, &public)
	if public.RunID != runID || public.Payload["revision_id"] != revision || public.Payload["token"] != "loop-notice-proof" || len(public.Deliveries) != 1 || public.Deliveries[0].Status != "delivered" || len(public.DeadLetters) != 0 {
		t.Fatalf("executed revision public readback: %+v", public)
	}
	entityID := flowidentity.EntityID("review")
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, selectedProofEndpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &entity)
	if entity.Entity.RunID != runID || entity.Entity.EntityID != entityID || entity.Entity.FlowInstance != "review" || entity.Entity.CurrentState != "reviewing" || entity.Fields["observed_revision"] != revision || entity.Fields["observed_token"] != "loop-notice-proof" {
		t.Fatalf("loop business fields lost current generation: %+v", entity)
	}
	activation := readForkLoopActivation(t, selectedProofReceiverStateReader, runID)
	if !activation.Generation().Valid() || activation.RevisionID != revision {
		t.Fatalf("durable loop generation: %+v", activation)
	}
	mutation, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (storetest.ReceiverFieldMutationStorage, error) {
		return storetest.ReadLoopObservedRevisionMutationStorage(ctx, selectedProofReceiverStateReader, runID, entityID, eventID)
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, writer, writerID, step := mutation.New, mutation.WriterType, mutation.WriterID, mutation.Step
	var written string
	if err := json.Unmarshal([]byte(raw), &written); err != nil {
		t.Fatal(err)
	}
	mutations, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
		return storetest.CountLoopObservedRevisionMutationsStorage(ctx, selectedProofReceiverStateReader, runID, entityID, eventID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if written != revision || writer != "platform" || writerID != "workflow_engine" || step != "mutate" || mutations != 1 {
		t.Fatalf("exact loop effect: revision=%s writer=%s/%s/%s mutations=%d", written, writer, writerID, step, mutations)
	}
	delivery := public.Deliveries[0]
	route := readServedForkDeliveryEvidence(t, selectedProofReceiverStateReader, runID)[delivery.DeliveryID].route
	node, nodeRecipient := route.Recipient.Node()
	if !nodeRecipient || node.FlowPath() != "review" || node.NodeID() != "controller" || route.Target.Code() != "existing_entity" || route.Target.Route() != (events.RouteIdentity{FlowID: "review", FlowInstance: "review", EntityID: entityID}) {
		t.Fatalf("loop effect borrowed recipient or target identity: %+v", route)
	}
	if delivery.Target != (operatorread.OperatorDeliveryTarget{Kind: "existing_entity", FlowID: "review", FlowInstance: "review", EntityID: entityID}) {
		t.Fatalf("loop delivery lost exact target: %+v", delivery)
	}
	settlement, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (storetest.LoopReceiverSettlementStorage, error) {
		return storetest.ReadLoopReceiverSettlementStorage(ctx, selectedProofReceiverStateReader, runID, delivery.DeliveryID)
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, outcomeClaim := settlement.Claim, settlement.OutcomeClaim
	outcome, selectionContext, disposition := settlement.Outcome, settlement.Context, settlement.Disposition
	outcomes, err := readServedSelectedRole(context.Background(), selectedProofReceiverStateReader, func(ctx context.Context) (int, error) {
		return storetest.CountForkReceiverSettledAttemptsStorage(ctx, selectedProofReceiverStateReader, delivery.DeliveryID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim != 1 || outcomeClaim != 1 || outcomes != 1 || outcome != "delivered" || selectionContext != "none" || disposition != "not_applicable" {
		t.Fatalf("loop settlement claim=%d/%d outcomes=%d outcome=%s selection=%s/%s", claim, outcomeClaim, outcomes, outcome, selectionContext, disposition)
	}
	requireForkReceiverNoPublicationOrNotice(t, selectedProofReceiverStateReader, runID, eventID)
	return route
}
