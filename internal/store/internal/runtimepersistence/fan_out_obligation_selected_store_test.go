package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type selectedFanOutOwner interface {
	pipeline.FanOutObligationOwner
}

type selectedFanOutResourceOwner interface {
	selectedFanOutOwner
	UpsertBundleCatalogWithData(context.Context, bundlecatalog.Upsert, durabledata.Catalog) (bundlecatalog.UpsertResult, error)
	ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
}

type selectedFanOutDiagnosticOwner interface {
	selectedFanOutOwner
	LoadRunDebugReport(context.Context, string, operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error)
}

type selectedFanOutLifecycleOwner interface {
	selectedFanOutDiagnosticOwner
	PipelineObligations() runtimepipelineobligation.Store
	StopRunControl(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error)
}

type selectedFanOutMixedRouteOwner interface {
	selectedFanOutOwner
	storeTestDurableEventBusStore
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type selectedFanOutTxSummaryOwner interface {
	SummarizeFanOutRunTx(context.Context, *sql.Tx, string, time.Time) (fanoutobligation.RunSummary, error)
}

type fanOutOwnerFixture struct {
	runID      string
	eventID    string
	deliveryID string
	packageKey string
	elementID  string
	createdAt  time.Time
	bundleHash string
}

func TestFanOutSelectedStoreOwnerParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = newPostgresStoreWithBackend(mustPostgresBackend(db))
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				owner = store
			}

			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 10, time.Now().UTC().Truncate(time.Microsecond))
			claimAt := fixture.createdAt.Add(time.Second)
			intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "worker-a", BundleHash: fixture.bundleHash, Now: claimAt, Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim first fan-out intent: found=%v err=%v", found, err)
			}
			if intent.Request.Cardinality != 10 || intent.Cursor != 0 || intent.NextChunkSize != fanoutobligation.InitialChunkSize {
				t.Fatalf("claimed intent = %#v", intent)
			}
			input, err := owner.LoadFanOutEvaluation(ctx, claim)
			if err != nil {
				t.Fatalf("load bounded fan-out source: %v", err)
			}
			if input.StartOrdinal != 0 || len(input.Items) != fanoutobligation.InitialChunkSize || fmt.Sprint(input.Items[0]) != "item-000" || fmt.Sprint(input.Items[3]) != "item-003" {
				t.Fatalf("bounded source = start %d items %#v", input.StartOrdinal, input.Items)
			}

			stale := claim
			stale.Generation++
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(stale, 0, 4, claimAt.Add(time.Second))); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("stale generation commit error = %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)

			first, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 4, claimAt.Add(2*time.Second)))
			if err != nil {
				t.Fatalf("commit first fan-out chunk: %v", err)
			}
			if first.Intent.Cursor != 4 || first.Intent.Status != fanoutobligation.StatusOpen {
				t.Fatalf("first committed intent = %#v", first.Intent)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 4, 4)

			_, secondClaim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "worker-b", BundleHash: fixture.bundleHash, Now: claimAt.Add(3 * time.Second), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim remaining fan-out range: found=%v err=%v", found, err)
			}
			secondInput, err := owner.LoadFanOutEvaluation(ctx, secondClaim)
			if err != nil {
				t.Fatalf("load remaining fan-out range: %v", err)
			}
			if secondInput.StartOrdinal != 4 || len(secondInput.Items) != 5 || fmt.Sprint(secondInput.Items[0]) != "item-004" || fmt.Sprint(secondInput.Items[4]) != "item-008" {
				t.Fatalf("remaining source = start %d items %#v", secondInput.StartOrdinal, secondInput.Items)
			}
			second, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(secondClaim, 4, 5, claimAt.Add(4*time.Second)))
			if err != nil {
				t.Fatalf("commit second fan-out chunk: %v", err)
			}
			if second.Intent.Cursor != 9 || second.Intent.Status != fanoutobligation.StatusOpen {
				t.Fatalf("second committed intent = %#v", second.Intent)
			}
			_, terminalClaim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "worker-c", BundleHash: fixture.bundleHash, Now: claimAt.Add(5 * time.Second), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim terminal fan-out range: found=%v err=%v", found, err)
			}
			closed, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(terminalClaim, 9, 1, claimAt.Add(6*time.Second)))
			if err != nil {
				t.Fatalf("commit terminal fan-out chunk: %v", err)
			}
			if closed.Intent.Cursor != 10 || closed.Intent.Status != fanoutobligation.StatusClosed {
				t.Fatalf("closed intent = %#v", closed.Intent)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 10, 10)

			summary, err := owner.FanOutRunSummary(ctx, fixture.runID, claimAt.Add(7*time.Second))
			if err != nil {
				t.Fatalf("summarize closed fan-out: %v", err)
			}
			if summary.BlocksCompletion() || summary.SemanticRejected != 10 || summary.Committed != 0 || summary.Owed != 0 {
				t.Fatalf("closed fan-out summary = %#v", summary)
			}

			cancelFixture := seedFanOutOwnerIntent(t, ctx, db, fixture, 25, fixture.createdAt.Add(time.Minute))
			if err := owner.CancelRunFanOut(ctx, fixture.runID, "run terminated", fixture.createdAt.Add(2*time.Minute)); err != nil {
				t.Fatalf("cancel fan-out suffix: %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, cancelFixture, 0, 0)
			summary, err = owner.FanOutRunSummary(ctx, fixture.runID, fixture.createdAt.Add(3*time.Minute))
			if err != nil {
				t.Fatalf("summarize canceled fan-out: %v", err)
			}
			if summary.Canceled != 25 || summary.BlocksCompletion() {
				t.Fatalf("canceled fan-out summary = %#v", summary)
			}
		})
	}
}

func TestFanOutChunkCommitsMixedRealEventBusPlansAtomicallyOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runtimeInstanceID := uuid.NewString()
			ctx := runtimeauthoractivity.WithScope(
				testAuthorActivityContext(),
				runtimeauthoractivity.BundleScope(runtimeInstanceID, authorActivityTestBundleHash),
			)
			var (
				owner        selectedFanOutMixedRouteOwner
				summaryOwner selectedFanOutTxSummaryOwner
				db           *sql.DB
				postgres     bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				store := admitTestPostgresStore(t, db)
				owner = store
				summaryOwner = store.pipelinePostgresOwner
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, owner = store.backend.ConstructionHandle(), store
				summaryOwner = store.pipelineSQLiteOwner
			}

			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 4, time.Now().UTC().Truncate(time.Microsecond))
			runCtx := runtimecorrelation.WithRunID(ctx, fixture.runID)
			scope, ok := runtimeauthoractivity.ScopeFromContext(runCtx)
			if !ok {
				t.Fatal("mixed-route proof requires exact author scope")
			}
			descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, 3)
			for _, name := range []string{"mixed.none", "mixed.one", "mixed.multi"} {
				descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{EventType: "producer/" + name, Disposition: runtimeauthoractivity.StoryAuthored})
			}
			lease, err := owner.RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatalf("register mixed-route event catalog: %v", err)
			}
			t.Cleanup(lease.Release)

			bundle := fanOutMixedRouteBundle()
			bus, err := newStoreTestEventBus(t, owner, runtimebus.EventBusOptions{
				ContractBundle:    semanticview.Wrap(bundle),
				BundleSourceFact:  mustStoreTestEphemeralBundleSourceFact(fixture.bundleHash),
				RuntimeInstanceID: runtimeInstanceID,
			})
			if err != nil {
				t.Fatalf("construct mixed-route EventBus: %v", err)
			}
			sourceRoute := events.RouteIdentity{FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString()}.Normalized()
			routingSource, err := events.NewStaticFlowRoutingSource(sourceRoute)
			if err != nil {
				t.Fatalf("construct mixed-route source: %v", err)
			}
			planCtx := runtimedelivery.WithRoute(runCtx, events.DeliveryRoute{Target: events.MustExistingEntityTarget(sourceRoute)})
			plans := make([]runtimebus.EnginePublicationPlan, 0, 3)
			for ordinal, name := range []string{"mixed.none", "mixed.one", "mixed.multi"} {
				event := eventtest.ChildForProducerWithRoutingSource(
					uuid.NewString(), events.EventType("producer/"+name), eventtest.Producer(events.EventProducerNode, "fan-out-producer"), "",
					[]byte(`{}`), 0,
					events.EventLineage{RunID: fixture.runID, ParentEventID: fixture.eventID, ExecutionMode: executionmode.Live},
					events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute), routingSource, fixture.createdAt.Add(time.Duration(ordinal+1)*time.Second),
				)
				prepared, prepareErr := bus.PrepareEnginePublications(planCtx, []runtimeengine.EmitIntent{{Event: event}})
				if prepareErr != nil || len(prepared) != 1 {
					t.Fatalf("prepare %s publication = plans:%d err:%v", name, len(prepared), prepareErr)
				}
				plan, ok := prepared[0].(runtimebus.EnginePublicationPlan)
				if !ok {
					t.Fatalf("prepared %s plan has type %T", name, prepared[0])
				}
				plans = append(plans, plan)
			}
			commands := []runtimebus.PublicationCommand{plans[0].PublicationCommand(), plans[1].PublicationCommand(), plans[2].PublicationCommand()}
			if len(commands[0].Commit.DeliveryRoutes) != 0 || !commands[0].Commit.RouteSettlement.NoDelivery() {
				t.Fatalf("no-route plan = routes:%#v settlement:%#v", commands[0].Commit.DeliveryRoutes, commands[0].Commit.RouteSettlement)
			}
			if len(commands[1].Commit.DeliveryRoutes) != 1 || len(commands[1].Activations) != 0 {
				t.Fatalf("one-route plan = routes:%#v activations:%#v", commands[1].Commit.DeliveryRoutes, commands[1].Activations)
			}
			if len(commands[2].Commit.DeliveryRoutes) != 3 || len(commands[2].Activations) != 0 {
				t.Fatalf("multi-route plan = routes:%#v activations:%#v", commands[2].Commit.DeliveryRoutes, commands[2].Activations)
			}
			materializing := 0
			for _, route := range commands[2].Commit.DeliveryRoutes {
				if route.Target.Code() == "materializing_entity" {
					materializing++
				}
			}
			if materializing != 1 {
				t.Fatalf("multi-route plan materializing targets = %d in %#v", materializing, commands[2].Commit.DeliveryRoutes)
			}

			_, claim, found, err := owner.ClaimFanOutIntent(runCtx, pipeline.FanOutClaimRequest{
				Owner: "mixed-route-worker", BundleHash: fixture.bundleHash, Now: fixture.createdAt.Add(10 * time.Second), Lease: time.Minute,
			})
			if err != nil || !found {
				t.Fatalf("claim mixed-route intent: found=%v err=%v", found, err)
			}
			failure := rejectedFanOutChunk(claim, 3, 1, fixture.createdAt.Add(11*time.Second)).Outcomes[0].Failure
			outcomes := []pipeline.FanOutChunkOutcome{
				{Ordinal: 0, Publication: plans[0]},
				{Ordinal: 1, Publication: plans[1]},
				{Ordinal: 2, Publication: plans[2]},
				{Ordinal: 3, Failure: failure},
			}
			committed, err := owner.CommitFanOutChunk(runCtx, pipeline.FanOutChunkCommand{
				Claim: claim, Outcomes: outcomes, Now: fixture.createdAt.Add(11 * time.Second),
			})
			if err != nil {
				t.Fatalf("commit mixed-route chunk: %v", err)
			}
			if committed.Intent.Cursor != 4 || committed.Intent.Status != fanoutobligation.StatusClosed || len(committed.Publications) != 3 {
				t.Fatalf("mixed-route commit = intent:%#v publications:%d", committed.Intent, len(committed.Publications))
			}
			var committedEvents, committedDeliveries, outcomesCount int
			if err := db.QueryRowContext(runCtx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_id<>$2`, fixture.runID, fixture.eventID).Scan(&committedEvents); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(runCtx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND event_id<>$2`, fixture.runID, fixture.eventID).Scan(&committedDeliveries); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(runCtx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, fixture.runID).Scan(&outcomesCount); err != nil {
				t.Fatal(err)
			}
			childReadback, found, err := owner.LoadPreparedPublishEvent(runCtx, plans[2].DurablePublicationEventID())
			if err != nil || !found || len(childReadback.DeliveryRoutes) != 3 {
				t.Fatalf("child materializing route readback = found:%v value:%#v err:%v", found, childReadback, err)
			}
			readbackMaterializing := 0
			for _, route := range childReadback.DeliveryRoutes {
				if route.Target.Code() == "materializing_entity" {
					readbackMaterializing++
				}
			}
			if committedEvents != 3 || committedDeliveries != 4 || outcomesCount != 4 || readbackMaterializing != 1 {
				t.Fatalf("mixed-route durable facts = events:%d deliveries:%d outcomes:%d", committedEvents, committedDeliveries, outcomesCount)
			}

			tx, err := db.BeginTx(runCtx, nil)
			if err != nil {
				t.Fatalf("begin fan-out settlement summary transaction: %v", err)
			}
			defer tx.Rollback()
			summary, err := summaryOwner.SummarizeFanOutRunTx(runCtx, tx, fixture.runID, fixture.createdAt.Add(12*time.Second))
			if err != nil {
				t.Fatalf("summarize mixed-route fan-out in one transaction: %v", err)
			}
			if summary.Committed != 3 || summary.SemanticRejected != 1 || summary.Settled != 1 || summary.Unsettled != 2 || summary.BlocksCompletion() {
				t.Fatalf("mixed-route fan-out settlement summary = %#v", summary)
			}
			var sameTransactionOutcomes int
			if err := tx.QueryRowContext(runCtx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, fixture.runID).Scan(&sameTransactionOutcomes); err != nil {
				t.Fatalf("reuse fan-out summary transaction: %v", err)
			}
			if sameTransactionOutcomes != 4 {
				t.Fatalf("same-transaction fan-out outcomes = %d, want 4", sameTransactionOutcomes)
			}
		})
	}
}

func fanOutMixedRouteBundle() *runtimecontracts.WorkflowContractBundle {
	eventNames := []string{"mixed.none", "mixed.one", "mixed.multi"}
	outputPins := make([]runtimecontracts.FlowOutputEventPin, 0, len(eventNames))
	producerEvents := make(map[string]runtimecontracts.EventCatalogEntry, len(eventNames))
	for _, eventName := range eventNames {
		outputPins = append(outputPins, runtimecontracts.FlowOutputEventPin{Event: eventName})
		producerEvents[eventName] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}}
	}
	producerSchema := runtimecontracts.FlowSchemaDocument{
		Mode: runtimecontracts.FlowModeStatic,
		Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: outputPins}},
	}
	producer := runtimecontracts.FlowContractView{
		Path: "producer", Paths: runtimecontracts.FlowContractPaths{ID: "producer", Flow: "producer", PackageKey: "."}, Schema: producerSchema, Events: producerEvents,
	}
	flow := func(id, mode, eventName string, nodeIDs ...string) runtimecontracts.FlowContractView {
		handlers := make(map[string]runtimecontracts.SystemNodeContract, len(nodeIDs))
		for _, nodeID := range nodeIDs {
			handler := runtimecontracts.SystemNodeEventHandler{}
			if id == "child" {
				handler.CreateEntity = true
			}
			handlers[nodeID] = runtimecontracts.SystemNodeContract{
				ID: nodeID, ExecutionType: "system_node", SubscribesTo: []string{eventName},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{eventName: handler},
			}
		}
		schema := runtimecontracts.FlowSchemaDocument{
			Mode: mode, Entity: "test_entity", InitialState: "active", States: []string{"active"},
			Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{
				EventPins: []runtimecontracts.FlowInputEventPin{{Event: eventName}},
			}},
		}
		return runtimecontracts.FlowContractView{
			Path: id, Paths: runtimecontracts.FlowContractPaths{ID: id, Flow: id, PackageKey: "."}, Schema: schema, Nodes: handlers,
		}
	}
	one := flow("one", runtimecontracts.FlowModeStatic, "mixed.one", "one-node")
	multi := flow("multi", runtimecontracts.FlowModeStatic, "mixed.multi", "multi-a", "multi-b")
	child := flow("child", runtimecontracts.FlowModeSingleton, "mixed.multi", "child-node")
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{producer, one, multi, child}}
	connects := []runtimecontracts.FlowPackageConnect{
		{SourceFile: "package.yaml", SourceLine: 1, Event: "mixed.one", From: "producer", To: "one"},
		{SourceFile: "package.yaml", SourceLine: 2, Event: "mixed.multi", From: "producer", To: "multi"},
		{SourceFile: "package.yaml", SourceLine: 3, Event: "mixed.multi", From: "producer", To: "child"},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Package: runtimecontracts.ProjectPackageDocument{Name: "mixed-route", Version: "1.0.0"},
		PackageTree: []runtimecontracts.LoadedProjectPackage{{
			Key: ".", Paths: runtimecontracts.ProjectPackagePaths{PackageFile: "package.yaml"},
			Manifest: runtimecontracts.ProjectPackageDocument{Name: "mixed-route", Version: "1.0.0", Connect: connects},
		}},
		RootEntities: runtimecontracts.EntityContractsDocument{"test_entity": {Fields: map[string]runtimecontracts.EntityFieldDecl{}}},
		FlowTree: runtimecontracts.FlowTree{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"producer": &root.Children[0], "one": &root.Children[1], "multi": &root.Children[2], "child": &root.Children[3],
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"producer": producerSchema, "one": one.Schema, "multi": multi.Schema, "child": child.Schema,
		},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		panic(err)
	}
	return bundle
}

func TestFanOutCardinalityMatrixIsConstantAtTriggerAndExactAfterPumpOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = admitTestPostgresStore(t, db)
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, owner = store.backend.ConstructionHandle(), store
			}

			base := time.Now().UTC().Truncate(time.Microsecond)
			for caseIndex, cardinality := range []int{0, 1, 10, 25, 500, 1000} {
				t.Run(fmt.Sprintf("n_%d", cardinality), func(t *testing.T) {
					fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, cardinality, base.Add(time.Duration(caseIndex)*time.Minute))
					var eventsCount, deliveriesCount, intentsCount, outcomesCount int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1`, fixture.runID).Scan(&eventsCount); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, fixture.runID).Scan(&deliveriesCount); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&intentsCount); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, fixture.runID).Scan(&outcomesCount); err != nil {
						t.Fatal(err)
					}
					if eventsCount != 1 || deliveriesCount != 1 || intentsCount != 1 || outcomesCount != 0 {
						t.Fatalf("trigger rows for N=%d = events:%d deliveries:%d intents:%d outcomes:%d", cardinality, eventsCount, deliveriesCount, intentsCount, outcomesCount)
					}

					now := fixture.createdAt.Add(time.Second)
					turns := 0
					for {
						intent, claim, found, err := claimFanOutForRun(t, ctx, owner, fixture.runID, fixture.bundleHash, now)
						if err != nil {
							t.Fatalf("claim N=%d turn %d: %v", cardinality, turns, err)
						}
						if !found {
							break
						}
						input, err := owner.LoadFanOutEvaluation(ctx, claim)
						if err != nil {
							t.Fatalf("load N=%d turn %d: %v", cardinality, turns, err)
						}
						if input.StartOrdinal != intent.Cursor || len(input.Items) == 0 || len(input.Items) > intent.NextChunkSize {
							t.Fatalf("bounded N=%d turn %d input = start:%d len:%d intent:%#v", cardinality, turns, input.StartOrdinal, len(input.Items), intent)
						}
						if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, intent.Cursor, len(input.Items), now.Add(time.Second))); err != nil {
							t.Fatalf("commit N=%d turn %d: %v", cardinality, turns, err)
						}
						turns++
						now = now.Add(2 * time.Second)
						if turns > cardinality+1 {
							t.Fatalf("N=%d did not converge", cardinality)
						}
					}
					assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, cardinality, cardinality)
					if cardinality > 0 {
						var minOrdinal, maxOrdinal, distinctOrdinals int
						if err := db.QueryRowContext(ctx, `SELECT MIN(ordinal),MAX(ordinal),COUNT(DISTINCT ordinal) FROM fan_out_outcomes WHERE run_id=$1`, fixture.runID).Scan(&minOrdinal, &maxOrdinal, &distinctOrdinals); err != nil {
							t.Fatal(err)
						}
						if minOrdinal != 0 || maxOrdinal != cardinality-1 || distinctOrdinals != cardinality {
							t.Fatalf("N=%d ordinal evidence = min:%d max:%d distinct:%d", cardinality, minOrdinal, maxOrdinal, distinctOrdinals)
						}
					}
				})
			}
		})
	}
}

func TestFanOutRepresentativeStoreSizeDoesNotChangeN25OrN500ProgressOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = admitTestPostgresStore(t, db)
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, owner = store.backend.ConstructionHandle(), store
			}
			base := time.Now().UTC().Truncate(time.Microsecond)
			for caseIndex, cardinality := range []int{25, 500} {
				t.Run(fmt.Sprintf("n_%d", cardinality), func(t *testing.T) {
					fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, cardinality, base.Add(time.Duration(caseIndex)*time.Hour))
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					for index := 0; index < 256; index++ {
						closed := fixture
						closed.elementID = uuid.NewString()
						closed.createdAt = fixture.createdAt.Add(-time.Duration(index+1) * time.Second)
						insertFanOutOwnerIntent(t, ctx, tx, closed, 0, closed.createdAt)
					}
					if err := tx.Commit(); err != nil {
						t.Fatalf("commit representative closed-intent population: %v", err)
					}
					var population int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents`).Scan(&population); err != nil {
						t.Fatal(err)
					}
					if population < 257 {
						t.Fatalf("representative fan-out population = %d, want at least 257", population)
					}
					now := fixture.createdAt.Add(time.Second)
					for turns := 0; ; turns++ {
						intent, claim, found, err := claimFanOutForRun(t, ctx, owner, fixture.runID, fixture.bundleHash, now)
						if err != nil {
							t.Fatalf("claim representative N=%d turn %d: %v", cardinality, turns, err)
						}
						if !found {
							break
						}
						input, err := owner.LoadFanOutEvaluation(ctx, claim)
						if err != nil {
							t.Fatalf("load representative N=%d turn %d: %v", cardinality, turns, err)
						}
						if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, intent.Cursor, len(input.Items), now.Add(time.Second))); err != nil {
							t.Fatalf("commit representative N=%d turn %d: %v", cardinality, turns, err)
						}
						now = now.Add(2 * time.Second)
						if turns > cardinality {
							t.Fatalf("representative N=%d did not converge", cardinality)
						}
					}
					assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, cardinality, cardinality)
					intentFactKey := "intent|" + fixture.deliveryID + "|" + fixture.packageKey + "|" + fixture.elementID
					outcomeFactPattern := "outcome|" + fixture.deliveryID + "|" + fixture.packageKey + "|" + fixture.elementID + "|%"
					var revisionFacts, distinctFacts, repeatedOutcomeFacts int
					if err := db.QueryRowContext(ctx, `
						SELECT COUNT(*), COUNT(DISTINCT fact_key)
						FROM run_fork_fact_revisions
						WHERE run_id=$1 AND family='fan_out_obligations' AND (fact_key=$2 OR fact_key LIKE $3)
					`, fixture.runID, intentFactKey, outcomeFactPattern).Scan(&revisionFacts, &distinctFacts); err != nil {
						t.Fatalf("count representative fan-out revision facts: %v", err)
					}
					if err := db.QueryRowContext(ctx, `
						SELECT COUNT(*) FROM (
							SELECT fact_key
							FROM run_fork_fact_revisions
							WHERE run_id=$1 AND family='fan_out_obligations' AND fact_key LIKE $2
							GROUP BY fact_key HAVING COUNT(*) <> 1
						) repeated
					`, fixture.runID, outcomeFactPattern).Scan(&repeatedOutcomeFacts); err != nil {
						t.Fatalf("count repeated fan-out outcome revision facts: %v", err)
					}
					if distinctFacts != cardinality+1 || repeatedOutcomeFacts != 0 || revisionFacts > 2*cardinality+2 {
						t.Fatalf("representative N=%d revision facts = rows:%d distinct:%d repeated_outcomes:%d, want linear changed-fact evidence", cardinality, revisionFacts, distinctFacts, repeatedOutcomeFacts)
					}
				})
			}
		})
	}
}

func TestFanOutSchemaRejectsImpossibleStateAndDuplicateOutcomeOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = admitTestPostgresStore(t, db)
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, owner = store.backend.ConstructionHandle(), store
			}
			base := time.Now().UTC().Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 2, base)
			invalid := []struct {
				name string
				sql  string
				args []any
			}{
				{name: "open_at_cardinality", sql: `UPDATE fan_out_intents SET cursor=cardinality WHERE run_id=$1`, args: []any{fixture.runID}},
				{name: "open_with_reason", sql: `UPDATE fan_out_intents SET blocked_reason='forbidden' WHERE run_id=$1`, args: []any{fixture.runID}},
				{name: "closed_with_suffix", sql: `UPDATE fan_out_intents SET status='closed' WHERE run_id=$1`, args: []any{fixture.runID}},
				{name: "blocked_without_reason", sql: `UPDATE fan_out_intents SET status='blocked' WHERE run_id=$1`, args: []any{fixture.runID}},
				{name: "canceled_without_reason", sql: `UPDATE fan_out_intents SET status='canceled' WHERE run_id=$1`, args: []any{fixture.runID}},
				{name: "blocked_with_claim", sql: `UPDATE fan_out_intents SET status='blocked',blocked_reason='{}',claim_owner='hostile',claim_generation=1,lease_expires_at=$1 WHERE run_id=$2`, args: []any{base.Add(time.Minute), fixture.runID}},
			}
			for _, test := range invalid {
				t.Run(test.name, func(t *testing.T) {
					if _, err := db.ExecContext(ctx, test.sql, test.args...); err == nil {
						t.Fatalf("fresh schema accepted impossible fan-out state %s", test.name)
					}
					assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
				})
			}

			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "schema-worker", BundleHash: fixture.bundleHash, Now: base.Add(time.Second), Lease: time.Minute,
			})
			if err != nil || !found {
				t.Fatalf("claim schema fixture: found=%v err=%v", found, err)
			}
			command := rejectedFanOutChunk(claim, 0, 1, base.Add(2*time.Second))
			if _, err := owner.CommitFanOutChunk(ctx, command); err != nil {
				t.Fatalf("commit valid schema outcome: %v", err)
			}
			insert := `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,package_key,element_id,ordinal,outcome_kind,failure,created_at) VALUES ($1,$2,$3,$4,0,'semantic_rejected',$5,$6)`
			if postgres {
				insert = `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,package_key,element_id,ordinal,outcome_kind,failure,created_at) VALUES ($1::uuid,$2::uuid,$3,$4,0,'semantic_rejected',$5::jsonb,$6)`
			}
			if _, err := db.ExecContext(ctx, insert, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, string(command.Outcomes[0].Failure), base.Add(3*time.Second)); err == nil {
				t.Fatal("fresh schema accepted duplicate fan-out ordinal")
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
		})
	}
}

func TestFanOutDiagnosticsAndTestQuiescenceUseDurableOwnerOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutDiagnosticOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = newPostgresStoreWithBackend(mustPostgresBackend(db))
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				owner = store
			}

			now := time.Now().UTC().Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 10, now.Add(-time.Second))
			if err := acknowledgePipelineEventFixture(ctx, owner, fixture.eventID); err != nil {
				t.Fatalf("settle trigger pipeline obligation: %v", err)
			}

			report, err := owner.LoadRunDebugReport(ctx, fixture.runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatalf("load fan-out run diagnostics: %v", err)
			}
			want := fanoutobligation.RunSummary{
				RunID: fixture.runID, Intents: 1, Open: 1, Cardinality: 10, Owed: 10,
				BlockedIntents: []fanoutobligation.BlockedIntentDiagnosis{},
				MinNextChunk:   fanoutobligation.InitialChunkSize, MaxNextChunk: fanoutobligation.InitialChunkSize,
			}
			if report.FanOut.OldestAgeMS < 1000 {
				t.Fatalf("fan-out oldest age = %dms, want at least 1000ms", report.FanOut.OldestAgeMS)
			}
			report.FanOut.OldestAgeMS = 0
			if !reflect.DeepEqual(report.FanOut, want) {
				t.Fatalf("fan-out diagnostics = %#v, want %#v", report.FanOut, want)
			}
			if report.TestQuiescence.Ready || report.TestQuiescence.FanOutOwed != 10 {
				t.Fatalf("fan-out test quiescence = %#v, want sole owed blocker", report.TestQuiescence)
			}

			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "diagnostic-worker", BundleHash: fixture.bundleHash, Now: now, Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim diagnostic fan-out: found=%v err=%v", found, err)
			}
			blockedFailure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "fan_out_test_pump_blocked", "runtime.fan_out", "test", nil))
			if !ok {
				t.Fatal("construct blocked fan-out diagnosis")
			}
			if err := owner.BlockFanOutClaim(ctx, pipeline.FanOutBlockRequest{Claim: claim, Now: now.Add(time.Second), Failure: blockedFailure}); err != nil {
				t.Fatalf("block diagnostic fan-out: %v", err)
			}
			report, err = owner.LoadRunDebugReport(ctx, fixture.runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatalf("load blocked fan-out diagnostics: %v", err)
			}
			if report.FanOut.Open != 0 || report.FanOut.Blocked != 1 || report.FanOut.Owed != 10 || len(report.FanOut.BlockedIntents) != 1 {
				t.Fatalf("blocked fan-out diagnostics = %#v", report.FanOut)
			}
			diagnosis := report.FanOut.BlockedIntents[0]
			if diagnosis.TriggeringDeliveryID != fixture.deliveryID || diagnosis.PackageKey != fixture.packageKey || diagnosis.ElementID != fixture.elementID || diagnosis.Cursor != 0 || diagnosis.Owed != 10 || diagnosis.Failure.Detail.Code != blockedFailure.Detail.Code {
				t.Fatalf("typed blocked fan-out diagnosis = %#v", diagnosis)
			}
			if report.TestQuiescence.Ready || report.TestQuiescence.FanOutOwed != 10 {
				t.Fatalf("blocked fan-out test quiescence = %#v", report.TestQuiescence)
			}

			if err := owner.CancelRunFanOut(ctx, fixture.runID, "test cancellation", now.Add(2*time.Second)); err != nil {
				t.Fatalf("cancel diagnostic fan-out: %v", err)
			}
			report, err = owner.LoadRunDebugReport(ctx, fixture.runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatalf("reload canceled fan-out diagnostics: %v", err)
			}
			if report.FanOut.Canceled != 10 || report.FanOut.Owed != 0 || report.FanOut.BlocksCompletion() {
				t.Fatalf("canceled fan-out diagnostics = %#v", report.FanOut)
			}
			if !report.TestQuiescence.Ready || report.TestQuiescence.FanOutOwed != 0 {
				t.Fatalf("canceled fan-out test quiescence = %#v, want ready", report.TestQuiescence)
			}
		})
	}
}

func TestFanOutSemanticRejectionSampleIsDeterministicAcrossRestartOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			first, second, db, postgres := newFanOutOwnerPairForTest(t, backend)
			firstDiagnostics := first.(selectedFanOutDiagnosticOwner)
			secondDiagnostics := second.(selectedFanOutDiagnosticOwner)
			now := time.Now().UTC().Truncate(time.Microsecond)
			firstIntent := seedFanOutOwnerFixture(t, ctx, db, first, postgres, 1, now)
			secondIntent := seedFanOutOwnerIntent(t, ctx, db, firstIntent, 1, now.Add(time.Second))
			intents := []fanOutOwnerFixture{firstIntent, secondIntent}
			sort.Slice(intents, func(i, j int) bool { return intents[i].elementID < intents[j].elementID })

			// Insert in reverse semantic order; selection must ignore row order and time.
			for index := len(intents) - 1; index >= 0; index-- {
				fixture := intents[index]
				failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(
					runtimefailures.ClassSchemaInvalid, "fan_out_sample_invalid", "test", "semantic_rejection",
					map[string]any{"marker": fixture.elementID},
				))
				if !ok {
					t.Fatal("construct semantic rejection sample failure")
				}
				failureJSON, err := runtimefailures.MarshalEnvelope(failure)
				if err != nil {
					t.Fatal(err)
				}
				update := `UPDATE fan_out_intents SET cursor=1,status='closed',updated_at=$1 WHERE run_id=$2 AND triggering_delivery_id=$3 AND package_key=$4 AND element_id=$5`
				insert := `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,package_key,element_id,ordinal,outcome_kind,failure,created_at) VALUES ($1,$2,$3,$4,0,'semantic_rejected',$5,$6)`
				failureValue := any(string(failureJSON))
				if postgres {
					update = `UPDATE fan_out_intents SET cursor=1,status='closed',updated_at=$1 WHERE run_id=$2::uuid AND triggering_delivery_id=$3::uuid AND package_key=$4 AND element_id=$5`
					insert = `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,package_key,element_id,ordinal,outcome_kind,failure,created_at) VALUES ($1::uuid,$2::uuid,$3,$4,0,'semantic_rejected',$5::jsonb,$6)`
				}
				if _, err := db.ExecContext(ctx, update, now.Add(time.Duration(index+2)*time.Second), fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID); err != nil {
					t.Fatalf("close semantic rejection intent: %v", err)
				}
				if _, err := db.ExecContext(ctx, insert, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, failureValue, now.Add(time.Duration(index+2)*time.Second)); err != nil {
					t.Fatalf("insert semantic rejection outcome: %v", err)
				}
			}

			for _, owner := range []selectedFanOutDiagnosticOwner{firstDiagnostics, secondDiagnostics} {
				report, err := owner.LoadRunDebugReport(ctx, firstIntent.runID, operatorread.RunDebugQueryOptions{})
				if err != nil {
					t.Fatalf("load semantic rejection diagnosis: %v", err)
				}
				sample := report.FanOut.SemanticRejectionSample
				if report.FanOut.SemanticRejected != 2 || sample == nil {
					t.Fatalf("fan-out semantic rejection diagnosis = %#v", report.FanOut)
				}
				want := intents[0]
				if sample.TriggeringDeliveryID != want.deliveryID || sample.PackageKey != want.packageKey || sample.ElementID != want.elementID || sample.Ordinal != 0 || sample.Failure.Detail.Attributes["marker"] != want.elementID {
					t.Fatalf("deterministic semantic rejection sample = %#v, want intent %#v", sample, want)
				}
			}
		})
	}
}

func TestFanOutLifecycleBlocksCompletionAndStopCancelsClaimedSuffixOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				selected any
				owner    selectedFanOutLifecycleOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				store := admitTestPostgresStore(t, db)
				selected, owner, postgres = store, store, true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				selected, owner = store, store
			}

			base := time.Now().UTC().Truncate(time.Microsecond)
			completing := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, base)
			flowInsert := `INSERT OR IGNORE INTO flow_instances (instance_id,flow_template,mode,config,status) VALUES (?,?,'static','{}','active')`
			if postgres {
				flowInsert = `INSERT INTO flow_instances (instance_id,flow_template,mode,config,status) VALUES ($1,$2,'static','{}'::jsonb,'active') ON CONFLICT (instance_id) DO NOTHING`
			}
			if _, err := db.ExecContext(ctx, flowInsert, semanticRunFixtureFlow, semanticRunFixtureFlow); err != nil {
				t.Fatalf("seed terminal fan-out flow: %v", err)
			}
			if err := materializeCompletedRunEntityForTest(ctx, selected, completing.runID); err != nil {
				t.Fatalf("seed terminal fan-out entity: %v", err)
			}
			if err := acknowledgePipelineEventFixture(ctx, selected, completing.eventID); err != nil {
				t.Fatalf("acknowledge completion trigger: %v", err)
			}
			presence, err := owner.PipelineObligations().GlobalWorkPresence(ctx)
			if err != nil || !presence.ProcessingEligible {
				t.Fatalf("open fan-out global work = %#v err=%v", presence, err)
			}
			terminals := map[string][]string{semanticRunFixtureFlow: {"completed"}}
			if err := executeRunCompletionCandidateForEvent(ctx, selected, completing.eventID, nil, terminals); err != nil {
				t.Fatalf("execute blocked completion candidate: %v", err)
			}
			assertPortableRunStatus(t, ctx, db, completing.runID, "running", false)

			candidateOwner := selected.(runtimerunlifecycle.OperationOwner)
			candidateRegistrar := selected.(runtimerunlifecycle.CandidateRegistrar)
			if disposition, err := candidateOwner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(completing.runID)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
				t.Fatalf("request racing completion candidate = %s/%v", disposition, err)
			}
			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrenceForBundle(t, process, completing.bundleHash)
			runtimeCtx := worklifetime.WithRuntimeOccurrence(ctx, occurrence)
			intercept := &runLifecycleSameRevisionInterceptStore{
				delegate:       selected.(runtimerunlifecycle.CandidateStore),
				firstStarted:   make(chan struct{}),
				releaseFirst:   make(chan struct{}),
				secondExecuted: make(chan struct{}),
			}
			executor := newRunLifecycleParityExecutorForScope(
				t,
				intercept,
				occurrence,
				completing.bundleHash,
				runtimerunlifecycle.NewTerminalCatalog(nil, terminals),
			)
			registration, err := candidateRegistrar.RegisterCompletionCandidateSink(
				runtimeCtx,
				runtimerunlifecycle.CandidateScope{BundleHash: completing.bundleHash},
				executor,
			)
			if err != nil {
				t.Fatalf("register racing completion executor: %v", err)
			}
			defer registration.Release()
			if err := executor.Start(runtimeCtx); err != nil {
				t.Fatalf("start racing completion executor: %v", err)
			}
			awaitRunLifecycleSignal(t, intercept.firstStarted, "fan-out-blocked candidate execution")

			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "completion-worker", BundleHash: completing.bundleHash, Now: base.Add(time.Second), Lease: time.Minute,
			})
			if err != nil || !found {
				t.Fatalf("claim final completion range: found=%v err=%v", found, err)
			}
			if _, err := owner.CommitFanOutChunk(runtimeCtx, rejectedFanOutChunk(claim, 0, 1, base.Add(2*time.Second))); err != nil {
				t.Fatalf("close completion fan-out: %v", err)
			}
			close(intercept.releaseFirst)
			awaitRunLifecycleSignal(t, intercept.secondExecuted, "fan-out-close same-revision recheck")
			awaitRunLifecycleState(t, selected.(runLifecycleCandidateParityStore), completing.runID, runtimerunlifecycle.StateCompleted)
			if err := executor.Retire(context.Background()); err != nil {
				t.Fatalf("retire racing completion executor: %v", err)
			}
			retireRunLifecycleExecutorOccurrence(t, occurrence)
			retireRunLifecycleProcess(t, process)
			assertPortableRunStatus(t, ctx, db, completing.runID, "completed", true)

			stopping := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 3, base.Add(10*time.Second))
			_, staleClaim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "stopped-worker", BundleHash: stopping.bundleHash, Now: base.Add(11 * time.Second), Lease: time.Minute,
			})
			if err != nil || !found {
				t.Fatalf("claim suffix before stop: found=%v err=%v", found, err)
			}
			state, err := owner.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{
				RunID: stopping.runID, Reason: "test stop", ControlledBy: "test", Now: base.Add(12 * time.Second),
			})
			if err != nil || state.Status != "cancelled" || state.ControlStatus != "stopped" {
				t.Fatalf("stop fan-out run = %#v err=%v", state, err)
			}
			summary, err := owner.FanOutRunSummary(ctx, stopping.runID, base.Add(13*time.Second))
			if err != nil || summary.Canceled != 3 || summary.Owed != 0 || summary.BlocksCompletion() {
				t.Fatalf("stopped fan-out summary = %#v err=%v", summary, err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(staleClaim, 0, 1, base.Add(14*time.Second))); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("stopped stale worker commit error = %v", err)
			}
			presence, err = owner.PipelineObligations().GlobalWorkPresence(ctx)
			if err != nil || presence.ProcessingEligible {
				t.Fatalf("terminal fan-out global work = %#v err=%v", presence, err)
			}
		})
	}
}

func assertPortableRunStatus(t *testing.T, ctx context.Context, db *sql.DB, runID, want string, ended bool) {
	t.Helper()
	var status string
	var endedAt any
	if err := db.QueryRowContext(ctx, `SELECT status,ended_at FROM runs WHERE run_id=$1`, runID).Scan(&status, &endedAt); err != nil {
		t.Fatalf("load run status: %v", err)
	}
	if status != want || (endedAt != nil) != ended {
		t.Fatalf("run status = %q ended=%v, want %q ended=%v", status, endedAt != nil, want, ended)
	}
}

func TestFanOutFairnessLeaseRecoveryAndStaleFencingAcrossOwnersOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				firstOwner  selectedFanOutOwner
				secondOwner selectedFanOutOwner
				db          *sql.DB
				postgres    bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				storeBackend := mustPostgresBackend(db)
				firstOwner = newPostgresStoreWithBackend(storeBackend)
				second := newPostgresStoreWithBackend(storeBackend)
				second.acceptCurrentSchemaForTest()
				registerTestAuthorActivityCatalog(t, second)
				secondOwner = second
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				firstOwner = store
				second := NewSQLiteRuntimeStoreForTest(db)
				second.schema.AcceptCurrentForTest()
				registerTestAuthorActivityCatalog(t, second)
				secondOwner = second
			}

			base := time.Now().UTC().Truncate(time.Microsecond)
			slow := seedFanOutOwnerFixture(t, ctx, db, firstOwner, postgres, 3, base)
			fast := seedFanOutOwnerIntent(t, ctx, db, slow, 3, base.Add(time.Second))
			newer := seedFanOutOwnerIntent(t, ctx, db, slow, 3, base.Add(2*time.Second))

			_, slowClaim, found, err := firstOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "process-a", BundleHash: slow.bundleHash, Now: base.Add(10 * time.Second), Lease: 2 * time.Second,
			})
			if err != nil || !found || slowClaim.Key.ElementRef.ElementID != slow.elementID {
				t.Fatalf("old slow claim = %#v found=%v err=%v", slowClaim, found, err)
			}
			_, fastClaim, found, err := secondOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "process-b", BundleHash: slow.bundleHash, Now: base.Add(10 * time.Second), Lease: 5 * time.Second,
			})
			if err != nil || !found || fastClaim.Key.ElementRef.ElementID != fast.elementID {
				t.Fatalf("parallel fast claim = %#v found=%v err=%v", fastClaim, found, err)
			}
			if _, err := secondOwner.CommitFanOutChunk(ctx, rejectedFanOutChunk(fastClaim, 0, 1, base.Add(11*time.Second))); err != nil {
				t.Fatalf("serve fast intent: %v", err)
			}
			if err := firstOwner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{
				Claim: slowClaim, Now: base.Add(12 * time.Second), ObservedDuration: 1250 * time.Millisecond,
			}); err != nil {
				t.Fatalf("release slow retrying intent: %v", err)
			}

			_, newerClaim, found, err := secondOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "process-b", BundleHash: slow.bundleHash, Now: base.Add(13 * time.Second), Lease: 5 * time.Second,
			})
			if err != nil || !found || newerClaim.Key.ElementRef.ElementID != newer.elementID {
				t.Fatalf("unserved newer claim = %#v found=%v err=%v", newerClaim, found, err)
			}
			if _, err := secondOwner.CommitFanOutChunk(ctx, rejectedFanOutChunk(newerClaim, 0, 1, base.Add(14*time.Second))); err != nil {
				t.Fatalf("serve newer intent: %v", err)
			}

			_, expiringClaim, found, err := secondOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "process-b", BundleHash: slow.bundleHash, Now: base.Add(15 * time.Second), Lease: 2 * time.Second,
			})
			if err != nil || !found || expiringClaim.Key.ElementRef.ElementID != fast.elementID {
				t.Fatalf("least-recently-served claim = %#v found=%v err=%v", expiringClaim, found, err)
			}
			_, reclaimed, found, err := firstOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "process-a", BundleHash: slow.bundleHash, Now: base.Add(18 * time.Second), Lease: 5 * time.Second,
			})
			if err != nil || !found || reclaimed.Key != expiringClaim.Key || reclaimed.Generation != expiringClaim.Generation+1 {
				t.Fatalf("expired claim recovery = old:%#v new:%#v found=%v err=%v", expiringClaim, reclaimed, found, err)
			}
			if err := secondOwner.ReleaseFanOutClaim(ctx, expiringClaim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("stale release error = %v", err)
			}
			if _, err := secondOwner.CommitFanOutChunk(ctx, rejectedFanOutChunk(expiringClaim, 1, 1, base.Add(19*time.Second))); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("stale progress error = %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fast, 1, 1)
		})
	}
}

func TestFanOutTwoLevelRestartResumesParentPrefixAndNestedPendingOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			firstOwner, restartedOwner, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := time.Now().UTC().Truncate(time.Microsecond)
			parent := seedFanOutOwnerFixture(t, ctx, db, firstOwner, postgres, 9, base)
			nested := seedFanOutOwnerChildFixture(t, ctx, db, firstOwner, postgres, parent, 3, base.Add(time.Second))

			intent, claim, found, err := firstOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "before-restart", BundleHash: parent.bundleHash, Now: base.Add(2 * time.Second), Lease: time.Minute})
			if err != nil || !found || intent.Request.Key != (fanoutobligation.IntentKey{RunID: parent.runID, TriggeringDeliveryID: parent.deliveryID, ElementRef: runtimecontracts.FanOutElementRef{PackageKey: parent.packageKey, ElementID: parent.elementID}}) {
				t.Fatalf("claim parent before restart = %#v found=%v err=%v", intent.Request.Key, found, err)
			}
			input, err := firstOwner.LoadFanOutEvaluation(ctx, claim)
			if err != nil || len(input.Items) != 4 {
				t.Fatalf("load parent prefix before restart = %d err=%v", len(input.Items), err)
			}
			if _, err := firstOwner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, len(input.Items), base.Add(3*time.Second))); err != nil {
				t.Fatalf("commit parent prefix before restart: %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, parent, 4, 4)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, nested, 0, 0)

			now := base.Add(4 * time.Second)
			for turns := 0; turns < 16; turns++ {
				intent, claim, found, err := restartedOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "after-restart", BundleHash: parent.bundleHash, Now: now, Lease: time.Minute})
				if err != nil {
					t.Fatalf("claim after restart turn %d: %v", turns, err)
				}
				if !found {
					break
				}
				input, err := restartedOwner.LoadFanOutEvaluation(ctx, claim)
				if err != nil {
					t.Fatalf("load after restart turn %d: %v", turns, err)
				}
				if _, err := restartedOwner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, intent.Cursor, len(input.Items), now.Add(time.Second))); err != nil {
					t.Fatalf("commit after restart turn %d: %v", turns, err)
				}
				now = now.Add(2 * time.Second)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, parent, 9, 9)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, nested, 3, 3)
			var duplicateOrdinals int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT triggering_delivery_id,package_key,element_id,ordinal,COUNT(*) AS copies FROM fan_out_outcomes WHERE run_id=$1 GROUP BY triggering_delivery_id,package_key,element_id,ordinal HAVING COUNT(*)<>1) duplicates`, parent.runID).Scan(&duplicateOrdinals); err != nil || duplicateOrdinals != 0 {
				t.Fatalf("duplicate ordinals after two-level restart = %d err=%v", duplicateOrdinals, err)
			}
		})
	}
}

func TestFanOutCancellationPreservesParentPrefixAndCancelsClaimedNestedSuffixOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, secondOwner, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := time.Now().UTC().Truncate(time.Microsecond)
			parent := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 9, base)
			nested := seedFanOutOwnerChildFixture(t, ctx, db, owner, postgres, parent, 3, base.Add(time.Second))

			parentIntent, parentClaim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "parent-worker", BundleHash: parent.bundleHash, Now: base.Add(2 * time.Second), Lease: time.Minute})
			if err != nil || !found || parentIntent.Request.Key.ElementRef.ElementID != parent.elementID {
				t.Fatalf("claim cancel parent: intent=%#v found=%v err=%v", parentIntent.Request.Key, found, err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(parentClaim, 0, 4, base.Add(3*time.Second))); err != nil {
				t.Fatalf("commit parent prefix before cancellation: %v", err)
			}
			nestedIntent, nestedClaim, found, err := secondOwner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "nested-worker", BundleHash: parent.bundleHash, Now: base.Add(4 * time.Second), Lease: time.Minute})
			if err != nil || !found || nestedIntent.Request.Key.ElementRef.ElementID != nested.elementID {
				t.Fatalf("claim nested suffix before cancellation: intent=%#v found=%v err=%v", nestedIntent.Request.Key, found, err)
			}
			if err := owner.CancelRunFanOut(ctx, parent.runID, "parent run canceled", base.Add(5*time.Second)); err != nil {
				t.Fatalf("cancel parent and nested fan-out obligations: %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, parent, 4, 4)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, nested, 0, 0)
			if _, err := secondOwner.CommitFanOutChunk(ctx, rejectedFanOutChunk(nestedClaim, 0, 1, base.Add(6*time.Second))); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
				t.Fatalf("claimed nested worker after parent cancellation error = %v", err)
			}
			summary, err := owner.FanOutRunSummary(ctx, parent.runID, base.Add(7*time.Second))
			if err != nil || summary.Cardinality != 12 || summary.Cursor != 4 || summary.Canceled != 8 || summary.Owed != 0 || summary.BlocksCompletion() {
				t.Fatalf("parent/nested cancellation summary = %#v err=%v", summary, err)
			}
		})
	}
}

func TestFanOutRetryableReleaseHalvesWithoutSemanticProgressOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = newPostgresStoreWithBackend(mustPostgresBackend(db))
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				owner = store
			}
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 10, time.Now().UTC().Truncate(time.Microsecond))
			claimAt := fixture.createdAt.Add(time.Second)
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "retryable-worker", BundleHash: fixture.bundleHash, Now: claimAt, Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim retryable fan-out: found=%v err=%v", found, err)
			}
			if err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{
				Claim: claim, Now: claimAt.Add(time.Second), ObservedDuration: 1250 * time.Millisecond,
			}); err != nil {
				t.Fatalf("release retryable fan-out: %v", err)
			}
			var cursor, outcomes, nextChunk int
			var lastChunkMS int64
			if err := db.QueryRowContext(ctx, `
				SELECT i.cursor, i.next_chunk_size, i.last_chunk_ms,
					(SELECT COUNT(*) FROM fan_out_outcomes o WHERE o.run_id=i.run_id AND o.triggering_delivery_id=i.triggering_delivery_id AND o.package_key=i.package_key AND o.element_id=i.element_id)
				FROM fan_out_intents i
				WHERE i.run_id=$1 AND i.triggering_delivery_id=$2 AND i.package_key=$3 AND i.element_id=$4
			`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID).Scan(&cursor, &nextChunk, &lastChunkMS, &outcomes); err != nil {
				t.Fatalf("load retryable fan-out release: %v", err)
			}
			if cursor != 0 || outcomes != 0 || nextChunk != 2 || lastChunkMS != 1250 {
				t.Fatalf("retryable release = cursor:%d outcomes:%d chunk:%d latency:%dms", cursor, outcomes, nextChunk, lastChunkMS)
			}
		})
	}
}

func TestFanOutClaimIsScopedToExactAdmittedBundleOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = newPostgresStoreWithBackend(mustPostgresBackend(db))
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				owner = store
			}
			createdAt := time.Now().UTC().Truncate(time.Microsecond)
			foreign := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 2, createdAt)
			local := seedFanOutOwnerIntent(t, ctx, db, foreign, 2, createdAt.Add(time.Second))
			foreignHash := "bundle-v1:sha256:" + strings.Repeat("f", 64)
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET bundle_hash=$1 WHERE run_id=$2 AND triggering_delivery_id=$3 AND package_key=$4 AND element_id=$5`, foreignHash, foreign.runID, foreign.deliveryID, foreign.packageKey, foreign.elementID); err != nil {
				t.Fatalf("bind hostile foreign fan-out bundle: %v", err)
			}

			intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "local-runtime", BundleHash: local.bundleHash, Now: createdAt.Add(2 * time.Second), Lease: time.Minute,
			})
			if err != nil || !found {
				t.Fatalf("claim local-bundle fan-out: found=%v err=%v", found, err)
			}
			if intent.Request.Key.ElementRef.ElementID != local.elementID || intent.Request.PlanRef.BundleHash != local.bundleHash {
				t.Fatalf("local runtime claimed foreign obligation: %#v", intent.Request)
			}
			if err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
				t.Fatal(err)
			}
			foreignIntent, _, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
				Owner: "foreign-runtime", BundleHash: foreignHash, Now: createdAt.Add(3 * time.Second), Lease: time.Minute,
			})
			if err != nil || !found || foreignIntent.Request.Key.ElementRef.ElementID != foreign.elementID {
				t.Fatalf("foreign runtime claim = intent:%#v found=%v err=%v", foreignIntent.Request, found, err)
			}
		})
	}
}

func TestRunForkFanOutMaterializationPreservesPrefixAndResumesIndependently(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	fixture := seedFanOutOwnerFixture(t, ctx, db, pg, true, 3, createdAt)

	issuedEventID := uuid.NewString()
	seedPostgresSemanticEventRecordFixture(t, ctx, db, issuedEventID, fixture.runID, "items.child", events.EventProducerPlatform, "fan-out-test", "", "", createdAt.Add(time.Second))
	if _, err := db.ExecContext(ctx, `
		INSERT INTO fan_out_outcomes (
			run_id, triggering_delivery_id, package_key, element_id,
			ordinal, outcome_kind, event_id, failure, created_at
		) VALUES ($1::uuid,$2::uuid,$3,$4,0,'committed',$5::uuid,NULL,$6)
	`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, issuedEventID, createdAt.Add(time.Second)); err != nil {
		t.Fatalf("seed committed fan-out prefix: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE fan_out_intents
		SET cursor=1, updated_at=$5
		WHERE run_id=$1::uuid AND triggering_delivery_id=$2::uuid AND package_key=$3 AND element_id=$4
	`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, createdAt.Add(time.Second)); err != nil {
		t.Fatalf("advance source fan-out prefix: %v", err)
	}
	captureRunForkTestRevision(t, db, fixture.runID)

	forkPointEventID := uuid.NewString()
	seedPostgresSemanticEventRecordFixture(t, ctx, db, forkPointEventID, fixture.runID, "fork.point", events.EventProducerPlatform, "fork-test", "", "", createdAt.Add(2*time.Second))
	captureRunForkTestRevision(t, db, fixture.runID)

	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: forkPointEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	if materialized.MaterializedFanOutCount != 1 {
		t.Fatalf("MaterializedFanOutCount = %d, want 1", materialized.MaterializedFanOutCount)
	}
	var childCursor int
	var childBundleHash, inheritedEventID string
	var ownedEventID sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT i.cursor, i.bundle_hash, o.event_id::text, o.source_event_id::text
		FROM fan_out_intents i
		JOIN fan_out_outcomes o USING (run_id, triggering_delivery_id, package_key, element_id)
		WHERE i.run_id=$1::uuid AND o.ordinal=0
	`, materialized.ForkRunID).Scan(&childCursor, &childBundleHash, &ownedEventID, &inheritedEventID); err != nil {
		t.Fatalf("load materialized fork fan-out prefix: %v", err)
	}
	if childCursor != 1 || childBundleHash != fixture.bundleHash || ownedEventID.Valid || inheritedEventID != issuedEventID {
		t.Fatalf("materialized prefix = cursor:%d bundle:%q event:%v inherited:%q", childCursor, childBundleHash, ownedEventID, inheritedEventID)
	}

	intent, claim, found, err := pg.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "fork-worker", BundleHash: fixture.bundleHash, Now: createdAt.Add(3 * time.Second), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("claim materialized fork fan-out: intent=%#v found=%v err=%v", intent, found, err)
	}
	if intent.Request.Key.RunID == fixture.runID {
		if err := pg.ReleaseFanOutClaim(ctx, claim); err != nil {
			t.Fatalf("yield source fan-out claim: %v", err)
		}
		intent, claim, found, err = pg.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "fork-worker", BundleHash: fixture.bundleHash, Now: createdAt.Add(4 * time.Second), Lease: time.Minute})
	}
	if err != nil || !found || intent.Request.Key.RunID != materialized.ForkRunID {
		t.Fatalf("fair claim did not reach materialized fork fan-out: intent=%#v found=%v err=%v", intent, found, err)
	}
	input, err := pg.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		t.Fatalf("load materialized fork fan-out evaluation: %v", err)
	}
	if input.StartOrdinal != 1 || len(input.Items) != 2 || fmt.Sprint(input.Items[0]) != "item-001" || input.Trigger.RunID() != fixture.runID {
		t.Fatalf("fork evaluation = start:%d items:%#v trigger_run:%q", input.StartOrdinal, input.Items, input.Trigger.RunID())
	}
	closed, err := pg.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 1, 2, createdAt.Add(5*time.Second)))
	if err != nil {
		t.Fatalf("close materialized fork fan-out: %v", err)
	}
	if closed.Intent.Status != fanoutobligation.StatusClosed || closed.Intent.Cursor != 3 {
		t.Fatalf("closed fork intent = %#v", closed.Intent)
	}
	assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
}

func TestRunForkFanOutMaterializationRetainsExactEntityRevisionSource(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	fixture := seedFanOutOwnerFixture(t, ctx, db, pg, true, 3, createdAt)
	entityID := uuid.NewString()
	mutationID := uuid.NewString()
	itemsJSON := `["entity-000","entity-001","entity-002"]`
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, bookkeeping, accumulator, revision,
			entered_state_at, created_at, updated_at
		) VALUES ($1::uuid,$2::uuid,'root/one','fan_out_fixture','queued','{}'::jsonb,$3::jsonb,'{}'::jsonb,'{}'::jsonb,1,$4,$4,$4)
	`, fixture.runID, entityID, `{"items":`+itemsJSON+`}`, createdAt); err != nil {
		t.Fatalf("seed entity fan-out state: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			mutation_id, run_id, entity_id, domain, path, old_value, new_value,
			caused_by_event, writer_type, writer_id, handler_step, created_at
		) VALUES
			($5::uuid,$1::uuid,$2::uuid,'authored_field','items','null'::jsonb,$3::jsonb,$4::uuid,'platform','fan-out-test','source', $6),
			($7::uuid,$1::uuid,$2::uuid,'lifecycle_state','','null'::jsonb,'"queued"'::jsonb,$4::uuid,'platform','fan-out-test','source', $6)
	`, fixture.runID, entityID, itemsJSON, fixture.eventID, mutationID, createdAt, uuid.NewString()); err != nil {
		t.Fatalf("seed entity fan-out source revision: %v", err)
	}
	var capsuleRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1::uuid`, fixture.runID).Scan(&capsuleRaw); err != nil {
		t.Fatalf("load entity fan-out capsule: %v", err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
		t.Fatal(err)
	}
	capsule.EntityID = entityID
	capsuleRaw, _ = json.Marshal(capsule)
	if _, err := db.ExecContext(ctx, `
		UPDATE fan_out_intents
		SET source_kind='entity_field_revision', source_event_id=NULL,
			source_run_id=$1::uuid, source_entity_id=$2::uuid, source_mutation_id=$3::uuid,
			capsule=$4::jsonb, updated_at=$5
		WHERE run_id=$1::uuid
	`, fixture.runID, entityID, mutationID, capsuleRaw, createdAt); err != nil {
		t.Fatalf("bind entity fan-out source: %v", err)
	}
	captureRunForkTestRevision(t, db, fixture.runID)
	forkPointEventID := uuid.NewString()
	seedPostgresSemanticEventRecordFixture(t, ctx, db, forkPointEventID, fixture.runID, "fork.entity", events.EventProducerPlatform, "fork-test", "", "", createdAt.Add(time.Second))
	captureRunForkTestRevision(t, db, fixture.runID)

	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: forkPointEventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork entity source: %v", err)
	}
	var sourceRunID, sourceMutationID string
	if err := db.QueryRowContext(ctx, `SELECT source_run_id::text,source_mutation_id::text FROM fan_out_intents WHERE run_id=$1::uuid`, materialized.ForkRunID).Scan(&sourceRunID, &sourceMutationID); err != nil {
		t.Fatalf("load materialized entity source: %v", err)
	}
	if sourceRunID != fixture.runID || sourceMutationID != mutationID {
		t.Fatalf("materialized entity source = run:%q mutation:%q", sourceRunID, sourceMutationID)
	}
	_, claim, found, err := claimFanOutForRun(t, ctx, pg, materialized.ForkRunID, fixture.bundleHash, createdAt.Add(2*time.Second))
	if err != nil || !found {
		t.Fatalf("claim fork entity fan-out: found=%v err=%v", found, err)
	}
	input, err := pg.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		t.Fatalf("load fork entity fan-out: %v", err)
	}
	if got := fmt.Sprint(input.Items); got != "[entity-000 entity-001 entity-002]" {
		t.Fatalf("fork entity items = %s", got)
	}
}

func TestRunForkFanOutTerminalStatesNeverReissueInChild(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	base := time.Now().UTC().Truncate(time.Microsecond)

	t.Run("closed semantic rejection prefix", func(t *testing.T) {
		fixture := seedFanOutOwnerFixture(t, ctx, db, pg, true, 3, base)
		_, claim, found, err := pg.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "close-before-fork", BundleHash: fixture.bundleHash, Now: base.Add(time.Second), Lease: time.Minute})
		if err != nil || !found {
			t.Fatalf("claim terminal fork source: found=%v err=%v", found, err)
		}
		if _, err := pg.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 3, base.Add(2*time.Second))); err != nil {
			t.Fatalf("close fan-out before fork: %v", err)
		}
		materialized := materializeFanOutForkAtCurrentRevision(t, ctx, db, pg, fixture, "fork.closed", base.Add(3*time.Second))
		var status string
		var cursor, outcomes, open int
		if err := db.QueryRowContext(ctx, `SELECT status,cursor,(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1::uuid),(SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1::uuid AND status='open') FROM fan_out_intents WHERE run_id=$1::uuid`, materialized.ForkRunID).Scan(&status, &cursor, &outcomes, &open); err != nil {
			t.Fatal(err)
		}
		if status != "closed" || cursor != 3 || outcomes != 3 || open != 0 {
			t.Fatalf("closed child fan-out = status:%s cursor:%d outcomes:%d open:%d", status, cursor, outcomes, open)
		}
		var rejected int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1::uuid AND outcome_kind='semantic_rejected' AND event_id IS NULL AND source_event_id IS NULL AND failure IS NOT NULL`, materialized.ForkRunID).Scan(&rejected); err != nil || rejected != 3 {
			t.Fatalf("child semantic rejection evidence = %d err=%v", rejected, err)
		}
	})

	t.Run("canceled suffix", func(t *testing.T) {
		fixture := seedFanOutOwnerFixture(t, ctx, db, pg, true, 3, base.Add(time.Minute))
		if err := pg.CancelRunFanOut(ctx, fixture.runID, "canceled before fork", base.Add(time.Minute+time.Second)); err != nil {
			t.Fatalf("cancel fan-out before fork: %v", err)
		}
		materialized := materializeFanOutForkAtCurrentRevision(t, ctx, db, pg, fixture, "fork.canceled", base.Add(time.Minute+2*time.Second))
		var status, reason string
		var cursor, outcomes, open int
		if err := db.QueryRowContext(ctx, `SELECT status,cursor,COALESCE(blocked_reason,''),(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1::uuid),(SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1::uuid AND status='open') FROM fan_out_intents WHERE run_id=$1::uuid`, materialized.ForkRunID).Scan(&status, &cursor, &reason, &outcomes, &open); err != nil {
			t.Fatal(err)
		}
		if status != "canceled" || cursor != 0 || reason != "canceled before fork" || outcomes != 0 || open != 0 {
			t.Fatalf("canceled child fan-out = status:%s cursor:%d reason:%q outcomes:%d open:%d", status, cursor, reason, outcomes, open)
		}
	})
}

func TestFanOutEntityRevisionRejectsUnrelatedRunWithoutProgressOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = admitTestPostgresStore(t, db)
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, owner = store.backend.ConstructionHandle(), store
			}

			createdAt := time.Now().UTC().Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 3, createdAt)
			entityID, mutationID := seedFanOutEntityRevision(t, ctx, db, postgres, fixture.runID, `[{"name":"own-000","score":7.25},{"name":"own-001","score":-2},{"name":"own-002","score":1e3}]`, createdAt)
			bindFanOutEntityRevision(t, ctx, db, postgres, fixture, fixture.runID, entityID, mutationID, createdAt)

			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "own-run-source", BundleHash: fixture.bundleHash, Now: createdAt.Add(time.Second), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim own-run entity source: found=%v err=%v", found, err)
			}
			input, err := owner.LoadFanOutEvaluation(ctx, claim)
			if err != nil {
				t.Fatalf("own-run entity source = %#v err=%v", input.Items, err)
			}
			firstEntityItem, ok := input.Items[0].(map[string]any)
			if !ok || firstEntityItem["score"] != json.Number("7.25") {
				t.Fatalf("own-run entity numeric carrier = %#v", input.Items)
			}
			projectedEntityScore, err := workflowexpr.EvalValueExpressionWithOptions(
				"item.score",
				workflowexpr.ValueContext{FanOut: map[string]any{"item": input.Items[0]}},
				workflowexpr.ValueExpressionOptions{AllowBareItem: true},
			)
			if err != nil || projectedEntityScore != float64(7.25) {
				t.Fatalf("own-run entity projected score = %#v err=%v", projectedEntityScore, err)
			}
			if err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
				t.Fatal(err)
			}

			unrelatedRunID := uuid.NewString()
			requireRunFixtureForTest(t, ctx, owner, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: unrelatedRunID, StartedAt: createdAt})
			unrelatedEntityID, unrelatedMutationID := seedFanOutEntityRevision(t, ctx, db, postgres, unrelatedRunID, `["hostile-000","hostile-001","hostile-002"]`, createdAt)
			bindFanOutEntityRevision(t, ctx, db, postgres, fixture, unrelatedRunID, unrelatedEntityID, unrelatedMutationID, createdAt)

			_, hostileClaim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "unrelated-run-source", BundleHash: fixture.bundleHash, Now: createdAt.Add(2 * time.Second), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim unrelated-run entity source: found=%v err=%v", found, err)
			}
			if _, err := owner.LoadFanOutEvaluation(ctx, hostileClaim); err == nil || !strings.Contains(err.Error(), "outside intent run") {
				t.Fatalf("unrelated-run entity source error = %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}

func TestFanOutTriggerRejectsUnrelatedRunWithoutProgressOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = admitTestPostgresStore(t, db)
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, owner = store.backend.ConstructionHandle(), store
			}

			createdAt := time.Now().UTC().Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 3, createdAt)
			unrelatedRunID := uuid.NewString()
			requireRunFixtureForTest(t, ctx, owner, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: unrelatedRunID, StartedAt: createdAt})
			unrelatedEventID := uuid.NewString()
			unrelated := eventtest.ExistingRunRootIngressWithRoutingSource(
				unrelatedEventID, events.EventType("unrelated.items.ready"), "hostile-source", "",
				[]byte(`{"items":["hostile-000","hostile-001","hostile-002"]}`), 0, unrelatedRunID,
				events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(uuid.NewString()), createdAt,
			)
			if err := insertCanonicalEventRecordFixture(ctx, owner, unrelated); err != nil {
				t.Fatalf("insert unrelated fan-out trigger: %v", err)
			}
			var capsuleRaw []byte
			if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&capsuleRaw); err != nil {
				t.Fatal(err)
			}
			var capsule fanoutobligation.Capsule
			if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
				t.Fatal(err)
			}
			capsule.Lineage.RunID = unrelatedRunID
			capsule.Lineage.ParentEventID = unrelatedEventID
			capsuleRaw, err := json.Marshal(capsule)
			if err != nil {
				t.Fatal(err)
			}
			query := `UPDATE fan_out_intents SET source_event_id=?,capsule=?,updated_at=? WHERE run_id=?`
			if postgres {
				query = `UPDATE fan_out_intents SET source_event_id=$1::uuid,capsule=$2::jsonb,updated_at=$3 WHERE run_id=$4::uuid`
			}
			if _, err := db.ExecContext(ctx, query, unrelatedEventID, capsuleRaw, createdAt, fixture.runID); err != nil {
				t.Fatalf("bind unrelated fan-out trigger: %v", err)
			}
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "unrelated-trigger", BundleHash: fixture.bundleHash, Now: createdAt.Add(time.Second), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim unrelated trigger: found=%v err=%v", found, err)
			}
			if _, err := owner.LoadFanOutEvaluation(ctx, claim); err == nil || !strings.Contains(err.Error(), "triggering event run") {
				t.Fatalf("unrelated trigger error = %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}

func TestFanOutResourceVersionSourceRequiresPinAndForkInheritsIt(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var (
				owner    selectedFanOutResourceOwner
				db       *sql.DB
				postgres bool
			)
			if backend == "postgres" {
				_, db, _ = testutil.StartPostgres(t)
				owner = admitTestPostgresStore(t, db)
				postgres = true
			} else {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				owner = store
			}
			createdAt := time.Now().UTC().Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 5, createdAt)
			ref, err := durabledata.ParseDeclarationRef(".", "fanout.items")
			if err != nil {
				t.Fatal(err)
			}
			schema := map[string]any{
				"type": "object", "additionalProperties": false,
				"required": []string{"slug", "score"},
				"properties": map[string]any{
					"slug": map[string]any{"type": "string"}, "score": map[string]any{"type": "integer"},
				},
			}
			compiled, defects := durabledata.CompileJSONL(ref, schema, "slug", []byte("{\"slug\":\"probe\",\"score\":1}\n"))
			if len(defects) != 0 {
				t.Fatalf("compile resource fan-out schema: %#v", defects)
			}
			catalog := durabledata.Catalog{BundleHash: fixture.bundleHash, Declarations: []durabledata.Declaration{{
				Name: "fanout.items", Ref: ref, BusinessKey: "slug", SchemaDigest: compiled.Manifest.SchemaDigest, CanonicalSchema: compiled.CanonicalSchema,
			}}}
			if _, err := owner.UpsertBundleCatalogWithData(ctx, bundlecatalog.Upsert{
				BundleHash: fixture.bundleHash, ContentYAML: "api_version: swarm.bundle.catalog.test.v1\n",
				ParsedJSON: map[string]any{"projection_version": "swarm.bundle.catalog.v2", "agents": []any{}},
				Metadata:   map[string]any{"source": "fan-out-resource-test"},
			}, catalog); err != nil {
				t.Fatalf("register resource fan-out catalog: %v", err)
			}
			imported, err := owner.ExecuteDataSourceOperation(ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: fixture.bundleHash,
				Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl",
				Input: []byte("{\"slug\":\"echo\",\"score\":5}\n{\"slug\":\"alpha\",\"score\":1}\n{\"slug\":\"delta\",\"score\":4}\n{\"slug\":\"bravo\",\"score\":2}\n{\"slug\":\"charlie\",\"score\":3}\n"),
			})
			if err != nil {
				t.Fatalf("import resource fan-out source: %v", err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO resource_version_pins (run_id,package_key,event_name,schema_digest,version_id,selection,pinned_at) VALUES ($1,$2,$3,$4,$5,'explicit',$6)`, fixture.runID, ref.PackageKey, ref.EventName, imported.SchemaDigest, imported.Candidate.VersionID, createdAt); err != nil {
				t.Fatalf("pin resource fan-out version: %v", err)
			}
			if _, err := db.ExecContext(ctx, `
				UPDATE fan_out_intents
				SET source_kind='resource_version', source_event_id=NULL, source_field=NULL,
					source_resource_package_key=$2, source_resource_event_name=$3, source_resource_version_id=$4,
					updated_at=$5
				WHERE run_id=$1
			`, fixture.runID, ref.PackageKey, ref.EventName, imported.Candidate.VersionID, createdAt); err != nil {
				t.Fatalf("bind resource fan-out source: %v", err)
			}

			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "resource-worker", BundleHash: fixture.bundleHash, Now: createdAt.Add(time.Second), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim resource fan-out: found=%v err=%v", found, err)
			}
			input, err := owner.LoadFanOutEvaluation(ctx, claim)
			if err != nil {
				t.Fatalf("load resource fan-out: %v", err)
			}
			if len(input.Items) != 4 || input.Items[0].(map[string]any)["slug"] != "alpha" || input.Items[3].(map[string]any)["slug"] != "delta" {
				t.Fatalf("canonical bounded resource items = %#v", input.Items)
			}
			if input.Items[0].(map[string]any)["score"] != json.Number("1") {
				t.Fatalf("resource numeric carrier = %#v", input.Items[0])
			}
			projectedResourceScore, err := workflowexpr.EvalValueExpressionWithOptions(
				"item.score",
				workflowexpr.ValueContext{FanOut: map[string]any{"item": input.Items[0]}},
				workflowexpr.ValueExpressionOptions{AllowBareItem: true},
			)
			if err != nil || projectedResourceScore != int64(1) {
				t.Fatalf("resource projected score = %#v err=%v", projectedResourceScore, err)
			}
			if err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `DELETE FROM resource_version_pins WHERE run_id=$1 AND package_key=$2 AND event_name=$3`, fixture.runID, ref.PackageKey, ref.EventName); err != nil {
				t.Fatal(err)
			}
			_, hostileClaim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "resource-hostile", BundleHash: fixture.bundleHash, Now: createdAt.Add(2 * time.Second), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim unpinned resource fan-out: found=%v err=%v", found, err)
			}
			if _, err := owner.LoadFanOutEvaluation(ctx, hostileClaim); !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("unpinned resource load error = %v, want missing exact pin", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
			if err := owner.ReleaseFanOutClaim(ctx, hostileClaim); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO resource_version_pins (run_id,package_key,event_name,schema_digest,version_id,selection,pinned_at) VALUES ($1,$2,$3,$4,$5,'explicit',$6)`, fixture.runID, ref.PackageKey, ref.EventName, imported.SchemaDigest, imported.Candidate.VersionID, createdAt); err != nil {
				t.Fatal(err)
			}

			if !postgres {
				return
			}
			captureRunForkTestRevision(t, db, fixture.runID)
			forkPointEventID := uuid.NewString()
			seedPostgresSemanticEventRecordFixture(t, ctx, db, forkPointEventID, fixture.runID, "fork.resource", events.EventProducerPlatform, "fork-test", "", "", createdAt.Add(3*time.Second))
			captureRunForkTestRevision(t, db, fixture.runID)
			forkOwner := owner.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			materialized, err := forkOwner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: forkPointEventID})
			if err != nil {
				t.Fatalf("materialize resource fan-out fork: %v", err)
			}
			var childPins int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_version_pins WHERE run_id=$1::uuid AND version_id=$2`, materialized.ForkRunID, imported.Candidate.VersionID).Scan(&childPins); err != nil || childPins != 1 {
				t.Fatalf("fork resource pins = %d err=%v, want 1", childPins, err)
			}
			_, childClaim, found, err := claimFanOutForRun(t, ctx, owner, materialized.ForkRunID, fixture.bundleHash, createdAt.Add(4*time.Second))
			if err != nil || !found {
				t.Fatalf("claim fork resource fan-out: found=%v err=%v", found, err)
			}
			childInput, err := owner.LoadFanOutEvaluation(ctx, childClaim)
			if err != nil || len(childInput.Items) != 4 || childInput.Items[0].(map[string]any)["slug"] != "alpha" {
				t.Fatalf("fork resource input = %#v err=%v", childInput, err)
			}
		})
	}
}

func TestRunForkFanOutSelectedBundleProofRebindsOrRejectsBeforeMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	fixture := seedFanOutOwnerFixture(t, ctx, db, pg, true, 2, createdAt)
	captureRunForkTestRevision(t, db, fixture.runID)
	forkPointEventID := uuid.NewString()
	seedPostgresSemanticEventRecordFixture(t, ctx, db, forkPointEventID, fixture.runID, "fork.selected-bundle", events.EventProducerPlatform, "fork-test", "", "", createdAt.Add(time.Second))
	captureRunForkTestRevision(t, db, fixture.runID)

	targetHash := "bundle-v1:sha256:" + strings.Repeat("c", 64)
	seedStoreTestPersistedBundle(t, db, targetHash)
	targetSource, err := runtimecorrelation.NewPersistedBundleSourceFact(targetHash)
	if err != nil {
		t.Fatal(err)
	}
	base := runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: forkPointEventID, BundleSourceFact: targetSource}
	if _, err := pg.MaterializeRunFork(ctx, base); err == nil || !strings.Contains(err.Error(), "has no proof") {
		t.Fatalf("missing selected fan-out proof error = %v", err)
	}
	changed := base
	changed.FanOutPlanRefs = []runtimecontracts.FanOutPlanRef{{
		BundleHash:     targetHash,
		ElementRef:     runtimecontracts.FanOutElementRef{PackageKey: fixture.packageKey, ElementID: fixture.elementID},
		SemanticDigest: "sha256:" + strings.Repeat("f", 64),
	}}
	if _, err := pg.MaterializeRunFork(ctx, changed); err == nil || !strings.Contains(err.Error(), "changed pending fan_out") {
		t.Fatalf("changed selected fan-out proof error = %v", err)
	}
	var prematureForks int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1::uuid`, fixture.runID).Scan(&prematureForks); err != nil || prematureForks != 0 {
		t.Fatalf("forks after rejected selected proofs = %d err=%v", prematureForks, err)
	}
	matching := changed
	matching.FanOutPlanRefs[0].SemanticDigest = "sha256:" + strings.Repeat("2", 64)
	materialized, err := pg.MaterializeRunFork(ctx, matching)
	if err != nil {
		t.Fatalf("materialize semantically unchanged selected fan-out: %v", err)
	}
	var childBundleHash, childPlanHash string
	if err := db.QueryRowContext(ctx, `
		SELECT r.bundle_hash,i.bundle_hash
		FROM runs r JOIN fan_out_intents i ON i.run_id=r.run_id
		WHERE r.run_id=$1::uuid
	`, materialized.ForkRunID).Scan(&childBundleHash, &childPlanHash); err != nil {
		t.Fatal(err)
	}
	if childBundleHash != targetHash || childPlanHash != targetHash {
		t.Fatalf("selected fork bundle ownership = run:%q plan:%q, want %q", childBundleHash, childPlanHash, targetHash)
	}
}

func claimFanOutForRun(t *testing.T, ctx context.Context, owner selectedFanOutOwner, runID, bundleHash string, at time.Time) (fanoutobligation.Intent, fanoutobligation.Claim, bool, error) {
	t.Helper()
	for attempt := 0; attempt < 3; attempt++ {
		intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "fork-specific-worker", BundleHash: bundleHash, Now: at.Add(time.Duration(attempt) * time.Second), Lease: time.Minute})
		if err != nil || !found || intent.Request.Key.RunID == runID {
			return intent, claim, found, err
		}
		if err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
			return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, err
		}
	}
	return fanoutobligation.Intent{}, fanoutobligation.Claim{}, false, fmt.Errorf("fair fan-out claim did not reach run %s", runID)
}

func materializeFanOutForkAtCurrentRevision(t *testing.T, ctx context.Context, db *sql.DB, pg *PostgresStore, fixture fanOutOwnerFixture, eventName string, at time.Time) runfork.RunForkMaterialization {
	t.Helper()
	captureRunForkTestRevision(t, db, fixture.runID)
	forkPointEventID := uuid.NewString()
	seedPostgresSemanticEventRecordFixture(t, ctx, db, forkPointEventID, fixture.runID, events.EventType(eventName), events.EventProducerPlatform, "fork-test", "", "", at)
	captureRunForkTestRevision(t, db, fixture.runID)
	materialized, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: forkPointEventID})
	if err != nil {
		t.Fatalf("materialize %s fan-out fork: %v", eventName, err)
	}
	return materialized
}

func seedFanOutOwnerFixture(t *testing.T, ctx context.Context, db *sql.DB, selected any, postgres bool, cardinality int, createdAt time.Time) fanOutOwnerFixture {
	t.Helper()
	fixture := fanOutOwnerFixture{runID: uuid.NewString(), eventID: uuid.NewString(), deliveryID: uuid.NewString(), packageKey: "root", elementID: uuid.NewString(), createdAt: createdAt}
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: fixture.runID, StartedAt: createdAt})
	if err := db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, fixture.runID).Scan(&fixture.bundleHash); err != nil {
		t.Fatalf("load fan-out fixture bundle hash: %v", err)
	}
	items := make([]string, cardinality)
	for index := range items {
		items[index] = fmt.Sprintf("item-%03d", index)
	}
	payload, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		t.Fatalf("encode fan-out source: %v", err)
	}
	trigger := eventtest.ExistingRunRootIngressWithRoutingSource(
		fixture.eventID, events.EventType("items.ready"), "fan-out-test", "", payload, 0, fixture.runID,
		events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(uuid.NewString()), createdAt,
	)
	if err := insertCanonicalEventRecordFixture(ctx, selected, trigger); err != nil {
		t.Fatalf("insert canonical fan-out trigger: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin fan-out fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	target := events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "root", FlowInstance: "root"})
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("fan-out-source")), Target: target}
	routeIdentity, err := route.Identity()
	if err != nil {
		t.Fatalf("construct fan-out trigger route: %v", err)
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		t.Fatalf("encode fan-out trigger target: %v", err)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_deliveries (delivery_id,run_id,event_id,route_identity,subscriber_type,subscriber_id,agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,agent_flow_instance_path,delivery_target_route,delivery_context,delivery_payload_projection,connect_execution_claim,execution_authority_kind,authority_bundle_hash,authority_bundle_source,execution_authority_id,execution_authority_generation,status,retry_count,max_retries,claim_version,settled_at,created_at,updated_at) VALUES ($1,$2,$3,$4,'node',$5,'','','','','','',$6,$7,$7,$7,'normal_runtime',$8,'persisted','fan-out-test',1,'delivered',0,3,0,$9,$9,$9)`, fixture.deliveryID, fixture.runID, fixture.eventID, events.EncodeDeliveryRouteIdentity(routeIdentity), route.Recipient.ID(), string(targetJSON), `{}`, "bundle-v1:sha256:"+strings.Repeat("1", 64), createdAt)
	insertFanOutOwnerIntent(t, ctx, tx, fixture, cardinality, createdAt)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit fan-out fixture: %v", err)
	}
	return fixture
}

func seedFanOutOwnerChildFixture(t *testing.T, ctx context.Context, db *sql.DB, selected any, postgres bool, parent fanOutOwnerFixture, cardinality int, createdAt time.Time) fanOutOwnerFixture {
	t.Helper()
	fixture := fanOutOwnerFixture{
		runID: parent.runID, eventID: uuid.NewString(), deliveryID: uuid.NewString(), packageKey: parent.packageKey,
		elementID: uuid.NewString(), createdAt: createdAt, bundleHash: parent.bundleHash,
	}
	items := make([]string, cardinality)
	for index := range items {
		items[index] = fmt.Sprintf("nested-%03d", index)
	}
	payload, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		t.Fatal(err)
	}
	trigger := eventtest.ExistingRunRootIngressWithRoutingSource(
		fixture.eventID, events.EventType("nested.items.ready"), "fan-out-nested-test", "", payload, 1, fixture.runID,
		events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(uuid.NewString()), createdAt,
	)
	if err := insertCanonicalEventRecordFixture(ctx, selected, trigger); err != nil {
		t.Fatalf("insert canonical nested fan-out trigger: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	target := events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "root", FlowInstance: "root"})
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("nested-fan-out-source")), Target: target}
	routeIdentity, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	targetJSON, err := json.Marshal(target)
	if err != nil {
		t.Fatal(err)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_deliveries (delivery_id,run_id,event_id,route_identity,subscriber_type,subscriber_id,agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,agent_flow_instance_path,delivery_target_route,delivery_context,delivery_payload_projection,connect_execution_claim,execution_authority_kind,authority_bundle_hash,authority_bundle_source,execution_authority_id,execution_authority_generation,status,retry_count,max_retries,claim_version,settled_at,created_at,updated_at) VALUES ($1,$2,$3,$4,'node',$5,'','','','','','',$6,$7,$7,$7,'normal_runtime',$8,'persisted','fan-out-test',1,'delivered',0,3,0,$9,$9,$9)`, fixture.deliveryID, fixture.runID, fixture.eventID, events.EncodeDeliveryRouteIdentity(routeIdentity), route.Recipient.ID(), string(targetJSON), `{}`, "bundle-v1:sha256:"+strings.Repeat("1", 64), createdAt)
	producer, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	capsule := fanoutobligation.Capsule{
		NodeKey: "root.nested-fan-out-source", ExecutionFlowID: "root", Route: runtimeflowidentity.StoredRoute("root", "root", "root"),
		HandlerEventKey: "nested.items.ready", ProducerSource: producer,
		Lineage: events.EventLineage{RunID: fixture.runID, ParentEventID: fixture.eventID, ExecutionMode: executionmode.Live},
	}
	capsuleJSON, err := json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	status := fanoutobligation.StatusOpen
	if cardinality == 0 {
		status = fanoutobligation.StatusClosed
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_intents (run_id,triggering_delivery_id,package_key,element_id,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,'event_payload_field',$7,'items',$8,0,$9,4,$10,$11,$11)`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, fixture.bundleHash, "sha256:"+strings.Repeat("3", 64), fixture.eventID, cardinality, string(status), string(capsuleJSON), createdAt)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit nested fan-out fixture: %v", err)
	}
	return fixture
}

func newFanOutOwnerPairForTest(t *testing.T, backend string) (selectedFanOutOwner, selectedFanOutOwner, *sql.DB, bool) {
	t.Helper()
	if backend == "postgres" {
		_, db, _ := testutil.StartPostgres(t)
		storeBackend := mustPostgresBackend(db)
		first := newPostgresStoreWithBackend(storeBackend)
		second := newPostgresStoreWithBackend(storeBackend)
		second.acceptCurrentSchemaForTest()
		registerTestAuthorActivityCatalog(t, second)
		return first, second, db, true
	}
	first := newBootstrappedSQLiteRuntimeStoreForTest(t)
	db := first.backend.ConstructionHandle()
	second := NewSQLiteRuntimeStoreForTest(db)
	second.schema.AcceptCurrentForTest()
	registerTestAuthorActivityCatalog(t, second)
	return first, second, db, false
}

func seedFanOutEntityRevision(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, runID, itemsJSON string, createdAt time.Time) (string, string) {
	t.Helper()
	entityID := uuid.NewString()
	mutationID := uuid.NewString()
	stateQuery := `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,slug,name,current_state,gates,fields,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES (?,?, 'root/one','fan_out_fixture','fan-out-fixture','Fan Out Fixture','queued','{}',?,'{}','{}',1,?,?,?)`
	mutationQuery := `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,created_at) VALUES (?,?,?,'authored_field','items','null',?,'platform','fan-out-test','source',?)`
	stateArgs := []any{runID, entityID, `{"items":` + itemsJSON + `}`, createdAt, createdAt, createdAt}
	if postgres {
		stateQuery = `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,slug,name,current_state,gates,fields,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES ($1::uuid,$2::uuid,'root/one','fan_out_fixture','fan-out-fixture','Fan Out Fixture','queued','{}'::jsonb,$3::jsonb,'{}'::jsonb,'{}'::jsonb,1,$4,$4,$4)`
		mutationQuery = `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,old_value,new_value,writer_type,writer_id,handler_step,created_at) VALUES ($1::uuid,$2::uuid,$3::uuid,'authored_field','items','null'::jsonb,$4::jsonb,'platform','fan-out-test','source',$5)`
		stateArgs = []any{runID, entityID, `{"items":` + itemsJSON + `}`, createdAt}
	}
	if _, err := db.ExecContext(ctx, stateQuery, stateArgs...); err != nil {
		t.Fatalf("seed fan-out entity state: %v", err)
	}
	if _, err := db.ExecContext(ctx, mutationQuery, mutationID, runID, entityID, itemsJSON, createdAt); err != nil {
		t.Fatalf("seed fan-out entity revision: %v", err)
	}
	return entityID, mutationID
}

func bindFanOutEntityRevision(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, fixture fanOutOwnerFixture, sourceRunID, entityID, mutationID string, updatedAt time.Time) {
	t.Helper()
	var capsuleRaw []byte
	if err := db.QueryRowContext(ctx, `SELECT capsule FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID).Scan(&capsuleRaw); err != nil {
		t.Fatalf("load fan-out entity source capsule: %v", err)
	}
	var capsule fanoutobligation.Capsule
	if err := json.Unmarshal(capsuleRaw, &capsule); err != nil {
		t.Fatal(err)
	}
	capsule.EntityID = entityID
	capsuleRaw, err := json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	query := `UPDATE fan_out_intents SET source_kind='entity_field_revision',source_event_id=NULL,source_run_id=?,source_entity_id=?,source_field='items',source_mutation_id=?,capsule=?,updated_at=? WHERE run_id=? AND triggering_delivery_id=? AND package_key=? AND element_id=?`
	if postgres {
		query = `UPDATE fan_out_intents SET source_kind='entity_field_revision',source_event_id=NULL,source_run_id=$1::uuid,source_entity_id=$2::uuid,source_field='items',source_mutation_id=$3::uuid,capsule=$4::jsonb,updated_at=$5 WHERE run_id=$6::uuid AND triggering_delivery_id=$7::uuid AND package_key=$8 AND element_id=$9`
	}
	if _, err := db.ExecContext(ctx, query, sourceRunID, entityID, mutationID, capsuleRaw, updatedAt, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID); err != nil {
		t.Fatalf("bind fan-out entity revision: %v", err)
	}
}

func seedFanOutOwnerIntent(t *testing.T, ctx context.Context, db *sql.DB, parent fanOutOwnerFixture, cardinality int, createdAt time.Time) fanOutOwnerFixture {
	t.Helper()
	fixture := parent
	fixture.elementID = uuid.NewString()
	fixture.createdAt = createdAt
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin additional fan-out intent: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	insertFanOutOwnerIntent(t, ctx, tx, fixture, cardinality, createdAt)
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit additional fan-out intent: %v", err)
	}
	return fixture
}

func insertFanOutOwnerIntent(t *testing.T, ctx context.Context, tx *sql.Tx, fixture fanOutOwnerFixture, cardinality int, createdAt time.Time) {
	t.Helper()
	producer, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatalf("construct fan-out producer source: %v", err)
	}
	capsule := fanoutobligation.Capsule{
		NodeKey: "root.fan-out-source", ExecutionFlowID: "root", Route: runtimeflowidentity.StoredRoute("root", "root", "root"),
		HandlerEventKey: "items.ready", ProducerSource: producer,
		Lineage: events.EventLineage{RunID: fixture.runID, ParentEventID: fixture.eventID, ExecutionMode: executionmode.Live},
	}
	capsuleJSON, err := json.Marshal(capsule)
	if err != nil {
		t.Fatalf("encode fan-out capsule: %v", err)
	}
	status := fanoutobligation.StatusOpen
	if cardinality == 0 {
		status = fanoutobligation.StatusClosed
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_intents (run_id,triggering_delivery_id,package_key,element_id,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,'event_payload_field',$7,'items',$8,0,$9,4,$10,$11,$11)`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, fixture.bundleHash, "sha256:"+strings.Repeat("2", 64), fixture.eventID, cardinality, string(status), string(capsuleJSON), createdAt)
}

func rejectedFanOutChunk(claim fanoutobligation.Claim, start, count int, at time.Time) pipeline.FanOutChunkCommand {
	failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "fan_out_test_item_invalid", "test", "commit_fan_out_chunk", nil))
	if !ok {
		panic("construct fan-out test failure")
	}
	failureJSON, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		panic(err)
	}
	outcomes := make([]pipeline.FanOutChunkOutcome, count)
	for index := range outcomes {
		outcomes[index] = pipeline.FanOutChunkOutcome{Ordinal: start + index, Failure: failureJSON}
	}
	return pipeline.FanOutChunkCommand{Claim: claim, Outcomes: outcomes, Now: at}
}

func assertFanOutCursorAndOutcomeCount(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture, cursor, outcomes int) {
	t.Helper()
	var gotCursor, gotOutcomes int
	if err := db.QueryRowContext(ctx, `SELECT cursor FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID).Scan(&gotCursor); err != nil {
		t.Fatalf("load fan-out cursor: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID).Scan(&gotOutcomes); err != nil {
		t.Fatalf("count fan-out outcomes: %v", err)
	}
	if gotCursor != cursor || gotOutcomes != outcomes {
		t.Fatalf("fan-out progress = cursor %d outcomes %d, want %d/%d", gotCursor, gotOutcomes, cursor, outcomes)
	}
}
