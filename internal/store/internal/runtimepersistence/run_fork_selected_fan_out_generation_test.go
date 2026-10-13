package runtimepersistence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type selectedForkFanOutConsumer struct {
	receiver eventreceiver.ExecutionVariant
	delivery deliverylifecycle.ExecutionAuthority
	work     *worklifetime.SelectedForkOccurrence
}

func activateSelectedForkFanOutForTest(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, source semanticview.Source, child runfork.RunForkMaterialization, request runforkreadiness.MaterializeRequest) (context.Context, pipeline.FanOutObligationOwner, selectedForkFanOutConsumer) {
	t.Helper()
	store := fixture.store.(selectedCompletionAuthorityStore)
	storeTestWorkOwner(t)
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	process := selectedMaterializationProcessForTest(t, work, fixture.store)
	declarations, err := agenttopology.NewSelectedDeclarationPlan(request.SourceArtifactFact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Preparation.Actors) != 0 || request.Preparation.DeclarationPlanFingerprint != declarations.Revision {
		t.Fatal("selected fan-out fixture requires its actual empty prepared actor census")
	}
	binding, err := fixture.store.(interface {
		RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
	}).RequireRunForkSelectedContractBinding(ctx, child.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	containerFingerprint, err := canonicaljson.Hash(request.RecipientPlanning)
	if err != nil {
		t.Fatal(err)
	}
	actorFingerprint, err := canonicaljson.Hash(request.Preparation.Actors)
	if err != nil {
		t.Fatal(err)
	}
	issue := runfork.SelectedContractRuntimeExecutionIssueRequest{
		Preparation:     runfork.SelectedForkPreparationBinding{ForkRunID: child.ForkRunID, SelectedForkPreparation: request.Preparation},
		DeclarationPlan: declarations,
		Admission: runfork.RunForkSelectedContractExecutionAdmission{
			Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
			NonMutating: true, ForkRunID: child.ForkRunID, SourceRunID: binding.SourceRunID, ForkPoint: binding.ForkPoint, ForkEventID: binding.ForkEventID,
			ContractSelection: binding.ContractSelection, ContractBindingOwner: binding.Owner,
			AdmissionOwner: request.FrontierAdmission.Owner, AdmissionUse: runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
			ExecutionModelOwner: runfork.RunForkSelectedContractExecutionModelOwner, DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
			SourceWorkflowName: source.WorkflowName(), SourceWorkflowVersion: source.WorkflowVersion(),
			RouteTopology: &request.RouteTopology, RecipientPlanning: &request.RecipientPlanning,
		},
		ContainerPlanFingerprint: containerFingerprint, ActorCensusFingerprint: actorFingerprint,
		EffectiveConfigFingerprint: "sha256:" + request.Preparation.Coordinates.ConfigurationFingerprint,
		ExecutionMode:              effects.ExecutionModeLive, Now: time.Now().UTC(),
	}
	issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, issue)
	if err != nil {
		t.Fatalf("issue retained selected fan-out execution: %v", err)
	}
	authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "fork-specific-selected-worker", time.Minute)
	if err != nil {
		t.Fatalf("claim retained selected fan-out execution: %v", err)
	}
	grant := selectedFixtureGrantForAuthority(t, ctx, selectedCompletionFixture{
		process: process, store: store, forkRun: child.ForkRunID, request: issue,
	}, issued, authority)
	t.Cleanup(func() {
		if err := grant.Retire(context.Background()); err != nil {
			t.Error(err)
		}
		if err := store.QuiesceRunForkSelectedContractRuntimeExecution(context.Background(), authority); err != nil {
			t.Error(err)
		}
		if _, err := store.CloseRunForkSelectedContractRuntimeExecution(context.Background(), issued.ExecutionID); err != nil {
			t.Error(err)
		}
	})
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	owner, err := fixture.store.(interface {
		BindSelectedFanOutGrant(startupownership.GrantEvidence) (pipeline.FanOutObligationOwner, error)
	}).BindSelectedFanOutGrant(evidence)
	if err != nil || owner == nil {
		t.Fatalf("bind exact selected fan-out grant: owner=%T err=%v", owner, err)
	}
	consumer := selectedForkFanOutConsumer{}
	consumer.work, err = work.process.NewSelectedFork(ctx, worklifetime.SelectedForkIdentity{ExecutionID: issued.ExecutionID, RunID: child.ForkRunID, Generation: issued.Generation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := consumer.work.RetireAndWait(context.Background()); err != nil {
			t.Error(err)
		}
	})
	managed, err := managedexecution.New(managedexecution.KindSelectedContractFork, issued.ExecutionID, issued.Generation, child.ForkRunID, issued.ActorCensusFingerprint, evidence.BundleHash, request.Preparation.SurfaceIDs())
	if err != nil {
		t.Fatal(err)
	}
	consumer.receiver, err = eventreceiver.SelectedContractForkExecution(authority, managed, newCompletionControllerForTest(store), correlation.RuntimeLineage{
		Owner: runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner, RunID: child.ForkRunID,
		RowCategory: correlation.RuntimeLineageRowCategoryRuntimeContainer, SelectedForkOwner: runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner,
		Classification: correlation.RuntimeLineageClassificationForkLocal, SelectedForkContext: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer.delivery, err = deliverylifecycle.NewSelectedExecutionAuthority(request.SourceArtifactFact, issued.ExecutionID, child.ForkRunID, issued.Generation)
	if err != nil {
		t.Fatal(err)
	}
	ctx = worklifetime.WithProcess(ctx, work.process)
	ctx = worklifetime.WithOccurrence(ctx, consumer.work)
	ctx = startupownership.WithGenerationGrantEvidence(ctx, evidence)
	ctx = correlation.WithRunID(ctx, child.ForkRunID)
	ctx, err = consumer.receiver.Bind(ctx, effects.ExecutionModeLive)
	if err != nil {
		t.Fatal(err)
	}
	_, sourceEvents, _, err := runfork.RunForkContractFrontierEvidenceBinding(request.FrontierAdmission)
	if err != nil || len(sourceEvents) != 0 {
		t.Fatalf("settled fan-out source unexpectedly has replay frontier: %v err=%v", sourceEvents, err)
	}
	activation, err := fixture.store.(interface {
		ActivateRunForkForSelectedContractExecution(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)
	}).ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID: child.ForkRunID, AllowSourceFreeze: true, ExecutionSource: source,
		DataPins: child.DataPins, InheritedSchedules: request.Readiness, AllowedSourceEventIDs: sourceEvents,
		FrontierAdmission: request.FrontierAdmission, RouteTopology: request.RouteTopology, RecipientPlanning: request.RecipientPlanning,
	})
	if err != nil || !activation.Activated || !activation.SourceFrozen || activation.ForkRunStatus != runfork.RunForkActivatedStatus {
		t.Fatalf("activate selected fork before child serving: %+v err=%v", activation, err)
	}
	return ctx, owner, consumer
}

func consumeSelectedForkFanOutEmissions(t *testing.T, fixture authorActivityReceiptFixture, backend string, source semanticview.Source, ctx context.Context, owner pipeline.FanOutObligationOwner, intent fanoutobligation.Intent, claim fanoutobligation.Claim, emissions []engine.EmitIntent, consumer selectedForkFanOutConsumer, handle timeridentity.TimerHandle, expectedFailure string) {
	t.Helper()
	selected := fixture.store.(storeTestDurableEventBusStore)
	workflow := fixture.store.(workflowTestSelectedStore)
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("source artifact fact missing")
	}
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatal(err)
	}
	scope, ok := authoractivity.ScopeFromContext(ctx)
	if !ok {
		t.Fatal("author scope missing")
	}
	lease, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	eventBus, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: fact, WorkOwner: consumer.work,
		ReceiverExecution: consumer.receiver, DeliveryAuthority: consumer.delivery,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := eventBus.PrepareEnginePublications(ctx, emissions)
	if err != nil || len(prepared) != len(emissions) || len(emissions) != intent.Request.Cardinality {
		t.Fatalf("admit actual selected ordinal emissions: count=%d err=%v", len(prepared), err)
	}
	outcomes := make([]pipeline.FanOutChunkOutcome, 0, len(prepared))
	for ordinal, plan := range prepared {
		publication, ok := plan.(bus.EnginePublicationPlan)
		if !ok {
			t.Fatalf("unexpected publication %T", plan)
		}
		routes := publication.PublicationCommand().Commit.DeliveryRoutes
		if len(routes) != 1 || routes[0].Recipient != events.MustNodeDeliveryRecipient(mustPersistenceRootNode("item-consumer")) || routes[0].Target.Route().EntityID != intent.Request.Key.RunID {
			t.Fatalf("ordinal did not admit exact child consumer: %+v", routes)
		}
		admitted := publication.PublicationCommand().Commit.Event.Event()
		originalOrigin, originalInherited := emissions[ordinal].Event.InheritedFanOutOrigin()
		admittedOrigin, admittedInherited := admitted.InheritedFanOutOrigin()
		if admitted.ID() == "" || !bytes.Equal(admitted.Payload(), emissions[ordinal].Event.Payload()) || originalOrigin != admittedOrigin || !originalInherited || !admittedInherited {
			t.Fatal("publication admission changed ordinal payload/origin or omitted durable identity")
		}
		emissions[ordinal].Event = admitted
		outcomes = append(outcomes, pipeline.FanOutChunkOutcome{Ordinal: ordinal, Publication: plan})
	}
	if err := eventBus.ReleaseEnginePublications(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	requireFanOutOriginNamedAdmission(t, fixture, backend, eventBus, ctx, owner, claim, emissions[0].Event)
	group, err := owner.BeginFanOutPublicationGroup(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := group.Close(context.WithoutCancel(ctx)); err != nil {
			t.Error(err)
		}
	}()
	requests := make([]pipeline.FanOutPublicationRequest, len(emissions))
	for ordinal, emission := range emissions {
		requests[ordinal] = pipeline.FanOutPublicationRequest{Ordinal: ordinal, Intent: emission}
	}
	grouped, err := eventBus.PrepareFanOutPublications(ctx, group, requests)
	if err != nil || len(grouped) != len(prepared) {
		t.Fatalf("prepare exact inherited group: count=%d err=%v", len(grouped), err)
	}
	for ordinal, member := range grouped {
		publication, ok := member.Publication.(bus.EnginePublicationPlan)
		if member.Err != nil || !ok || member.Ordinal != ordinal {
			t.Fatalf("prepare inherited ordinal %d: %+v", ordinal, member)
		}
		previous := prepared[ordinal].(bus.EnginePublicationPlan).PublicationCommand().Commit
		actual := publication.PublicationCommand().Commit
		if !reflect.DeepEqual(previous.Event.Event(), actual.Event.Event()) || !reflect.DeepEqual(previous.DeliveryRoutes, actual.DeliveryRoutes) {
			t.Fatal("group preparation changed exact admitted event or child recipients")
		}
		prepared[ordinal] = member.Publication
		outcomes[ordinal].Publication = member.Publication
	}
	if err := eventBus.SealFanOutPublications(ctx, group, len(emissions), prepared); err != nil {
		t.Fatal(err)
	}
	beforeCommit := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
	committed, err := owner.CommitFanOutChunk(ctx, pipeline.FanOutChunkCommand{Claim: claim, Outcomes: outcomes, Now: time.Now().UTC()})
	if err != nil || committed.Intent.Cursor != len(emissions) {
		if !reflect.DeepEqual(beforeCommit, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
			t.Fatal("failed ordinal commit mutated persisted state")
		}
		t.Fatalf("commit ordinal plans: cursor=%d err=%v", committed.Intent.Cursor, err)
	}
	nodes, err := pipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
		Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
		DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
		DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
		SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: consumer.receiver, WorkOwner: consumer.work,
	})
	if coordinator == nil {
		t.Fatal("real selected pipeline dependencies incomplete")
	}
	eventBus.SetInterceptors(coordinator)
	if err := eventBus.FinalizeFanOutPublications(ctx, group, committed.Publications); err != nil {
		t.Fatal(err)
	}
	join, ok := handle.JoinRef()
	if !ok {
		t.Fatal("selected child barrier lost its join handle")
	}
	var historicalState pipeline.WorkflowInstance
	var historicalLoop loopruntime.Activation
	var historicalBefore map[string][]string
	if expectedFailure != "" {
		if expectedFailure != "loop_revision_stale" {
			t.Fatalf("unsupported selected historical disposition: %s", expectedFailure)
		}
		identity, err := flowidentity.NewRunScopedFlowInstance(intent.Request.Key.RunID, intent.Request.Capsule.Route)
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		historicalState, found, err = workflow.LoadWorkflowInstance(ctx, identity)
		if err != nil || !found {
			t.Fatalf("historical receiver before processing: found=%v err=%v", found, err)
		}
		historicalLoop = readForkBarrierLoop(t, ctx, fixture.db, intent.Request.Key.RunID)
		if join.Generation().Attempt != 1 || historicalLoop.Attempt != 2 || !historicalLoop.OwnsGeneration(join.Generation()) {
			t.Fatalf("historical fence lost owned attempt-1/current-attempt-2 counterexample: captured=%+v current=%+v", join.Generation(), historicalLoop)
		}
		historicalBefore = snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
		t.Log("F07 historical fencing disposition: https://github.com/division-sh/swarm/issues/642#issuecomment-6148331654; original success oracle retained at testdata/future_capabilities/fork_selected_fan_out_success_test.go.txt; original RED retained at ~/.local/state/agent-a-642/c4-f07-restored-selected-full-race1.log")
	}
	fenced := 0
	for _, emit := range emissions {
		if err := eventBus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{emit}); err != nil {
			t.Fatalf("dispatch actual committed ordinal: %v", err)
		}
		persisted, found, err := selected.LoadPreparedPublishEvent(ctx, emit.Event.ID())
		if err != nil || !found || len(persisted.DeliveryRoutes) != 1 {
			t.Fatalf("ordinal durable readback: found=%v err=%v", found, err)
		}
		identity, err := flowidentity.NewRunScopedFlowInstance(intent.Request.Key.RunID, intent.Request.Capsule.Route)
		if err != nil {
			t.Fatal(err)
		}
		state, found, err := workflow.LoadWorkflowInstance(ctx, identity)
		if err != nil || !found {
			t.Fatalf("downstream business state readback: found=%v err=%v", found, err)
		}
		var payload map[string]any
		if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
			t.Fatal(err)
		}
		deliveries, err := ReadEventDeliveryDiagnosticRowsForTest(ctx, selected, emit.Event.ID())
		wantDeliveryStatus := "delivered"
		if expectedFailure != "" {
			wantDeliveryStatus = "dead_letter"
		}
		if err != nil || len(deliveries) != 1 || deliveries[0].Status != wantDeliveryStatus {
			diagnostic, diagnosticErr := ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, emit.Event.ID())
			authority, authorityPresent := effects.AuthorityFromContext(ctx)
			grant, grantPresent := startupownership.GenerationGrantEvidenceFromContext(ctx)
			readback := persisted.Event.Event()
			origin, inherited := readback.InheritedFanOutOrigin()
			t.Logf("failed selected ordinal readback: event_id=%s run_id=%s payload=%s origin=%s inherited=%v routes=%+v captured_loop=%s current_state=%s current_buckets=%s",
				readback.ID(), readback.RunID(), readback.Payload(), forkTestJSON(t, origin), inherited, persisted.DeliveryRoutes,
				forkTestJSON(t, intent.Request.Capsule.Loop), state.CurrentState, forkTestJSON(t, state.StateBuckets))
			t.Logf("failed selected ordinal authorization: claim=%+v delivery_authority=%+v selected_authority_present=%v selected_authority=%+v generation_grant_present=%v generation_grant=%+v",
				claim, consumer.delivery, authorityPresent, authority, grantPresent, grant)
			t.Fatalf("downstream settlement=%+v err=%v canonical_deliveries=%v canonical_dead_letters=%v canonical_receipts=%v canonical_read_err=%v",
				deliveries, err, diagnostic.Rows, diagnostic.DeadLetters, diagnostic.Receipts, diagnosticErr)
		}
		if expectedFailure == "" {
			if state.Fields["processed_value"] != payload["value"] {
				t.Fatalf("downstream business write missing: fields=%v payload=%v deliveries=%+v", state.Fields, payload, deliveries)
			}
		} else {
			requireSelectedForkHistoricalDeliveryFailure(t, ctx, selected, persisted, consumer.delivery, deliveries, historicalLoop, join.Generation().RevisionID)
			if !reflect.DeepEqual(historicalState, state) || !reflect.DeepEqual(historicalLoop, readForkBarrierLoop(t, ctx, fixture.db, intent.Request.Key.RunID)) {
				t.Fatalf("historical receiver business/loop state changed: before=%+v after=%+v", historicalState, state)
			}
			requireSelectedForkHistoricalBusinessUnchanged(t, historicalBefore, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres"))
			fenced++
		}
		before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")
		if err := eventBus.EngineDispatcher().DispatchPostCommit(ctx, []engine.EmitIntent{emit}); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres")) {
			t.Fatal("duplicate downstream consumption mutated history")
		}
		requireFanOutOriginReadback(t, fixture, backend, ctx, emit.Event)
	}
	progress, err := ReadFanOutIntentCompletionStorageForTest(ctx, selected, claim.Key)
	if err != nil || progress.Status != "closed" || progress.Cursor != len(emissions) || progress.Cardinality != len(emissions) || len(progress.Outcomes) != len(emissions) {
		t.Fatalf("selected ordinal completion readback: %+v err=%v", progress, err)
	}
	for ordinal, outcome := range progress.Outcomes {
		if outcome.Ordinal != ordinal || outcome.EventID != emissions[ordinal].Event.ID() || outcome.Kind != "committed" {
			t.Fatalf("selected ordinal outcome %d: %+v", ordinal, outcome)
		}
	}
	if expectedFailure != "" && fenced != len(emissions) {
		t.Fatalf("historical failure accounting lost committed ordinals: fenced=%d committed=%d", fenced, len(emissions))
	}
	advanceFanOutBarriersForTest(t, ctx, selected, intent.Request.Key.RunID, time.Now().UTC())
	wantStatus := fanoutbarrier.StatusClosedPending
	wantSummary := &fanoutbarrier.Summary{Total: len(emissions), Succeeded: len(emissions)}
	wantSchedule := handle.TaskID()
	if expectedFailure != "" {
		wantStatus, wantSummary, wantSchedule = fanoutbarrier.StatusSuppressedGenerationSuperseded, nil, ""
		if !reflect.DeepEqual(historicalLoop, readForkBarrierLoop(t, ctx, fixture.db, intent.Request.Key.RunID)) {
			t.Fatal("historical barrier suppression changed the current receiver loop")
		}
		requireSelectedForkHistoricalBusinessUnchanged(t, historicalBefore, snapshotForkHistoricalExecutionTables(t, fixture.db, backend == "postgres"))
	} else if join.Generation().Valid() && join.Generation() != readForkBarrierLoop(t, ctx, fixture.db, intent.Request.Key.RunID).Generation() {
		t.Fatal("current selected barrier no longer owns its captured generation")
	}
	assertFanOutBarrierState(t, ctx, fixture.db, intent.Request.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.SemanticPath, wantStatus, wantSummary, wantSchedule)
	checkpoint := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.checkpoint", "operator", "", []byte(`{}`), 0, intent.Request.Key.RunID, events.EventEnvelope{}, eventtest.RootRoutingSource(intent.Request.Key.RunID), time.Now().UTC())
	if err := insertCanonicalEventRecordFixture(ctx, fixture.store, checkpoint); err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, ctx, fixture.db, intent.Request.Key.RunID, backend == "postgres")
	plan, err := fixture.store.(selectedActivityProjectionStore).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: intent.Request.Key.RunID, At: checkpoint.ID()})
	if err != nil || len(plan.FanOutObligations) != 1 || len(plan.FanOutObligations[0].Outcomes) != len(emissions) {
		t.Fatalf("fixed-revision origin/outcome readback: %+v %v", plan.FanOutObligations, err)
	}
	barrier := plan.FanOutObligations[0].Barrier
	if barrier == nil || barrier.Registration.Handle != handle || barrier.Status != wantStatus || !reflect.DeepEqual(barrier.Summary, wantSummary) || barrier.ScheduleKey != wantSchedule {
		t.Fatalf("fixed revision lost exact child barrier: %+v", barrier)
	}
	if expectedFailure != "" {
		if !barrier.Status.Terminal() || barrier.ScheduleActivationID != "" {
			t.Fatalf("historical generation suppression retained executable completion work: %+v", barrier)
		}
		t.Logf("F07 historical fencing complete: ordinals=%d native_deliveries=dead_letter native_receipts=dead_letter class=%s code=%s retryable=false barrier=%s business_unchanged=true source_isolation_checked=true",
			fenced, failures.ClassStaleArrival, expectedFailure, barrier.Status)
	}
}

func selectedForkStaleFailureReadback(raw []byte) (failures.Envelope, error) {
	var failure failures.Envelope
	if err := json.Unmarshal(raw, &failure); err != nil {
		return failure, err
	}
	if err := failures.ValidateEnvelope(failure); err != nil {
		return failure, err
	}
	if failure.Class != failures.ClassStaleArrival || failure.Detail.Code != "loop_revision_stale" || failure.Retryable || !failure.Deterministic || failure.Component != "runtime.engine" || failure.Operation != "loop" {
		return failure, fmt.Errorf("historical delivery lacks exact deterministic nonretryable loop stale failure: %+v", failure)
	}
	return failure, nil
}

func requireSelectedForkHistoricalDeliveryFailure(t *testing.T, ctx context.Context, selected storeTestDurableEventBusStore, persisted bus.PreparedPublishEvent, authority deliverylifecycle.ExecutionAuthority, deliveries []EventDeliveryDiagnosticRow, current loopruntime.Activation, suppliedRevision string) {
	t.Helper()
	event := persisted.Event.Event()
	obligation, err := deliverylifecycle.NewObligation(event.ID(), event.RunID(), persisted.DeliveryRoutes[0], authority)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := selected.(interface {
		Snapshot(context.Context, string) (deliverylifecycle.Snapshot, error)
	}).Snapshot(ctx, obligation.DeliveryID())
	if err != nil || snapshot.Status != deliverylifecycle.StatusDeadLetter || snapshot.ReasonCode != "handler_terminal_failure" || snapshot.Failure == nil || snapshot.RetryScheduled || snapshot.RetryCount != 0 || snapshot.SettledAt.IsZero() {
		t.Fatalf("native historical delivery settlement: %+v err=%v", snapshot, err)
	}
	deliveryFailure, err := selectedForkStaleFailureReadback([]byte(forkTestJSON(t, snapshot.Failure)))
	if err != nil {
		t.Fatal(err)
	}
	if deliveries[0].Reason != "handler_terminal_failure" || snapshot.EventID != event.ID() || snapshot.RunID != event.RunID() || snapshot.SubscriberID != deliveries[0].SubscriberID || snapshot.Authority != authority || !reflect.DeepEqual(snapshot.Route, persisted.DeliveryRoutes[0]) {
		t.Fatalf("historical native delivery changed exact receiver/selected authority: %+v diagnostic=%+v", snapshot, deliveries)
	}
	if deliveryFailure.Detail.Attributes["supplied_revision_id"] != suppliedRevision || deliveryFailure.Detail.Attributes["current_revision_id"] != current.RevisionID || deliveryFailure.Detail.Attributes["attempt"] != float64(current.Attempt) || deliveryFailure.Detail.Attributes["trigger_event_id"] != event.ID() {
		t.Fatalf("native stale failure lost exact historical/current generation: %+v", deliveryFailure)
	}
	receipt, err := ReadLatestPlatformPipelineReceiptStorageForTest(ctx, selected, event.ID())
	if err != nil || !receipt.Found || receipt.SubscriberID != "pipeline" || receipt.Outcome != "dead_letter" {
		t.Fatalf("historical native pipeline receipt: %+v err=%v", receipt, err)
	}
	receiptFailure, err := selectedForkStaleFailureReadback(receipt.Failure)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic, err := ReadNodeDeliveryDiagnosticStorageForTest(ctx, selected, event.ID())
	if err != nil || len(diagnostic.Rows) != 1 || len(diagnostic.DeadLetters) != 1 || len(diagnostic.Receipts) != 1 {
		t.Fatalf("historical native failure cardinality: %+v err=%v", diagnostic, err)
	}
	prefix, raw, found := strings.Cut(diagnostic.DeadLetters[0], " detail=")
	if !found || prefix != snapshot.SubscriberID+" failure="+string(failures.ClassStaleArrival) {
		t.Fatalf("historical native dead letter lost exact receiver/class: %s", diagnostic.DeadLetters[0])
	}
	deadLetterFailure, err := selectedForkStaleFailureReadback([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	prefix, raw, found = strings.Cut(diagnostic.Receipts[0], " failure=")
	if !found || prefix != "platform/pipeline outcome=dead_letter reason=handler_terminal_failure" {
		t.Fatalf("historical native receipt changed exact terminal accounting: %s", diagnostic.Receipts[0])
	}
	diagnosticReceiptFailure, err := selectedForkStaleFailureReadback([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []failures.Envelope{receiptFailure, deadLetterFailure, diagnosticReceiptFailure} {
		if forkTestJSON(t, failure) != forkTestJSON(t, deliveryFailure) {
			t.Fatalf("historical delivery/receipt/dead-letter failures disagree: delivery=%+v other=%+v", deliveryFailure, failure)
		}
	}
	t.Logf("historical ordinal native accounting: event_id=%s delivery_id=%s class=%s code=%s retryable=%v captured_revision=%s current_revision=%s current_attempt=%d",
		event.ID(), snapshot.DeliveryID, deliveryFailure.Class, deliveryFailure.Detail.Code, deliveryFailure.Retryable, suppliedRevision, current.RevisionID, current.Attempt)
}

func requireSelectedForkHistoricalBusinessUnchanged(t *testing.T, before, after map[string][]string) {
	t.Helper()
	for _, table := range []string{"entity_state", "entity_mutations", "flow_instances", "activity_attempts", "runtime_external_effect_operations", "runtime_external_effect_attempts"} {
		if _, present := before[table]; !present || !reflect.DeepEqual(before[table], after[table]) {
			t.Fatalf("historical refusal changed business/control/effect history or lacked %s coverage", table)
		}
	}
	if !reflect.DeepEqual(selectedForkNonDiagnosticEvents(t, before), selectedForkNonDiagnosticEvents(t, after)) {
		t.Fatal("historical refusal emitted downstream business or completion output")
	}
}

func selectedForkNonDiagnosticEvents(t *testing.T, snapshot map[string][]string) []string {
	t.Helper()
	column := -1
	for i, name := range snapshot["events/columns"] {
		if name == "event_name" {
			column = i
		}
	}
	if column < 0 {
		t.Fatal("historical event snapshot lacks canonical event-name coverage")
	}
	var events []string
	for _, raw := range snapshot["events"] {
		var row []any
		if err := json.Unmarshal([]byte(raw), &row); err != nil || len(row) <= column {
			t.Fatalf("historical event snapshot is invalid: %s err=%v", raw, err)
		}
		name, ok := row[column].(string)
		if !ok {
			t.Fatal("historical event snapshot has a non-text event name")
		}
		if name != "platform.runtime_log" {
			events = append(events, raw)
		}
	}
	return events
}

func TestSelectedForkHistoricalFailureReadback(t *testing.T) {
	failure, found := failures.EnvelopeFromError(failures.New(failures.ClassStaleArrival, "loop_revision_stale", "runtime.engine", "loop", map[string]any{"attempt": 2}))
	if !found {
		t.Fatal("stale failure fixture lacks a typed envelope")
	}
	if _, err := selectedForkStaleFailureReadback([]byte(forkTestJSON(t, failure))); err != nil {
		t.Fatal(err)
	}
	for _, cell := range []struct {
		name   string
		mutate func(*failures.Envelope)
	}{
		{"wrong_class", func(f *failures.Envelope) { f.Class = failures.ClassUnexpectedArrival }},
		{"wrong_code", func(f *failures.Envelope) { f.Detail.Code = "loop_revision_unexpected" }},
		{"retryable", func(f *failures.Envelope) { f.Retryable = true }},
		{"nondeterministic", func(f *failures.Envelope) { f.Deterministic = false }},
		{"wrong_component", func(f *failures.Envelope) { f.Component = "fixture" }},
		{"wrong_operation", func(f *failures.Envelope) { f.Operation = "publication" }},
	} {
		t.Run(cell.name, func(t *testing.T) {
			changed := failure
			cell.mutate(&changed)
			if _, err := selectedForkStaleFailureReadback([]byte(forkTestJSON(t, changed))); err == nil {
				t.Fatal("historical readback accepted an inexact refusal")
			}
		})
	}
	if _, err := selectedForkStaleFailureReadback([]byte(`{`)); err == nil {
		t.Fatal("historical readback accepted malformed failure JSON")
	}
}
