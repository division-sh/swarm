package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestNativeWorkflowTimerFaultsRefuseUnsupportedSelectedStore(t *testing.T) {
	for _, fault := range []func(context.Context, any, string, string) error{
		storetest.RemoveWorkflowTimer, storetest.SetWorkflowTimerForeignDeclaration,
		storetest.SetWorkflowTimerMalformedName, storetest.SetWorkflowTimerForeignRouteAndMalformedName,
	} {
		for _, selected := range []any{nil, &struct{}{}} {
			if err := fault(context.Background(), selected, uuid.NewString(), uuid.NewString()); err == nil {
				t.Fatal("timer fault accepted an unsupported selected store")
			}
		}
	}
}

type pipelineNativeDeliveryPublicationStore interface {
	timerReplaySelectedStore
	storetest.RunFixtureStore
	CommitPublication(context.Context, runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error)
}

func workflowHandlerNativeCoordinator(t *testing.T, selected timerReplaySelectedStore, options pipeline.PipelineCoordinatorOptions, fact correlation.SourceArtifactFact) (*pipeline.PipelineCoordinator, *runtimebus.EventBus) {
	t.Helper()
	var componentEvents []string
	if bundle, found := semanticview.Bundle(options.Module.SemanticSource()); found {
		for _, timer := range bundle.Semantics.Timers {
			if timer.StageOwned {
				componentEvents = append(componentEvents, "platform.stage_timer")
				break
			}
		}
	}
	bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: options.Module.SemanticSource(), SourceArtifactFact: fact, WorkOwner: pipelineExternalTestWorkOwnerForSource(t, fact)}, componentEvents...)
	if err != nil {
		t.Fatal(err)
	}
	options.WorkOwner = pipelineExternalTestWorkOwnerForSource(t, fact)
	pc := newTimerReplayCoordinator(t, bus, selected, options, fact)
	t.Cleanup(func() {
		join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bus.WaitForQuiescence(join); err != nil {
			t.Error(err)
		}
	})
	return pc, bus
}

func pipelineDeliveryNativeFixture(t *testing.T, backend string, source semanticview.Source) *pipeline.PipelineDeliveryNativeFixtureForTest {
	t.Helper()
	base, selected, reopen, probe := openWorkflowHandlerNativeFixture(t, backend, source)
	return pipelineDeliveryNativeFixtureFromSelected(t, source, base, selected, reopen, probe)
}

func pipelineDeliveryNativeFixtureFromSelected(t *testing.T, source semanticview.Source, base pipeline.WorkflowHandlerNativeFixtureForTest, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore, probe *storetest.TransactionCollector) *pipeline.PipelineDeliveryNativeFixtureForTest {
	t.Helper()
	bundle, _ := semanticview.Bundle(source)
	fact := sourceartifactfixture.RequireArtifact(t, base.Context, selected.(sourceartifactfixture.Writer), bundle.SourceArtifact)
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(fact, authorActivityTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(base.Context, authority); err != nil {
		t.Fatal(err)
	}
	fixture := &pipeline.PipelineDeliveryNativeFixtureForTest{WorkflowHandlerNativeFixtureForTest: base, Store: selected, Authority: authority}
	fixture.HumanTasks = selected.(decisioncard.HumanTaskStore)
	fixture.HumanExpiry = selected.(pipeline.HumanTaskExpiry)
	fixture.CreateHumanReply = func(ctx context.Context, run, request, reply string) error {
		at := time.Now().UTC()
		event := eventtest.PersistedRuntimeControlForProducer(request, "provider.requested", eventtest.Producer(events.EventProducerPlatform, "test"), "", []byte(`{}`), 0, run, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, at)
		storetest.CommitSemanticEvent(t, ctx, selected, event)
		return selected.(interface {
			CreateReplyContext(context.Context, replycontext.Record) error
		}).CreateReplyContext(ctx, replycontext.Record{
			ID: reply, RunID: run, RequestEventID: request, RequesterFlowID: ".", ProviderFlowID: ".",
			RequestOutputPin: "provider_requested", ReplyInputPin: "provider_replied", ProviderInputPin: "provider_requested", ProviderOutputPin: "provider_replied",
			Origin: events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run}, RequestCorrelationID: request, State: replycontext.StateOpen, CreatedAt: at, UpdatedAt: at,
		})
	}
	fixture.AdmitSchedule = selected.(runtimegenericschedule.Store).AdmitGenericScheduleOutcome
	fixture.RemoveTimer = func(ctx context.Context, run, activation string) error {
		return storetest.RemoveWorkflowTimer(ctx, selected, run, activation)
	}
	fixture.SetTimerForeignDeclaration = func(ctx context.Context, run, activation string) error {
		return storetest.SetWorkflowTimerForeignDeclaration(ctx, selected, run, activation)
	}
	fixture.SetTimerMalformedName = func(ctx context.Context, run, activation string) error {
		return storetest.SetWorkflowTimerMalformedName(ctx, selected, run, activation)
	}
	fixture.SetTimerForeignRouteMalformedName = func(ctx context.Context, run, activation string) error {
		return storetest.SetWorkflowTimerForeignRouteAndMalformedName(ctx, selected, run, activation)
	}
	var retire func()
	var predecessor *worklifetime.RuntimeOccurrence
	projectionOnly := false
	fixture.Construct = func(ctx context.Context, instance pipeline.WorkflowInstance) error {
		if projectionOnly {
			return fmt.Errorf("reopened native projection cannot construct predecessor execution work")
		}
		return base.Construct(ctx, instance)
	}
	fixture.ConstructInitial = func(ctx context.Context, instance pipeline.WorkflowInstance, lifecycle pipeline.WorkflowLifecycleMutationPlan) (pipeline.CommittedWorkflowLifecycleMutation, error) {
		if projectionOnly {
			return pipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("reopened native projection cannot construct predecessor execution work")
		}
		command, err := flowactivationfixture.Command(ctx, instance, lifecycle, instance.CreatedAt)
		if err != nil {
			return pipeline.CommittedWorkflowLifecycleMutation{}, err
		}
		committed, err := selected.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
		if err != nil {
			return pipeline.CommittedWorkflowLifecycleMutation{}, err
		}
		if !committed.Acknowledged || !committed.Created {
			return pipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("native initial construction did not acknowledge exact creation")
		}
		return committed.Lifecycle, nil
	}
	fixture.ReopenProjection = func() pipeline.WorkflowPersistence {
		if projectionOnly {
			t.Fatal("native predecessor has already been reopened")
		}
		if retire == nil {
			t.Fatal("native projection reopen requires its predecessor coordinator")
		}
		retire()
		projectionOnly = true
		if err := selected.Close(); err != nil {
			t.Fatal(err)
		}
		selected = reopen()
		return pipeline.NewWorkflowPersistence(selected)
	}
	fixture.ReopenExecution = func() *pipeline.PipelineDeliveryNativeFixtureForTest {
		if projectionOnly || retire == nil || predecessor == nil {
			t.Fatal("native execution reopen requires its exact live predecessor")
		}
		retire()
		projectionOnly = true
		if err := selected.Close(); err != nil {
			t.Fatal(err)
		}
		replacePipelineExternalWorkOccurrenceForSource(t, fact, predecessor)
		nextBase, nextSelected, nextReopen, nextProbe := workflowHandlerNativeFixtureFromSelected(t, source, reopen(), reopen)
		return pipelineDeliveryNativeFixtureFromSelected(t, source, nextBase, nextSelected, nextReopen, nextProbe)
	}
	fixture.TransitionWire = func(ctx context.Context, run, path string) (json.RawMessage, error) {
		return storetest.ReadWorkflowTransitionEvidenceWire(ctx, selected, run, path)
	}
	fixture.SetTransitionWire = func(ctx context.Context, run, path string, wire json.RawMessage) (int64, error) {
		return storetest.SetWorkflowTransitionEvidenceWire(ctx, selected, run, path, wire)
	}
	fixture.NewCoordinator = func(options pipeline.PipelineCoordinatorOptions) *pipeline.PipelineCoordinator {
		if fixture.Continuations != nil {
			t.Fatal("native delivery fixture already has its exact component coordinator")
		}
		pc, bus := workflowHandlerNativeCoordinator(t, selected, options, fact)
		work := pipelineExternalTestWorkOwnerForSource(t, fact)
		predecessor = work
		fixture.Context = worklifetime.WithOccurrence(base.Context, work)
		continuations, err := deliverycontinuation.New(selected, selected, authority, work, bus, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := bus.SetDeliveryContinuationOwner(continuations); err != nil {
			t.Fatal(err)
		}
		bus.SetInterceptors(pc)
		fixture.ObserveHumanRequester = func(ctx context.Context, agent string) {
			identity := runtimebustest.IdentityForRun(t, correlation.RunIDFromContext(ctx), agent, "")
			intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents."+agent+".intent", "Observe the human task outcome.")
			if err != nil {
				t.Fatal(err)
			}
			prompt, err := agentintent.IntentOnlyPrompt(intent)
			if err != nil {
				t.Fatal(err)
			}
			config := actors.AgentConfig{ID: agent, Identity: identity, Type: "managed", Role: "worker", Model: "cheap", ResolvedLLMBackend: "anthropic", ExecutionMode: "live", EntityID: correlation.RunIDFromContext(ctx), Intent: intent, Prompt: prompt}
			if err := agentfixture.UpsertStaticForSource(t, ctx, selected.(agentfixture.Store), manager.PersistedAgent{Config: config, Status: "active", StartedAt: time.Now().UTC()}, fact); err != nil {
				t.Fatal(err)
			}
			bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{Identity: identity, EntityID: correlation.RunIDFromContext(ctx)})
			runtimebustest.SubscribeForRun(t, bus, correlation.RunIDFromContext(ctx), agent, "human_task.approved", "human_task.rejected", "human_task.deferred", "human_task.expired")
			t.Cleanup(func() { runtimebustest.Unsubscribe(bus, agent) })
		}
		fixture.Continuations = continuations
		fixture.JoinExecution = func(ctx context.Context) error {
			if err := work.WaitForQuiescence(ctx); err != nil {
				return err
			}
			return bus.WaitForQuiescence(ctx)
		}
		retire = func() {
			join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := continuations.Retire(join); err != nil {
				t.Fatal(err)
			}
			if err := bus.WaitForQuiescence(join); err != nil {
				t.Fatal(err)
			}
			if _, err := work.RetireAndWait(join); err != nil {
				t.Fatal(err)
			}
		}
		// This component cut admits real carriers without starting autonomous
		// recovery workers; the tests drive the original dispatcher explicitly.
		t.Cleanup(func() {
			join, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := continuations.Retire(join); err != nil {
				t.Error(err)
			}
		})
		return pc
	}
	fixture.PublishNode = func(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
		if projectionOnly {
			return fmt.Errorf("reopened native projection cannot publish predecessor execution work")
		}
		if fixture.Continuations == nil {
			return fmt.Errorf("native pipeline publication requires its configured continuation owner")
		}
		committed, err := commitPipelineNativeDeliveryPublication(t, ctx, selected.(pipelineNativeDeliveryPublicationStore), event, route, authority)
		if err != nil {
			return err
		}
		return fixture.Continuations.AcceptCommitted(committed.DeliveryHandoffs)
	}
	fixture.RetryEligible = func(ctx context.Context, event events.Event, route events.DeliveryRoute) error {
		return storetest.MakeManagerRetryEligible(ctx, selected, event, route)
	}
	fixture.HoldUnstampedAdmissionTransaction = func(ctx context.Context, run string) (func() error, error) {
		return storetest.HoldUnstampedPipelineAdmissionTransaction(ctx, selected, run)
	}
	fixture.EventCount = func(ctx context.Context, run, name string) (int, error) {
		return storetest.ReadLifecycleEventCardinality(ctx, selected, run, name)
	}
	fixture.EventIDCount = func(ctx context.Context, event string) int {
		return storetest.ObserveEventCardinality(t, ctx, selected, event)
	}
	fixture.MutationCount = func(ctx context.Context, run, event, path, writer, step string) (int, error) {
		rows := storetest.ObserveEntityMutationHistory(t, ctx, selected, run)
		count := 0
		for _, row := range rows {
			if row.CausedByEvent == event && row.Domain == "authored_field" && row.Path == path && row.WriterID == writer && row.HandlerStep == step {
				count++
			}
		}
		return count, nil
	}
	fixture.Cards = selected
	fixture.ApplyDecision = storetest.DecisionCardDomain(selected).ApplyDecisionForTest
	fixture.ApplyDeferral = storetest.DecisionCardDomain(selected).ApplyDeferralForTest
	fixture.SetCardInsertFault = func(ctx context.Context, run string, enabled bool) error {
		return storetest.SetWorkflowGateCardInsertFault(ctx, selected, run, enabled)
	}
	fixture.HideMutationTable = func(ctx context.Context, hidden bool) error {
		return storetest.HideWorkflowMutationTable(ctx, selected, hidden)
	}
	fixture.AdmitAttachment = func(ctx context.Context, run, path string) (pipeline.DynamicFlowRuntimeActivationAttempt, pipeline.DynamicFlowRuntimeReadinessPlan, func()) {
		readiness, found, err := selected.(pipeline.DynamicFlowRuntimeReadinessPersistence).LoadDynamicFlowRuntimeReadiness(ctx, run, flowidentity.StoredRoute(".", path, path))
		if err != nil || !found {
			t.Fatalf("load exact native attachment plan: found=%t error=%v", found, err)
		}
		admit, closeOwner := newTimerReplayAttachmentOwnerForSource(t, ctx, selected, fact)
		return admit(readiness.Plan), readiness.Plan, closeOwner
	}
	fixture.MutationHistory = func(ctx context.Context, run, entity string) []pipeline.PipelineNativeMutationRowForTest {
		var out []pipeline.PipelineNativeMutationRowForTest
		for _, row := range storetest.ObserveEntityMutationHistory(t, ctx, selected, run) {
			if row.EntityID == entity {
				out = append(out, pipeline.PipelineNativeMutationRowForTest{Domain: row.Domain, Path: row.Path, WriterType: row.WriterType, WriterID: row.WriterID, HandlerStep: row.HandlerStep, CausedByEvent: row.CausedByEvent, OldValue: append(json.RawMessage(nil), row.OldValue...), NewValue: append(json.RawMessage(nil), row.NewValue...)})
			}
		}
		return out
	}
	fixture.TrackedMutationProjection = func(ctx context.Context, run, entity string) (mutationlog.EntityStateProjection, []mutationlog.ProjectionMutation, error) {
		snapshot, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, selected, run, entity)
		if err != nil {
			return mutationlog.EntityStateProjection{}, nil, err
		}
		projection := mutationlog.EntityStateProjection{CurrentState: snapshot.CurrentState}
		for _, owner := range []struct {
			raw json.RawMessage
			dst *map[string]any
		}{{snapshot.Fields, &projection.Fields}, {snapshot.Bookkeeping, &projection.Bookkeeping}, {snapshot.Gates, &projection.Gates}, {snapshot.Accumulator, &projection.Accumulator}} {
			if err := json.Unmarshal(owner.raw, owner.dst); err != nil {
				return mutationlog.EntityStateProjection{}, nil, err
			}
		}
		var records []mutationlog.ProjectionMutation
		for _, row := range snapshot.Mutations {
			var value any
			if len(row.NewValue) != 0 {
				if err := json.Unmarshal(row.NewValue, &value); err != nil {
					return mutationlog.EntityStateProjection{}, nil, err
				}
			}
			records = append(records, mutationlog.ProjectionMutation{Domain: mutationlog.Domain(row.Domain), Path: row.Path, NewValue: value})
		}
		return projection, records, nil
	}
	fixture.ProbeMutationMissingDomainPath = func(ctx context.Context, run, entity string) error {
		return storetest.ProbeWorkflowMutationMissingDomainPath(ctx, selected, run, entity)
	}
	fixture.NumericFlowPath = func(ctx context.Context, run, path string) (int64, error) {
		return storetest.SetWorkflowProjectionNumericFlowPath(ctx, selected, run, path)
	}
	fixture.AmbiguousFields = func(ctx context.Context, run, entity string) (int64, error) {
		return storetest.AddAmbiguousWorkflowProjectionFields(ctx, selected, run, entity)
	}
	fixture.ActiveTerminatedTimestamp = func(ctx context.Context, run, path string, at time.Time) (int64, error) {
		return storetest.SetWorkflowProjectionActiveTerminatedTimestamp(ctx, selected, run, path, at)
	}
	fixture.EntityStateOwner = func(ctx context.Context, entity string) (string, error) {
		rows, err := storetest.ReadWorkflowStateObservationRows(ctx, selected)
		if err != nil {
			return "", err
		}
		var owner string
		for _, row := range rows {
			if row.EntityID != entity {
				continue
			}
			if owner != "" {
				return "", fmt.Errorf("entity state observation has multiple owners")
			}
			owner = row.FlowInstance
		}
		if owner == "" {
			return "", fmt.Errorf("entity state observation has no owner")
		}
		return owner, nil
	}
	fixture.AccumulatorHistory = func(ctx context.Context, run, entity string) ([]mutationlog.ProjectionMutation, error) {
		rows := storetest.ObserveEntityMutationHistory(t, ctx, selected, run)
		var records []mutationlog.ProjectionMutation
		// The existing observation port returns newest first; projection folding
		// consumes its exact reverse, not a different query or inferred ordering.
		for index := len(rows) - 1; index >= 0; index-- {
			row := rows[index]
			if row.EntityID != entity || row.Domain != "accumulator" {
				continue
			}
			var value any
			if len(row.NewValue) != 0 {
				if err := json.Unmarshal(row.NewValue, &value); err != nil {
					return nil, err
				}
			}
			records = append(records, mutationlog.ProjectionMutation{Domain: mutationlog.DomainAccumulator, Path: row.Path, NewValue: value})
		}
		return records, nil
	}
	fixture.DeliveryCount = func(ctx context.Context, event, node string) (int, error) {
		return storetest.ReadServedDeliveryStatusCount(ctx, selected, event, "node", node)
	}
	fixture.FanOutStorage = func(ctx context.Context, run, event string, node identity.ExecutableNode, ref contracts.FanOutElementRef) ([]pipeline.PipelineFanOutStorageForTest, error) {
		rows, err := storetest.ReadFanOutTriggeredIntentStorage(ctx, selected, run, event, node, ref)
		if err != nil {
			return nil, err
		}
		var result []pipeline.PipelineFanOutStorageForTest
		for _, row := range rows {
			result = append(result, pipeline.PipelineFanOutStorageForTest{DeliveryID: row.TriggeringDeliveryID, SemanticDigest: row.SemanticDigest, Status: row.Status, Capsule: row.Capsule, Source: row.Source, Cursor: row.Cursor, Cardinality: row.Cardinality})
		}
		return result, nil
	}
	fixture.FanOutIntentCount = func(ctx context.Context, run string) (int, error) {
		summary, err := selected.FanOutRunSummary(ctx, run, time.Now().UTC())
		return summary.Intents, err
	}
	fixture.NodeDeliverySnapshot = func(ctx context.Context, run, event, node string) (deliverylifecycle.Snapshot, error) {
		rows, err := storetest.ReadReceiverDeliveryStorage(ctx, selected, run)
		if err != nil {
			return deliverylifecycle.Snapshot{}, err
		}
		var found *deliverylifecycle.Snapshot
		for _, row := range rows {
			if row.EventID != event {
				continue
			}
			snapshot, err := selected.Snapshot(ctx, row.DeliveryID)
			if err != nil {
				return deliverylifecycle.Snapshot{}, err
			}
			if snapshot.SubscriberClass != deliverylifecycle.SubscriberNode || snapshot.SubscriberID != node {
				continue
			}
			if found != nil {
				return deliverylifecycle.Snapshot{}, fmt.Errorf("multiple native deliveries for exact event/node")
			}
			found = &snapshot
		}
		if found == nil {
			return deliverylifecycle.Snapshot{}, deliverylifecycle.ErrNotFound
		}
		return *found, nil
	}
	fixture.ContainedFootprint = func(ctx context.Context, run, path, entity string) (pipeline.PipelineContainedFootprintForTest, error) {
		var out pipeline.PipelineContainedFootprintForTest
		rows, err := storetest.ReadReceiverDeliveryStorage(ctx, selected, run)
		if err != nil {
			return out, err
		}
		for _, row := range rows {
			var target events.DeliveryTargetOwnership
			if err := json.Unmarshal([]byte(row.Target), &target); err != nil {
				return out, err
			}
			if target.Route().FlowInstance == path {
				out.Deliveries++
			}
		}
		out.Headers, err = storetest.CountWorkflowHeadersForPathCreatedSince(ctx, selected, path, time.Time{})
		if err != nil {
			return out, err
		}
		states, err := storetest.ReadWorkflowStateObservationRows(ctx, selected)
		if err != nil {
			return out, err
		}
		for _, row := range states {
			if row.EntityID == entity {
				out.EntitiesByID++
			}
			if row.FlowInstance == path {
				out.EntitiesByPath++
			}
		}
		return out, nil
	}
	fixture.SelectionFact = func(ctx context.Context, eventID, deliveryID string) (handlerselection.HandlerRuleSelectionFact, int, error) {
		rows, err := storetest.ReadHandlerSelectionStorage(ctx, selected, eventID)
		if err != nil || len(rows) == 0 {
			return handlerselection.HandlerRuleSelectionFact{}, len(rows), err
		}
		if len(rows) != 1 || rows[0].DeliveryID != deliveryID {
			return handlerselection.HandlerRuleSelectionFact{}, len(rows), fmt.Errorf("native selection storage differs from exact node delivery")
		}
		row := rows[0]
		fact, err := handlerselection.Hydrate(row.Context, row.Disposition, row.FlowPath, row.Family, row.SemanticPath, row.DisplayLabel)
		return fact, len(rows), err
	}
	fixture.Transactions = func() pipeline.PipelineDeliveryNativeTransactionCountsForTest {
		counts := probe.Snapshot()
		return pipeline.PipelineDeliveryNativeTransactionCountsForTest{
			WorkflowCommits:   counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits,
			WorkflowRollbacks: counts.ByOperation[storetest.TransactionWorkflowMutation].RollbackAttempts,
			Claims:            counts.ByOperation[storetest.TransactionDeliveryClaim].WriteCommits,
			Renewals:          counts.ByOperation[storetest.TransactionDeliveryRenew].WriteCommits,
			Settlements:       counts.ByOperation[storetest.TransactionDeliverySettle].WriteCommits,
			OtherCommits:      counts.ByOperation[storetest.TransactionOther].WriteCommits,
			Active:            counts.Active,
		}
	}
	return fixture
}

func commitPipelineNativeDeliveryPublication(t *testing.T, ctx context.Context, selected pipelineNativeDeliveryPublicationStore, event events.Event, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) (runtimebus.CommittedPublication, error) {
	t.Helper()
	var err error
	if _, bound := event.PayloadAdmission(); !bound {
		event, err = eventfixture.BindPayload(event)
		if err != nil {
			return runtimebus.CommittedPublication{}, err
		}
	}
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	if err := storetest.EnsureRunForAdmittedEvent(ctx, selected, admitted, event.CreatedAt()); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	claim, err := selected.PipelineObligations().ClaimPublication(ctx, event.ID())
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	defer func() {
		if err := selected.PipelineObligations().Release(context.WithoutCancel(ctx), claim); err != nil {
			t.Error(err)
		}
	}()
	scope := authoractivity.BundleScope(event.ID(), authority.SourceArtifact().BundleHash())
	descriptor := authoractivity.EventDescriptor{EventType: string(event.Type()), Disposition: authoractivity.StoryDifferent}
	catalog, err := selected.RegisterAuthorActivityEventCatalog(scope, []authoractivity.EventDescriptor{descriptor})
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	defer catalog.Release()
	result, err := selected.CommitPublication(ctx, runtimebus.PublicationCommand{
		Commit:      runtimebus.CommitPublishRequest{Event: admitted, RouteSettlement: settlement, DeliveryRoutes: []events.DeliveryRoute{route}, DeliveryAuthority: authority, ReplayScope: pipelineobligation.ScopeSubscribed, PipelineClaim: claim, Disposition: storetest.AcknowledgedPipelineDisposition()},
		AuthorScope: scope, HasAuthorScope: true, AuthorDescriptor: descriptor, HasAuthorDescriptor: true,
	})
	if !result.Acknowledged {
		return runtimebus.CommittedPublication{}, fmt.Errorf("native pipeline publication did not acknowledge commit: %v", err)
	}
	return result, err
}

func TestPipelineDeliveryNativeRecipePreservesExactStoreAndAuthorityBothStores(t *testing.T) {
	pipeline.VerifyPipelineDeliveryNativeRecipePreservesExactStoreAndAuthorityForTest(t, pipelineDeliveryNativeFixture)
}
