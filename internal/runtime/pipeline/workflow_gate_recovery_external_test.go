package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const (
	gateRecoveryBundle = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherGateBundle    = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func withLiveGateExecution(ctx context.Context) context.Context {
	return runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
}

type gateRecoveryModule struct {
	source semanticview.Source
}

func (m gateRecoveryModule) SemanticSource() semanticview.Source                   { return m.source }
func (gateRecoveryModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition { return nil }
func (gateRecoveryModule) WorkflowNodes() []runtimepipeline.WorkflowNode           { return nil }
func (gateRecoveryModule) GuardRegistry() runtimepipeline.GuardRegistry            { return nil }
func (gateRecoveryModule) ActionRegistry() runtimepipeline.ActionRegistry          { return nil }

type proposedEffectProofModule struct {
	source   semanticview.Source
	workflow *runtimepipeline.WorkflowDefinition
	nodes    []runtimepipeline.WorkflowNode
}

func (m proposedEffectProofModule) SemanticSource() semanticview.Source { return m.source }
func (m proposedEffectProofModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}
func (m proposedEffectProofModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}
func (proposedEffectProofModule) GuardRegistry() runtimepipeline.GuardRegistry   { return nil }
func (proposedEffectProofModule) ActionRegistry() runtimepipeline.ActionRegistry { return nil }

type gateRecoveryStoreCase struct {
	name          string
	postgres      bool
	db            *sql.DB
	events        runtimebus.EventStore
	cards         decisioncard.Store
	workflowStore *runtimepipeline.WorkflowInstanceStore
}

type proposedEffectProofCredentialStore struct {
	value string
	gets  atomic.Int32
}

type proposedEffectRouteProofBus struct {
	published       []events.Event
	publishContexts []events.DeliveryContext
	outbox          []runtimeengine.EmitIntent
	dispatched      []runtimeengine.EmitIntent
	eventBus        *runtimebus.EventBus
}

type proposedEffectRouteProofOutbox struct{ bus *proposedEffectRouteProofBus }
type proposedEffectRouteProofDispatcher struct{ bus *proposedEffectRouteProofBus }

func (*proposedEffectRouteProofBus) SubscribeInternal(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*proposedEffectRouteProofBus) Publish(context.Context, events.Event) error { return nil }
func (*proposedEffectRouteProofBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*proposedEffectRouteProofBus) ResolveSubscribedRecipients(string) []string { return nil }
func (*proposedEffectRouteProofBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}
func (b *proposedEffectRouteProofBus) EngineOutbox() runtimeengine.OutboxWriter {
	return proposedEffectRouteProofOutbox{bus: b}
}
func (b *proposedEffectRouteProofBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return proposedEffectRouteProofDispatcher{bus: b}
}
func (b *proposedEffectRouteProofBus) RuntimeMutationRunner() runtimepipeline.RuntimeMutationRunner {
	if b == nil || b.eventBus == nil {
		return nil
	}
	return b.eventBus.RuntimeMutationRunner()
}
func (b *proposedEffectRouteProofBus) PublishInMutation(ctx context.Context, evt events.Event) error {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return errors.New("proposed-effect proof publication requires pipeline transaction")
	}
	if b.eventBus == nil {
		return errors.New("proposed-effect proof publication requires event bus")
	}
	if err := b.eventBus.PublishInMutation(ctx, evt); err != nil {
		return err
	}
	b.published = append(b.published, events.NewContextDeliveryEvent(evt, events.DeliveryContextFromContext(ctx)).Event())
	b.publishContexts = append(b.publishContexts, events.DeliveryContextFromContext(ctx))
	return nil
}
func (o proposedEffectRouteProofOutbox) WriteOutbox(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil || o.bus.eventBus == nil {
		return errors.New("proposed-effect proof outbox requires pipeline event mutation")
	}
	if err := o.bus.eventBus.EngineOutbox().WriteOutbox(ctx, intents); err != nil {
		return err
	}
	o.bus.outbox = append(o.bus.outbox, intents...)
	return nil
}
func (d proposedEffectRouteProofDispatcher) DispatchPostCommit(_ context.Context, intents []runtimeengine.EmitIntent) error {
	d.bus.dispatched = append(d.bus.dispatched, intents...)
	return nil
}

func (s *proposedEffectProofCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	s.gets.Add(1)
	if key != "provider_token" {
		return "", false, nil
	}
	return s.value, true, nil
}

func (*proposedEffectProofCredentialStore) Set(context.Context, string, string) error {
	return errors.New("proof credential store is read-only")
}

func (*proposedEffectProofCredentialStore) List(context.Context) ([]string, error) {
	return []string{"provider_token"}, nil
}

func (*proposedEffectProofCredentialStore) Delete(context.Context, string) error {
	return errors.New("proof credential store is read-only")
}

func recoveryStageAnchor(t *testing.T, card decisioncard.Card) decisioncard.StageGateAnchor {
	t.Helper()
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		t.Fatal(err)
	}
	return anchor
}

type gateRecoveryFairnessInterceptor struct {
	deferred map[string]struct{}
}

type gateRecoveryPoisonInterceptor struct {
	poisonEventID string
}

type gateRecoveryCountingInterceptor struct {
	delegate runtimebus.EventInterceptor
	calls    atomic.Int32
}

func (i *gateRecoveryCountingInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.Type() == events.EventType("mailbox.card_decided") {
		i.calls.Add(1)
	}
	return i.delegate.Intercept(ctx, evt)
}

type failOncePostgresNormalRunConvergence struct {
	*store.PostgresStore
	calls atomic.Int32
}

func (s *failOncePostgresNormalRunConvergence) ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error {
	if s.calls.Add(1) == 1 {
		return errors.New("planted normal run convergence failure")
	}
	return s.PostgresStore.ConvergeNormalRunCompletion(ctx, eventID, workflowTerminalStates, flowTerminalStates)
}

type failOnceSQLiteNormalRunConvergence struct {
	*store.SQLiteRuntimeStore
	calls atomic.Int32
}

func (s *failOnceSQLiteNormalRunConvergence) ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error {
	if s.calls.Add(1) == 1 {
		return errors.New("planted normal run convergence failure")
	}
	return s.SQLiteRuntimeStore.ConvergeNormalRunCompletion(ctx, eventID, workflowTerminalStates, flowTerminalStates)
}

type selectivePostgresNormalRunConvergenceFailure struct {
	*store.PostgresStore
	eventID string
	calls   atomic.Int32
}

func (s *selectivePostgresNormalRunConvergenceFailure) ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error {
	if eventID == s.eventID {
		s.calls.Add(1)
		return errors.New("planted persistent normal run convergence failure")
	}
	return s.PostgresStore.ConvergeNormalRunCompletion(ctx, eventID, workflowTerminalStates, flowTerminalStates)
}

type selectiveSQLiteNormalRunConvergenceFailure struct {
	*store.SQLiteRuntimeStore
	eventID string
	calls   atomic.Int32
}

func (s *selectiveSQLiteNormalRunConvergenceFailure) ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error {
	if eventID == s.eventID {
		s.calls.Add(1)
		return errors.New("planted persistent normal run convergence failure")
	}
	return s.SQLiteRuntimeStore.ConvergeNormalRunCompletion(ctx, eventID, workflowTerminalStates, flowTerminalStates)
}

func (i gateRecoveryPoisonInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if evt.ID() != i.poisonEventID {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return false, nil, runtimepipelineobligation.Continue(), runtimefailures.New(runtimefailures.ClassSchemaInvalid, "decision_route_fixture_invalid", "test", "poison_route", nil)
}

func (i gateRecoveryFairnessInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if _, ok := i.deferred[evt.ID()]; !ok {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	failure := runtimefailures.Normalize(
		runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "decision_card_bundle_unavailable", "test", "fairness", nil),
		"test",
		"fairness",
	)
	return true, nil, runtimepipelineobligation.DeferExecution(
		"decision_card_bundle_unavailable",
		time.Now().UTC().Add(runtimepipelineobligation.DecisionRouteRetryDelay),
		&failure,
	), nil
}

func gateRecoveryDeferred(outcome runtimepipelineobligation.ExecutionOutcome) bool {
	disposition, ok := outcome.Disposition()
	return ok && disposition.Kind() == runtimepipelineobligation.DispositionDeferred
}

func TestWorkflowGateUnavailablePinRecoversThroughPersistedEventBusOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testWorkflowGateUnavailablePinRecovery(t, tc.open(t))
		})
	}
}

func TestApprovedActivityHoldsThenDispatchesExactFrozenInputOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			const providerSecret = "provider-secret-not-in-effect"
			credentials := &proposedEffectProofCredentialStore{value: providerSecret}
			var calls atomic.Int32
			body := make(chan map[string]any, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if got := r.Header.Get("Authorization"); got != "Bearer "+providerSecret {
					t.Errorf("provider authorization = %q", got)
				}
				var got map[string]any
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Errorf("decode provider body: %v", err)
				} else {
					body <- got
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "provider-1"})
			}))
			defer server.Close()

			bundle := proposedEffectProofBundle(server.URL)
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source}, "support.reply_drafted")
			if err != nil {
				t.Fatal(err)
			}
			module := proposedEffectProofModule{
				source:   source,
				workflow: runtimepipeline.NewWorkflowDefinition("support", []runtimepipeline.WorkflowStage{{Name: "drafting"}}, nil),
				nodes: []runtimepipeline.WorkflowNode{{
					ID: "support", Subscriptions: []events.EventType{"support.reply_drafted", "send_support_reply.revision_requested", "send_support_reply.rejected", "platform.activity_requested"},
					Produces:      []events.EventType{"send_support_reply.succeeded", "send_support_reply.failed", "send_support_reply.revision_requested", "send_support_reply.rejected"},
					ExecutionType: runtimecontracts.SystemNodeExecutionType,
					Policies: map[string]runtimepipeline.WorkflowEventPolicy{
						"support.reply_drafted":       {Consume: true, RequireEntity: true},
						"platform.activity_requested": {Consume: true, RequireEntity: true},
					},
				}},
			}
			newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
				return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, WorkflowStore: selected.workflowStore, DecisionCards: selected.cards, BundleHash: bundleHash,
					Credentials: credentials,
				})
			}
			coordinator := newCoordinator(gateRecoveryBundle)
			bus.SetInterceptors(coordinator)

			runID, entityID := uuid.NewString(), uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "support", WorkflowVersion: "1", CurrentState: "drafting",
				Metadata: map[string]any{"entity_id": entityID, "run_id": runID, "flow_path": "root", "instance_id": entityID},
			}); err != nil {
				t.Fatal(err)
			}
			const replyContextID = "reply-context-proposed-effect"
			sourceEvent := eventtest.ForDelivery(eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("support.reply_drafted"), "support-agent", "task-1",
				[]byte(`{"chat_id":"support-room","text":"Exact frozen reply"}`), 0, runID, "",
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()),
				events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}},
			)
			sourceRoute := seedProposedEffectProofDelivery(t, selected, sourceEvent, "support")
			sourceCtx := events.WithDeliveryContext(ctx, sourceEvent.DeliveryContext())
			sourceCtx = runtimedelivery.WithRoute(sourceCtx, sourceRoute)
			forward, _, _, err := coordinator.Intercept(sourceCtx, sourceEvent)
			if err != nil {
				t.Fatalf("execute proposal source: %v", err)
			}
			if forward {
				t.Fatalf("proposal source was not consumed by its workflow node: type=%s entity=%q nodes=%#v target=%#v", sourceEvent.Type(), sourceEvent.EntityID(), coordinator.WorkflowNodes(), sourceEvent.TargetRoute())
			}
			waitForGateRecoveryQuiescence(t, bus, ctx)
			if got := calls.Load(); got != 0 {
				t.Fatalf("provider calls while proposal pending = %d, want 0", got)
			}
			if got := credentials.gets.Load(); got != 0 {
				t.Fatalf("credential resolutions while proposal pending = %d, want 0", got)
			}
			items, _, err := selected.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, AnchorKind: string(decisioncard.AnchorKindProposedEffect), Limit: 10})
			if err != nil || len(items) != 1 {
				t.Fatalf("pending proposed-effect cards = %#v, %v; handler failure: %s", items, err, proposedEffectProofFailure(t, selected, sourceEvent.ID()))
			}
			card, err := selected.cards.GetDecisionCard(ctx, items[0].CardID)
			if err != nil {
				t.Fatal(err)
			}
			assertProposedEffectProofCounts(t, selected, runID, 0, 0)
			input, ok := card.Snapshot.Context.Lookup("input")
			if !ok || input.Kind() == 0 {
				t.Fatalf("frozen effect input missing from card: %#v", card.Snapshot.Context)
			}
			continuation, err := selected.cards.(decisioncard.ProposedEffectStore).LoadProposedEffectContinuation(ctx, card.CardID)
			if err != nil {
				t.Fatal(err)
			}
			if continuation.ReplyContextID != replyContextID {
				t.Fatalf("proposed-effect reply context = %q, want %q", continuation.ReplyContextID, replyContextID)
			}
			effect, err := continuation.EffectValue()
			if err != nil {
				t.Fatal(err)
			}
			rawEffect, err := canonicaljson.Encode(effect)
			if err != nil {
				t.Fatal(err)
			}
			rawSnapshot, err := decisioncard.SnapshotJSON(card)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(rawEffect), providerSecret) || strings.Contains(string(rawSnapshot), providerSecret) {
				t.Fatal("provider credential leaked into the immutable effect or decision snapshot")
			}
			decisionEventID := uuid.NewString()
			if _, err := selected.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator",
				ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("approve proposed effect: %v", err)
			}
			decisionPayload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
			decisionEvent := eventtest.RuntimeControl(decisionEventID, events.EventType("mailbox.card_decided"), "platform", "", decisionPayload, 0, runID, "",
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "root"), time.Now().UTC())
			storetest.CommitSemanticEvent(t, ctx, selected.events, decisionEvent)
			forward, emitted, outcome, err := newCoordinator(otherGateBundle).Intercept(ctx, decisionEvent)
			if err != nil || !gateRecoveryDeferred(outcome) {
				t.Fatalf("route approval under changed bundle = forward:%v emitted:%d outcome:%#v error:%v, want recoverable deferral", forward, len(emitted), outcome, err)
			}
			assertProposedEffectProofCounts(t, selected, runID, 0, 0)
			if got := calls.Load(); got != 0 {
				t.Fatalf("provider calls under changed bundle = %d, want 0", got)
			}
			if got := credentials.gets.Load(); got != 0 {
				t.Fatalf("credential resolutions under changed bundle = %d, want 0", got)
			}
			deferred, err := selected.cards.(decisioncard.ProposedEffectStore).LoadProposedEffectContinuation(ctx, card.CardID)
			if err != nil || deferred.State != decisioncard.ProposedEffectDecisionCommitted || deferred.RouteEventID != "" {
				t.Fatalf("bundle-deferred proposed effect = %#v, %v", deferred, err)
			}
			bus.SetInterceptors()
			forward, emitted, _, err = coordinator.Intercept(ctx, decisionEvent)
			if err != nil {
				t.Fatalf("route approval decision: %v; continuation route=%s/%s/%s", err, deferred.FlowID, deferred.FlowInstance, deferred.EntityID)
			}
			if forward {
				t.Fatal("approval decision was not consumed by the proposed-effect authority")
			}
			releasedRequest := loadProposedEffectProofRequest(t, selected, runID)
			changedCoordinator := newCoordinator(otherGateBundle)
			if changedForward, changedEmitted, changedOutcome, changedErr := changedCoordinator.Intercept(ctx, releasedRequest); changedErr != nil || !gateRecoveryDeferred(changedOutcome) {
				t.Fatalf("consume released request under changed bundle = forward:%v emitted:%d outcome:%#v error:%v, want recoverable deferral", changedForward, len(changedEmitted), changedOutcome, changedErr)
			}
			if got := calls.Load(); got != 0 {
				t.Fatalf("provider calls while released request pin unavailable = %d, want 0", got)
			}
			bus.SetInterceptors(coordinator)
			if consumed, _, _, consumeErr := coordinator.Intercept(ctx, releasedRequest); consumeErr != nil || consumed {
				t.Fatalf("consume released activity request under pinned bundle = forward:%v error:%v", consumed, consumeErr)
			}
			waitForGateRecoveryQuiescence(t, bus, ctx)
			assertProposedEffectProofCounts(t, selected, runID, 1, 1)
			if got := calls.Load(); got != 1 {
				t.Fatalf("provider calls after approval = %d, want 1", got)
			}
			if got := credentials.gets.Load(); got != 1 {
				t.Fatalf("credential resolutions after approval = %d, want 1", got)
			}
			select {
			case got := <-body:
				if got["chat_id"] != "support-room" || got["text"] != "Exact frozen reply" {
					t.Fatalf("provider input = %#v", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("provider body was not observed")
			}
			readback, err := selected.cards.(decisioncard.ProposedEffectStore).ProposedEffectReadback(ctx, card.CardID)
			if err != nil || readback.DispatchState != "succeeded" {
				t.Fatalf("proposed-effect readback = %#v, %v", readback, err)
			}
			coordinator = newCoordinator(gateRecoveryBundle)
			bus.SetInterceptors(coordinator)
			released := loadProposedEffectProofRequest(t, selected, runID)
			forward, _, _, err = coordinator.Intercept(ctx, released)
			if err != nil {
				t.Fatalf("replay persisted approved request: %v", err)
			}
			if forward {
				t.Fatal("persisted approved request replay was not consumed")
			}
			waitForGateRecoveryQuiescence(t, bus, ctx)
			assertProposedEffectProofCounts(t, selected, runID, 1, 1)
			if got := calls.Load(); got != 1 {
				t.Fatalf("provider calls after persisted replay = %d, want 1", got)
			}
			if _, _, _, err := changedCoordinator.Intercept(ctx, decisionEvent); err != nil {
				t.Fatalf("approval route replay after commit acknowledgment loss under changed bundle: %v", err)
			}
			waitForGateRecoveryQuiescence(t, bus, ctx)
			if got := calls.Load(); got != 1 {
				t.Fatalf("provider calls after duplicate approval = %d, want 1", got)
			}

			routeWithoutDispatch := func(verdict, wantEvent string, fields map[string]any) {
				t.Helper()
				proposal := eventtest.ForDelivery(eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("support.reply_drafted"), "support-agent", "task-1",
					[]byte(`{"chat_id":"support-room","text":"Needs another operator outcome"}`), 0, runID, "",
					events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()),
					events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}},
				)
				proposalRoute := seedProposedEffectProofDelivery(t, selected, proposal, "support")
				proposalCtx := events.WithDeliveryContext(ctx, proposal.DeliveryContext())
				proposalCtx = runtimedelivery.WithRoute(proposalCtx, proposalRoute)
				consumed, _, _, routeErr := coordinator.Intercept(proposalCtx, proposal)
				if routeErr != nil || consumed {
					t.Fatalf("create %s proposal = forward:%v error:%v", verdict, consumed, routeErr)
				}
				waitForGateRecoveryQuiescence(t, bus, ctx)
				pending, _, routeErr := selected.cards.ListDecisionCards(ctx, decisioncard.ListOptions{
					RunID: runID, Status: decisioncard.StatusPending, AnchorKind: string(decisioncard.AnchorKindProposedEffect), Limit: 10,
				})
				if routeErr != nil || len(pending) != 1 {
					t.Fatalf("pending %s proposal = %#v, %v", verdict, pending, routeErr)
				}
				pendingCard, routeErr := selected.cards.GetDecisionCard(ctx, pending[0].CardID)
				if routeErr != nil {
					t.Fatal(routeErr)
				}
				pendingContinuation, routeErr := selected.cards.(decisioncard.ProposedEffectStore).LoadProposedEffectContinuation(ctx, pendingCard.CardID)
				if routeErr != nil || pendingContinuation.ReplyContextID != replyContextID {
					t.Fatalf("%s proposed-effect reply context = %q, %v; want %q", verdict, pendingContinuation.ReplyContextID, routeErr, replyContextID)
				}
				admittedFields, routeErr := canonicaljson.FromGo(fields)
				if routeErr != nil {
					t.Fatal(routeErr)
				}
				decisionID := uuid.NewString()
				if _, routeErr = selected.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
					CardID: pendingCard.CardID, Verdict: verdict, Fields: admittedFields, ActorTokenID: "operator",
					ObservedContentHash: pendingCard.CardContentHash, DecisionEventID: decisionID, Now: time.Now().UTC(),
				}); routeErr != nil {
					t.Fatalf("decide %s: %v", verdict, routeErr)
				}
				payload, _ := json.Marshal(map[string]any{"card_id": pendingCard.CardID})
				decision := eventtest.RuntimeControl(decisionID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "root"), time.Now().UTC())
				storetest.CommitSemanticEvent(t, ctx, selected.events, decision)
				consumed, _, _, routeErr = coordinator.Intercept(ctx, decision)
				if routeErr != nil || consumed {
					t.Fatalf("route %s = forward:%v error:%v", verdict, consumed, routeErr)
				}
				waitForGateRecoveryQuiescence(t, bus, ctx)
				assertProposedEffectProofCounts(t, selected, runID, 1, 1)
				assertProposedEffectOutcomeCount(t, selected, runID, wantEvent, 1)
				if _, _, _, routeErr = newCoordinator(otherGateBundle).Intercept(ctx, decision); routeErr != nil {
					t.Fatalf("%s route replay after commit acknowledgment loss under changed bundle: %v", verdict, routeErr)
				}
				if got := calls.Load(); got != 1 {
					t.Fatalf("provider calls after %s = %d, want 1", verdict, got)
				}
				readback, routeErr := selected.cards.(decisioncard.ProposedEffectStore).ProposedEffectReadback(ctx, pendingCard.CardID)
				if routeErr != nil || readback.DispatchState != "not_dispatched" {
					t.Fatalf("%s readback = %#v, %v", verdict, readback, routeErr)
				}
			}
			routeWithoutDispatch("revise", "send_support_reply.revision_requested", map[string]any{"feedback": "Please rewrite it."})
			routeWithoutDispatch("reject", "send_support_reply.rejected", map[string]any{"reason": "Do not send."})
		})
	}
}

func TestProposedEffectCompletedRouteReplaysBeforeBundleFenceAndPreservesReplyContextOnBothStores(t *testing.T) {
	for _, storeCase := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, verdict := range []string{"approve", "revise", "reject"} {
			t.Run(storeCase.name+"/"+verdict, func(t *testing.T) {
				selected := storeCase.open(t)
				ctx := testAuthorActivityContext(t, context.Background())
				runID, entityID := uuid.NewString(), uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				now := time.Date(2026, 7, 14, 22, 0, 0, 0, time.UTC)
				input, err := canonicaljson.FromGo(map[string]any{"chat_id": "support-room", "text": "Exact approved text"})
				if err != nil {
					t.Fatal(err)
				}
				sourceEventID := uuid.NewString()
				sourceEvent := eventtest.RunCreatingRootIngress(sourceEventID, events.EventType("support.reply_drafted"), "support-agent", "task-1",
					[]byte(`{"chat_id":"support-room","text":"Exact approved text"}`), 0, runID, "",
					events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), now)
				storetest.CommitSemanticEvent(t, ctx, selected.events, sourceEvent)
				requestEventID := activityidentity.RequestEventID(activityidentity.Fact{
					RunID: runID, SourceEventID: sourceEventID, EntityID: entityID, FlowID: "support", NodeID: "support",
					HandlerEventKey: "support.reply_drafted", ActivityID: "send_support_reply", Tool: "provider_write", Attempt: 1,
				})
				continuation := decisioncard.ProposedEffectContinuation{
					CardID: decisioncard.ProposedEffectCardID(requestEventID, "support_reply"), RunID: runID,
					RequestEventID: requestEventID, ActivityID: "send_support_reply", Tool: "provider_write",
					BundleHash: gateRecoveryBundle, WorkflowVersion: "1", Input: input,
					EffectClass:  runtimecontracts.ActivityEffectClassNonIdempotentWrite,
					SuccessEvent: "send_support_reply.succeeded", FailureEvent: "send_support_reply.failed",
					RevisionEvent: "send_support_reply.revision_requested", RejectedEvent: "send_support_reply.rejected",
					RetryMaxAttempts: 1, ForkPolicy: runtimecontracts.ActivityForkRequireConfirmation,
					EntityID: entityID, NodeID: "support", FlowID: "support", FlowInstance: "root", HandlerEventKey: "support.reply_drafted",
					SourceEventID: sourceEventID, SourceRunID: runID, SourceTaskID: "task-1",
					ExecutionMode: executionmode.Live, ReplyContextID: "reply-context-route-proof", State: decisioncard.ProposedEffectPending,
					CreatedAt: now, UpdatedAt: now,
				}.Canonical()
				effect, err := continuation.EffectValue()
				if err != nil {
					t.Fatal(err)
				}
				continuation.EffectContentHash, err = canonicaljson.HashValue(effect)
				if err != nil {
					t.Fatal(err)
				}
				anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
					RequestEventID: requestEventID, ActivityID: continuation.ActivityID, Decision: "support_reply",
					Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: "root", EntityID: entityID},
				})
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := decisioncard.FreezeSnapshot("support_reply", "", map[string]any{"input": input.Interface()}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {Verdict: "approve"},
					"revise":  {Verdict: "revise", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
					"reject":  {Verdict: "reject", Input: map[string]runtimecontracts.WorkflowGateInputField{"reason": {Type: "text"}}},
				})
				if err != nil {
					t.Fatal(err)
				}
				card, err := decisioncard.New(decisioncard.Card{
					CardID: continuation.CardID, RunID: runID, Anchor: anchor, Snapshot: snapshot,
					ExecutionMode:     "live",
					EffectContentHash: continuation.EffectContentHash, BundleHash: gateRecoveryBundle,
					WorkflowVersion: "1", CreatedAt: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				proposedStore := selected.cards.(decisioncard.ProposedEffectStore)
				if err := proposedStore.CreateProposedEffectCard(ctx, card, continuation); err != nil {
					t.Fatal(err)
				}
				fields := semanticvalue.EmptyObject()
				if verdict == "revise" {
					fields, _ = canonicaljson.FromGo(map[string]any{"feedback": "Please revise."})
				} else if verdict == "reject" {
					fields, _ = canonicaljson.FromGo(map[string]any{"reason": "Do not send."})
				}
				decisionEventID := uuid.NewString()
				if _, err := selected.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
					CardID: card.CardID, Verdict: verdict, Fields: fields, ActorTokenID: "operator",
					ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: now.Add(time.Minute),
				}); err != nil {
					t.Fatal(err)
				}
				payload, _ := canonicaljson.Bytes(map[string]any{"card_id": card.CardID})
				decisionEvent := eventtest.RuntimeControl(decisionEventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "root"), now.Add(time.Minute))
				storetest.CommitSemanticEvent(t, ctx, selected.events, decisionEvent)
				source := semanticview.Wrap(proposedEffectProofBundle("http://127.0.0.1:1"))
				canonicalBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source},
					"platform.activity_requested", "send_support_reply.revision_requested", "send_support_reply.rejected")
				if err != nil {
					t.Fatal(err)
				}
				bus := &proposedEffectRouteProofBus{eventBus: canonicalBus}
				coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
					Module: gateRecoveryModule{source: source}, WorkflowStore: selected.workflowStore,
					DecisionCards: selected.cards, BundleHash: gateRecoveryBundle,
				})
				forward, emitted, _, err := coordinator.Intercept(ctx, decisionEvent)
				if err != nil || forward || len(emitted) != 0 {
					t.Fatalf("route %s = forward:%v emitted:%d error:%v", verdict, forward, len(emitted), err)
				}
				stored, err := proposedStore.LoadProposedEffectContinuation(ctx, card.CardID)
				if err != nil || stored.RouteEventID != decisionEventID {
					t.Fatalf("routed continuation = %#v, %v", stored, err)
				}
				if verdict == "approve" {
					if len(bus.outbox) != 1 || len(bus.dispatched) != 1 {
						t.Fatalf("approve route intents = outbox:%d dispatched:%d, want 1/1", len(bus.outbox), len(bus.dispatched))
					}
					request, err := canonicaljson.Decode(bus.outbox[0].Event.Payload())
					bundleValue, bundlePresent := request.Lookup("bundle_hash")
					bundleHash, bundleText := bundleValue.String()
					versionValue, versionPresent := request.Lookup("workflow_version")
					workflowVersion, versionText := versionValue.String()
					if err != nil || !bundlePresent || !bundleText || bundleHash != gateRecoveryBundle || !versionPresent || !versionText || workflowVersion != "1" {
						t.Fatalf("released request contract pin = bundle:%q/%v/%v version:%q/%v/%v error:%v", bundleHash, bundlePresent, bundleText, workflowVersion, versionPresent, versionText, err)
					}
				} else {
					if len(bus.published) != 1 || len(bus.publishContexts) != 1 {
						t.Fatalf("%s route publications = events:%d contexts:%d, want 1/1", verdict, len(bus.published), len(bus.publishContexts))
					}
					if gotEvent, gotContext := bus.published[0].DeliveryContext().ReplyContextID(), bus.publishContexts[0].ReplyContextID(); gotEvent != continuation.ReplyContextID || gotContext != continuation.ReplyContextID {
						t.Fatalf("%s reply authority = event:%q context:%q, want %q", verdict, gotEvent, gotContext, continuation.ReplyContextID)
					}
				}
				beforeOutbox, beforePublished := len(bus.outbox), len(bus.published)
				changed := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
					Module: gateRecoveryModule{source: source}, WorkflowStore: selected.workflowStore,
					DecisionCards: selected.cards, BundleHash: otherGateBundle,
				})
				if _, _, _, err := changed.Intercept(ctx, decisionEvent); err != nil {
					t.Fatalf("%s terminal route replay under changed bundle: %v", verdict, err)
				}
				if len(bus.outbox) != beforeOutbox || len(bus.published) != beforePublished {
					t.Fatalf("%s terminal route replay duplicated work", verdict)
				}
			})
		}
	}
}

func TestApprovedActivityProposalCreationRollsBackWorkflowCardAndContinuationOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			bundle := proposedEffectProofBundle("http://127.0.0.1:1")
			handler := bundle.Nodes["support"].EventHandlers["support.reply_drafted"]
			handler.AdvancesTo = "queued"
			node := bundle.Nodes["support"]
			node.EventHandlers["support.reply_drafted"] = handler
			bundle.Nodes["support"] = node
			bundle.Semantics.NodeHandlers["support"]["support.reply_drafted"] = handler

			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source}, "support.reply_drafted")
			if err != nil {
				t.Fatal(err)
			}
			module := proposedEffectProofModule{
				source:   source,
				workflow: runtimepipeline.NewWorkflowDefinition("support", []runtimepipeline.WorkflowStage{{Name: "drafting"}, {Name: "queued"}}, nil),
				nodes: []runtimepipeline.WorkflowNode{{
					ID: "support", Subscriptions: []events.EventType{"support.reply_drafted"},
					Produces:      []events.EventType{"send_support_reply.succeeded", "send_support_reply.failed", "send_support_reply.revision_requested", "send_support_reply.rejected"},
					ExecutionType: runtimecontracts.SystemNodeExecutionType,
					Policies:      map[string]runtimepipeline.WorkflowEventPolicy{"support.reply_drafted": {Consume: true, RequireEntity: true}},
				}},
			}
			coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, WorkflowStore: selected.workflowStore, DecisionCards: selected.cards, BundleHash: gateRecoveryBundle,
			})
			bus.SetInterceptors(coordinator)

			runID, entityID := uuid.NewString(), uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, WorkflowName: "support", WorkflowVersion: "1", CurrentState: "drafting",
				Metadata: map[string]any{"entity_id": entityID, "run_id": runID, "flow_path": "root", "instance_id": entityID},
			}); err != nil {
				t.Fatal(err)
			}
			installProposedEffectCreateFailure(t, selected)

			event := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType("support.reply_drafted"), "support-agent", "task-rollback",
				[]byte(`{"chat_id":"support-room","text":"must roll back"}`), 0, runID, "",
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			route := seedProposedEffectProofDelivery(t, selected, event, "support")
			forward, _, _, err := coordinator.Intercept(runtimedelivery.WithRoute(ctx, route), event)
			if err != nil || forward {
				t.Fatalf("proposal failure interception = forward:%v error:%v", forward, err)
			}

			instance, ok, err := selected.workflowStore.Load(ctx, entityID)
			if err != nil || !ok || instance.CurrentState != "drafting" {
				t.Fatalf("workflow after rollback = %#v, %v, %v", instance, ok, err)
			}
			items, _, err := selected.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
			if err != nil || len(items) != 0 {
				t.Fatalf("decision cards after rollback = %#v, %v", items, err)
			}
			assertProposedEffectProofCounts(t, selected, runID, 0, 0)
			var continuations int
			query := `SELECT COUNT(*) FROM proposed_effect_continuations WHERE run_id = ?`
			if selected.postgres {
				query = `SELECT COUNT(*) FROM proposed_effect_continuations WHERE run_id = $1::uuid`
			}
			if err := selected.db.QueryRowContext(ctx, query, runID).Scan(&continuations); err != nil || continuations != 0 {
				t.Fatalf("proposed-effect continuations after rollback = %d, %v", continuations, err)
			}
			deliveryQuery := `SELECT status FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = 'support'`
			if selected.postgres {
				deliveryQuery = `SELECT status FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = 'support'`
			}
			var deliveryStatus string
			if err := selected.db.QueryRowContext(ctx, deliveryQuery, event.ID()).Scan(&deliveryStatus); err != nil || deliveryStatus != "dead_letter" {
				t.Fatalf("planted persistence failure delivery status = %q, %v", deliveryStatus, err)
			}
		})
	}
}

func installProposedEffectCreateFailure(t *testing.T, selected gateRecoveryStoreCase) {
	t.Helper()
	statement := `CREATE TRIGGER fail_proposed_effect_create BEFORE INSERT ON proposed_effect_continuations BEGIN SELECT RAISE(ABORT, 'injected proposed-effect persistence failure'); END`
	if selected.postgres {
		statement = `
			CREATE FUNCTION fail_proposed_effect_create_fn() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'injected proposed-effect persistence failure'; END;
			$$ LANGUAGE plpgsql;
			CREATE TRIGGER fail_proposed_effect_create BEFORE INSERT ON proposed_effect_continuations
			FOR EACH ROW EXECUTE FUNCTION fail_proposed_effect_create_fn()`
	}
	if _, err := selected.db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowGateStartupRecoverySettlesTerminalNoEmitOutcomeOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{
		{name: "sqlite", open: openSQLiteGateRecoveryStore},
		{name: "postgres", open: openPostgresGateRecoveryStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testWorkflowGateStartupTerminalRecovery(t, tc.open(t))
		})
	}
}

func TestDecisionRouteObligationFairnessAdmitsNewWorkBehindFullDeferredPageOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			ctx := testAuthorActivityContext(t, context.Background())
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			deferred := map[string]struct{}{}
			oldAt := time.Now().UTC().Add(-25 * time.Hour)
			for i := 0; i < 200; i++ {
				eventID := seedGateRecoveryRouteObligation(t, selected, runID, oldAt.Add(time.Duration(i)*time.Millisecond))
				deferred[eventID] = struct{}{}
				setGateRecoveryRouteAttempt(t, selected, eventID, 1)
			}
			newEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{Interceptors: []runtimebus.EventInterceptor{gateRecoveryFairnessInterceptor{deferred: deferred}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := bus.SweepUndispatched(ctx, 200); err != nil {
				t.Fatal(err)
			}
			assertGateRecoveryProcessedReceipt(t, selected, newEventID)
			if got := gateRecoveryPipelineReceiptCount(t, selected, firstGateRecoveryEventID(deferred)); got != 0 {
				t.Fatalf("deferred retry receipt count = %d, want 0", got)
			}
		})
	}
}

func TestDecisionRouteObligationQuarantinesPoisonAndContinuesOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			poisonEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC().Add(-time.Minute))
			validEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				Interceptors: []runtimebus.EventInterceptor{gateRecoveryPoisonInterceptor{poisonEventID: poisonEventID}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := bus.SweepUndispatched(testAuthorActivityContext(t, context.Background()), 10); err != nil || got != 2 {
				t.Fatalf("poison route sweep recovered = %d, %v; want 2 handled obligations, nil", got, err)
			}
			assertGateRecoveryObligationStatus(t, selected, poisonEventID, "quarantined")
			assertGateRecoveryErrorReceipt(t, selected, poisonEventID, "event_interceptor_failed")
			assertGateRecoveryProcessedReceipt(t, selected, validEventID)
			if got, err := bus.SweepUndispatched(testAuthorActivityContext(t, context.Background()), 10); err != nil || got != 0 {
				t.Fatalf("second poison route sweep recovered = %d, %v; want 0, nil", got, err)
			}
		})
	}
}

func TestDecisionRouteStartupRecoveryQuarantinesPoisonAndContinuesOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			poisonEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC().Add(-time.Minute))
			validEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				Interceptors: []runtimebus.EventInterceptor{gateRecoveryPoisonInterceptor{poisonEventID: poisonEventID}},
			})
			if err != nil {
				t.Fatal(err)
			}
			recovery := runtimepipeline.NewRecoveryManagerWith(bus)
			if err := recovery.Recover(testAuthorActivityContext(t, context.Background())); err != nil {
				t.Fatalf("startup poison route recovery: %v", err)
			}
			assertGateRecoveryObligationStatus(t, selected, poisonEventID, "quarantined")
			assertGateRecoveryErrorReceipt(t, selected, poisonEventID, "event_interceptor_failed")
			assertGateRecoveryProcessedReceipt(t, selected, validEventID)
			if err := recovery.Recover(testAuthorActivityContext(t, context.Background())); err != nil {
				t.Fatalf("second startup poison route recovery: %v", err)
			}
		})
	}
}

func TestDecisionRouteSettlementRetriesConvergenceWithoutReroutingOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, recovery := range []string{"periodic", "startup"} {
			t.Run(tc.name+"/"+recovery, func(t *testing.T) {
				testDecisionRouteSettlementRetry(t, tc.open(t), recovery)
			})
		}
	}
}

func TestDecisionRouteSettlementFailureDefersAndDoesNotStarveOnBothStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, form := range []string{"synchronous", "acknowledged"} {
			for _, recovery := range []string{"periodic", "startup"} {
				t.Run(tc.name+"/"+form+"/"+recovery, func(t *testing.T) {
					testDecisionRouteSettlementFailureFairness(t, tc.open(t), form, recovery)
				})
			}
		}
	}
}

func testDecisionRouteSettlementFailureFairness(t *testing.T, selected gateRecoveryStoreCase, form, recovery string) {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	insertGateRecoveryRun(t, selected, runID)
	failing := seedGateRecoveryForegroundRoute(t, selected, runID, time.Now().UTC().Add(-2*time.Minute))
	valid := seedGateRecoveryForegroundRoute(t, selected, runID, time.Now().UTC().Add(-time.Minute))

	var failingStore runtimebus.EventStore
	var convergenceCalls *atomic.Int32
	if selected.postgres {
		wrapped := &selectivePostgresNormalRunConvergenceFailure{PostgresStore: selected.events.(*store.PostgresStore), eventID: failing.event.ID()}
		failingStore = wrapped
		convergenceCalls = &wrapped.calls
	} else {
		wrapped := &selectiveSQLiteNormalRunConvergenceFailure{SQLiteRuntimeStore: selected.events.(*store.SQLiteRuntimeStore), eventID: failing.event.ID()}
		failingStore = wrapped
		convergenceCalls = &wrapped.calls
	}
	bundle := gateRecoveryContractBundle()
	bus, err := newScopedTestEventBus(t, failingStore, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
		Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, WorkflowStore: selected.workflowStore,
		DecisionCards: selected.cards, BundleHash: gateRecoveryBundle,
	})
	counting := &gateRecoveryCountingInterceptor{delegate: coordinator}
	bus.SetInterceptors(counting)

	switch form {
	case "synchronous":
		if err := bus.Publish(ctx, failing.event); err != nil {
			t.Fatalf("synchronous failing settlement: %v", err)
		}
	case "acknowledged":
		if err := bus.PublishAcknowledged(ctx, failing.event); err != nil {
			t.Fatalf("acknowledged failing settlement: %v", err)
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for %s failing settlement: %v", form, err)
	}
	assertGateRecoveryProcessedReceipt(t, selected, failing.event.ID())
	assertGateRecoveryObligationStatus(t, selected, failing.event.ID(), "pending")
	if got := counting.calls.Load(); got != 1 {
		t.Fatalf("foreground route calls = %d, want 1", got)
	}

	persistGateRecoveryRouteEvent(t, selected, valid.event)
	setGateRecoveryRouteAttempt(t, selected, failing.event.ID(), 0)
	setGateRecoveryRouteAttempt(t, selected, valid.event.ID(), 0)
	makeGateRecoveryRouteDue(t, selected, failing.event.ID(), time.Now().UTC().Add(-2*time.Second))
	makeGateRecoveryRouteDue(t, selected, valid.event.ID(), time.Now().UTC().Add(-time.Second))

	switch recovery {
	case "periodic":
		if _, err := bus.SweepUndispatched(ctx, 10); err != nil {
			t.Fatalf("periodic settlement fairness: %v", err)
		}
	case "startup":
		if err := runtimepipeline.NewRecoveryManagerWith(bus).Recover(ctx); err != nil {
			t.Fatalf("startup settlement fairness: %v", err)
		}
	}

	assertGateRecoveryProcessedReceipt(t, selected, failing.event.ID())
	assertGateRecoveryObligationStatus(t, selected, failing.event.ID(), "pending")
	assertGateRecoveryObligationAttempt(t, selected, failing.event.ID(), 1)
	assertGateRecoveryProcessedReceipt(t, selected, valid.event.ID())
	assertGateRecoveryObligationStatus(t, selected, valid.event.ID(), "completed")
	if got := convergenceCalls.Load(); got != 2 {
		t.Fatalf("failing route convergence calls after %s recovery = %d, want foreground plus one settlement attempt", recovery, got)
	}
	if got := counting.calls.Load(); got != 2 {
		t.Fatalf("route calls after %s recovery = %d, want failing route once plus valid route once", recovery, got)
	}
}

func testDecisionRouteSettlementRetry(t *testing.T, selected gateRecoveryStoreCase, recovery string) {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	runID, entityID := uuid.NewString(), uuid.NewString()
	insertGateRecoveryRun(t, selected, runID)
	ctx = withLiveGateExecution(runtimecorrelation.WithRunID(ctx, runID))
	bundle := gateRecoveryTerminalContractBundle()
	setupBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatal(err)
	}
	setupCoordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(setupBus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
		Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, WorkflowStore: selected.workflowStore,
		DecisionCards: selected.cards, BundleHash: gateRecoveryBundle,
	})
	at := time.Now().UTC().Add(-time.Minute)
	if err := selected.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "launch/settlement-" + uuid.NewString(), StorageRef: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: at,
		Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := selected.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return setupCoordinator.ArmFlowInstanceInitialStageLifecycle(txctx, entityID)
	}); err != nil {
		t.Fatal(err)
	}
	items, _, err := selected.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("settlement decision cards = %#v, %v", items, err)
	}
	card, err := selected.cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	if err := selected.workflowStore.CommitDecision(ctx, card, eventID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := selected.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash,
		DecisionEventID: eventID, Now: at,
	}); err != nil {
		t.Fatal(err)
	}

	var failingStore runtimebus.EventStore
	if selected.postgres {
		failingStore = &failOncePostgresNormalRunConvergence{PostgresStore: selected.events.(*store.PostgresStore)}
	} else {
		failingStore = &failOnceSQLiteNormalRunConvergence{SQLiteRuntimeStore: selected.events.(*store.SQLiteRuntimeStore)}
	}
	bus, err := newScopedTestEventBus(t, failingStore, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, selected.db, runtimepipeline.PipelineCoordinatorOptions{
		Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, WorkflowStore: selected.workflowStore,
		DecisionCards: selected.cards, BundleHash: gateRecoveryBundle,
	})
	counting := &gateRecoveryCountingInterceptor{delegate: coordinator}
	bus.SetInterceptors(counting)
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	evt := eventtest.RuntimeControl(eventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), recoveryStageAnchor(t, card).FlowInstance), at)
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("foreground decision route: %v", err)
	}
	if got := counting.calls.Load(); got != 1 {
		t.Fatalf("foreground route calls = %d, want 1", got)
	}
	assertGateRecoveryProcessedReceipt(t, selected, eventID)
	assertGateRecoveryObligationStatus(t, selected, eventID, "pending")

	makeGateRecoveryRouteDue(t, selected, eventID, time.Now().UTC().Add(-time.Second))
	switch recovery {
	case "periodic":
		if _, err := bus.SweepUndispatched(ctx, 10); err != nil {
			t.Fatalf("periodic settlement retry: %v", err)
		}
	case "startup":
		if err := runtimepipeline.NewRecoveryManagerWith(bus).Recover(ctx); err != nil {
			t.Fatalf("startup settlement retry: %v", err)
		}
	}
	if got := counting.calls.Load(); got != 1 {
		t.Fatalf("route calls after %s settlement = %d, want 1", recovery, got)
	}
	assertGateRecoveryObligationStatus(t, selected, eventID, "completed")
}

func TestDecisionRouteForegroundFailureQuarantinesOnBothStoresAndPublicationForms(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, form := range []string{"synchronous", "acknowledged"} {
			t.Run(tc.name+"/"+form, func(t *testing.T) {
				selected := tc.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				fixture := seedGateRecoveryForegroundRoute(t, selected, runID, time.Now().UTC())
				bundle := gateRecoveryContractBundle()
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
					ContractBundle: semanticview.Wrap(bundle),
					Interceptors:   []runtimebus.EventInterceptor{gateRecoveryPoisonInterceptor{poisonEventID: fixture.event.ID()}},
				})
				if err != nil {
					t.Fatal(err)
				}

				switch form {
				case "synchronous":
					if err := bus.Publish(testAuthorActivityContext(t, context.Background()), fixture.event); err == nil {
						t.Fatal("synchronous poison route publish succeeded, want interceptor failure")
					}
				case "acknowledged":
					if err := bus.PublishAcknowledged(testAuthorActivityContext(t, context.Background()), fixture.event); err != nil {
						t.Fatalf("acknowledged poison route publish: %v", err)
					}
					waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(t, context.Background()), 5*time.Second)
					defer cancel()
					if err := bus.WaitForQuiescence(waitCtx); err != nil {
						t.Fatalf("wait for acknowledged poison route: %v", err)
					}
				}

				assertGateRecoveryObligationStatus(t, selected, fixture.event.ID(), "quarantined")
				assertGateRecoveryErrorReceipt(t, selected, fixture.event.ID(), "event_interceptor_failed")
				assertGateRecoveryActivation(t, selected.workflowStore, runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), fixture.entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
				card, err := selected.cards.GetDecisionCard(testAuthorActivityContext(t, context.Background()), fixture.cardID)
				if err != nil {
					t.Fatalf("read quarantined decision card: %v", err)
				}
				if card.Status != decisioncard.StatusDecided || card.DecisionEventID != fixture.event.ID() {
					t.Fatalf("quarantined decision card = status:%q event:%q, want decided/%q", card.Status, card.DecisionEventID, fixture.event.ID())
				}

				validEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
				if _, err := bus.SweepUndispatched(testAuthorActivityContext(t, context.Background()), 10); err != nil {
					t.Fatalf("sweep unrelated route behind quarantined foreground failure: %v", err)
				}
				assertGateRecoveryProcessedReceipt(t, selected, validEventID)
				assertGateRecoveryObligationStatus(t, selected, validEventID, "completed")
				if got, err := bus.SweepUndispatched(testAuthorActivityContext(t, context.Background()), 10); err != nil || got != 0 {
					t.Fatalf("second foreground quarantine sweep recovered = %d, %v; want 0, nil", got, err)
				}
			})
		}
	}
}

type gateRecoveryForegroundFixture struct {
	event    events.Event
	entityID string
	cardID   string
}

func seedGateRecoveryForegroundRoute(t *testing.T, tc gateRecoveryStoreCase, runID string, at time.Time) gateRecoveryForegroundFixture {
	t.Helper()
	ctx := withLiveGateExecution(runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
	entityID := uuid.NewString()
	bundle := gateRecoveryContractBundle()
	setupBus, err := newScopedTestEventBus(t, tc.events, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(setupBus, tc.db, runtimepipeline.PipelineCoordinatorOptions{
		Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, WorkflowStore: tc.workflowStore,
		DecisionCards: tc.cards, BundleHash: gateRecoveryBundle,
	})
	if err := tc.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "launch/foreground-" + uuid.NewString(), StorageRef: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: at,
		Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return coordinator.ArmFlowInstanceInitialStageLifecycle(txctx, entityID)
	}); err != nil {
		t.Fatal(err)
	}
	items, _, err := tc.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
	if err != nil {
		t.Fatalf("list foreground decision cards: %v", err)
	}
	var cardID string
	for _, item := range items {
		if item.Scope.EntityID == entityID && item.Status == decisioncard.StatusPending {
			cardID = item.CardID
			break
		}
	}
	if cardID == "" {
		t.Fatalf("foreground pending decision card for entity %s missing from %#v", entityID, items)
	}
	card, err := tc.cards.GetDecisionCard(ctx, cardID)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	if err := tc.workflowStore.CommitDecision(ctx, card, eventID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash,
		DecisionEventID: eventID, Now: at,
	}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	evt := eventtest.RuntimeControl(eventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), recoveryStageAnchor(t, card).FlowInstance), at)
	return gateRecoveryForegroundFixture{event: evt, entityID: entityID, cardID: card.CardID}
}

func seedGateRecoveryRouteObligation(t *testing.T, tc gateRecoveryStoreCase, runID string, at time.Time) string {
	t.Helper()
	snapshot, err := decisioncard.FreezeSnapshot("launch_review", "", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}})
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		FlowInstance: "launch/recovery", FlowID: "launch", EntityID: uuid.NewString(),
		Stage: "awaiting_review", StageActivationID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: runID, Anchor: anchor,
		ExecutionMode: "live",
		Snapshot:      snapshot,
		BundleHash:    gateRecoveryBundle, EffectiveCadence: decisioncard.Cadence{ReminderInterval: "24h", InputDraftTTL: "15m"}, CreatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.cards.CreateDecisionCard(testAuthorActivityContext(t, context.Background()), card); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	if _, err := tc.cards.DecideDecisionCard(testAuthorActivityContext(t, context.Background()), decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: at}); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	stageAnchor := recoveryStageAnchor(t, card)
	evt := eventtest.RuntimeControl(eventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, stageAnchor.EntityID), stageAnchor.FlowInstance), at)
	storetest.CommitSemanticEventWithRoutes(t, testAuthorActivityContext(t, context.Background()), tc.events, evt, nil, runtimereplayclaim.CommittedReplayScopeSubscribed)
	return eventID
}

func persistGateRecoveryRouteEvent(t *testing.T, tc gateRecoveryStoreCase, evt events.Event) {
	t.Helper()
	storetest.CommitSemanticEventWithRoutes(t, testAuthorActivityContext(t, context.Background()), tc.events, evt, nil, runtimereplayclaim.CommittedReplayScopeSubscribed)
}

func setGateRecoveryRouteAttempt(t *testing.T, tc gateRecoveryStoreCase, eventID string, attempt int) {
	t.Helper()
	query := `UPDATE decision_card_route_obligations SET attempt_count = ?, next_attempt_at = ? WHERE event_id = ?`
	args := []any{attempt, time.Now().UTC().Add(-time.Second), eventID}
	if tc.postgres {
		query = `UPDATE decision_card_route_obligations SET attempt_count = $1, next_attempt_at = $2 WHERE event_id = $3::uuid`
	}
	if _, err := tc.db.ExecContext(testAuthorActivityContext(t, context.Background()), query, args...); err != nil {
		t.Fatal(err)
	}
}

func assertGateRecoveryObligationAttempt(t *testing.T, tc gateRecoveryStoreCase, eventID string, want int) {
	t.Helper()
	query := `SELECT attempt_count FROM decision_card_route_obligations WHERE event_id = ?`
	if tc.postgres {
		query = `SELECT attempt_count FROM decision_card_route_obligations WHERE event_id = $1::uuid`
	}
	var got int
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, eventID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decision route attempt count for %s = %d, want %d", eventID, got, want)
	}
}

func firstGateRecoveryEventID(ids map[string]struct{}) string {
	for id := range ids {
		return id
	}
	return ""
}

func testWorkflowGateStartupTerminalRecovery(t *testing.T, tc gateRecoveryStoreCase) {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	runID, entityID := uuid.NewString(), uuid.NewString()
	insertGateRecoveryRun(t, tc, runID)
	ctx = withLiveGateExecution(runtimecorrelation.WithRunID(ctx, runID))
	bundle := gateRecoveryTerminalContractBundle()
	bus, err := newScopedTestEventBus(t, tc.events, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatal(err)
	}
	newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
		return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, tc.db, runtimepipeline.PipelineCoordinatorOptions{
			Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, WorkflowStore: tc.workflowStore,
			DecisionCards: tc.cards, BundleHash: bundleHash,
		})
	}
	matching := newCoordinator(gateRecoveryBundle)
	enteredAt := time.Now().UTC().Add(-25 * time.Hour)
	if err := tc.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "launch/review-terminal", StorageRef: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: enteredAt,
		Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return matching.ArmFlowInstanceInitialStageLifecycle(txctx, entityID)
	}); err != nil {
		t.Fatal(err)
	}
	items, _, err := tc.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("decision cards = %#v, %v", items, err)
	}
	card, err := tc.cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	decidedAt := enteredAt.Add(time.Minute)
	if err := tc.workflowStore.CommitDecision(ctx, card, eventID, decidedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: decidedAt}); err != nil {
		t.Fatal(err)
	}
	bus.SetInterceptors(newCoordinator(otherGateBundle))
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	evt := eventtest.RuntimeControl(eventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), recoveryStageAnchor(t, card).FlowInstance), decidedAt)
	if err := bus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatal(err)
	}
	makeGateRecoveryRouteDue(t, tc, eventID, time.Now().Add(-time.Second))
	bus.SetInterceptors(matching)
	if err := runtimepipeline.NewRecoveryManagerWith(bus).Recover(ctx); err != nil {
		t.Fatal(err)
	}
	assertGateRecoveryActivation(t, tc.workflowStore, ctx, entityID, "completed", gateruntime.StatusRouted)
	var status string
	query := `SELECT status FROM runs WHERE run_id = ?`
	if tc.postgres {
		query = `SELECT status FROM runs WHERE run_id = $1::uuid`
	}
	if err := tc.db.QueryRowContext(ctx, query, runID).Scan(&status); err != nil || status != "completed" {
		t.Fatalf("terminal no-emit recovered run status = %q, %v", status, err)
	}
	assertGateRecoveryProcessedReceipt(t, tc, eventID)
}

func testWorkflowGateUnavailablePinRecovery(t *testing.T, tc gateRecoveryStoreCase) {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	entityID := uuid.NewString()
	insertGateRecoveryRun(t, tc, runID)
	ctx = withLiveGateExecution(runtimecorrelation.WithRunID(ctx, runID))

	bundle := gateRecoveryContractBundle()
	bus, err := newScopedTestEventBus(t, tc.events, runtimebus.EventBusOptions{ContractBundle: semanticview.Wrap(bundle)})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	outcomeAgent := "gate-outcome-recorder"
	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: outcomeAgent})
	outcomeEvents := runtimebustest.Subscribe(t, bus, outcomeAgent, events.EventType("launch.approved"))
	t.Cleanup(func() { runtimebustest.Unsubscribe(bus, outcomeAgent) })

	newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
		return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, tc.db, runtimepipeline.PipelineCoordinatorOptions{
			Module:        gateRecoveryModule{source: semanticview.Wrap(bundle)},
			WorkflowStore: tc.workflowStore,
			DecisionCards: tc.cards,
			BundleHash:    bundleHash,
		})
	}
	matching := newCoordinator(gateRecoveryBundle)

	scenarioAt := time.Now().UTC().Add(-25 * time.Hour)
	if err := tc.workflowStore.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: "launch/review-1", StorageRef: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: scenarioAt,
		Metadata: map[string]any{"entity_id": entityID, "run_id": runID},
	}); err != nil {
		t.Fatalf("Upsert workflow instance: %v", err)
	}
	if err := tc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return matching.ArmFlowInstanceInitialStageLifecycle(txctx, entityID)
	}); err != nil {
		t.Fatalf("arm initial gate: %v", err)
	}
	items, _, err := tc.cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: runID, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("decision cards = %#v, %v", items, err)
	}
	card, err := tc.cards.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatalf("GetDecisionCard: %v", err)
	}
	decisionEventID := uuid.NewString()
	decidedAt := scenarioAt.Add(time.Minute)
	if err := tc.workflowStore.CommitDecision(ctx, card, decisionEventID, decidedAt); err != nil {
		t.Fatalf("CommitDecision: %v", err)
	}
	if _, err := tc.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{
		CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator-1",
		ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: decidedAt,
	}); err != nil {
		t.Fatalf("DecideDecisionCard: %v", err)
	}

	bus.SetInterceptors(newCoordinator(otherGateBundle))
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	decisionEvent := eventtest.RuntimeControl(
		decisionEventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), recoveryStageAnchor(t, card).FlowInstance), decidedAt,
	)
	if err := bus.PublishAcknowledged(ctx, decisionEvent); err != nil {
		t.Fatalf("PublishAcknowledged: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for unavailable-pin dispatch: %v", err)
	}
	assertGateRecoveryActivation(t, tc.workflowStore, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
	if got := gateRecoveryPipelineReceiptCount(t, tc, decisionEventID); got != 0 {
		t.Fatalf("unavailable pin pipeline receipt count = %d, want 0", got)
	}
	recovery := runtimepipeline.NewRecoveryManagerWith(bus)
	if err := recovery.Recover(ctx); err != nil {
		t.Fatalf("Recover while pin unavailable: %v", err)
	}
	if got := gateRecoveryPipelineReceiptCount(t, tc, decisionEventID); got != 0 {
		t.Fatalf("unavailable pin recovery wrote terminal receipt count = %d, want 0", got)
	}

	bus.SetInterceptors(matching)
	makeGateRecoveryRouteDue(t, tc, decisionEventID, time.Now().Add(-time.Second))
	if err := recovery.Recover(ctx); err != nil {
		t.Fatalf("Recover after pin restore: %v", err)
	}
	assertGateRecoveryActivation(t, tc.workflowStore, ctx, entityID, "operating", gateruntime.StatusRouted)
	outcomeEventID := gateRecoveryOutcomeEventID(t, tc, decisionEventID)
	if got := gateRecoveryDeliveryCount(t, tc, outcomeEventID, outcomeAgent); got != 1 {
		t.Fatalf("outcome delivery manifest count = %d, want 1", got)
	}
	select {
	case got := <-outcomeEvents:
		if got.ID() != outcomeEventID {
			t.Fatalf("delivered outcome id = %s, want %s", got.ID(), outcomeEventID)
		}
		if err := got.Complete(); err != nil {
			t.Fatalf("complete authored gate outcome delivery: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for authored gate outcome delivery")
	}
	assertGateRecoveryProcessedReceipt(t, tc, decisionEventID)

	if err := recovery.Recover(ctx); err != nil {
		t.Fatalf("idempotent second Recover: %v", err)
	}
	if got := gateRecoveryOutcomeEventCount(t, tc, decisionEventID); got != 1 {
		t.Fatalf("authored outcome count after idempotent recovery = %d, want 1", got)
	}
}

func makeGateRecoveryRouteDue(t *testing.T, tc gateRecoveryStoreCase, eventID string, due time.Time) {
	t.Helper()
	query := `UPDATE decision_card_route_obligations SET next_attempt_at = ? WHERE event_id = ?`
	if tc.postgres {
		query = `UPDATE decision_card_route_obligations SET next_attempt_at = $1 WHERE event_id = $2::uuid`
	}
	if _, err := tc.db.ExecContext(testAuthorActivityContext(t, context.Background()), query, due.UTC(), eventID); err != nil {
		t.Fatalf("make decision route obligation due: %v", err)
	}
}

func openSQLiteGateRecoveryStore(t *testing.T) gateRecoveryStoreCase {
	selected := storetest.StartSQLiteRuntimeStore(t)
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(selected.DB, selected)
	workflowStore.ConfigureDeliveryLifecycleStore(selected)
	return gateRecoveryStoreCase{
		name: "sqlite", db: selected.DB, events: selected, cards: selected,
		workflowStore: workflowStore,
	}
}

func openPostgresGateRecoveryStore(t *testing.T) gateRecoveryStoreCase {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	workflowStore.ConfigureDeliveryLifecycleStore(selected)
	return gateRecoveryStoreCase{
		name: "postgres", postgres: true, db: db, events: selected, cards: selected,
		workflowStore: workflowStore,
	}
}

func proposedEffectProofBundle(serverURL string) *runtimecontracts.WorkflowContractBundle {
	handler := runtimecontracts.SystemNodeEventHandler{Activity: runtimecontracts.ActivitySpec{
		ID: "send_support_reply", Tool: "provider_write",
		Input: map[string]runtimecontracts.ExpressionValue{
			"chat_id": runtimecontracts.CELExpression("payload.chat_id"),
			"text":    runtimecontracts.CELExpression("payload.text"),
		},
		Approval: &runtimecontracts.ActivityApprovalSpec{Decision: "support_reply"},
	}}
	return &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"support": {
				ID: "support", ExecutionType: runtimecontracts.SystemNodeExecutionType,
				SubscribesTo: []string{"support.reply_drafted", "send_support_reply.revision_requested", "send_support_reply.rejected"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"support.reply_drafted":                 handler,
					"send_support_reply.revision_requested": {},
					"send_support_reply.rejected":           {},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "support", Version: "1", InitialStage: "drafting",
			EventOwners: map[string][]string{
				"support.reply_drafted":                 {"support"},
				"send_support_reply.revision_requested": {"support"},
				"send_support_reply.rejected":           {"support"},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"support": {
					"support.reply_drafted":                 handler,
					"send_support_reply.revision_requested": {},
					"send_support_reply.rejected":           {},
				},
			},
			EffectiveNodes: map[string]runtimecontracts.SystemNodeEffectiveSemantics{
				"support": {
					ID: "support", ExecutionType: runtimecontracts.SystemNodeExecutionType,
					RuntimeSubscriptions: []string{"support.reply_drafted", "send_support_reply.revision_requested", "send_support_reply.rejected"},
					Produces:             []string{"send_support_reply.succeeded", "send_support_reply.failed", "send_support_reply.revision_requested", "send_support_reply.rejected"},
				},
			},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"provider_write": {
				HandlerType: "http", EffectClass: string(runtimecontracts.ActivityEffectClassNonIdempotentWrite),
				Credentials: []string{"provider_token"},
				InputSchema: runtimecontracts.ToolInputSchema{
					Type: "object", Required: []string{"chat_id", "text"},
					Properties: map[string]runtimecontracts.ToolInputSchema{"chat_id": {Type: "string"}, "text": {Type: "string"}},
				},
				OutputSchema: runtimecontracts.ToolInputSchema{Type: "object"},
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "POST", URL: strings.TrimRight(serverURL, "/"),
					Headers: map[string]string{"Authorization": "Bearer {{credentials.provider_token}}"},
					Body:    map[string]any{"chat_id": "{{input.chat_id}}", "text": "{{input.text}}"},
				},
			},
		},
	}
}

func waitForGateRecoveryQuiescence(t *testing.T, bus *runtimebus.EventBus, ctx context.Context) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for event bus quiescence: %v", err)
	}
}

func assertProposedEffectProofCounts(t *testing.T, selected gateRecoveryStoreCase, runID string, requests, attempts int) {
	t.Helper()
	requestQuery := `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'`
	attemptQuery := `SELECT COUNT(*) FROM activity_attempts WHERE run_id = ?`
	if selected.postgres {
		requestQuery = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'`
		attemptQuery = `SELECT COUNT(*) FROM activity_attempts WHERE run_id = $1::uuid`
	}
	var gotRequests, gotAttempts int
	if err := selected.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), requestQuery, runID).Scan(&gotRequests); err != nil {
		t.Fatal(err)
	}
	if err := selected.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), attemptQuery, runID).Scan(&gotAttempts); err != nil {
		t.Fatal(err)
	}
	if gotRequests != requests || gotAttempts != attempts {
		t.Fatalf("durable activity counts = requests:%d attempts:%d, want %d/%d", gotRequests, gotAttempts, requests, attempts)
	}
}

func assertProposedEffectOutcomeCount(t *testing.T, selected gateRecoveryStoreCase, runID, eventType string, want int) {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = ?`
	if selected.postgres {
		query = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	var got int
	if err := selected.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, runID, eventType).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s event count = %d, want %d", eventType, got, want)
	}
}

func seedProposedEffectProofDelivery(t *testing.T, selected gateRecoveryStoreCase, evt events.Event, nodeID string) events.DeliveryRoute {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: nodeID}
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected.events, evt, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
	return route
}

func proposedEffectProofFailure(t *testing.T, selected gateRecoveryStoreCase, eventID string) string {
	t.Helper()
	query := `SELECT COALESCE(CAST(failure AS TEXT), '') FROM dead_letters WHERE original_event_id = ? ORDER BY created_at DESC LIMIT 1`
	if selected.postgres {
		query = `SELECT COALESCE(failure::text, '') FROM dead_letters WHERE original_event_id = $1::uuid ORDER BY created_at DESC LIMIT 1`
	}
	var failure string
	if err := selected.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, eventID).Scan(&failure); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "<no dead letter>"
		}
		return err.Error()
	}
	return failure
}

func loadProposedEffectProofRequest(t *testing.T, selected gateRecoveryStoreCase, runID string) events.Event {
	t.Helper()
	query := `SELECT event_id, event_name, payload, chain_depth, COALESCE(source_event_id, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'`
	if selected.postgres {
		query = `SELECT event_id::text, event_name, payload, chain_depth, COALESCE(source_event_id::text, ''), COALESCE(entity_id::text, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'`
	}
	var eventID, eventType, parentID, entityID, flowInstance string
	var payload []byte
	var depth int
	if err := selected.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, runID).Scan(&eventID, &eventType, &payload, &depth, &parentID, &entityID, &flowInstance); err != nil {
		t.Fatal(err)
	}
	envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
	envelope = events.EnvelopeForFlowInstance(envelope, flowInstance)
	return eventtest.PersistedProjection(eventID, events.EventType(eventType), "workflow-runtime", "", payload, depth, runID, parentID, envelope, time.Now().UTC())
}

func gateRecoveryContractBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: nil,
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"launch.approved": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{}}},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "launch", Version: "1", InitialStage: "awaiting_review",
			Gates: []runtimecontracts.WorkflowGatePlan{{
				Stage: "awaiting_review", Decision: "launch_review",
				Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{
					"approve": {Verdict: "approve", AdvancesTo: "operating", Emit: runtimecontracts.EmitSpec{Event: "launch.approved"}},
				},
			}},
		},
	}
}

func gateRecoveryTerminalContractBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema: nil,
		Semantics: runtimecontracts.WorkflowSemanticView{
			Name: "launch", Version: "1", InitialStage: "awaiting_review", TerminalStages: []string{"completed"},
			Gates: []runtimecontracts.WorkflowGatePlan{{
				Stage: "awaiting_review", Decision: "launch_review",
				Outcomes: map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "completed"}},
			}},
		},
	}
}

func insertGateRecoveryRun(t *testing.T, tc gateRecoveryStoreCase, runID string) {
	t.Helper()
	query := `INSERT INTO runs (run_id, status) VALUES (?, 'running')`
	if tc.postgres {
		query = `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`
	}
	if _, err := tc.db.ExecContext(testAuthorActivityContext(t, context.Background()), query, runID); err != nil {
		t.Fatalf("insert run: %v", err)
	}
}

func assertGateRecoveryActivation(t *testing.T, workflowStore *runtimepipeline.WorkflowInstanceStore, ctx context.Context, entityID, stage string, status gateruntime.Status) {
	t.Helper()
	instance, ok, err := workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		t.Fatalf("Load workflow instance = %#v, %v, %v", instance, ok, err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		t.Fatalf("StateCarrierFromPersisted: %v", err)
	}
	activation, found, err := gateruntime.Load(carrier.StateBuckets, "", "launch_review")
	if err != nil || !found || instance.CurrentState != stage || activation.Status != status {
		t.Fatalf("gate state = stage:%s activation:%#v found:%v err:%v, want %s/%s", instance.CurrentState, activation, found, err, stage, status)
	}
}

func gateRecoveryPipelineReceiptCount(t *testing.T, tc gateRecoveryStoreCase, eventID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if tc.postgres {
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var count int
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts: %v", err)
	}
	return count
}

func assertGateRecoveryProcessedReceipt(t *testing.T, tc gateRecoveryStoreCase, eventID string) {
	t.Helper()
	query := `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if tc.postgres {
		query = `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var outcome, reason string
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, eventID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load final pipeline receipt: %v", err)
	}
	if outcome != "success" || reason != "decision_route_processed" {
		t.Fatalf("final pipeline receipt = %s/%s, want success/decision_route_processed", outcome, reason)
	}
}

func assertGateRecoveryObligationStatus(t *testing.T, tc gateRecoveryStoreCase, eventID, want string) {
	t.Helper()
	query := `SELECT status FROM decision_card_route_obligations WHERE event_id = ?`
	if tc.postgres {
		query = `SELECT status FROM decision_card_route_obligations WHERE event_id = $1::uuid`
	}
	var got string
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, eventID).Scan(&got); err != nil || got != want {
		t.Fatalf("decision route obligation status = %q, %v; want %q", got, err, want)
	}
}

func assertGateRecoveryErrorReceipt(t *testing.T, tc gateRecoveryStoreCase, eventID, wantReason string) {
	t.Helper()
	query := `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if tc.postgres {
		query = `SELECT outcome, COALESCE(reason_code, '') FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var outcome, reason string
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, eventID).Scan(&outcome, &reason); err != nil {
		t.Fatalf("load quarantined pipeline receipt: %v", err)
	}
	if outcome != "dead_letter" || reason != wantReason {
		t.Fatalf("quarantined pipeline receipt = %s/%s, want dead_letter/%s", outcome, reason, wantReason)
	}
}

func gateRecoveryOutcomeEventID(t *testing.T, tc gateRecoveryStoreCase, parentEventID string) string {
	t.Helper()
	query := `SELECT event_id FROM events WHERE event_name = 'launch.approved' AND source_event_id = ?`
	if tc.postgres {
		query = `SELECT event_id::text FROM events WHERE event_name = 'launch.approved' AND source_event_id = $1::uuid`
	}
	var eventID string
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, parentEventID).Scan(&eventID); err != nil {
		t.Fatalf("load authored outcome event: %v", err)
	}
	return eventID
}

func gateRecoveryOutcomeEventCount(t *testing.T, tc gateRecoveryStoreCase, parentEventID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE event_name = 'launch.approved' AND source_event_id = ?`
	if tc.postgres {
		query = `SELECT COUNT(*) FROM events WHERE event_name = 'launch.approved' AND source_event_id = $1::uuid`
	}
	var count int
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, parentEventID).Scan(&count); err != nil {
		t.Fatalf("count authored outcome events: %v", err)
	}
	return count
}

func gateRecoveryDeliveryCount(t *testing.T, tc gateRecoveryStoreCase, eventID, recipient string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_deliveries WHERE event_id = ? AND subscriber_id = ?`
	args := []any{eventID, recipient}
	if tc.postgres {
		query = `SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_id = $2`
	}
	var count int
	if err := tc.db.QueryRowContext(testAuthorActivityContext(t, context.Background()), query, args...).Scan(&count); err != nil {
		t.Fatalf("count authored outcome deliveries: %v", err)
	}
	return count
}
