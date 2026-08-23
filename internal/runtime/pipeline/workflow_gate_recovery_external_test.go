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
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

const (
	gateRecoveryBundle = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	otherGateBundle    = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func withLiveGateExecution(ctx context.Context) context.Context {
	bound, err := eventreceiver.NormalExecution().Bind(ctx, executionmode.Live)
	if err != nil {
		panic(err)
	}
	return bound
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
	name        string
	postgres    bool
	db          *sql.DB
	events      gateRecoverySelectedStore
	lifecycle   runtimerunlifecycle.CandidateStore
	cards       gateRecoveryDecisionStore
	persistence runtimepipeline.WorkflowPersistence
}

type gateRecoverySelectedStore interface {
	scopedTestDurableStore
}

type gateRecoveryDecisionStore interface {
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.HumanTaskExpiry
	ExpireDecisionCardInputDrafts(context.Context, time.Time) (int, error)
}

type gateRecoveryRuntimeBus interface {
	runtimepipeline.Bus
	runtimepipeline.WorkflowDeliveryRuntime
}

type proposedEffectProofCredentialStore struct {
	value string
	gets  atomic.Int32
}

func newGateRecoveryCoordinator(bus gateRecoveryRuntimeBus, selected gateRecoveryStoreCase, opts runtimepipeline.PipelineCoordinatorOptions) *runtimepipeline.PipelineCoordinator {
	opts.ExecutionPosture = executionposture.Live
	opts.ReceiverExecution = eventreceiver.NormalExecution()
	opts.Persistence = selected.persistence
	opts.DeliveryStore = selected.events
	opts.DeadLetters = selected.events
	opts.DecisionCards = selected.cards
	opts.ProposedEffects = selected.cards
	opts.HumanTasks = selected.cards
	opts.DecisionCardDraftExpiry = selected.cards
	opts.HumanTaskExpiry = selected.cards
	opts.DeliveryRuntime = bus
	opts.RunLifecycle = selected.events
	if opts.PipelineObligations == nil {
		opts.PipelineObligations = selected.events.PipelineObligations()
	}
	return runtimepipeline.NewPipelineCoordinatorWithOptions(bus, opts)
}

type proposedEffectRouteProofBus struct {
	published       []events.Event
	publishContexts []events.DeliveryContext
	outbox          []runtimeengine.EmitIntent
	dispatched      []runtimeengine.EmitIntent
	eventBus        *runtimebus.EventBus
	prepared        map[string]runtimeengine.EmitIntent
}

type proposedEffectRouteProofDispatcher struct{ bus *proposedEffectRouteProofBus }

func (b *proposedEffectRouteProofBus) PipelineObligationOwner() runtimepipelineobligation.Store {
	if b == nil || b.eventBus == nil {
		return nil
	}
	return b.eventBus.PipelineObligationOwner()
}

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
func (b *proposedEffectRouteProofBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return proposedEffectRouteProofDispatcher{bus: b}
}

func (b *proposedEffectRouteProofBus) PrepareEnginePublications(ctx context.Context, intents []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	plans, err := b.eventBus.PrepareEnginePublications(ctx, intents)
	if err != nil {
		return nil, err
	}
	if b.prepared == nil {
		b.prepared = make(map[string]runtimeengine.EmitIntent, len(intents))
	}
	for _, intent := range intents {
		b.prepared[intent.Event.ID()] = intent
	}
	return plans, nil
}

func (b *proposedEffectRouteProofBus) ReleaseEnginePublications(ctx context.Context, plans []runtimeengine.DurablePublicationPlan) error {
	return b.eventBus.ReleaseEnginePublications(ctx, plans)
}

func (b *proposedEffectRouteProofBus) FinalizeEnginePublications(ctx context.Context, evidence []runtimeengine.CommittedDurablePublication) error {
	if err := b.eventBus.FinalizeEnginePublications(ctx, evidence); err != nil {
		return err
	}
	for _, item := range evidence {
		committed, ok := item.(runtimebus.CommittedEnginePublication)
		if !ok || !committed.NewlyInserted() {
			continue
		}
		intent := item.CommittedDurablePublicationIntent()
		if intent.Event.Type() == events.EventType("platform.activity_requested") {
			b.outbox = append(b.outbox, intent)
		} else {
			b.published = append(b.published, events.NewContextDeliveryEvent(intent.Event, intent.Context).Event())
			b.publishContexts = append(b.publishContexts, intent.Context)
		}
	}
	return nil
}
func (b *proposedEffectRouteProofBus) DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error) {
	if b == nil || b.eventBus == nil {
		return runtimedelivery.ExecutionAuthority{}, errors.New("proposed-effect proof delivery authority requires event bus")
	}
	return b.eventBus.DeliveryAuthority()
}
func (b *proposedEffectRouteProofBus) AcquireDeliveryContinuation(deliveryID string) (worklifetime.DeliveryContinuation, error) {
	if b == nil || b.eventBus == nil {
		return nil, errors.New("proposed-effect proof delivery continuation requires event bus")
	}
	return b.eventBus.AcquireDeliveryContinuation(deliveryID)
}
func (b *proposedEffectRouteProofBus) ReleaseDeliveryContinuation(deliveryID string) error {
	if b == nil || b.eventBus == nil {
		return errors.New("proposed-effect proof delivery continuation requires event bus")
	}
	return b.eventBus.ReleaseDeliveryContinuation(deliveryID)
}
func (b *proposedEffectRouteProofBus) RetainDeliveryContinuation(snapshot runtimedelivery.Snapshot) error {
	if b == nil || b.eventBus == nil {
		return errors.New("proposed-effect proof delivery continuation requires event bus")
	}
	return b.eventBus.RetainDeliveryContinuation(snapshot)
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

func gateRecoveryRetryReleased(outcome runtimepipelineobligation.ExecutionOutcome) bool {
	retry, ok := outcome.RetryRelease()
	return ok && retry.ReasonCode() == "activity_contract_pin_unavailable"
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
			bundleSource := mustAuthorActivityTestBundleSourceFactForHash(gateRecoveryBundle)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, BundleSourceFact: bundleSource,
			},
				"support.reply_drafted",
				"support/send_support_reply.revision_requested",
				"support/send_support_reply.rejected",
			)
			if err != nil {
				t.Fatal(err)
			}
			module := proposedEffectProofModule{
				source:   source,
				workflow: runtimepipeline.NewWorkflowDefinition("support", []runtimepipeline.WorkflowStage{{Name: "drafting"}}, nil),
				nodes: []runtimepipeline.WorkflowNode{{
					Node: externalPipelineSourceNode(t, source, "", "support"), Subscriptions: []events.EventType{"support.reply_drafted", "send_support_reply.revision_requested", "send_support_reply.rejected", "platform.activity_requested"},
					Produces:      []events.EventType{"send_support_reply.succeeded", "send_support_reply.failed", "send_support_reply.revision_requested", "send_support_reply.rejected"},
					ExecutionType: runtimecontracts.SystemNodeExecutionType,
					Policies: map[string]runtimepipeline.WorkflowEventPolicy{
						"support.reply_drafted":       {Consume: true},
						"platform.activity_requested": {Consume: true},
					},
				}},
			}
			newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
				return newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, Persistence: selected.persistence, DecisionCards: selected.cards,
					BundleSourceFact: mustAuthorActivityTestBundleSourceFactForHash(bundleHash),
					Credentials:      credentials,
				})
			}
			coordinator := newCoordinator(gateRecoveryBundle)
			bus.SetInterceptors(coordinator)

			runID, entityID := uuid.NewString(), uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := runtimecorrelation.WithBundleSourceFact(testAuthorActivityContext(t, context.Background()), bundleSource)
			ctx = withLiveGateExecution(runtimecorrelation.WithRunID(ctx, runID))
			enteredAt := time.Now().UTC()
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "support", WorkflowVersion: "1", CurrentState: "drafting",
				EnteredStageAt: enteredAt, CreatedAt: enteredAt,
				Fields:     map[string]any{"entity_id": entityID, "run_id": runID, "flow_path": runID, "instance_id": runID},
				EntityType: "test_entity",
			}, enteredAt); err != nil {
				t.Fatal(err)
			}
			const replyContextID = "reply-context-proposed-effect"
			sourceEvent := eventtest.ForDelivery(eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType("support.reply_drafted"), "support-agent", "task-1",
				[]byte(`{"chat_id":"support-room","text":"Exact frozen reply"}`), 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()),
				events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}},
			)
			sourceRoute := seedProposedEffectProofDelivery(t, selected, bus, sourceEvent, externalPipelineSourceNode(t, source, "", "support"), entityID)
			if !sourceRoute.Target.ExistingEntity() || sourceRoute.Target.Route().EntityID != entityID {
				t.Fatalf("approval execution target = %#v, want exact existing owner %s", sourceRoute.Target, entityID)
			}
			sourceCtx := events.WithDeliveryContext(ctx, sourceEvent.DeliveryContext())
			sourceDelivery, err := events.NewDeliveryEvent(sourceEvent, sourceRoute)
			if err != nil {
				t.Fatalf("construct proposal source delivery: %v", err)
			}
			forward, _, sourceOutcome, err := coordinator.InterceptDeliveryRoute(sourceCtx, sourceDelivery, sourceRoute)
			if err != nil {
				t.Fatalf("execute proposal source: %v", err)
			}
			if disposition, ok := sourceOutcome.Disposition(); ok {
				t.Fatalf("execute proposal source disposition = %s/%s failure=%#v", disposition.Kind(), disposition.ReasonCode(), disposition.Failure())
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
			continuation, err := selected.cards.LoadProposedEffectContinuation(ctx, card.CardID)
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
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), runID), time.Now().UTC())
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
			deferred, err := selected.cards.LoadProposedEffectContinuation(ctx, card.CardID)
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
			if changedForward, changedEmitted, changedOutcome, changedErr := changedCoordinator.Intercept(ctx, releasedRequest); changedErr != nil || !gateRecoveryRetryReleased(changedOutcome) {
				t.Fatalf("consume released request under changed bundle = forward:%v emitted:%d outcome:%#v error:%v, want replayable claim release", changedForward, len(changedEmitted), changedOutcome, changedErr)
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
			readback, err := selected.cards.ProposedEffectReadback(ctx, card.CardID)
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
				proposal := eventtest.ForDelivery(eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType("support.reply_drafted"), "support-agent", "task-1",
					[]byte(`{"chat_id":"support-room","text":"Needs another operator outcome"}`), 0, runID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC()),
					events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}},
				)
				proposalRoute := seedProposedEffectProofDelivery(t, selected, bus, proposal, externalPipelineSourceNode(t, source, "", "support"), entityID)
				proposalCtx := events.WithDeliveryContext(ctx, proposal.DeliveryContext())
				proposalDelivery, deliveryErr := events.NewDeliveryEvent(proposal, proposalRoute)
				if deliveryErr != nil {
					t.Fatalf("construct %s proposal delivery: %v", verdict, deliveryErr)
				}
				consumed, _, _, routeErr := coordinator.InterceptDeliveryRoute(proposalCtx, proposalDelivery, proposalRoute)
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
				pendingContinuation, routeErr := selected.cards.LoadProposedEffectContinuation(ctx, pendingCard.CardID)
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
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), runID), time.Now().UTC())
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
				readback, routeErr := selected.cards.ProposedEffectReadback(ctx, pendingCard.CardID)
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
				supportNode := externalPipelineNode(t, "", "support")
				supportOwner := activityidentity.MustNodeOwner(supportNode)
				insertGateRecoveryRun(t, selected, runID)
				now := time.Date(2026, 7, 14, 22, 0, 0, 0, time.UTC)
				input, err := canonicaljson.FromGo(map[string]any{"chat_id": "support-room", "text": "Exact approved text"})
				if err != nil {
					t.Fatal(err)
				}
				sourceEventID := uuid.NewString()
				sourceEvent := eventtest.ExistingRunRootIngress(sourceEventID, events.EventType("support.reply_drafted"), "support-agent", "task-1",
					[]byte(`{"chat_id":"support-room","text":"Exact approved text"}`), 0, runID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), now)
				storetest.CommitSemanticEvent(t, ctx, selected.events, sourceEvent)
				requestEventID := activityidentity.RequestEventID(activityidentity.Fact{
					RunID: runID, SourceEventID: sourceEventID, EntityID: entityID,
					Owner: supportOwner, ExecutionFlowID: "support",
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
					EntityID: entityID, NodeID: supportOwner.Key(), FlowID: "support", FlowInstance: runID, HandlerEventKey: "support.reply_drafted",
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
					Scope:  decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: runID, EntityID: entityID},
					Source: eventtest.StaticFlowRoutingSource("support", runID, entityID),
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
				proposedStore := selected.cards
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
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), runID), now.Add(time.Minute))
				storetest.CommitSemanticEvent(t, ctx, selected.events, decisionEvent)
				source := semanticview.Wrap(proposedEffectProofBundle("http://127.0.0.1:1"))
				canonicalBus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source},
					"platform.activity_requested", "send_support_reply.revision_requested", "send_support_reply.rejected")
				if err != nil {
					t.Fatal(err)
				}
				bus := &proposedEffectRouteProofBus{eventBus: canonicalBus}
				coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: gateRecoveryModule{source: source}, Persistence: selected.persistence,
					DecisionCards: selected.cards, BundleSourceFact: mustAuthorActivityTestBundleSourceFactForHash(gateRecoveryBundle),
				})
				forward, emitted, _, err := coordinator.Intercept(withLiveGateExecution(ctx), decisionEvent)
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
				changed := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: gateRecoveryModule{source: source}, Persistence: selected.persistence,
					DecisionCards: selected.cards, BundleSourceFact: mustAuthorActivityTestBundleSourceFactForHash(otherGateBundle),
				})
				if _, _, _, err := changed.Intercept(withLiveGateExecution(ctx), decisionEvent); err != nil {
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
			supportNode := externalPipelineSourceNode(t, source, "", "support")
			bundleSource := mustAuthorActivityTestBundleSourceFactForHash(gateRecoveryBundle)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, BundleSourceFact: bundleSource,
			}, "support.reply_drafted")
			if err != nil {
				t.Fatal(err)
			}
			module := proposedEffectProofModule{
				source:   source,
				workflow: runtimepipeline.NewWorkflowDefinition("support", []runtimepipeline.WorkflowStage{{Name: "drafting"}, {Name: "queued"}}, nil),
				nodes: []runtimepipeline.WorkflowNode{{
					Node: supportNode, Subscriptions: []events.EventType{"support.reply_drafted"},
					Produces:      []events.EventType{"send_support_reply.succeeded", "send_support_reply.failed", "send_support_reply.revision_requested", "send_support_reply.rejected"},
					ExecutionType: runtimecontracts.SystemNodeExecutionType,
					Policies:      map[string]runtimepipeline.WorkflowEventPolicy{"support.reply_drafted": {Consume: true}},
				}},
			}
			coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, Persistence: selected.persistence, DecisionCards: selected.cards,
				BundleSourceFact: bundleSource,
			})
			bus.SetInterceptors(coordinator)

			runID, entityID := uuid.NewString(), uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := runtimecorrelation.WithBundleSourceFact(testAuthorActivityContext(t, context.Background()), bundleSource)
			ctx = withLiveGateExecution(runtimecorrelation.WithRunID(ctx, runID))
			enteredAt := time.Now().UTC()
			if _, err := coordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "support", WorkflowVersion: "1", CurrentState: "drafting",
				EnteredStageAt: enteredAt, CreatedAt: enteredAt,
				Fields:     map[string]any{"entity_id": entityID, "run_id": runID, "flow_path": runID, "instance_id": runID},
				EntityType: "test_entity",
			}, enteredAt); err != nil {
				t.Fatal(err)
			}
			installProposedEffectCreateFailure(t, selected)

			event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType("support.reply_drafted"), "support-agent", "task-rollback",
				[]byte(`{"chat_id":"support-room","text":"must roll back"}`), 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Now().UTC())
			route := seedProposedEffectProofDelivery(t, selected, bus, event, supportNode, entityID)
			delivery, deliveryErr := events.NewDeliveryEvent(event, route)
			if deliveryErr != nil {
				t.Fatalf("construct proposal failure delivery: %v", deliveryErr)
			}
			forward, _, failureOutcome, err := coordinator.InterceptDeliveryRoute(ctx, delivery, route)
			if err != nil || forward {
				t.Fatalf("proposal failure interception = forward:%v error:%v", forward, err)
			}
			disposition, disposed := failureOutcome.Disposition()
			if !disposed || disposition.Kind() != runtimepipelineobligation.DispositionDeadLetter || disposition.Failure() == nil {
				t.Fatalf("proposal failure disposition = %#v, present=%v; want typed dead letter", disposition, disposed)
			}

			instance, ok, err := coordinator.Load(ctx, testWorkflowInstanceRoute(runID))
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
			deliveryQuery := `SELECT status FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
			if selected.postgres {
				deliveryQuery = `SELECT status FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
			}
			var deliveryStatus string
			if err := selected.db.QueryRowContext(ctx, deliveryQuery, event.ID(), supportNode.Key()).Scan(&deliveryStatus); err != nil || deliveryStatus != "dead_letter" {
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
			setGateRecoveryRouteAttempt(t, selected, newEventID, 2)
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{Interceptors: []runtimebus.EventInterceptor{gateRecoveryFairnessInterceptor{deferred: deferred}}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := bus.SweepPipelineObligations(ctx, 202)
			if err != nil {
				t.Fatal(err)
			}
			if result.Settled != 201 || result.Examined != 201 || !result.Exhausted || result.Blocked {
				t.Fatalf("deferred-prefix sweep = %#v", result)
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
			if result, err := bus.SweepPipelineObligations(testAuthorActivityContext(t, context.Background()), 10); err != nil || result.Settled != 2 {
				t.Fatalf("poison route sweep recovered = %d, %v; want 2 handled obligations, nil", result.Settled, err)
			}
			assertGateRecoveryObligationStatus(t, selected, poisonEventID, "quarantined")
			assertGateRecoveryErrorReceipt(t, selected, poisonEventID, "event_interceptor_failed")
			assertGateRecoveryProcessedReceipt(t, selected, validEventID)
			if result, err := bus.SweepPipelineObligations(testAuthorActivityContext(t, context.Background()), 10); err != nil || result.Settled != 0 {
				t.Fatalf("second poison route sweep recovered = %d, %v; want 0, nil", result.Settled, err)
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
				assertGateRecoveryActivation(t, fixture.coordinator, runtimecorrelation.WithRunID(testAuthorActivityContext(t, context.Background()), runID), fixture.entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
				card, err := selected.cards.GetDecisionCard(testAuthorActivityContext(t, context.Background()), fixture.cardID)
				if err != nil {
					t.Fatalf("read quarantined decision card: %v", err)
				}
				if card.Status != decisioncard.StatusDecided || card.DecisionEventID != fixture.event.ID() {
					t.Fatalf("quarantined decision card = status:%q event:%q, want decided/%q", card.Status, card.DecisionEventID, fixture.event.ID())
				}

				validEventID := seedGateRecoveryRouteObligation(t, selected, runID, time.Now().UTC())
				if _, err := bus.SweepPipelineObligations(testAuthorActivityContext(t, context.Background()), 10); err != nil {
					t.Fatalf("sweep unrelated route behind quarantined foreground failure: %v", err)
				}
				assertGateRecoveryProcessedReceipt(t, selected, validEventID)
				assertGateRecoveryObligationStatus(t, selected, validEventID, "completed")
				if result, err := bus.SweepPipelineObligations(testAuthorActivityContext(t, context.Background()), 10); err != nil || result.Settled != 0 {
					t.Fatalf("second foreground quarantine sweep recovered = %d, %v; want 0, nil", result.Settled, err)
				}
			})
		}
	}
}

type gateRecoveryForegroundFixture struct {
	event       events.Event
	entityID    string
	cardID      string
	coordinator *runtimepipeline.PipelineCoordinator
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
	setupCoordinator := newGateRecoveryCoordinator(setupBus, tc, runtimepipeline.PipelineCoordinatorOptions{
		Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, Persistence: tc.persistence,
		DecisionCards: tc.cards, BundleSourceFact: mustAuthorActivityTestBundleSourceFactForHash(gateRecoveryBundle),
	})
	if _, err := setupCoordinator.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: at,
		Fields:     map[string]any{"entity_id": entityID, "run_id": runID, "flow_path": runID, "instance_id": runID},
		EntityType: "test_entity",
	}, at); err != nil {
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
	if err := setupCoordinator.CommitDecision(ctx, card, eventID, at); err != nil {
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
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), recoveryStageAnchor(t, card).Route.InstancePath), at)
	return gateRecoveryForegroundFixture{event: evt, entityID: entityID, cardID: card.CardID, coordinator: setupCoordinator}
}

func seedGateRecoveryRouteObligation(t *testing.T, tc gateRecoveryStoreCase, runID string, at time.Time) string {
	t.Helper()
	snapshot, err := decisioncard.FreezeSnapshot("launch_review", "", nil, map[string]runtimecontracts.WorkflowGateOutcomePlan{"approve": {Verdict: "approve", AdvancesTo: "operating"}})
	if err != nil {
		t.Fatal(err)
	}
	entityID := uuid.NewString()
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		Route: runtimeflowidentity.RouteForInstancePath("launch/recovery"), FlowID: "launch", EntityID: entityID,
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), Source: eventtest.ConcreteTemplateRoutingSource("launch", "launch/recovery", entityID),
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
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, stageAnchor.EntityID), stageAnchor.Route.InstancePath), at)
	storetest.CommitSemanticEventWithRoutes(t, testAuthorActivityContext(t, context.Background()), tc.events, evt, nil, runtimepipelineobligation.ScopeSubscribed)
	return eventID
}

func persistGateRecoveryRouteEvent(t *testing.T, tc gateRecoveryStoreCase, evt events.Event) {
	t.Helper()
	storetest.CommitSemanticEventWithRoutes(t, testAuthorActivityContext(t, context.Background()), tc.events, evt, nil, runtimepipelineobligation.ScopeSubscribed)
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
		return newGateRecoveryCoordinator(bus, tc, runtimepipeline.PipelineCoordinatorOptions{
			Module: gateRecoveryModule{source: semanticview.Wrap(bundle)}, Persistence: tc.persistence,
			DecisionCards: tc.cards, BundleSourceFact: mustAuthorActivityTestBundleSourceFactForHash(bundleHash),
		})
	}
	matching := newCoordinator(gateRecoveryBundle)
	enteredAt := time.Now().UTC().Add(-25 * time.Hour)
	if _, err := matching.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: enteredAt,
		Fields:     map[string]any{"entity_id": entityID, "run_id": runID, "flow_path": runID, "instance_id": runID},
		EntityType: "test_entity",
	}, enteredAt); err != nil {
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
	if err := matching.CommitDecision(ctx, card, eventID, decidedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.cards.DecideDecisionCard(ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: eventID, Now: decidedAt}); err != nil {
		t.Fatal(err)
	}
	bus.SetInterceptors(newCoordinator(otherGateBundle))
	payload, _ := json.Marshal(map[string]any{"card_id": card.CardID})
	evt := eventtest.RuntimeControl(eventID, events.EventType("mailbox.card_decided"), "platform", "", payload, 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), recoveryStageAnchor(t, card).Route.InstancePath), decidedAt)
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
	assertGateRecoveryActivation(t, matching, ctx, entityID, "completed", gateruntime.StatusRouted)
	completion, err := storetest.ExecuteRunCompletionCandidate(
		ctx,
		tc.lifecycle,
		gateRecoveryBundle,
		runID,
		runtimerunlifecycle.NewTerminalCatalog(
			bundle.Semantics.TerminalStages,
			map[string][]string{bundle.Semantics.Name: bundle.Semantics.TerminalStages},
		),
	)
	if err != nil {
		t.Fatalf("execute durable completion candidate: %v", err)
	}
	if completion.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible {
		t.Fatalf("durable completion outcome = %#v, want terminally eligible", completion)
	}
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
	bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
		Identity: runtimebustest.Identity(t, outcomeAgent, ""),
	})
	outcomeEvents := runtimebustest.Subscribe(t, bus, outcomeAgent, events.EventType("launch.approved"))
	t.Cleanup(func() { runtimebustest.Unsubscribe(bus, outcomeAgent) })

	newCoordinator := func(bundleHash string) *runtimepipeline.PipelineCoordinator {
		return newGateRecoveryCoordinator(bus, tc, runtimepipeline.PipelineCoordinatorOptions{
			Module:           gateRecoveryModule{source: semanticview.Wrap(bundle)},
			Persistence:      tc.persistence,
			DecisionCards:    tc.cards,
			BundleSourceFact: mustAuthorActivityTestBundleSourceFactForHash(bundleHash),
		})
	}
	matching := newCoordinator(gateRecoveryBundle)

	scenarioAt := time.Now().UTC().Add(-25 * time.Hour)
	if _, err := matching.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: "launch", WorkflowVersion: "1",
		CurrentState: "awaiting_review", EnteredStageAt: scenarioAt,
		Fields:     map[string]any{"entity_id": entityID, "run_id": runID, "flow_path": runID, "instance_id": runID},
		EntityType: "test_entity",
	}, scenarioAt); err != nil {
		t.Fatalf("materialize workflow instance: %v", err)
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
	if err := matching.CommitDecision(ctx, card, decisionEventID, decidedAt); err != nil {
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
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), recoveryStageAnchor(t, card).Route.InstancePath), decidedAt,
	)
	if err := bus.PublishAcknowledged(ctx, decisionEvent); err != nil {
		t.Fatalf("PublishAcknowledged: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for unavailable-pin dispatch: %v", err)
	}
	assertGateRecoveryActivation(t, matching, ctx, entityID, "awaiting_review", gateruntime.StatusDecisionCommitted)
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
	assertGateRecoveryActivation(t, matching, ctx, entityID, "operating", gateruntime.StatusRouted)
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
	persistence := runtimepipeline.NewWorkflowPersistence(selected)
	result := gateRecoveryStoreCase{
		name: "sqlite", db: storetest.Database(selected), events: selected, cards: selected,
		lifecycle: selected, persistence: persistence,
	}
	return result
}

func openPostgresGateRecoveryStore(t *testing.T) gateRecoveryStoreCase {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	persistence := runtimepipeline.NewWorkflowPersistence(selected)
	result := gateRecoveryStoreCase{
		name: "postgres", postgres: true, db: db, events: selected, cards: selected,
		lifecycle: selected, persistence: persistence,
	}
	return result
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
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{"test_entity": {Fields: map[string]runtimecontracts.EntityFieldDecl{}}},
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
			"provider_write": runtimecontracts.MustToolSchemaEntry(runtimecontracts.WithToolHandler(runtimecontracts.MustToolHandlerKind("http")), runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(string(runtimecontracts.ActivityEffectClassNonIdempotentWrite))), runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"), runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"chat_id": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string")), "text": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("string"))}), runtimecontracts.ToolSchemaRequired("chat_id", "text")),

				runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaKind("object"))), runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
				Method: "POST", URL: strings.TrimRight(serverURL, "/"),
				Headers: map[string]string{"Authorization": "Bearer {{credentials.provider_token}}"},
				Body:    map[string]any{"chat_id": "{{input.chat_id}}", "text": "{{input.text}}"},
			}), runtimecontracts.WithToolCredentials([]string{"provider_token"}...)),
		},
	}
	return bundle
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
		rows, err := selected.db.QueryContext(testAuthorActivityContext(t, context.Background()), `SELECT event_name FROM events WHERE run_id = ? ORDER BY created_at`, runID)
		if selected.postgres {
			rows, err = selected.db.QueryContext(testAuthorActivityContext(t, context.Background()), `SELECT event_name FROM events WHERE run_id = $1::uuid ORDER BY created_at`, runID)
		}
		var names []string
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var name string
				if rows.Scan(&name) == nil {
					names = append(names, name)
				}
			}
		}
		t.Fatalf("%s event count = %d, want %d; run events=%v readback_error=%v", eventType, got, want, names, err)
	}
}

func seedProposedEffectProofDelivery(t *testing.T, selected gateRecoveryStoreCase, bus *runtimebus.EventBus, evt events.Event, node runtimeidentity.ExecutableNode, entityID string) events.DeliveryRoute {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	target := events.RouteIdentity{FlowID: "support", FlowInstance: evt.RunID(), EntityID: entityID}
	owner := events.MustExistingEntityTarget(target)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: owner}
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected.events, evt, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
	proof, err := selected.events.ProveHandoff(ctx, evt.ID(), route)
	if err != nil {
		t.Fatalf("prove proposed-effect delivery handoff: %v", err)
	}
	if err := bus.SetDeliveryAuthority(proof.Authority()); err != nil {
		t.Fatalf("bind proposed-effect delivery execution authority: %v", err)
	}
	if err := bus.AcceptCommittedDeliveryHandoffs([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatalf("transfer proposed-effect delivery handoff: %v", err)
	}
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
	query := `SELECT event_id FROM events WHERE run_id = ? AND event_name = 'platform.activity_requested'`
	if selected.postgres {
		query = `SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.activity_requested'`
	}
	var eventID string
	ctx := testAuthorActivityContext(t, context.Background())
	if err := selected.db.QueryRowContext(ctx, query, runID).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	return storetest.LoadCanonicalEventRecord(t, ctx, selected.events, eventID)
}

func gateRecoveryContractBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		RootSchema:   nil,
		RootEntities: runtimecontracts.EntityContractsDocument{"test_entity": {Fields: map[string]runtimecontracts.EntityFieldDecl{}}},
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
		RootSchema:   nil,
		RootEntities: runtimecontracts.EntityContractsDocument{"test_entity": {Fields: map[string]runtimecontracts.EntityFieldDecl{}}},
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
	if tc.postgres {
		runlifecyclefixture.RequirePostgres(t, testAuthorActivityContext(t, context.Background()), tc.db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	} else {
		runlifecyclefixture.RequireSQLite(t, testAuthorActivityContext(t, context.Background()), tc.db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})
	}
}

func assertGateRecoveryActivation(t *testing.T, workflowStore *runtimepipeline.PipelineCoordinator, ctx context.Context, entityID, stage string, status gateruntime.Status) {
	t.Helper()
	instance, ok, err := workflowStore.Load(ctx, testWorkflowInstanceRoute(runtimecorrelation.RunIDFromContext(ctx)))
	if err != nil || !ok {
		t.Fatalf("Load workflow instance = %#v, %v, %v", instance, ok, err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
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
