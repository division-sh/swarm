package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/tests/support/store/testutil/agentfixture"
	"github.com/google/uuid"
)

type liveAwaitProofStore interface {
	completionSettlementTestStore
	agentFixtureFlowStore
	bus.CommitPublicationOwner
	bus.PreparedPublishEventReader
	bus.EventStore
	replycontext.Store
	sessions.Registry
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
}

type liveAwaitProofFixture struct {
	store     liveAwaitProofStore
	authority effects.Authority
	origin    deliverylifecycle.Claim
	context   context.Context
	config    actors.AgentConfig
	input     events.Event
	route     events.RouteIdentity
	life      events.InstanceLife
}

// Real native publication transactions, synthetic provider evidence; no live provider or invocation-resume proof.
func TestLiveCompletionAwaitPublicationAtomicReadyOnlyBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, mode := range []string{"exact", "prefix", "same_run_other_request", "wrong_call", "non_emit", "wrong_frame", "late_reply_conflict", "duplicate_without_progress", "cancelled", "root_exact"} {
			t.Run(backend.name+"/"+mode, func(t *testing.T) {
				selected := backend.open(t).store.(liveAwaitProofStore)
				fixture := newLiveAwaitProofFixture(t, selected, mode == "root_exact")
				mode = strings.TrimPrefix(mode, "root_")
				ctx, handle, payload, messages, output := settledLiveAwaitProof(t, fixture, mode)
				event := liveAwaitEvent(t, fixture, output.EventID, "await.request", output.CreatedAt)
				record := liveAwaitRecord(fixture, event)
				command, publishCtx := liveAwaitCommand(t, fixture, ctx, event, &record)
				command.Commit.CompletionAwait = &effects.CompletionAwaitPublication{Attempt: handle.Attempt(), CallIndex: 0, CompletedResults: json.RawMessage(`[]`)}
				if mode == "prefix" {
					command.Commit.CompletionAwait.CallIndex = 1
					command.Commit.CompletionAwait.CompletedResults = json.RawMessage(`[{"tool_call_id":"first","name":"emit_request","ok":true,"result":{"retained":true}}]`)
				}
				if mode == "same_run_other_request" {
					event = liveAwaitEvent(t, fixture, uuid.NewString(), "await.request", output.CreatedAt)
					record = liveAwaitRecord(fixture, event)
					command, publishCtx = liveAwaitCommand(t, fixture, ctx, event, &record)
					command.Commit.CompletionAwait = &effects.CompletionAwaitPublication{Attempt: handle.Attempt(), CallIndex: 0, CompletedResults: json.RawMessage(`[]`)}
				}
				if mode == "wrong_call" {
					command.Commit.CompletionAwait.CallIndex = 1
				}
				if mode == "duplicate_without_progress" {
					plain := command
					plain.Commit.CompletionAwait, plain.Commit.ReplyCreations = nil, nil
					plain.Commit.DeliveryRoutes = nil
					noDelivery, settlementErr := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryNoSubscriberByDesign, command.Commit.RouteSettlement.Ledger())
					if settlementErr != nil {
						t.Fatal(settlementErr)
					}
					plain.Commit.RouteSettlement = noDelivery
					command.Commit.DeliveryRoutes, command.Commit.RouteSettlement = nil, plain.Commit.RouteSettlement
					if result, err := selected.CommitPublication(publishCtx, plain); err != nil || !result.Acknowledged {
						t.Fatalf("ordinary committed publication precondition: %+v %v", result, err)
					}
				}
				if mode == "late_reply_conflict" {
					priorEvent := liveAwaitEvent(t, fixture, uuid.NewString(), "await.request", output.CreatedAt)
					prior := liveAwaitRecord(fixture, priorEvent)
					priorCommand, priorCtx := liveAwaitCommand(t, fixture, ctx, priorEvent, &prior)
					if result, err := selected.CommitPublication(priorCtx, priorCommand); err != nil || !result.Acknowledged {
						t.Fatalf("ordinary correlation collision precondition: %+v %v", result, err)
					}
				}
				if mode == "cancelled" {
					var cancel context.CancelFunc
					publishCtx, cancel = context.WithCancel(publishCtx)
					cancel()
				}
				result, err := selected.CommitPublication(publishCtx, command)
				if mode == "exact" && err == nil && result.Acknowledged && result.AppendOutcome == bus.EventAppendInserted {
					// Both callers now see the committed occurrence; native locking
					// must serialize their duplicate readbacks without a repair/write.
					var joined sync.WaitGroup
					duplicates := make([]bus.CommittedPublication, 2)
					failures := make([]error, 2)
					for i := range duplicates {
						joined.Add(1)
						go func() {
							defer joined.Done()
							duplicates[i], failures[i] = selected.CommitPublication(publishCtx, command)
						}()
					}
					joined.Wait()
					for i, duplicate := range duplicates {
						if failures[i] != nil || !duplicate.Acknowledged || duplicate.AppendOutcome != bus.EventAppendExactDuplicate || len(duplicate.DeliveryHandoffs) != 0 {
							t.Fatalf("concurrent exact duplicate %d did not remain read-only: %+v %v", i, duplicate, failures[i])
						}
					}
				}
				if mode != "exact" && mode != "prefix" {
					if err == nil || result.Acknowledged {
						t.Fatalf("invalid await committed: mode=%s result=%+v error=%v", mode, result, err)
					}
					if mode == "duplicate_without_progress" && !strings.Contains(err.Error(), "retained completion progress") {
						t.Fatalf("did not reach the missing-progress readback: %v", err)
					}
					if (mode == "same_run_other_request" || mode == "wrong_call" || mode == "non_emit") && !strings.Contains(err.Error(), "not this exact callable emit") {
						t.Fatalf("did not reach the exact request/call/capability discriminator: %v", err)
					}
					if mode == "wrong_frame" && !strings.Contains(err.Error(), "committed agent-turn target") {
						t.Fatalf("did not reach the committed frame discriminator: %v", err)
					}
					if mode == "cancelled" && !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation was normalized into another refusal: %v", err)
					}
					if mode == "late_reply_conflict" {
						collision := fmt.Sprintf("reply context correlation collision for flow %q origin %q and correlation %q; use a unique carried correlation value for each in-flight request", record.RequesterFlowID, record.Origin.FlowInstance, record.RequestCorrelationID)
						if !strings.Contains(err.Error(), "commit reply context creation: "+collision) {
							t.Fatalf("did not reach the late correlation refusal after progress staging: %v", err)
						}
					}
					if _, loadErr := selected.LoadReplyContext(ctx, record.ID); !errors.Is(loadErr, replycontext.ErrNotFound) {
						t.Fatalf("refusal created a reply obligation: %v", loadErr)
					}
					if mode != "duplicate_without_progress" {
						if _, found, loadErr := selected.LoadPreparedPublishEvent(ctx, event.ID()); loadErr != nil || found {
							t.Fatalf("refusal left publication/delivery facts: found=%t err=%v", found, loadErr)
						}
					}
					if mode != "late_reply_conflict" {
						return
					}
					// The first request never committed. The second emit has a different
					// valid binding on the SAME attempt, so leaked first-call progress
					// would fail the native NULL-or-exact progress update, not pass it.
					surface, ok := managedcapabilities.FromContext(ctx)
					if !ok {
						t.Fatal("original settled capability surface absent")
					}
					candidates, readErr := llm.ReadCompletionOutputCandidates(payload, handle.Attempt().Adapter, handle.Attempt().OperationID, surface)
					if readErr != nil || len(candidates) != 2 || candidates[1].EventID == event.ID() || candidates[1].ToolName != "emit_other" {
						t.Fatalf("distinct callable emit after failed first request: %+v %v", candidates, readErr)
					}
					prefix, encodeErr := json.Marshal([]map[string]any{{"tool_call_id": "first", "name": "emit_request", "ok": false, "failure": map[string]any{"reason": err.Error()}}})
					if encodeErr != nil {
						t.Fatal(encodeErr)
					}
					event = liveAwaitEvent(t, fixture, candidates[1].EventID, "await.request", candidates[1].CreatedAt)
					record = liveAwaitRecord(fixture, event)
					record.RequestCorrelationID = "after-rollback"
					command, publishCtx = liveAwaitCommand(t, fixture, ctx, event, &record)
					command.Commit.CompletionAwait = &effects.CompletionAwaitPublication{Attempt: handle.Attempt(), CallIndex: 1, CompletedResults: prefix}
					result, err = selected.CommitPublication(publishCtx, command)
				}
				if err != nil || !result.Acknowledged || result.AppendOutcome != bus.EventAppendInserted || len(result.DeliveryHandoffs) != 1 {
					t.Fatalf("atomic await publication: result=%+v err=%v", result, err)
				}
				retained, err := selected.LoadReplyContext(ctx, record.ID)
				wantCall := []string{"first", "second"}[command.Commit.CompletionAwait.CallIndex]
				wantPrefix, canonicalErr := canonicaljson.Bytes(command.Commit.CompletionAwait.CompletedResults)
				if canonicalErr != nil {
					t.Fatal(canonicalErr)
				}
				if err != nil || retained.Await == nil || retained.Await.OperationID != handle.Attempt().OperationID || retained.Await.CallID != wantCall || retained.Await.RequestEventID != event.ID() || retained.Await.CallIndex != command.Commit.CompletionAwait.CallIndex || retained.Await.CompletedResults != string(wantPrefix) {
					t.Fatalf("binding absent after publication: record=%+v err=%v", retained, err)
				}
				for range 2 {
					if repeated, err := selected.CommitPublication(ctx, command); err != nil || !repeated.Acknowledged || repeated.AppendOutcome != bus.EventAppendExactDuplicate || len(repeated.DeliveryHandoffs) != 0 {
						t.Fatalf("exact duplicate must read, not repeat/repair: %+v %v", repeated, err)
					}
				}
				projection, err := json.Marshal(messages)
				if err != nil {
					t.Fatal(err)
				}
				if err := handle.ProjectCompletionConversation(ctx, effects.CompletionConversationProjection{Payload: payload, SessionID: fixture.authority.Target.SessionID, Identity: fixture.authority.Target.AgentIdentity, Memory: fixture.authority.Target.Memory, TurnCount: 1, Messages: projection}); err != nil {
					t.Fatal(err)
				}
				if err := handle.ConsumeCompletionResponse(ctx, nil, nil); err == nil || !strings.Contains(err.Error(), "pending invocation-bound reply work") {
					t.Fatalf("pending await response became consumed: %v", err)
				}
				reply := liveAwaitEvent(t, fixture, uuid.NewString(), "await.reply", output.CreatedAt.Add(time.Second))
				replyCommand, replyCtx := liveAwaitCommand(t, fixture, ctx, reply, nil)
				replyCommand.Commit.ReplyClaims = []replycontext.ClaimCommand{{Expected: retained, ReplyEventID: reply.ID()}}
				if ready, err := selected.CommitPublication(replyCtx, replyCommand); err != nil || !ready.Acknowledged || len(ready.DeliveryHandoffs) != 0 {
					t.Fatalf("immediate reply was not ready-only: %+v %v", ready, err)
				}
				ready, err := selected.LoadReplyContext(ctx, record.ID)
				if err != nil || ready.State != replycontext.StateTerminal || ready.AcceptedReplyEventID != reply.ID() || !reflect.DeepEqual(ready.Await, retained.Await) {
					t.Fatalf("reply rewrote the original binding or failed to retain readiness: %+v %v", ready, err)
				}
				if routes, err := selected.ListEventDeliveryRecipients(ctx, reply.ID()); err != nil || len(routes) != 0 {
					t.Fatalf("ready reply created an invocation delivery: %v %v", routes, err)
				}
				plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: record.RunID, At: reply.ID()})
				if err != nil {
					t.Fatalf("actual emitted await fact was not retained by historical planning: %v", err)
				}
				found := 0
				for _, captured := range plan.ReplyContexts {
					if captured.ID == ready.ID {
						found++
						if !reflect.DeepEqual(captured, ready) {
							t.Fatalf("historical plan changed the complete actual await record: got=%+v want=%+v", captured, ready)
						}
					}
				}
				if found != 1 {
					t.Fatalf("historical plan must retain the singular emitted await fact, got %d", found)
				}
			})
		}
	}
}

func newLiveAwaitProofFixture(t *testing.T, selected liveAwaitProofStore, root bool) liveAwaitProofFixture {
	t.Helper()
	documents := map[string]string{
		"schema.yaml":          "stages: []\n",
		"review/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"review/schema.yaml":   "instance: request_id\nstages:\n  active: {}\ninputs: [review.created, turn.started]\noutputs: [await.request, await.reply]\n",
		"review/state.yaml":    "request_id: text\n",
		"review/events.yaml":   "review.created:\n  request_id: text\nturn.started:\nawait.request:\nawait.reply:\n",
		"review/agents.yaml":   "reviewer:\n  model: regular\n  memory: true\n  intent: {inline: Request and retain the reply.}\n  subscriptions: [turn.started]\n",
	}
	if root {
		documents = map[string]string{
			"schema.yaml": "stages:\n  active: {}\ninputs: [turn.started]\noutputs: [await.request, await.reply]\n",
			"events.yaml": "turn.started:\nawait.request:\nawait.reply:\n",
			"agents.yaml": "reviewer:\n  model: regular\n  intent: {inline: Request and retain the reply.}\n  subscriptions: [turn.started]\n",
		}
	}
	bundle := loadLifecyclePersistenceFixtureForTest(t, documents)
	source := semanticview.Wrap(bundle)
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	run := uuid.NewString()
	fact := mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	ctx := correlation.WithSourceArtifactFact(correlation.WithRunID(storeTestWorkContext(t, testAuthorActivityContextForBundle(fact.BundleHash())), run), fact)
	ctx = originalStoreFixtureContext(t, correlation.WithRootAdmission(ctx, rooted.Admission()), selected)
	ctx = requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: run, Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash(), RootAdmission: rooted.Admission()})
	storedRoot, err := selected.LoadRunRootedSource(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	if err := storedRoot.Validate(); err != nil || !storedRoot.Admission().Equal(rooted.Admission()) {
		t.Fatalf("original run differs from its complete admitted source/root: %+v %v", storedRoot.Admission().Reference(), err)
	}
	coordinate, err := agenttopology.NewSourceCoordinate(rooted.Admission())
	if err != nil {
		t.Fatal(err)
	}
	sources, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected, sources, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	ctx = authoractivity.WithScope(correlation.WithRuntimeInstanceID(ctx, evidence.RuntimeInstanceID), authoractivity.BundleScope(evidence.RuntimeInstanceID, fact.BundleHash()))
	ctx, err = startupownership.WithRuntimeMutationAuthority(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	activationBus := &sqliteFlowActivationBus{}
	manager := ownStoreTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(activationBus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact, SemanticSource: source,
		WorkflowInstances: configureAgentFixtureFlowLifecycle(t, selected, activationBus, bundle), LLMBackend: "anthropic",
		DeliveryStore: selected, WorkOwner: storeTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(),
		PersistenceRoles: runtimemanager.PersistenceRoles{LifecycleState: selected, LifecycleOutcome: selected, AgentRoutes: activationBus, FlowActivation: agentFixtureFlowActivationCommitter{store: selected}},
	}, selected))
	admission, err := agenttopology.StaticAdmission(sources.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.InstallStartupTopology(grant, admission, sources); err != nil {
		t.Fatal(err)
	}
	request := sqliteFlowActivationRequest(bundle, ".", run, "", run)
	request.Instance = flowidentity.Stored(source, ".", run, run, run, "")
	request.OccurredAt = time.Now().UTC().Truncate(time.Microsecond)
	plan, err := manager.PrepareFlowInstanceActivation(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: selected}).CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("actual root construction: %+v %v", committed, err)
	}
	if !root {
		if len(committed.Plan.Readiness.Agents) != 0 {
			t.Fatal("child fixture's constructed root must have no declared agent")
		}
		child, err := flowidentity.KeyedChild(source, committed.Plan.Identity, "review", uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(map[string]string{"request_id": child.InstanceID})
		if err != nil {
			t.Fatal(err)
		}
		request.Instance, request.ConstructorInput, request.ResolvedKey = child, "review.created", child.InstanceID
		request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "review.created", "constructor-fixture", "", payload, 0, run, events.EventEnvelope{}, request.OccurredAt)
		plan, err = manager.PrepareFlowInstanceActivation(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		committed, err = (agentFixtureFlowActivationCommitter{store: selected}).CommitFlowInstanceActivation(ctx, plan)
	}
	if err != nil || !committed.Acknowledged || !committed.Created || len(committed.Plan.Readiness.Agents) != 1 {
		t.Fatalf("actual declared roster construction: %+v %v", committed, err)
	}
	identity := committed.Plan.Readiness.Agents[0].Identity
	declared := semanticview.AgentDeclarations(source)
	if len(declared) != 1 {
		t.Fatal("fixture requires exactly one admitted agent declaration")
	}
	namePlan, err := semanticview.ScopedAgentNamePlan(source, declared[0])
	if err != nil {
		t.Fatal(err)
	}
	name, err := namePlan.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	actorRoute, err := committed.Plan.Identity.Route().AgentIdentityRoute()
	if err != nil || identity.Name != name || identity.Route != actorRoute || identity.RunID != run {
		t.Fatalf("constructed roster differs from its exact declared name/route: %+v %v", identity, err)
	}
	if err := manager.PreparePersistedDynamicFlowRuntimeProcessTopology(ctx, flowidentity.RunScopedFlowInstance{RunID: run, Route: committed.Plan.Identity.Route()}); err != nil {
		t.Fatalf("prepare exact committed agent roster without starting manager: %v", err)
	}
	admitReceiverConfigFixtureGrant(t, ctx, grant)
	state, found, err := selected.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found || state.ConfigRevision != committed.Plan.Readiness.Agents[0].ConfigRevision {
		t.Fatalf("current actor lifecycle: %+v %t %v", state, found, err)
	}
	started, err := grant.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		DiagnosticOrigin: runfork.LifecycleDiagnosticOrigin{Owner: runfork.LifecycleDiagnosticNormal, Causality: runfork.LifecycleDiagnosticObservation},
		OperationID:      uuid.NewString(), OperationKind: "start", RequestHash: "await-native-fixture-start", Identity: identity, AgentID: identity.AgentID(), Trigger: "start",
		ExpectedEpoch: state.RuntimeEpoch, ExpectedGeneration: state.Generation, ExpectedPhase: state.Phase,
		TargetEpoch: state.RuntimeEpoch, TargetGeneration: state.Generation + 1, TargetPhase: runtimemanager.AgentLifecycleRunning,
		ConfigRevision: state.ConfigRevision, RunMode: runtimemanager.AgentRunModeStandard, Topology: state.Topology, Now: time.Now().UTC(),
	})
	if err != nil || started.Identity != identity || started.Phase != runtimemanager.AgentLifecycleRunning {
		t.Fatalf("start exact declared actor without launching a backend: %+v %v", started, err)
	}
	state, found, err = selected.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found || state.Phase != runtimemanager.AgentLifecycleRunning {
		t.Fatalf("read exact running actor: %+v %t %v", state, found, err)
	}
	agents, err := selected.LoadAgents(ctx)
	if err != nil || len(agents) != 1 || agents[0].Config.Identity != identity || agents[0].Config.ExecutionMode != effects.ExecutionModeLive {
		t.Fatalf("actual live descriptor disagrees with its admitted roster: %+v %v", agents, err)
	}
	config := agents[0].Config
	if config.Memory.Enabled == root || (identity.FlowInstance() == "") != root {
		t.Fatal("fixture lost its memory-enabled child or memory-disabled root declaration coordinate")
	}
	token := effects.LifecycleToken{RuntimeEpoch: state.RuntimeEpoch, Identity: identity, AgentID: identity.AgentID(), Generation: state.Generation}
	ctx = effects.WithLifecycleToken(ctx, token)
	lease, err := selected.Acquire(ctx, identity, "await-worker")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := selected.ReleaseOutcome(context.WithoutCancel(ctx), lease)
		if err != nil || !result.Acknowledged {
			t.Errorf("release original session: %+v %v", result, err)
		}
	})
	authority := effects.NormalAgentAuthority(token, lease.LockOwner, lease.ExpiresAt)
	authority.ExecutionMode = config.ExecutionMode
	authority.Target = effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(), AgentID: identity.AgentID(), AgentIdentity: identity, RunID: run, SessionID: lease.SessionID, Memory: config.Memory, FlowInstance: identity.FlowInstance(), EntityID: committed.Plan.Identity.EntityID}
	fixture := liveAwaitProofFixture{store: selected, authority: authority, config: config,
		route: events.RouteIdentity{FlowID: committed.Plan.Identity.ScopeKey, FlowInstance: committed.Plan.Identity.InstancePath, EntityID: committed.Plan.Identity.EntityID},
		life:  events.InstanceLife{RunID: run, FlowInstance: committed.Plan.Identity.InstancePath, Life: committed.Plan.Instance.Life}}
	scope, _, path, err := authority.BusinessTurnCoordinates()
	if err != nil || scope != fixture.route.FlowID || path != fixture.route.FlowInstance {
		t.Fatalf("exact admitted declaration target cannot project its committed business-turn coordinate: scope=%q path=%q route=%+v error=%v", scope, path, fixture.route, err)
	}
	inputType := events.EventType("turn.started")
	if !root {
		inputType = "review/turn.started"
	}
	origin := eventtest.ExistingRunRootIngress(uuid.NewString(), inputType, "operator", "", json.RawMessage(`{}`), 0, run, events.EventEnvelope{}, request.OccurredAt)
	target, err := events.MustExistingEntityTarget(fixture.route).WithLife(fixture.life.Life)
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(fixture.authority.Target.AgentID), AgentIdentity: identity, Target: target, Context: events.DeliveryContext{Life: &fixture.life}}
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, origin, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	prepared, present, err := selected.LoadPreparedPublishEvent(ctx, origin.ID())
	if err != nil || !present || prepared.Event.ID() != origin.ID() || prepared.Event.Event().RunID() != run {
		t.Fatalf("original admitted publication missing before claim: %+v %t %v", prepared, present, err)
	}
	if err := prepared.Validate(); err != nil {
		t.Fatalf("original publication lost its exact admitted routes: %v", err)
	}
	fixture.input = prepared.Event.Event()
	claimed, err := claimDeliveryFixture(ctx, selected, fixture.input, route)
	if err != nil {
		t.Fatal(err)
	}
	fixture.origin = claimed.Claim
	snapshot, err := selected.Snapshot(ctx, claimed.Claim.DeliveryID())
	if err != nil || snapshot.RunID != run || snapshot.Route.AgentIdentity != identity || snapshot.Route.Target.Route() != fixture.route || snapshot.Route.Target.Life() != fixture.life.Life || !reflect.DeepEqual(snapshot.Route.Context.Life, &fixture.life) {
		t.Fatalf("claimed original delivery differs from committed construction/actor: %+v %v", snapshot, err)
	}
	fixture.context = effects.WithController(effects.WithAuthority(ctx, fixture.authority), newCompletionControllerForTest(selected))
	fixture.context = effects.WithSessionGrant(fixture.context, effects.SessionGrant{SessionID: lease.SessionID, GrantID: lease.GrantID, LockOwner: lease.LockOwner})
	fixture.context = deliverylifecycle.WithClaim(fixture.context, fixture.origin)
	fixture.context = effects.WithTurnInstanceSource(fixture.context, source)
	fixture.context = correlation.WithInboundEvent(fixture.context, fixture.input)
	return fixture
}

func liveAwaitEvent(t *testing.T, f liveAwaitProofFixture, id, eventType string, at time.Time) events.Event {
	t.Helper()
	source := eventtest.RootRoutingSource(f.route.EntityID)
	if f.route.FlowID != "." {
		source = eventtest.ConcreteTemplateRoutingSource(f.route.FlowID, f.route.FlowInstance, f.route.EntityID)
		eventType = f.route.FlowID + "/" + eventType
	}
	source, err := source.WithLife(f.life.Life)
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.ChildWithLineageAndRoutingSource(id, events.EventType(eventType), f.authority.Target.AgentID, "", json.RawMessage(`{}`), 1,
		events.LineageFromEvent(f.input), events.EventEnvelope{}, source, at)
}

func liveAwaitRecord(f liveAwaitProofFixture, event events.Event) replycontext.Record {
	return replycontext.Record{ID: replycontext.DeterministicID(event.ID(), f.route.FlowID, "await.request", "await.reply", f.route.FlowID, f.route), RunID: event.RunID(), SourceRunID: event.RunID(), RequestEventID: event.ID(), SourceRequestEventID: event.ID(),
		RequesterFlowID: f.route.FlowID, RequestOutputPin: "await.request", ReplyInputPin: "await.reply", ProviderFlowID: f.route.FlowID, ProviderInputPin: "await.request", ProviderOutputPin: "await.reply", Origin: f.route, OriginLife: f.life.Life, RequestCorrelationID: "same-key", CorrelationKey: "id", State: replycontext.StateOpen, CreatedAt: event.CreatedAt(), UpdatedAt: event.CreatedAt()}
}

func liveAwaitCommand(t *testing.T, f liveAwaitProofFixture, ctx context.Context, event events.Event, record *replycontext.Record) (bus.PublicationCommand, context.Context) {
	t.Helper()
	ctx, release, err := semanticEventFixtureContext(ctx, f.store, event)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := pipelineObligationOwnerForFixture(f.store).ClaimPublication(ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pipelineObligationOwnerForFixture(f.store).Release(context.WithoutCancel(ctx), claim); err != nil {
			t.Error(err)
		}
	})
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryNoSubscriberByDesign, ledger)
	if err != nil {
		t.Fatal(err)
	}
	command := bus.PublicationCommand{Commit: bus.CommitPublishRequest{Event: admitted, RouteSettlement: settlement, PipelineClaim: claim, ReplayScope: pipelineobligation.ScopeSubscribed}}
	command.AuthorScope, command.HasAuthorScope = authoractivity.ScopeFromContext(ctx)
	command.AuthorDescriptor = authoractivity.EventDescriptor{EventType: string(event.Type()), Disposition: authoractivity.StoryDifferent}
	command.HasAuthorDescriptor = true
	if record != nil {
		snapshot, err := f.store.Snapshot(ctx, f.origin.DeliveryID())
		if err != nil {
			t.Fatal(err)
		}
		command.Commit.DeliveryAuthority = snapshot.Authority
		command.Commit.ReplyCreations = []replycontext.Record{*record}
		command.Commit.DeliveryRoutes = []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(f.authority.Target.AgentID), AgentIdentity: f.authority.Target.AgentIdentity, Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}}}
		command.Commit.RouteSettlement = testRouteSettlement(event, command.Commit.DeliveryRoutes)
	}
	return command, ctx
}

func settledLiveAwaitProof(t *testing.T, f liveAwaitProofFixture, mode string) (context.Context, *effects.Handle, json.RawMessage, []llm.Message, llm.CompletionOutputCandidate) {
	t.Helper()
	const adapter, request = "mock_python", "private-live-await-proof"
	nonEmit := mode == "non_emit"
	ctx := withManagedCompletionTestSurface(t, effects.WithLogicalOperationIdentity(f.context, request), f.authority, adapter)
	base := managedCompletionTestSurface(t, f.authority, adapter)
	planned := []managedcapabilities.PlannedTool{}
	for _, name := range []string{"emit_request", "emit_other"} {
		kind := toolcapabilities.KindEmit
		if nonEmit && name == "emit_request" {
			kind = toolcapabilities.KindStandard
		}
		planned = append(planned, managedcapabilities.PlannedTool{Name: name, DefinitionHash: strings.Repeat("a", 64), Capability: toolcapabilities.Capability{Name: name, Kind: kind, Callable: true, Visible: true}, Bindings: []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingAPIDefinition, ExactName: name, RequiredEvidenceKind: "api_definition"}}})
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{ActorIdentity: base.ActorIdentity, RuntimeMode: base.RuntimeMode, Provider: base.Provider, Transport: base.Transport, ProviderContract: base.ProviderContract, Authority: base.Authority, CreatedAt: base.CreatedAt, Tools: planned})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"emit_request", "emit_other"} {
		surface, err = surface.Observe(managedcapabilities.DeliveryEvidence{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: name, Kind: "api_definition", Status: managedcapabilities.EvidenceConfirmed})
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx = managedcapabilities.WithContext(ctx, surface)
	source, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("original source admission absent")
	}
	prompt, err := agentintent.IntentOnlyPrompt(f.config.Intent)
	if err != nil {
		t.Fatal(err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(f.config.Intent, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	frame, err := agentframe.Complete(agentframe.SessionSeed{AgentIdentity: f.config.Identity, Role: f.config.Role, FlowID: f.config.FlowID,
		Intent: f.config.Intent, ProviderPrompt: providerPrompt, RuntimeMode: surface.RuntimeMode, Provider: surface.Provider, Transport: surface.Transport, ModelAlias: f.config.Model, Model: f.config.ResolvedModel},
		agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: f.input}, agentframe.Completion{BundleHash: source.BundleHash(), Surface: surface})
	if err != nil {
		t.Fatal(err)
	}
	handle, err := effects.BeginManagedCompletion(ctx, adapter, []byte(request), frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !handle.Attempt().InstanceOwnership.Equal(events.InstanceOwnership{Life: &f.life}) || handle.Attempt().Origin.Delivery.DeliveryID() != f.origin.DeliveryID() || handle.Attempt().Origin.Delivery.Version() != f.origin.Version() {
		t.Fatalf("positive native admission lost exact constructed ownership or original claim: %+v", handle.Attempt())
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"synthetic": true}); err != nil {
		t.Fatal(err)
	}
	settlement := completionSettlementForFrame(t, handle.Attempt().Authority.Target, f.authority, adapter, frame)
	settlement.AgentTurn.CapabilitySurfaceID = surface.ID
	settlement.AgentTurn.CapabilitySurface, err = json.Marshal(surface)
	if err != nil {
		t.Fatal(err)
	}
	assistant := llm.Message{Role: "assistant", Content: "request", ToolCalls: []llm.ToolCall{{ID: "first", Name: "emit_request", Arguments: map[string]any{}}, {ID: "second", Name: "emit_other", Arguments: map[string]any{}}}}
	messages := []llm.Message{{Role: "user", Content: "start"}, assistant}
	response := llm.Response{Message: assistant, ToolCalls: assistant.ToolCalls, Raw: json.RawMessage(`{"synthetic":true}`), ToolOutputAuthority: &llm.ToolOutputAuthority{ProviderOperationID: handle.Attempt().OperationID, SettledAt: time.Now().UTC().Truncate(time.Microsecond)}}
	frameID := frame.FrameID
	if mode == "wrong_frame" {
		frameID = "agent-frame:v1:" + uuid.NewString()
	}
	document := map[string]any{"version": "provider-response-continuation.v1", "adapter": adapter, "response": response, "usage": settlement.Usage,
		"projection": map[string]any{"session_id": f.authority.Target.SessionID, "expected_turn_count": 0, "turn_count": 1, "messages": messages, "identity": f.authority.Target.AgentIdentity, "memory": f.authority.Target.Memory, "frame_id": frameID}}
	payload, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := effects.AttachCompletionContinuationEvidence(settlement.Settlement.Evidence, []byte(request), payload); err != nil {
		t.Fatal(err)
	}
	if result, err := handle.SettleCompletion(ctx, settlement); err != nil || !result.Committed {
		t.Fatalf("genuine native synthetic completion settlement: %+v %v", result, err)
	}
	if handle.Attempt().InstanceOwnership.Life == nil {
		t.Fatal("provider admission did not retain actual constructed-flow life")
	}
	candidates, err := llm.ReadCompletionOutputCandidates(payload, adapter, handle.Attempt().OperationID, surface)
	if err != nil {
		t.Fatal(err)
	}
	if nonEmit {
		if len(candidates) != 1 || candidates[0].ToolName != "emit_other" {
			t.Fatalf("non-emit precondition: %+v", candidates)
		}
		// Derive the SAME selected call's coordinate through the existing read
		// owner. This counterfactual surface is never persisted or placed on
		// the handle: native admission must use its frozen standard-tool plan.
		coordinateTools := append([]managedcapabilities.PlannedTool(nil), planned...)
		coordinateTools[0].Capability.Kind = toolcapabilities.KindEmit
		coordinateSurface, err := managedcapabilities.New(managedcapabilities.Plan{ActorIdentity: base.ActorIdentity, RuntimeMode: base.RuntimeMode, Provider: base.Provider, Transport: base.Transport, ProviderContract: base.ProviderContract, Authority: base.Authority, CreatedAt: base.CreatedAt, Tools: coordinateTools})
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"emit_request", "emit_other"} {
			coordinateSurface, err = coordinateSurface.Observe(managedcapabilities.DeliveryEvidence{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: name, Kind: "api_definition", Status: managedcapabilities.EvidenceConfirmed})
			if err != nil {
				t.Fatal(err)
			}
		}
		coordinates, err := llm.ReadCompletionOutputCandidates(payload, adapter, handle.Attempt().OperationID, coordinateSurface)
		if err != nil || len(coordinates) != 2 || coordinates[0].ToolName != "emit_request" || coordinateSurface.ID == surface.ID {
			t.Fatalf("same-call hostile coordinate precondition: %+v %v", coordinates, err)
		}
		return ctx, handle, payload, messages, coordinates[0]
	}
	if len(candidates) != 2 || candidates[0].ToolName != "emit_request" {
		t.Fatalf("actual settled emit precondition: %+v", candidates)
	}
	if mode == "prefix" {
		return ctx, handle, payload, messages, candidates[1]
	}
	return ctx, handle, payload, messages, candidates[0]
}
