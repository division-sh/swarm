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
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/sessions"
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
	sessionID string
	agentID   string
}

// Real native publication transactions, synthetic provider evidence; no live provider or invocation-resume proof.
func TestLiveCompletionAwaitPublicationAtomicReadyOnlyBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, mode := range []string{"exact", "prefix", "same_run_other_request", "wrong_call", "non_emit", "wrong_frame", "late_reply_conflict", "duplicate_without_progress", "cancelled"} {
			t.Run(backend.name+"/"+mode, func(t *testing.T) {
				selected := backend.open(t).store.(liveAwaitProofStore)
				fixture := newLiveAwaitProofFixture(t, selected)
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
				if err := handle.ProjectCompletionConversation(ctx, effects.CompletionConversationProjection{Payload: payload, SessionID: fixture.sessionID, Identity: fixture.authority.Target.AgentIdentity, Memory: fixture.authority.Target.Memory, TurnCount: 1, Messages: projection}); err != nil {
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

func newLiveAwaitProofFixture(t *testing.T, selected liveAwaitProofStore) liveAwaitProofFixture {
	t.Helper()
	ctx := originalStoreFixtureContext(t, testAuthorActivityContext(), selected)
	run, agentID := uuid.NewString(), "await-agent"
	ctx = requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: run})
	identity := mustTestAgentIdentityForRun(run, agentID, "global")
	if err := agentfixture.UpsertStatic(t, ctx, selected, runtimemanager.PersistedAgent{Config: withRuntimePersistenceTestIntent(t, actors.AgentConfig{ID: agentID, Identity: identity, Role: "worker", Type: "managed", Model: "regular", LLMBackend: "claude_cli", ResolvedLLMBackend: "claude_cli", FlowID: "global", FlowPath: "global", Memory: agentmemory.Plan{Enabled: true}, ExecutionMode: "live"}), Status: "active", StartedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	state, found, err := selected.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found {
		t.Fatalf("current actor lifecycle: %+v %t %v", state, found, err)
	}
	token := effects.LifecycleToken{RuntimeEpoch: state.RuntimeEpoch, Identity: identity, AgentID: agentID, Generation: state.Generation}
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
	authority := effects.NormalAgentAuthority(token, lease.LockOwner, time.Now().UTC().Add(time.Minute))
	authority.ExecutionMode = effects.ExecutionModeLive
	authority.Target = effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(), AgentID: agentID, AgentIdentity: identity, RunID: run, SessionID: lease.SessionID, Memory: agentmemory.Plan{Enabled: true}, FlowInstance: "global"}
	fixture := liveAwaitProofFixture{store: selected, authority: authority, sessionID: lease.SessionID, agentID: agentID}
	owner := flowidentity.RunScopedFlowInstance{RunID: run, Route: flowidentity.RouteForInstancePath("global")}
	header := seedTurnTerminationHeaderForOwner(t, ctx, selected, owner, uuid.NewString(), time.Now().UTC(), contracts.FlowSchemaDocument{}, "")
	if header.Life != 1 {
		t.Fatalf("newly admitted original flow header life is not the fixture's retained first life: %d", header.Life)
	}
	fixture.authority.Target.EntityID = header.EntityID
	origin := managedCompletionTestEvent(fixture.authority)
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: identity, Context: events.DeliveryContext{Life: &events.InstanceLife{RunID: run, FlowInstance: "global", Life: header.Life}}}
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, origin, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, selected, origin, route)
	if err != nil {
		t.Fatal(err)
	}
	fixture.origin = claimed.Claim
	fixture.context = effects.WithController(effects.WithAuthority(ctx, fixture.authority), newCompletionControllerForTest(selected))
	fixture.context = effects.WithSessionGrant(fixture.context, effects.SessionGrant{SessionID: lease.SessionID, GrantID: lease.GrantID, LockOwner: lease.LockOwner})
	fixture.context = deliverylifecycle.WithClaim(fixture.context, fixture.origin)
	return fixture
}

func liveAwaitEvent(t *testing.T, f liveAwaitProofFixture, id, eventType string, at time.Time) events.Event {
	t.Helper()
	return eventtest.ChildWithLineageAndRoutingSource(id, events.EventType(eventType), f.agentID, "", json.RawMessage(`{}`), 1,
		events.LineageFromEvent(managedCompletionTestEvent(f.authority)), events.EventEnvelope{}, eventtest.StaticFlowRoutingSource("global", "global", f.authority.Target.EntityID), at)
}

func liveAwaitRecord(f liveAwaitProofFixture, event events.Event) replycontext.Record {
	route := events.RouteIdentity{FlowID: "global", FlowInstance: "global", EntityID: f.authority.Target.EntityID}
	return replycontext.Record{ID: replycontext.DeterministicID(event.ID(), "global", "await.request", "await.reply", "global", route), RunID: event.RunID(), SourceRunID: event.RunID(), RequestEventID: event.ID(), SourceRequestEventID: event.ID(),
		RequesterFlowID: "global", RequestOutputPin: "await.request", ReplyInputPin: "await.reply", ProviderFlowID: "global", ProviderInputPin: "await.request", ProviderOutputPin: "await.reply", Origin: route, OriginLife: 1, RequestCorrelationID: "same-key", CorrelationKey: "id", State: replycontext.StateOpen, CreatedAt: event.CreatedAt(), UpdatedAt: event.CreatedAt()}
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
		command.Commit.DeliveryRoutes = []events.DeliveryRoute{{Recipient: events.MustAgentDeliveryRecipient(f.agentID), AgentIdentity: f.authority.Target.AgentIdentity, Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: record.ID}}}}
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
	frame := managedCompletionTestFrameWithSurface(t, f.authority, managedCompletionTestEvent(f.authority), source.BundleHash(), surface)
	handle, err := effects.BeginManagedCompletion(ctx, adapter, []byte(request), frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkLaunched(ctx); err != nil {
		t.Fatal(err)
	}
	if err := handle.MarkResponseObserved(ctx, map[string]any{"synthetic": true}); err != nil {
		t.Fatal(err)
	}
	settlement := completionSettlementForAuthority(t, handle.Attempt().Authority.Target, f.authority, adapter)
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
		"projection": map[string]any{"session_id": f.sessionID, "expected_turn_count": 0, "turn_count": 1, "messages": messages, "identity": f.authority.Target.AgentIdentity, "memory": f.authority.Target.Memory, "frame_id": frameID}}
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
