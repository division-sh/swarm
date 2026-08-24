package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func managedCompletionTestSurface(t testing.TB, authority runtimeeffects.Authority, adapter string) managedcapabilities.Surface {
	t.Helper()
	executionKind := managedcapabilities.ExecutionNormalAgent
	executionAuthorityID := authority.ID
	runID := authority.Target.RunID
	if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		executionKind = managedcapabilities.ExecutionSelectedContractFork
		executionAuthorityID = authority.SelectedFork.ExecutionID
		if runID == "" {
			runID = authority.SelectedFork.ForkRunID
		}
	}
	transport := "api"
	switch adapter {
	case "claude_cli":
		transport = "cli"
	case "mock_python":
		transport = "in_process"
	}
	runtimeMode := "task"
	if authority.Target.Memory.Enabled {
		runtimeMode = "session"
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Target.AgentIdentity, RuntimeMode: runtimeMode,
		Provider: adapter, Transport: transport, ProviderContract: "store-test-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID,
			ExecutionKind: executionKind, ExecutionAuthorityID: executionAuthorityID,
			RunID: runID, SessionID: authority.Target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build managed completion test surface: %v", err)
	}
	return surface
}

func managedCompletionTestFrame(t testing.TB, authority runtimeeffects.Authority, adapter string) agentframe.Frame {
	return managedCompletionTestFrameWithEvent(t, authority, adapter, managedCompletionTestEvent(authority))
}

func managedCompletionTestFrameWithEvent(t testing.TB, authority runtimeeffects.Authority, adapter string, event events.Event) agentframe.Frame {
	t.Helper()
	surface := managedCompletionTestSurface(t, authority, adapter)
	intent, err := agentintent.Resolve(
		agentintent.SourceInline,
		"inline",
		"agents.yaml#agents.store-test.intent",
		"Complete the admitted store persistence test.",
	)
	if err != nil {
		t.Fatalf("resolve managed completion test intent: %v", err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("render managed completion test prompt: %v", err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatalf("assemble managed completion test provider prompt: %v", err)
	}
	frame, err := agentframe.Complete(agentframe.SessionSeed{
		AgentIdentity:  authority.Target.AgentIdentity,
		Role:           "store-test",
		Intent:         intent,
		ProviderPrompt: providerPrompt,
		RuntimeMode:    surface.RuntimeMode,
		Provider:       surface.Provider,
		Transport:      surface.Transport,
		ModelAlias:     "regular",
		Model:          "store-test-model",
	}, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event}, agentframe.Completion{
		BundleHash:   "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		BundleSource: "ephemeral",
		Surface:      surface,
	})
	if err != nil {
		t.Fatalf("complete managed completion test frame: %v", err)
	}
	return frame
}

func managedCompletionTestEvent(authority runtimeeffects.Authority) events.Event {
	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-completion-event:"+authority.Target.ID)).String()
	return managedCompletionTestEventWithIdentity(authority, eventID, "completion.test.requested")
}

func managedCompletionTestEventWithIdentity(authority runtimeeffects.Authority, eventID, eventType string) events.Event {
	return eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(
		eventID,
		events.EventType(eventType),
		"operator",
		"store-test",
		json.RawMessage(`{ "request": "store-test" }`),
		0,
		authority.Target.RunID,
		events.EventEnvelope{},
		events.RoutingSource{},
		time.Unix(1, 0).UTC(),
		authority.ExecutionMode,
	)
}

func beginManagedCompletionForTest(t testing.TB, ctx context.Context, adapter string, request []byte) (*runtimeeffects.Handle, error) {
	t.Helper()
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok {
		t.Fatal("managed completion test authority is missing")
	}
	event := managedCompletionTestEvent(authority)
	if causal, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		event = causal
	}
	return runtimeeffects.BeginManagedCompletion(ctx, adapter, request, managedCompletionTestFrameWithEvent(t, authority, adapter, event), nil)
}

func managedExecutionStoreTestContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"store-test-authority",
		1,
		"",
		"store-test-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed execution store test admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedNormalEffectStoreTestContext(t testing.TB, ctx context.Context, authority runtimeeffects.Authority) context.Context {
	t.Helper()
	ctx = managedExecutionStoreTestContext(t, ctx)
	admission, _ := managedexecution.FromContext(ctx)
	principal, err := authority.Normal.Identity.Fingerprint()
	if err != nil {
		t.Fatalf("fingerprint normal managed-effect identity: %v", err)
	}
	turnID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-turn:"+principal)).String()
	sessionID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-session:"+principal)).String()
	runID := managedNormalEffectStoreTestRunID(authority.Normal.AgentID)
	target := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID, AgentID: authority.Normal.AgentID,
		AgentIdentity: authority.Normal.Identity, SessionID: sessionID, Memory: agentmemory.PlatformDefault(),
		FlowInstance: authority.Normal.Identity.FlowInstance(),
	}
	ctx = runtimeeffects.WithUsageTarget(ctx, target)
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Normal.Identity, RuntimeMode: "task", Provider: "store-test", Transport: "api",
		ProviderContract: "store-test-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: admission.ExecutionAuthorityID,
			RunID: runID, SessionID: sessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build normal managed-effect test surface: %v", err)
	}
	return managedcapabilities.WithContext(ctx, surface)
}

func managedNormalEffectStoreTestRunID(agentID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("managed-effect-run:"+agentID)).String()
}

func managedSelectedExecutionStoreTestContext(t testing.TB, ctx context.Context, authority runtimeeffects.Authority) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork,
		authority.SelectedFork.ExecutionID,
		authority.SelectedFork.Generation,
		authority.SelectedFork.ForkRunID,
		"store-test-selected-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build selected managed execution store test admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

// persistManagedAgentTurnReadbackFixture creates reader evidence through the
// production authorization and settlement owners. Positive frameless rows are
// not a supported fixture surface.
func persistManagedAgentTurnReadbackFixture(t testing.TB, ctx context.Context, store completionSettlementTestStore, rec runtimellm.AgentTurnRecord) error {
	return persistManagedAgentTurnReadbackFixtureWithOptions(t, ctx, store, rec, managedAgentTurnFixtureOptions{})
}

type managedAgentTurnFixtureOptions struct {
	TurnID      string
	Now         time.Time
	Usage       *runtimeeffects.CompletionUsage
	OriginEvent *events.Event
}

func seedManagedTurnFixtureAgent(t testing.TB, ctx context.Context, store completionSettlementTestStore, agentID, flowInstance string) agentmemory.Identity {
	t.Helper()
	identity := testAgentMemoryIdentity(t, uuid.NewString(), agentID, flowInstance)
	memory := agentmemory.PlatformDefault()
	if strings.TrimSpace(flowInstance) != "" {
		memory = agentmemory.Authored(true)
	}
	if err := agentfixture.Upsert(t, ctx, store, runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
			ExecutionMode: "live", ID: agentID, Identity: identity.Agent, Role: "worker", Type: "managed",
			Model: "regular", LLMBackend: "anthropic", ResolvedLLMBackend: "anthropic",
			Memory: memory, FlowID: "global", FlowPath: flowInstance,
		}),
		Status: "active", StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed managed turn fixture agent: %v", err)
	}
	return identity
}

func persistManagedAgentTurnReadbackFixtureWithOptions(t testing.TB, ctx context.Context, store completionSettlementTestStore, rec runtimellm.AgentTurnRecord, options managedAgentTurnFixtureOptions) error {
	t.Helper()
	fixtureCtx := testAuthorActivityContext()
	plan, err := rec.Memory.Normalize()
	if err != nil {
		return err
	}
	if rec.Identity == (agentmemory.Identity{}) {
		rec.Identity = testAgentMemoryIdentity(t, rec.RunID, rec.AgentID, rec.FlowInstance)
	}
	identity := rec.Identity.Normalize()
	if strings.TrimSpace(rec.SessionID) == "" {
		return fmt.Errorf("session_id is required")
	}
	if err := identity.ValidateOwner(); err != nil {
		return err
	}
	if plan.Enabled {
		if err := identity.Validate(); err != nil {
			return err
		}
	}
	if strings.TrimSpace(rec.RunID) != identity.RunID ||
		strings.TrimSpace(rec.AgentID) != identity.AgentID() ||
		strings.Trim(strings.TrimSpace(rec.FlowInstance), "/") != identity.FlowInstance() {
		return fmt.Errorf("agent turn display fields do not match concrete identity")
	}
	rec = runtimellm.CanonicalizeTurnForPersistence(rec)
	if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocks(rec.TurnBlocks); err != nil {
		return fmt.Errorf("validate canonical runtime_log turn_blocks: %w", err)
	}
	lifecycle, found, err := store.LoadAgentLifecycleState(fixtureCtx, identity.Agent)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("managed turn fixture agent lifecycle is missing")
	}
	now := options.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	turnID := strings.TrimSpace(options.TurnID)
	if turnID == "" {
		turnID = uuid.NewString()
	}
	authority := runtimeeffects.NormalAgentAuthority(runtimeeffects.LifecycleToken{
		Identity: identity.Agent, AgentID: rec.AgentID,
		RuntimeEpoch: lifecycle.RuntimeEpoch, Generation: lifecycle.Generation,
	}, "store-test-owner", now.Add(time.Hour))
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: rec.RunID,
		AgentID: rec.AgentID, AgentIdentity: identity.Agent, SessionID: rec.SessionID,
		Memory: plan, FlowInstance: rec.FlowInstance, EntityID: rec.EntityID,
	}
	adapter := "anthropic_api"
	surface := managedCompletionTestSurface(t, authority, adapter)
	rec.CapabilitySurface = &surface
	eventID := strings.TrimSpace(rec.TriggerEventID)
	if eventID == "" {
		eventID = uuid.NewString()
	}
	eventType := strings.TrimSpace(rec.TriggerEventType)
	if eventType == "" {
		eventType = "completion.test.requested"
	}
	originEvent := managedCompletionTestEventWithIdentity(authority, eventID, eventType)
	if options.OriginEvent != nil {
		originEvent = *options.OriginEvent
		if originEvent.ID() != eventID || string(originEvent.Type()) != eventType || originEvent.RunID() != rec.RunID {
			return fmt.Errorf("managed turn fixture origin event does not match turn coordinates")
		}
	}
	origin, hasOrigin := runtimedelivery.ClaimFromContext(ctx)
	if !hasOrigin {
		origin = claimCompletionOriginEventForTest(t, fixtureCtx, store, authority, originEvent)
	}
	completionCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(fixtureCtx, authority), newCompletionControllerForTest(store))
	completionCtx = runtimedelivery.WithClaim(completionCtx, origin)
	completionCtx = runtimeeffects.WithLogicalOperationIdentity(completionCtx, "managed-turn-fixture:"+authority.Target.ID)
	completionCtx = withManagedCompletionTestSurface(t, completionCtx, authority, adapter)
	completionCtx = runtimecorrelation.WithInboundEvent(completionCtx, originEvent)
	frame := managedCompletionTestFrameWithEvent(t, authority, adapter, originEvent)
	handle, err := runtimeeffects.BeginManagedCompletion(completionCtx, adapter, rec.RequestPayload, frame, nil)
	if err != nil {
		return err
	}
	if err := handle.MarkLaunched(completionCtx); err != nil {
		return err
	}
	if err := handle.MarkResponseObserved(completionCtx, map[string]any{"fixture": authority.Target.ID}); err != nil {
		return err
	}
	if rec.ToolCalls == nil {
		rec.ToolCalls = []runtimellm.ToolCall{}
	}
	if rec.EmittedEvents == nil {
		rec.EmittedEvents = []string{}
	}
	if rec.TurnBlocks == nil {
		rec.TurnBlocks = []runtimellm.TurnBlock{}
	}
	toolCalls, err := json.Marshal(rec.ToolCalls)
	if err != nil {
		return err
	}
	emittedEvents, err := json.Marshal(rec.EmittedEvents)
	if err != nil {
		return err
	}
	turnBlocks, err := json.Marshal(rec.TurnBlocks)
	if err != nil {
		return err
	}
	latencyMS := int(rec.Latency / time.Millisecond)
	if latencyMS < 0 {
		latencyMS = 0
	}
	zero := int64(0)
	usage := runtimeeffects.CompletionUsage{
		ResolvedModel: "store-test-model", Exactness: runtimeeffects.CompletionUsageExact,
		InputTokens: &zero, OutputTokens: &zero,
	}
	if options.Usage != nil {
		usage = *options.Usage
	}
	state := runtimeeffects.StateSettled
	settlementFailure := rec.Failure
	if settlementFailure != nil {
		state = runtimeeffects.StateTerminalFailure
		usage = runtimeeffects.CompletionUsage{ResolvedModel: "store-test-model", Exactness: runtimeeffects.CompletionUsageUnavailable}
	}
	capabilityJSON, err := json.Marshal(surface)
	if err != nil {
		return err
	}
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: state},
		Usage:      usage,
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: authority.Target.ID, RunID: rec.RunID, AgentID: rec.AgentID, Identity: identity,
			SessionID: rec.SessionID, Memory: plan, FlowInstance: rec.FlowInstance, EntityID: rec.EntityID,
			TriggerEventID: eventID, TriggerEventType: eventType, TaskID: rec.TaskID,
			CapabilitySurfaceID: surface.ID, CapabilitySurface: capabilityJSON,
			ToolCalls: toolCalls, EmittedEvents: emittedEvents, RequestPayload: rec.RequestPayload,
			ResponsePayload: rec.ResponseRaw, TurnBlocks: turnBlocks, ParseOK: rec.ParseOK,
			LatencyMS: latencyMS, RetryCount: rec.RetryCount, Failure: settlementFailure,
		},
		Spend: runtimeeffects.CompletionSpend{
			EntityID: rec.EntityID, FlowInstance: rec.FlowInstance, AgentID: rec.AgentID, AgentIdentity: identity.Agent,
			Model: "regular", ModelAlias: "regular", BackendProfile: "store-test", Provider: adapter,
			Transport: surface.Transport, ResolvedModel: "store-test-model", CostUSD: 0, InvocationType: "agent_turn",
		},
		Now: now,
	}
	if settlementFailure != nil {
		settlement.Settlement.Failure = settlementFailure
	}
	result, err := handle.SettleCompletion(completionCtx, settlement)
	if err != nil {
		return err
	}
	if !result.Committed {
		return fmt.Errorf("managed turn fixture settlement did not commit")
	}
	if !hasOrigin {
		if _, err := store.SettleSuccess(fixtureCtx, origin, nil, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
			return fmt.Errorf("settle managed turn fixture origin: %w", err)
		}
	}
	return nil
}

func withManagedCompletionTestSurface(t testing.TB, ctx context.Context, authority runtimeeffects.Authority, adapter string) context.Context {
	t.Helper()
	kind := managedexecution.KindNormalRuntime
	executionAuthorityID := authority.ID
	generation := authority.FenceGeneration
	runID := ""
	if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
		kind = managedexecution.KindSelectedContractFork
		executionAuthorityID = authority.SelectedFork.ExecutionID
		generation = authority.SelectedFork.Generation
		runID = authority.SelectedFork.ForkRunID
	}
	admission, err := managedexecution.New(
		kind,
		executionAuthorityID,
		generation,
		runID,
		"store-test-completion-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed completion test admission: %v", err)
	}
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = managedcapabilities.WithContext(ctx, managedCompletionTestSurface(t, authority, adapter))
	if _, ok := runtimecorrelation.InboundEventFromContext(ctx); !ok {
		ctx = runtimecorrelation.WithInboundEvent(ctx, managedCompletionTestEvent(authority))
	}
	return ctx
}

func applyManagedCompletionTestSurface(t testing.TB, turn *runtimeeffects.CompletionAgentTurn, authority runtimeeffects.Authority, adapter string) {
	t.Helper()
	if turn == nil {
		t.Fatal("completion test turn is missing")
	}
	surface := managedCompletionTestSurface(t, authority, adapter)
	raw, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed completion test surface: %v", err)
	}
	turn.CapabilitySurfaceID = surface.ID
	turn.CapabilitySurface = raw
	event := managedCompletionTestEvent(authority)
	turn.TriggerEventID = event.ID()
	turn.TriggerEventType = string(event.Type())
}

func applyManagedCompletionContextSurface(t testing.TB, ctx context.Context, turn *runtimeeffects.CompletionAgentTurn) {
	t.Helper()
	if turn == nil {
		t.Fatal("completion test turn is missing")
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("managed completion test surface is missing")
	}
	raw, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed completion context surface: %v", err)
	}
	turn.CapabilitySurfaceID = surface.ID
	turn.CapabilitySurface = raw
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok {
		t.Fatal("managed completion test authority is missing")
	}
	event := managedCompletionTestEvent(authority)
	turn.TriggerEventID = event.ID()
	turn.TriggerEventType = string(event.Type())
}

type managedCapabilityTestStore interface {
	SaveManagedCapabilitySurface(context.Context, managedcapabilities.Surface) error
}

func seedManagedAgentTurnCapabilitySurface(
	t testing.TB,
	store managedCapabilityTestStore,
	runID string,
	identity agentidentity.Identity,
	sessionID, turnID, runtimeMode, scopeKey string,
) string {
	t.Helper()
	now := time.Unix(1, 0).UTC()
	authority := runtimeeffects.NormalAgentAuthority(
		runtimeeffects.LifecycleToken{
			RuntimeEpoch: 1,
			Identity:     identity,
			AgentID:      identity.AgentID(),
			Generation:   1,
		},
		"store-test-owner",
		now.Add(time.Hour),
	)
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID,
		AgentID: identity.AgentID(), AgentIdentity: identity, SessionID: sessionID,
		Memory: agentmemory.PlatformDefault(), FlowInstance: identity.FlowInstance(),
		EntityID: scopeKey,
	}
	if runtimeMode != "task" {
		authority.Target.Memory = agentmemory.Authored(true)
	}
	surface := managedCompletionTestSurface(t, authority, "anthropic_api")
	if err := store.SaveManagedCapabilitySurface(context.Background(), surface); err != nil {
		t.Fatalf("seed managed agent-turn capability surface: %v", err)
	}
	return surface.ID
}

func TestCompletionRecoveryRejectsSameSlugSiblingCapabilityPrincipal(t *testing.T) {
	identityA := testAgentIdentity(t, "recovery-worker", "review/inst-a")
	identityB := testAgentIdentity(t, "recovery-worker", "review/inst-b")
	targetA := runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(),
		AgentID: identityA.AgentID(), AgentIdentity: identityA, SessionID: uuid.NewString(),
		Memory: agentmemory.PlatformDefault(), FlowInstance: identityA.FlowInstance(),
	}
	authorityFor := func(identity agentidentity.Identity) runtimeeffects.Authority {
		target := targetA
		target.AgentIdentity = identity
		target.FlowInstance = identity.FlowInstance()
		token := runtimeeffects.LifecycleToken{
			RuntimeEpoch: 1, Identity: identity, AgentID: identity.AgentID(), Generation: 1,
		}
		authority := runtimeeffects.NormalAgentAuthority(token, "recovery-test-owner", time.Now().UTC().Add(time.Minute))
		authority.Target = target
		return authority
	}
	surfaceA := managedCompletionTestSurface(t, authorityFor(identityA), "anthropic_api")
	surfaceB := managedCompletionTestSurface(t, authorityFor(identityB), "anthropic_api")
	evidence := completionRecoveryAuthorityEvidence{
		ActorTokenID: identityA.AgentID(), ExecutionMode: string(runtimeeffects.ExecutionModeLive),
	}
	evidence.UsageTarget.Kind = string(targetA.Kind)
	evidence.UsageTarget.ID = targetA.ID
	evidence.UsageTarget.RunID = targetA.RunID
	evidence.UsageTarget.AgentID = targetA.AgentID
	evidence.UsageTarget.AgentIdentity = targetA.AgentIdentity
	evidence.UsageTarget.SessionID = targetA.SessionID
	evidence.UsageTarget.MemoryEnabled = targetA.Memory.Enabled
	evidence.UsageTarget.MemorySource = string(targetA.Memory.Source)
	evidence.UsageTarget.FlowInstance = targetA.FlowInstance
	authorityEvidence, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal recovery authority evidence: %v", err)
	}
	recovered := completionRecoveryAttempt{
		OperationID: uuid.NewString(), AttemptID: uuid.NewString(),
		AuthorityKind: string(runtimeeffects.AuthorityNormalAgent), AuthorityID: identityA.AgentID(),
		AuthorityEvidence: string(authorityEvidence),
		OperationMode:     string(runtimeeffects.ExecutionModeLive), AttemptMode: string(runtimeeffects.ExecutionModeLive),
		Adapter: "anthropic_api", Transport: "api", State: string(runtimeeffects.StateAuthorized),
		TargetKind: string(targetA.Kind), TargetID: targetA.ID,
	}
	frameBytes, err := agentframe.EncodeDurable(managedCompletionTestFrame(t, authorityFor(identityA), "anthropic_api"))
	if err != nil {
		t.Fatalf("encode recovery execution frame: %v", err)
	}
	recovered.AgentFrame = frameBytes
	identityFields, err := identityA.StorageFields()
	if err != nil {
		t.Fatalf("encode recovery identity: %v", err)
	}
	recovered.AgentID = identityFields.AgentID
	recovered.AgentNameOwner = identityFields.NameOwner
	recovered.AgentNameSource = identityFields.NameSource
	recovered.AgentRoutePresence = identityFields.RoutePresence
	recovered.FlowScopeKey = identityFields.FlowScopeKey
	recovered.FlowInstanceID = identityFields.FlowInstanceID
	recovered.FlowInstance = identityFields.FlowInstancePath
	recovered.RuntimeEpoch = 1
	recovered.Generation = 1
	recovered.OriginKind = string(runtimeeffects.CompletionOriginDelivery)
	recovered.OriginDeliveryID = uuid.NewString()
	recovered.OriginRunID = targetA.RunID
	recovered.OriginRouteIdentity = "managed-capability-recovery-origin"
	recovered.OriginClaimToken = uuid.NewString()
	recovered.OriginClaimVersion = 1
	recovered.OriginSubscriber = identityA.AgentID()
	encodeSurface := func(surface managedcapabilities.Surface) string {
		raw, err := json.Marshal(surface)
		if err != nil {
			t.Fatalf("marshal recovery capability surface: %v", err)
		}
		return string(raw)
	}

	recovered.CapabilitySurfaceID = surfaceB.ID
	recovered.CapabilitySurface = encodeSurface(surfaceB)
	if _, _, err := completionRecoverySettlement(
		recovered, runtimeeffects.StateTerminalFailure, nil, time.Now().UTC(),
	); err == nil {
		t.Fatal("completion recovery accepted a same-slug sibling capability principal")
	}

	recovered.CapabilitySurfaceID = surfaceA.ID
	recovered.CapabilitySurface = encodeSurface(surfaceA)
	if _, settlement, err := completionRecoverySettlement(
		recovered, runtimeeffects.StateTerminalFailure, nil, time.Now().UTC(),
	); err != nil {
		t.Fatalf("completion recovery rejected exact capability principal: %v", err)
	} else if settlement.AgentTurn != nil {
		t.Fatalf("authorized recovery materialized agent turn = %#v", settlement.AgentTurn)
	}

	recovered.State = string(runtimeeffects.StateLaunched)
	if _, settlement, err := completionRecoverySettlement(
		recovered, runtimeeffects.StateOutcomeUncertain, nil, time.Now().UTC(),
	); err != nil {
		t.Fatalf("launched completion recovery rejected exact capability principal: %v", err)
	} else if settlement.AgentTurn == nil || settlement.AgentTurn.Identity.Agent != identityA {
		t.Fatalf("recovered launched agent turn = %#v", settlement.AgentTurn)
	}
}
