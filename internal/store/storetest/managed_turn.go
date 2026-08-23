package storetest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

const managedTurnFixtureBundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type ManagedAgentTurnFixtureStore interface {
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	DeliveryLifecycleStore
	LoadAgentLifecycleState(context.Context, agentidentity.Identity) (runtimemanager.AgentLifecycleState, bool, error)
	SettleSuccess(context.Context, runtimedelivery.Claim, []string, time.Duration) (runtimedelivery.Snapshot, error)
}

type ManagedAgentTurnFixture struct {
	Store             ManagedAgentTurnFixtureStore
	Selected          any
	Identity          agentidentity.Identity
	RunID             string
	SessionID         string
	TurnID            string
	Memory            agentmemory.Plan
	EntityID          string
	TaskID            string
	Event             events.Event
	DeliveryRoute     *events.DeliveryRoute
	DeliveryCommitted bool
	TurnBlocks        []runtimellm.TurnBlock
	ParseOK           bool
	Latency           time.Duration
	CreatedAt         time.Time
	KeepDelivery      bool
	ExecutionMode     executionmode.Mode
	Adapter           string
}

type ManagedAgentTurnFixtureResult struct {
	Attempt runtimeeffects.Attempt
	Frame   agentframe.Frame
	Claim   runtimedelivery.Claim
}

type managedTurnFixtureSpendProjector struct{}

func (managedTurnFixtureSpendProjector) ProjectCommittedCompletionSpend(context.Context, runtimeeffects.CompletionSpendProjection) {
}

// PersistManagedAgentTurnFixture creates positive turn evidence through the
// same operation-owned authorization and settlement path used in production.
func PersistManagedAgentTurnFixture(t testing.TB, ctx context.Context, fixture ManagedAgentTurnFixture) ManagedAgentTurnFixtureResult {
	t.Helper()
	if fixture.Store == nil || fixture.Selected == nil {
		t.Fatal("managed turn fixture requires selected store owners")
	}
	bundleSource, err := runtimecorrelation.NewEphemeralBundleSourceFact(SemanticFixtureBundleHash)
	if err != nil {
		t.Fatalf("build managed turn fixture bundle source: %v", err)
	}
	const runtimeInstanceID = "00000000-0000-4000-8000-000000000001"
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, bundleSource)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeInstanceID, SemanticFixtureBundleHash))
	identity := fixture.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		t.Fatalf("managed turn fixture identity: %v", err)
	}
	memory, err := fixture.Memory.Normalize()
	if err != nil {
		t.Fatalf("managed turn fixture memory: %v", err)
	}
	if strings.TrimSpace(fixture.RunID) == "" || strings.TrimSpace(fixture.SessionID) == "" || strings.TrimSpace(fixture.TurnID) == "" {
		t.Fatal("managed turn fixture requires run, session, and turn IDs")
	}
	if fixture.Event.ID() == "" || fixture.Event.RunID() != strings.TrimSpace(fixture.RunID) {
		t.Fatal("managed turn fixture event does not match run")
	}
	now := fixture.CreatedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	lifecycle, found, err := fixture.Store.LoadAgentLifecycleState(ctx, identity)
	if err != nil || !found {
		t.Fatalf("load managed turn fixture lifecycle: found=%v err=%v", found, err)
	}
	authority := runtimeeffects.NormalAgentAuthority(runtimeeffects.LifecycleToken{
		RuntimeEpoch: lifecycle.RuntimeEpoch,
		Identity:     identity,
		AgentID:      identity.AgentID(),
		Generation:   lifecycle.Generation,
	}, "storetest-managed-turn", now.Add(time.Hour))
	mode := fixture.ExecutionMode
	if mode == "" {
		mode = fixture.Event.ExecutionMode()
	}
	if !mode.Valid() {
		t.Fatalf("managed turn fixture execution mode %q is invalid", mode)
	}
	authority.ExecutionMode = mode
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: fixture.TurnID,
		RunID: fixture.RunID, AgentID: identity.AgentID(), AgentIdentity: identity,
		SessionID: fixture.SessionID, Memory: memory, FlowInstance: identity.FlowInstance(), EntityID: fixture.EntityID,
	}
	adapter := strings.TrimSpace(fixture.Adapter)
	if adapter == "" {
		adapter = "anthropic_api"
	}
	surface := managedTurnFixtureSurface(t, authority, adapter)
	frame := managedTurnFixtureFrame(t, authority, surface, fixture.Event)

	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
	if fixture.DeliveryRoute != nil {
		route = fixture.DeliveryRoute.Normalized()
		equal, err := agentidentity.Equal(route.AgentIdentity, identity)
		if err != nil || !route.Recipient.IsAgent() || route.Recipient.ID() != identity.AgentID() || !equal {
			t.Fatalf("managed turn fixture delivery route does not match agent identity: equal=%v err=%v", equal, err)
		}
	}
	if !fixture.DeliveryCommitted {
		CommitDeliveryObligationsForPersistedEvent(t, ctx, fixture.Selected, fixture.Event, []events.DeliveryRoute{route})
	}
	claimed, err := ClaimDelivery(ctx, fixture.Store, fixture.Event, route)
	if err != nil {
		t.Fatalf("claim managed turn fixture delivery: %v", err)
	}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, authority.ID, authority.FenceGeneration, "", "storetest-managed-turn", managedTurnFixtureBundleHash, []string{surface.ID})
	if err != nil {
		t.Fatalf("build managed turn fixture admission: %v", err)
	}
	posture := executionposture.Live
	if mode == executionmode.Mock {
		posture = executionposture.MockOnly
	}
	controller := runtimeeffects.NewCompletionController(fixture.Store, fixture.Store, fixture.Store, managedTurnFixtureSpendProjector{}).WithExecutionPosture(posture)
	executionCtx := runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), controller)
	executionCtx = runtimeeffects.WithExecutionMode(executionCtx, mode)
	executionCtx = runtimeeffects.WithLogicalOperationIdentity(executionCtx, "storetest-managed-turn:"+fixture.TurnID)
	executionCtx = runtimedelivery.WithClaim(executionCtx, claimed.Claim)
	executionCtx = managedexecution.WithAdmission(executionCtx, admission)
	executionCtx = managedcapabilities.WithContext(executionCtx, surface)
	handle, err := runtimeeffects.BeginManagedCompletion(executionCtx, adapter, []byte(`{"fixture":"managed-turn"}`), frame, nil)
	if err != nil {
		t.Fatalf("authorize managed turn fixture: %v", err)
	}
	if err := handle.MarkLaunched(executionCtx); err != nil {
		t.Fatalf("launch managed turn fixture: %v", err)
	}
	if err := handle.MarkResponseObserved(executionCtx, map[string]any{"fixture": "managed-turn"}); err != nil {
		t.Fatalf("observe managed turn fixture response: %v", err)
	}
	capabilityJSON, err := json.Marshal(surface)
	if err != nil {
		t.Fatalf("marshal managed turn fixture capability: %v", err)
	}
	turnBlocks, err := json.Marshal(fixture.TurnBlocks)
	if err != nil {
		t.Fatalf("marshal managed turn fixture blocks: %v", err)
	}
	settlement := runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled},
		Usage:      runtimeeffects.CompletionUsage{ResolvedModel: "storetest-model", Exactness: runtimeeffects.CompletionUsageUnavailable},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: fixture.TurnID, RunID: fixture.RunID, AgentID: identity.AgentID(),
			Identity: agentmemory.Identity{RunID: fixture.RunID, Agent: identity}, Memory: memory,
			SessionID: fixture.SessionID, FlowInstance: identity.FlowInstance(), EntityID: fixture.EntityID,
			TriggerEventID: fixture.Event.ID(), TriggerEventType: string(fixture.Event.Type()), TaskID: fixture.TaskID,
			CapabilitySurfaceID: surface.ID, CapabilitySurface: capabilityJSON,
			ToolCalls: []byte(`[]`), EmittedEvents: []byte(`[]`), RequestPayload: []byte(`{"fixture":"managed-turn"}`),
			ResponsePayload: []byte(`{}`), TurnBlocks: turnBlocks, ParseOK: fixture.ParseOK,
			LatencyMS: int(fixture.Latency / time.Millisecond),
		},
		Spend: runtimeeffects.CompletionSpend{
			EntityID: fixture.EntityID, FlowInstance: identity.FlowInstance(), AgentID: identity.AgentID(), AgentIdentity: identity,
			Model: "regular", ModelAlias: "regular", BackendProfile: "storetest", Provider: adapter,
			Transport: "api", ResolvedModel: "storetest-model", InvocationType: "agent_turn",
		},
		Now: now,
	}
	result, err := handle.SettleCompletion(executionCtx, settlement)
	if err != nil || !result.Committed {
		t.Fatalf("settle managed turn fixture: committed=%v err=%v", result.Committed, err)
	}
	if !fixture.KeepDelivery {
		if _, err := fixture.Store.SettleSuccess(ctx, claimed.Claim, nil, fixture.Latency); err != nil {
			t.Fatalf("settle managed turn fixture delivery: %v", err)
		}
	}
	return ManagedAgentTurnFixtureResult{Attempt: handle.Attempt(), Frame: frame, Claim: claimed.Claim}
}

func managedTurnFixtureSurface(t testing.TB, authority runtimeeffects.Authority, adapter string) managedcapabilities.Surface {
	t.Helper()
	runtimeMode := "task"
	if authority.Target.Memory.Enabled {
		runtimeMode = "session"
	}
	transport := "api"
	if adapter == "mock_python" {
		transport = "in_process"
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: authority.Target.AgentIdentity, RuntimeMode: runtimeMode,
		Provider: adapter, Transport: transport, ProviderContract: "storetest-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: authority.Target.ID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: authority.ID,
			RunID: authority.Target.RunID, SessionID: authority.Target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build managed turn fixture capability: %v", err)
	}
	return surface
}

func managedTurnFixtureFrame(t testing.TB, authority runtimeeffects.Authority, surface managedcapabilities.Surface, event events.Event) agentframe.Frame {
	t.Helper()
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.storetest.intent", "Persist the admitted managed turn fixture.")
	if err != nil {
		t.Fatalf("resolve managed turn fixture intent: %v", err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("render managed turn fixture prompt: %v", err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatalf("assemble managed turn fixture prompt: %v", err)
	}
	frame, err := agentframe.Complete(agentframe.SessionSeed{
		AgentIdentity: authority.Target.AgentIdentity, Role: "storetest", Intent: intent, ProviderPrompt: providerPrompt,
		RuntimeMode: surface.RuntimeMode, Provider: surface.Provider, Transport: surface.Transport,
		ModelAlias: "regular", Model: "storetest-model",
	}, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event}, agentframe.Completion{
		BundleHash: managedTurnFixtureBundleHash, BundleSource: "persisted", Surface: surface,
	})
	if err != nil {
		t.Fatalf("complete managed turn fixture frame: %v", err)
	}
	if frame.FrameID != "agent-frame:v1:"+authority.Target.ID {
		t.Fatalf("managed turn fixture frame identity = %q", frame.FrameID)
	}
	return frame
}
