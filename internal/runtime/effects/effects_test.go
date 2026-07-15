package effects

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestCompletionAuthorityPreservesExecutionMode(t *testing.T) {
	token := LifecycleToken{RuntimeEpoch: 1, AgentID: "agent-1", Generation: 2}
	ctx := WithExecutionMode(WithLifecycleToken(context.Background(), token), ExecutionModeMock)
	authority, ok := CompletionAuthorityFromContext(ctx)
	if !ok {
		t.Fatal("CompletionAuthorityFromContext returned no authority")
	}
	if authority.ExecutionMode != ExecutionModeMock {
		t.Fatalf("execution mode = %q, want mock", authority.ExecutionMode)
	}
	ctx = WithUsageTarget(ctx, UsageTarget{Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: "agent-1", SessionID: uuid.NewString(), Memory: agentmemory.Authored(false)})
	authority, ok = CompletionAuthorityFromContext(ctx)
	if !ok || authority.ExecutionMode != ExecutionModeMock {
		t.Fatalf("targeted authority = %#v, want mock mode preserved", authority)
	}
}

type effectStoreProbe struct {
	authorizations []AuthorizeRequest
	launches       int
}

type completionStoreProbe struct {
	effectStoreProbe
}

func (*completionStoreProbe) SettleCompletion(context.Context, Attempt, CompletionSettlement) (CompletionSettlementResult, error) {
	return CompletionSettlementResult{}, nil
}

type completionProjectionProbe struct{}

func (completionProjectionProbe) ProjectCommittedCompletionSpend(context.Context, CompletionSpendProjection) {
}

func (*effectStoreProbe) IsExternalEffectAuthorityCurrent(context.Context, Authority) (bool, error) {
	return true, nil
}

func (p *effectStoreProbe) AuthorizeExternalAttempt(_ context.Context, authority Authority, req AuthorizeRequest) (Attempt, error) {
	p.authorizations = append(p.authorizations, req)
	return authorizedProbeAttempt(authority, req), nil
}

func (p *effectStoreProbe) MarkExternalAttemptLaunched(context.Context, Attempt, time.Time) error {
	p.launches++
	return nil
}

func (*effectStoreProbe) MarkExternalAttemptResponseObserved(context.Context, Attempt, map[string]any, time.Time) error {
	return nil
}

func (*effectStoreProbe) HeartbeatCompletionAttempt(context.Context, Attempt, time.Time, time.Duration) error {
	return nil
}

func (*effectStoreProbe) SettleExternalAttempt(context.Context, Settlement) error { return nil }

func authorizedProbeAttempt(authority Authority, req AuthorizeRequest) Attempt {
	return Attempt{
		OperationID: req.OperationID, AttemptID: req.AttemptID, Token: authority.Normal, Authority: authority,
		Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport,
		Ordinal: 1, AuthorizedAt: req.Now,
	}
}

func TestBeginFailsClosedWithoutManagedLifecycleAuthority(t *testing.T) {
	if _, err := Begin(context.Background(), "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without lifecycle authority")
	}

	bypass, err := Begin(WithDifferentOwner(context.Background(), OwnerRuntimeDependency), "authored_http_tool", []byte("request"), nil)
	if err != nil {
		t.Fatalf("explicit different-owner admission: %v", err)
	}
	if bypass == nil {
		t.Fatal("explicit different-owner admission returned a nil handle")
	}
	if err := bypass.MarkLaunched(context.Background()); err != nil {
		t.Fatalf("explicit different-owner launch: %v", err)
	}
	if current, err := ProjectionCurrent(context.Background()); err == nil || current {
		t.Fatalf("missing projection authority = current %v err=%v, want fail closed", current, err)
	}
	if current, err := ProjectionCurrent(WithDifferentOwner(context.Background(), OwnerRuntimeDependency)); err != nil || !current {
		t.Fatalf("different-owner projection = current %v err=%v", current, err)
	}
	if _, err := Begin(WithDifferentOwner(context.Background(), DifferentOwner("ad_hoc_owner")), "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted through an unregistered different owner")
	}
}

func TestBeginRequiresControllerAndLogicalIdentity(t *testing.T) {
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	withToken := WithLifecycleToken(context.Background(), token)
	if _, err := Begin(withToken, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without a controller")
	}

	withController := WithController(withToken, NewController(&effectStoreProbe{}))
	if _, err := Begin(withController, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without logical operation identity")
	}
}

func TestCompletionControllerRequiresSettlementProjectionOwner(t *testing.T) {
	store := &completionStoreProbe{}
	if NewController(store).CompletionEnabled() {
		t.Fatal("generic effect controller enabled completion without a spend projection owner")
	}
	if !NewCompletionController(store, completionProjectionProbe{}).CompletionEnabled() {
		t.Fatal("completion controller with settlement and projection owners is disabled")
	}
}

func TestBeginCompletionRejectsCapabilitySurfaceFromDifferentRun(t *testing.T) {
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "test-execution-authority", 1, "", "test-actors", "test-bundle", nil)
	if err != nil {
		t.Fatalf("build managed execution admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: token.AgentID,
		SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "global",
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: target.AgentID, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "test-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: target.ID, ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: admission.ExecutionAuthorityID, RunID: uuid.NewString(), SessionID: target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build managed capability surface: %v", err)
	}
	ctx := WithLifecycleToken(context.Background(), token)
	ctx = WithController(ctx, NewCompletionController(&completionStoreProbe{}, completionProjectionProbe{}))
	ctx = WithLogicalOperationIdentity(ctx, "event-123")
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = WithUsageTarget(ctx, target)
	ctx = managedcapabilities.WithContext(ctx, surface)

	_, err = BeginCompletion(ctx, "anthropic_api", []byte("request"), nil)
	if err == nil {
		t.Fatal("provider completion accepted a capability surface from a different run")
	}
	failure, ok := runtimefailures.EnvelopeFromError(err)
	if !ok || failure.Detail.Code != "managed_effect_turn_identity_mismatch" {
		t.Fatalf("failure = %#v ok=%v, want managed_effect_turn_identity_mismatch", failure, ok)
	}
}

func TestBeginNormalEffectRejectsCrossContextCapabilitySurfacesBeforeAuthorization(t *testing.T) {
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	tests := []struct {
		name     string
		mutate   func(*UsageTarget, *UsageTarget) string
		noTarget bool
		wantCode string
	}{
		{
			name:     "missing turn target",
			noTarget: true,
			wantCode: "managed_effect_turn_identity_mismatch",
		},
		{
			name: "different lifecycle actor",
			mutate: func(authorityTarget, surfaceTarget *UsageTarget) string {
				authorityTarget.AgentID = "agent-b"
				surfaceTarget.AgentID = "agent-b"
				return "agent-b"
			},
			wantCode: "managed_effect_execution_authority_mismatch",
		},
		{
			name: "different turn",
			mutate: func(_, surfaceTarget *UsageTarget) string {
				surfaceTarget.ID = uuid.NewString()
				return token.AgentID
			},
			wantCode: "managed_effect_turn_identity_mismatch",
		},
		{
			name: "different session",
			mutate: func(_, surfaceTarget *UsageTarget) string {
				surfaceTarget.SessionID = uuid.NewString()
				return token.AgentID
			},
			wantCode: "managed_effect_turn_identity_mismatch",
		},
		{
			name: "different run",
			mutate: func(_, surfaceTarget *UsageTarget) string {
				surfaceTarget.RunID = uuid.NewString()
				return token.AgentID
			},
			wantCode: "managed_effect_turn_identity_mismatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			probe := &effectStoreProbe{}
			admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "test-execution-authority", 1, "", "test-actors", "test-bundle", nil)
			if err != nil {
				t.Fatalf("build managed execution admission: %v", err)
			}
			authorityTarget := UsageTarget{
				Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: token.AgentID,
				SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "global",
			}
			surfaceTarget := authorityTarget
			surfaceActor := token.AgentID
			if tc.mutate != nil {
				surfaceActor = tc.mutate(&authorityTarget, &surfaceTarget)
			}
			surface := normalManagedEffectSurface(t, admission, surfaceTarget, surfaceActor)

			ctx := WithLifecycleToken(context.Background(), token)
			ctx = WithController(ctx, NewController(probe))
			ctx = WithLogicalOperationIdentity(ctx, "hostile-normal-effect:"+tc.name)
			ctx = managedexecution.WithAdmission(ctx, admission)
			if !tc.noTarget {
				ctx = WithUsageTarget(ctx, authorityTarget)
			}
			ctx = managedcapabilities.WithContext(ctx, surface)

			dispatches := 0
			handle, err := Begin(ctx, "authored_http_tool", []byte("request"), nil)
			if err == nil {
				if launchErr := handle.MarkLaunched(ctx); launchErr != nil {
					t.Fatalf("hostile effect reached launch with error: %v", launchErr)
				}
				dispatches++
			}
			failure, ok := runtimefailures.EnvelopeFromError(err)
			if !ok || failure.Detail.Code != tc.wantCode {
				t.Fatalf("failure = %#v ok=%v, want %s", failure, ok, tc.wantCode)
			}
			if len(probe.authorizations) != 0 || probe.launches != 0 || dispatches != 0 {
				t.Fatalf("hostile effect authorizations=%d launches=%d dispatches=%d, want zero", len(probe.authorizations), probe.launches, dispatches)
			}
		})
	}
}

func TestBeginSelectedEffectRejectsMissingOrCrossActorTurnBeforeAuthorization(t *testing.T) {
	executionID := uuid.NewString()
	forkRunID := uuid.NewString()
	authority := Authority{
		Kind: AuthoritySelectedContractFork, ID: executionID,
		SelectedFork: SelectedContractForkAuthority{
			ExecutionID: executionID, ForkRunID: forkRunID, Generation: 1,
			AdmissionFingerprint: "test-admission", ContainerPlanFingerprint: "test-container",
			ActorCensusFingerprint: "test-actors", EffectiveConfigFingerprint: "test-config",
		},
		ExecutionOwner: "test-selected-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
		ExecutionMode: ExecutionModeLive,
	}
	admission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork, executionID, 1, forkRunID,
		"test-actors", "test-config", nil,
	)
	if err != nil {
		t.Fatalf("build selected managed execution admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: forkRunID, AgentID: "agent-a",
		SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "global",
	}

	for _, tc := range []struct {
		name         string
		withTarget   bool
		surfaceActor string
	}{
		{name: "missing turn target", surfaceActor: target.AgentID},
		{name: "different actor", withTarget: true, surfaceActor: "agent-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &effectStoreProbe{}
			testAuthority := authority
			if tc.withTarget {
				testAuthority.Target = target
			}
			surface := selectedManagedEffectSurface(t, admission, target, tc.surfaceActor)
			ctx := WithAuthority(context.Background(), testAuthority)
			ctx = WithExecutionMode(ctx, ExecutionModeLive)
			ctx = WithController(ctx, NewController(probe))
			ctx = WithLogicalOperationIdentity(ctx, "hostile-selected-effect:"+tc.name)
			ctx = managedexecution.WithAdmission(ctx, admission)
			ctx = managedcapabilities.WithContext(ctx, surface)

			dispatches := 0
			handle, beginErr := Begin(ctx, "authored_http_tool", []byte("request"), nil)
			if beginErr == nil {
				if launchErr := handle.MarkLaunched(ctx); launchErr != nil {
					t.Fatalf("hostile effect reached launch with error: %v", launchErr)
				}
				dispatches++
			}
			failure, ok := runtimefailures.EnvelopeFromError(beginErr)
			if !ok || failure.Detail.Code != "managed_effect_turn_identity_mismatch" {
				t.Fatalf("failure = %#v ok=%v, want managed_effect_turn_identity_mismatch", failure, ok)
			}
			if len(probe.authorizations) != 0 || probe.launches != 0 || dispatches != 0 {
				t.Fatalf("hostile effect authorizations=%d launches=%d dispatches=%d, want zero", len(probe.authorizations), probe.launches, dispatches)
			}
		})
	}
}

func TestCompletionSettlementRejectsTurnCoordinateMismatch(t *testing.T) {
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	authority := testAuthority(token)
	authority.Target = UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: token.AgentID,
		SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "global", EntityID: uuid.NewString(),
	}
	inputTokens, outputTokens := int64(1), int64(1)
	settlement := CompletionSettlement{
		Settlement: Settlement{State: StateSettled},
		Usage: CompletionUsage{
			ResolvedModel: "test-model", Exactness: CompletionUsageExact, InputTokens: &inputTokens, OutputTokens: &outputTokens,
		},
		AgentTurn: &CompletionAgentTurn{
			TurnID: authority.Target.ID, RunID: authority.Target.RunID, AgentID: "different-agent",
			SessionID: authority.Target.SessionID, Memory: authority.Target.Memory, FlowInstance: authority.Target.FlowInstance,
			EntityID: authority.Target.EntityID, CapabilitySurfaceID: uuid.NewString(), CapabilitySurface: []byte(`{}`),
		},
		Spend: CompletionSpend{
			FlowInstance: "global", AgentID: token.AgentID, Model: "test-model", BackendProfile: "test",
			Provider: "test", Transport: "api", ResolvedModel: "test-model", InvocationType: "task",
		},
	}
	attempt := Attempt{AttemptID: uuid.NewString(), Authority: authority, Adapter: "anthropic_api"}
	if err := settlement.Validate(attempt); err == nil {
		t.Fatal("completion settlement accepted turn evidence for a different actor")
	}
}

func TestBeginDerivesStableOperationAndAttemptIdentity(t *testing.T) {
	probe := &effectStoreProbe{}
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	ctx := WithLogicalOperationIdentity(
		WithController(WithLifecycleToken(context.Background(), token), NewController(probe)),
		"event-123",
	)
	ctx = managedEffectTestContext(t, ctx, token.AgentID)
	first, err := Begin(ctx, "authored_http_tool", []byte("request"), map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	second, err := Begin(ctx, "authored_http_tool", []byte("request"), map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatalf("second begin through probe: %v", err)
	}
	if first.Attempt().OperationID != second.Attempt().OperationID || first.Attempt().AttemptID != second.Attempt().AttemptID {
		t.Fatalf("logical replay identities differ: first=%+v second=%+v", first.Attempt(), second.Attempt())
	}
	if len(probe.authorizations) != 2 || probe.authorizations[0].RequestFingerprint != probe.authorizations[1].RequestFingerprint {
		t.Fatalf("authorizations = %#v, want stable fingerprints", probe.authorizations)
	}
}

func managedEffectTestContext(t testing.TB, ctx context.Context, agentID string) context.Context {
	t.Helper()
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "test-execution-authority", 1, "", "test-actors", "test-bundle", nil)
	if err != nil {
		t.Fatalf("build managed execution test admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: agentID,
		SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "global",
	}
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = WithUsageTarget(ctx, target)
	return managedcapabilities.WithContext(ctx, normalManagedEffectSurface(t, admission, target, agentID))
}

func normalManagedEffectSurface(t testing.TB, admission managedexecution.Admission, target UsageTarget, actorID string) managedcapabilities.Surface {
	t.Helper()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: actorID, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "test-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: target.ID, ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: admission.ExecutionAuthorityID, RunID: target.RunID, SessionID: target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build managed capability test surface: %v", err)
	}
	return surface
}

func selectedManagedEffectSurface(t testing.TB, admission managedexecution.Admission, target UsageTarget, actorID string) managedcapabilities.Surface {
	t.Helper()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: actorID, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "test-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: target.ID,
			ExecutionKind:        managedcapabilities.ExecutionSelectedContractFork,
			ExecutionAuthorityID: admission.ExecutionAuthorityID, RunID: target.RunID, SessionID: target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build selected managed capability test surface: %v", err)
	}
	return surface
}

func TestCanonicalOperationIdentitySurvivesLifecycleGenerationChange(t *testing.T) {
	ctx := WithLogicalOperationIdentity(context.Background(), "event-123")
	first, err := canonicalOperationID(ctx, testAuthority(LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}), "authored_http_tool", map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := canonicalOperationID(ctx, testAuthority(LifecycleToken{RuntimeEpoch: 8, AgentID: "agent-a", Generation: 4}), "authored_http_tool", map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("operation identity changed across lifecycle generation: %s != %s", first, second)
	}
}

func TestLogicalOperationIdentitySegmentsSeparateSiblingEffectsAndRemainStable(t *testing.T) {
	base := WithLogicalOperationIdentity(context.Background(), "event-123")
	first := WithLogicalOperationIdentitySegment(base, "provider_turn:1")
	replay := WithLogicalOperationIdentitySegment(base, "provider_turn:1")
	second := WithLogicalOperationIdentitySegment(base, "provider_turn:2")
	token := LifecycleToken{RuntimeEpoch: 7, AgentID: "agent-a", Generation: 3}
	firstID, err := canonicalOperationID(first, testAuthority(token), "anthropic_api", nil)
	if err != nil {
		t.Fatal(err)
	}
	replayID, err := canonicalOperationID(replay, testAuthority(token), "anthropic_api", nil)
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := canonicalOperationID(second, testAuthority(token), "anthropic_api", nil)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != replayID {
		t.Fatalf("same logical child changed identity: %s != %s", firstID, replayID)
	}
	if firstID == secondID {
		t.Fatalf("sibling logical children share identity: %s", firstID)
	}
}

func testAuthority(token LifecycleToken) Authority {
	return NormalAgentAuthority(token, "test-owner", time.Now().UTC().Add(time.Minute))
}
