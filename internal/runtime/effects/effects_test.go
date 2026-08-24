package effects

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestDrainedCompletionInputHasNoMutableProjectionSurface(t *testing.T) {
	typeOf := reflect.TypeOf(DrainedCompletionSettlement{})
	want := map[string]reflect.Type{
		"Settlement": reflect.TypeOf(Settlement{}),
		"Usage":      reflect.TypeOf(CompletionUsage{}),
		"AgentTurn":  reflect.TypeOf(CompletionAgentTurn{}),
		"Spend":      reflect.TypeOf(CompletionSpend{}),
		"Now":        reflect.TypeOf(time.Time{}),
	}
	if typeOf.NumField() != len(want) {
		t.Fatalf("drained completion fields=%d, want exactly %d immutable evidence fields", typeOf.NumField(), len(want))
	}
	for name, fieldType := range want {
		field, ok := typeOf.FieldByName(name)
		if !ok || field.Type != fieldType {
			t.Fatalf("drained completion field %s=%v present=%v, want %v", name, field.Type, ok, fieldType)
		}
	}
	for _, forbidden := range []string{"ProviderHead", "Conversation", "Session", "Watchdog", "Output"} {
		if _, ok := typeOf.FieldByName(forbidden); ok {
			t.Fatalf("drained completion exposes forbidden mutable field %s", forbidden)
		}
	}
}

func effectLifecycleToken(t testing.TB, runtimeEpoch int64, agentID string, generation uint64) LifecycleToken {
	t.Helper()
	return LifecycleToken{
		Identity:     agentidentitytest.Runtime(t, agentID, "effects-test", "effects-test", "instance", "effects-test/instance"),
		RuntimeEpoch: runtimeEpoch,
		AgentID:      agentID,
		Generation:   generation,
	}
}

func TestCompletionAuthorityPreservesExecutionMode(t *testing.T) {
	token := effectLifecycleToken(t, 1, "agent-1", 2)
	ctx := WithExecutionMode(WithLifecycleToken(context.Background(), token), ExecutionModeMock)
	authority, ok := CompletionAuthorityFromContext(ctx)
	if !ok {
		t.Fatal("CompletionAuthorityFromContext returned no authority")
	}
	if authority.ExecutionMode != ExecutionModeMock {
		t.Fatalf("execution mode = %q, want mock", authority.ExecutionMode)
	}
	ctx = WithUsageTarget(ctx, UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: "agent-1",
		AgentIdentity: token.Identity, SessionID: uuid.NewString(), Memory: agentmemory.Authored(false),
		FlowInstance: token.Identity.FlowInstance(),
	})
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

func (*completionStoreProbe) RecoverCompletionContinuation(context.Context, CompletionContinuationRequest) (Attempt, bool, error) {
	return Attempt{}, false, nil
}

func (*completionStoreProbe) ProjectCompletionConversation(context.Context, Attempt, CompletionConversationProjection) error {
	return nil
}

func (*completionStoreProbe) ConsumeCompletionResponse(context.Context, Attempt, *agentframe.ToolContinuation) error {
	return nil
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
	token := effectLifecycleToken(t, 7, "agent-a", 3)
	withToken := WithLifecycleToken(context.Background(), token)
	if _, err := Begin(withToken, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without a controller")
	}

	withController := WithController(withToken, NewController(&effectStoreProbe{}).WithExecutionPosture(executionposture.Live))
	if _, err := Begin(withController, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("managed effect was admitted without logical operation identity")
	}
}

func TestCompletionControllerRequiresSettlementProjectionOwner(t *testing.T) {
	store := &completionStoreProbe{}
	if NewController(store).CompletionEnabled() {
		t.Fatal("generic effect controller enabled completion without a spend projection owner")
	}
	if !NewCompletionController(store, store, store, completionProjectionProbe{}).CompletionEnabled() {
		t.Fatal("completion controller with settlement and projection owners is disabled")
	}
}

func TestCompletionContinuationCannotDispatchOrResettle(t *testing.T) {
	store := &completionStoreProbe{}
	token := effectLifecycleToken(t, 7, "agent-a", 3)
	ctx := managedEffectTestContext(t, WithLifecycleToken(context.Background(), token), token.AgentID)
	authority, ok := CompletionAuthorityFromContext(ctx)
	if !ok {
		t.Fatal("completion authority missing")
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("capability surface missing")
	}
	evidence := map[string]any{}
	if err := AttachCompletionContinuationEvidence(evidence, []byte("request"), json.RawMessage(`{"version":"continuation"}`)); err != nil {
		t.Fatalf("attach completion continuation: %v", err)
	}
	rawEvidence, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal completion continuation evidence: %v", err)
	}
	attempt, err := AdmitCompletionContinuation(Attempt{
		OperationID: "operation", AttemptID: "attempt", Kind: KindProviderTurn, Authority: authority,
		Origin: CompletionOrigin{Kind: CompletionOriginDelivery},
	}, rawEvidence, Fingerprint([]byte("request")), surface, CompletionProjectionResponseSettled, nil)
	if err != nil {
		t.Fatalf("admit completion continuation: %v", err)
	}
	handle := &Handle{
		controller: NewCompletionController(store, store, store, completionProjectionProbe{}),
		attempt:    attempt,
	}

	checks := []struct {
		name string
		call func() error
	}{
		{name: "launch", call: func() error { return handle.MarkLaunched(context.Background()) }},
		{name: "heartbeat", call: func() error { return handle.Heartbeat(context.Background(), time.Second) }},
		{name: "response observation", call: func() error { return handle.MarkResponseObserved(context.Background(), nil) }},
		{name: "generic settlement", call: func() error { return handle.Succeed(context.Background(), nil) }},
		{name: "completion settlement", call: func() error {
			_, settleErr := handle.SettleCompletion(context.Background(), CompletionSettlement{})
			return settleErr
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); err == nil {
				t.Fatal("continuation handle reached a forbidden provider-attempt mutation")
			}
		})
	}
	if store.launches != 0 {
		t.Fatalf("provider launches = %d, want 0", store.launches)
	}
}

func TestBeginCompletionRejectsCapabilitySurfaceFromDifferentRun(t *testing.T) {
	token := effectLifecycleToken(t, 7, "agent-a", 3)
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "test-execution-authority", 1, "", "test-actors", "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", nil)
	if err != nil {
		t.Fatalf("build managed execution admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: token.AgentID,
		AgentIdentity: token.Identity, SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(),
		FlowInstance: token.Identity.FlowInstance(),
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: target.AgentIdentity, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "test-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: target.ID, ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: admission.ExecutionAuthorityID, RunID: uuid.NewString(), SessionID: target.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build managed capability surface: %v", err)
	}
	ctx := WithExecutionMode(WithLifecycleToken(context.Background(), token), ExecutionModeLive)
	store := &completionStoreProbe{}
	ctx = WithController(ctx, NewCompletionController(store, store, store, completionProjectionProbe{}).WithExecutionPosture(executionposture.Live))
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
	token := effectLifecycleToken(t, 7, "agent-a", 3)
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
			admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "test-execution-authority", 1, "", "test-actors", "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", nil)
			if err != nil {
				t.Fatalf("build managed execution admission: %v", err)
			}
			authorityTarget := UsageTarget{
				Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: token.AgentID,
				AgentIdentity: token.Identity, SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(),
				FlowInstance: token.Identity.FlowInstance(),
			}
			surfaceTarget := authorityTarget
			surfaceActor := token.AgentID
			if tc.mutate != nil {
				surfaceActor = tc.mutate(&authorityTarget, &surfaceTarget)
			}
			surface := normalManagedEffectSurface(t, admission, surfaceTarget, surfaceActor)

			ctx := WithExecutionMode(WithLifecycleToken(context.Background(), token), ExecutionModeLive)
			ctx = WithController(ctx, NewController(probe).WithExecutionPosture(executionposture.Live))
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

func TestManagedEffectRejectsSameSlugSiblingCapabilityPrincipalBeforeAuthorization(t *testing.T) {
	probe := &effectStoreProbe{}
	token := effectLifecycleToken(t, 7, "agent-a", 3)
	siblingIdentity := agentidentitytest.Runtime(
		t, token.AgentID, "effects-test", "effects-test", "sibling", "effects-test/sibling",
	)
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"test-execution-authority",
		1,
		"",
		"test-actors",
		"bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		nil,
	)
	if err != nil {
		t.Fatalf("build managed execution admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: token.AgentID,
		AgentIdentity: token.Identity, SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(),
		FlowInstance: token.Identity.FlowInstance(),
	}
	siblingTarget := target
	siblingTarget.AgentIdentity = siblingIdentity
	siblingTarget.FlowInstance = siblingIdentity.FlowInstance()

	ctx := WithExecutionMode(WithLifecycleToken(context.Background(), token), ExecutionModeLive)
	ctx = WithController(ctx, NewController(probe).WithExecutionPosture(executionposture.Live))
	ctx = WithLogicalOperationIdentity(ctx, "same-slug-sibling-capability")
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = WithUsageTarget(ctx, target)
	ctx = managedcapabilities.WithContext(ctx, normalManagedEffectSurface(t, admission, siblingTarget, token.AgentID))

	if _, err := Begin(ctx, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("same-slug sibling capability principal authorized an effect")
	}
	if len(probe.authorizations) != 0 {
		t.Fatalf("same-slug sibling reached store authorization %d times", len(probe.authorizations))
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
		"test-actors", "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", nil,
	)
	if err != nil {
		t.Fatalf("build selected managed execution admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: forkRunID, AgentID: "agent-a",
		AgentIdentity: effectLifecycleToken(t, 1, "agent-a", 1).Identity,
		SessionID:     uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "effects-test/instance",
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
			ctx = WithController(ctx, NewController(probe).WithExecutionPosture(executionposture.Live))
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

func TestAgentExecutionFrameOwnershipFailsClosedBeforeAuthorization(t *testing.T) {
	executionID := uuid.NewString()
	forkRunID := uuid.NewString()
	selectedAuthority := Authority{
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
		"test-actors", "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", nil,
	)
	if err != nil {
		t.Fatalf("build selected managed execution admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: forkRunID, AgentID: "agent-a",
		AgentIdentity: effectLifecycleToken(t, 1, "agent-a", 1).Identity,
		SessionID:     uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "effects-test/instance",
	}
	selectedAuthority.Target = target
	surface := selectedManagedEffectSurface(t, admission, target, target.AgentID)

	t.Run("managed agent turn requires frame", func(t *testing.T) {
		store := &completionStoreProbe{}
		controller := NewCompletionController(store, store, store, completionProjectionProbe{}).WithExecutionPosture(executionposture.Live)
		ctx := WithExecutionMode(WithController(WithAuthority(context.Background(), selectedAuthority), controller), ExecutionModeLive)
		ctx = WithLogicalOperationIdentity(ctx, "missing-agent-frame")
		ctx = managedexecution.WithAdmission(ctx, admission)
		ctx = managedcapabilities.WithContext(ctx, surface)
		if _, err := BeginCompletion(ctx, "anthropic_api", []byte("request"), nil); err == nil {
			t.Fatal("managed agent turn was authorized without an execution frame")
		} else if failure, ok := runtimefailures.EnvelopeFromError(err); !ok || failure.Detail.Code != "agent_execution_frame_missing_or_mismatched" {
			t.Fatalf("failure = %#v ok=%v, want agent_execution_frame_missing_or_mismatched", failure, ok)
		}
		if len(store.authorizations) != 0 {
			t.Fatalf("frame-less managed turn reached store authorization %d times", len(store.authorizations))
		}
	})

	t.Run("tool effect forbids frame", func(t *testing.T) {
		probe := &effectStoreProbe{}
		controller := NewController(probe).WithExecutionPosture(executionposture.Live)
		ctx := managedexecution.WithAdmission(WithAuthority(context.Background(), selectedAuthority), admission)
		frame := agentframe.Frame{}
		_, err := controller.Authorize(ctx, AuthorizeRequest{
			OperationID: uuid.NewString(), Adapter: "authored_http_tool", RequestFingerprint: "request-fingerprint",
			CapabilitySurface: &surface, AgentFrame: &frame,
		})
		if failure, ok := runtimefailures.EnvelopeFromError(err); !ok || failure.Detail.Code != "agent_execution_frame_owner_mismatch" {
			t.Fatalf("failure = %#v ok=%v, want agent_execution_frame_owner_mismatch", failure, ok)
		}
		if len(probe.authorizations) != 0 {
			t.Fatalf("tool frame reached store authorization %d times", len(probe.authorizations))
		}
	})

	t.Run("forkchat completion forbids frame", func(t *testing.T) {
		probe := &effectStoreProbe{}
		controller := NewController(probe).WithExecutionPosture(executionposture.Live)
		forkTurnID := uuid.NewString()
		authority := Authority{
			Kind: AuthorityConversationForkChat, ID: forkTurnID,
			ExecutionOwner: "forkchat-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
			ExecutionMode: ExecutionModeLive,
			ForkChat: ConversationForkChatAuthority{
				ForkTurnID: forkTurnID, ForkID: uuid.NewString(), BundleHash: "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				ActorTokenID: "actor-token", RequestOccurrenceID: uuid.NewString(), RequestHash: "request-hash",
			},
			Target: UsageTarget{Kind: UsageTargetConversationForkCompletion, ID: forkTurnID, Ordinal: 1},
		}
		frame := agentframe.Frame{}
		_, err := controller.Authorize(WithAuthority(context.Background(), authority), AuthorizeRequest{
			OperationID: uuid.NewString(), Adapter: "anthropic_api", RequestFingerprint: "request-fingerprint", AgentFrame: &frame,
		})
		if failure, ok := runtimefailures.EnvelopeFromError(err); !ok || failure.Detail.Code != "managed_capability_surface_owner_mismatch" {
			t.Fatalf("failure = %#v ok=%v, want managed_capability_surface_owner_mismatch", failure, ok)
		}
		if len(probe.authorizations) != 0 {
			t.Fatalf("forkchat frame reached store authorization %d times", len(probe.authorizations))
		}
	})

	t.Run("startup probe forbids frame", func(t *testing.T) {
		probe := &effectStoreProbe{}
		controller := NewController(probe).WithExecutionPosture(executionposture.Live)
		probeID := uuid.NewString()
		executionAuthorityID := uuid.NewString()
		startupSurface, err := managedcapabilities.New(managedcapabilities.Plan{
			ActorIdentity: target.AgentIdentity, RuntimeMode: "task", Provider: "claude_cli", Transport: "cli", ProviderContract: "claude_cli",
			Authority: managedcapabilities.Authority{
				Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID,
				ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: executionAuthorityID,
				StartupOwnerID: "startup-owner", StartupGeneration: 1,
			},
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("build startup surface: %v", err)
		}
		authority := Authority{
			Kind: AuthorityStartupProbe, ID: probeID, ExecutionOwner: "startup-owner",
			LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1, ExecutionMode: ExecutionModeLive,
			StartupProbe: StartupProbeAuthority{
				ProbeID: probeID, StartupAuthorityID: uuid.NewString(), StartupStateVersion: 1,
				ActorID: startupSurface.ActorID, ExecutionKind: string(managedcapabilities.ExecutionNormalAgent), ExecutionAuthorityID: executionAuthorityID,
			},
		}
		frame := agentframe.Frame{}
		_, err = controller.Authorize(WithAuthority(context.Background(), authority), AuthorizeRequest{
			OperationID: uuid.NewString(), Adapter: "claude_cli_startup_probe", RequestFingerprint: "request-fingerprint",
			CapabilitySurface: &startupSurface, AgentFrame: &frame,
		})
		if failure, ok := runtimefailures.EnvelopeFromError(err); !ok || failure.Detail.Code != "startup_probe_authority_invalid" {
			t.Fatalf("failure = %#v ok=%v, want startup_probe_authority_invalid", failure, ok)
		}
		if len(probe.authorizations) != 0 {
			t.Fatalf("startup frame reached store authorization %d times", len(probe.authorizations))
		}
	})
}

func TestManagedAgentFramePrelaunchBindingFailsClosedBeforeAuthorization(t *testing.T) {
	const (
		bundleE = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		bundleF = "bundle-v1:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	)
	tests := []struct {
		name          string
		origin        string
		admissionHash string
		bundleSource  string
		frameHash     string
		frameSource   string
		missingBundle bool
		missingEvent  bool
		mutateCausal  func(Authority, events.Event) events.Event
	}{
		{name: "missing bundle coordinate", origin: "delivery", admissionHash: bundleE, frameHash: bundleE, frameSource: "persisted", missingBundle: true},
		{name: "missing event coordinate", origin: "delivery", admissionHash: bundleE, bundleSource: "persisted", frameHash: bundleE, frameSource: "persisted", missingEvent: true},
		{name: "normal bundle hash", origin: "delivery", admissionHash: bundleF, bundleSource: "persisted", frameHash: bundleE, frameSource: "persisted"},
		{name: "normal bundle source", origin: "delivery", admissionHash: bundleE, bundleSource: "ephemeral", frameHash: bundleE, frameSource: "persisted"},
		{name: "selected bundle hash", origin: "selected", admissionHash: bundleF, bundleSource: "persisted", frameHash: bundleE, frameSource: "persisted"},
		{name: "selected bundle source", origin: "selected", admissionHash: bundleE, bundleSource: "ephemeral", frameHash: bundleE, frameSource: "persisted"},
		{name: "delivery event identity", origin: "delivery", admissionHash: bundleE, bundleSource: "persisted", frameHash: bundleE, frameSource: "persisted", mutateCausal: func(authority Authority, event events.Event) events.Event {
			return managedFrameBindingEvent(authority, uuid.NewString(), event.Type(), event.Payload())
		}},
		{name: "directive event type", origin: "directive", admissionHash: bundleE, bundleSource: "persisted", frameHash: bundleE, frameSource: "persisted", mutateCausal: func(authority Authority, event events.Event) events.Event {
			return managedFrameBindingEvent(authority, event.ID(), "effect.directive.changed", event.Payload())
		}},
		{name: "selected event run", origin: "selected", admissionHash: bundleE, bundleSource: "persisted", frameHash: bundleE, frameSource: "persisted", mutateCausal: func(authority Authority, event events.Event) events.Event {
			authority.Target.RunID = uuid.NewString()
			return managedFrameBindingEvent(authority, event.ID(), event.Type(), event.Payload())
		}},
		{name: "byte-distinct payload", origin: "delivery", admissionHash: bundleE, bundleSource: "persisted", frameHash: bundleE, frameSource: "persisted", mutateCausal: func(authority Authority, event events.Event) events.Event {
			return managedFrameBindingEvent(authority, event.ID(), event.Type(), json.RawMessage(`{"request":"effects"}`))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, frame, probe := managedFrameBindingContext(t, tc.origin, tc.admissionHash, tc.bundleSource, tc.frameHash, tc.frameSource, !tc.missingBundle, !tc.missingEvent)
			if tc.mutateCausal != nil {
				authority, ok := AuthorityFromContext(ctx)
				if !ok {
					t.Fatal("binding fixture authority is missing")
				}
				causal, ok := correlation.InboundEventFromContext(ctx)
				if !ok {
					t.Fatal("binding fixture causal event is missing")
				}
				ctx = correlation.WithInboundEvent(ctx, tc.mutateCausal(authority, causal))
			}
			dispatches := 0
			handle, err := BeginManagedCompletion(ctx, "anthropic_api", []byte("request"), frame, nil)
			if err == nil {
				if launchErr := handle.MarkLaunched(ctx); launchErr != nil {
					t.Fatalf("hostile frame reached launch with error: %v", launchErr)
				}
				dispatches++
			}
			if failure, ok := runtimefailures.EnvelopeFromError(err); !ok || failure.Detail.Code != "agent_execution_frame_authority_mismatch" {
				t.Fatalf("failure = %#v ok=%v, want agent_execution_frame_authority_mismatch", failure, ok)
			}
			if len(probe.authorizations) != 0 || probe.launches != 0 || dispatches != 0 {
				t.Fatalf("hostile frame authorizations=%d launches=%d dispatches=%d, want zero", len(probe.authorizations), probe.launches, dispatches)
			}
		})
	}
}

func TestManagedAgentFramePrelaunchBindingAcceptsExactNormalAndSelectedAuthority(t *testing.T) {
	const bundle = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	for _, origin := range []string{"delivery", "selected"} {
		t.Run(origin, func(t *testing.T) {
			ctx, frame, probe := managedFrameBindingContext(t, origin, bundle, "persisted", bundle, "persisted", true, true)
			handle, err := BeginManagedCompletion(ctx, "anthropic_api", []byte("request"), frame, nil)
			if err != nil {
				t.Fatalf("authorize exact %s frame: %v", origin, err)
			}
			if err := handle.MarkLaunched(ctx); err != nil {
				t.Fatalf("launch exact %s frame: %v", origin, err)
			}
			if len(probe.authorizations) != 1 || probe.launches != 1 {
				t.Fatalf("exact %s frame authorizations=%d launches=%d, want 1/1", origin, len(probe.authorizations), probe.launches)
			}
		})
	}
}

func managedFrameBindingContext(t testing.TB, origin, admissionHash, bundleSource, frameHash, frameSource string, includeBundle, includeEvent bool) (context.Context, agentframe.Frame, *completionStoreProbe) {
	t.Helper()
	selected := origin == "selected"
	runID := uuid.NewString()
	token := effectLifecycleToken(t, 7, "binding-agent", 3)
	authority := NormalAgentAuthority(token, "binding-owner", time.Now().UTC().Add(time.Minute))
	admissionKind := managedexecution.KindNormalRuntime
	executionID := "binding-execution"
	if selected {
		executionID = uuid.NewString()
		authority = Authority{
			Kind: AuthoritySelectedContractFork, ID: executionID,
			SelectedFork: SelectedContractForkAuthority{
				ExecutionID: executionID, ForkRunID: runID, Generation: 1,
				AdmissionFingerprint: "binding-admission", ContainerPlanFingerprint: "binding-container",
				ActorCensusFingerprint: "binding-actors", EffectiveConfigFingerprint: "binding-config",
			},
			ExecutionOwner: "binding-selected-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
			FenceGeneration: 1, ExecutionMode: ExecutionModeLive,
		}
		admissionKind = managedexecution.KindSelectedContractFork
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: runID, AgentID: token.AgentID,
		AgentIdentity: token.Identity, SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(),
		FlowInstance: token.Identity.FlowInstance(),
	}
	authority.Target = target
	admissionRunID := ""
	if selected {
		admissionRunID = runID
	}
	admission, err := managedexecution.New(admissionKind, executionID, 1, admissionRunID, "binding-actors", admissionHash, nil)
	if err != nil {
		t.Fatalf("build binding admission: %v", err)
	}
	var surface managedcapabilities.Surface
	if selected {
		surface = selectedManagedEffectSurface(t, admission, target, target.AgentID)
	} else {
		surface = normalManagedEffectSurface(t, admission, target, target.AgentID)
	}
	event := managedFrameBindingEvent(authority, uuid.NewString(), "effect.binding.requested", json.RawMessage(`{ "request": "effects" }`))
	frame := managedFrameBindingFrame(t, authority, surface, event, frameHash, frameSource)
	probe := &completionStoreProbe{}
	ctx := WithExecutionMode(WithAuthority(context.Background(), authority), ExecutionModeLive)
	ctx = WithController(ctx, NewCompletionController(probe, probe, probe, completionProjectionProbe{}).WithExecutionPosture(executionposture.Live))
	ctx = WithLogicalOperationIdentity(ctx, "binding:"+origin)
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = managedcapabilities.WithContext(ctx, surface)
	if includeBundle {
		fact, err := correlation.DecodeBundleSourceFact(admissionHash, bundleSource)
		if err != nil {
			t.Fatalf("build binding bundle source: %v", err)
		}
		ctx = correlation.WithBundleSourceFact(ctx, fact)
	}
	if includeEvent {
		ctx = correlation.WithInboundEvent(ctx, event)
	}
	switch origin {
	case "delivery":
		claim, err := deliverylifecycle.AdmitPersistedClaim(uuid.NewString(), runID, "binding-route", uuid.NewString(), 1, deliverylifecycle.SubscriberAgent, target.AgentID)
		if err != nil {
			t.Fatalf("build binding delivery claim: %v", err)
		}
		ctx = deliverylifecycle.WithClaim(ctx, claim)
	case "directive":
		ctx = WithDirectiveCompletionOrigin(ctx, agentcontrol.DirectiveExecutionOrigin{OperationID: uuid.NewString(), ExecutionOwnerID: uuid.NewString()})
	case "selected":
	default:
		t.Fatalf("unknown binding origin %q", origin)
	}
	return ctx, frame, probe
}

func managedFrameBindingEvent(authority Authority, eventID string, eventType events.EventType, payload json.RawMessage) events.Event {
	return eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(
		eventID, eventType, "operator", "binding-test", payload, 0, authority.Target.RunID,
		events.EventEnvelope{}, events.RoutingSource{}, time.Unix(1, 0).UTC(), authority.ExecutionMode,
	)
}

func managedFrameBindingFrame(t testing.TB, authority Authority, surface managedcapabilities.Surface, event events.Event, bundleHash, bundleSource string) agentframe.Frame {
	t.Helper()
	intent, err := agentintent.Resolve(agentintent.SourceInline, "inline", "agents.yaml#agents.binding.intent", "Process the admitted binding test.")
	if err != nil {
		t.Fatalf("resolve binding intent: %v", err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("render binding intent: %v", err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, nil, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatalf("assemble binding provider prompt: %v", err)
	}
	frame, err := agentframe.Complete(agentframe.SessionSeed{
		AgentIdentity: authority.Target.AgentIdentity, Role: "binding-test", Intent: intent, ProviderPrompt: providerPrompt,
		RuntimeMode: surface.RuntimeMode, Provider: surface.Provider, Transport: surface.Transport,
		ModelAlias: "regular", Model: "binding-model",
	}, agentframe.TurnDraft{Kind: agentframe.TurnInitial, Event: event}, agentframe.Completion{
		BundleHash: bundleHash, BundleSource: bundleSource, Surface: surface,
	})
	if err != nil {
		t.Fatalf("complete binding frame: %v", err)
	}
	return frame
}

func TestCompletionSettlementRejectsTurnCoordinateMismatch(t *testing.T) {
	token := effectLifecycleToken(t, 7, "agent-a", 3)
	authority := testAuthority(token)
	authority.Target = UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: token.AgentID,
		AgentIdentity: token.Identity, SessionID: uuid.NewString(), Memory: agentmemory.PlatformDefault(),
		FlowInstance: token.Identity.FlowInstance(), EntityID: uuid.NewString(),
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
			FlowInstance: token.Identity.FlowInstance(), AgentID: token.AgentID, AgentIdentity: token.Identity,
			Model: "test-model", BackendProfile: "test",
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
	token := effectLifecycleToken(t, 7, "agent-a", 3)
	ctx := WithLogicalOperationIdentity(
		WithController(WithLifecycleToken(context.Background(), token), NewController(probe).WithExecutionPosture(executionposture.Live)),
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

func TestMockOnlyPostureRejectsLiveExternalEffectBeforeAuthorization(t *testing.T) {
	probe := &effectStoreProbe{}
	token := effectLifecycleToken(t, 7, "agent-a", 3)
	ctx := WithLogicalOperationIdentity(
		WithController(
			WithExecutionMode(WithLifecycleToken(context.Background(), token), ExecutionModeLive),
			NewController(probe).WithExecutionPosture(executionposture.MockOnly),
		),
		"event-live-under-mock-only",
	)
	ctx = managedEffectTestContext(t, ctx, token.AgentID)
	if _, err := Begin(ctx, "authored_http_tool", []byte("request"), nil); err == nil {
		t.Fatal("mock_only posture admitted a live external effect")
	}
	if len(probe.authorizations) != 0 || probe.launches != 0 {
		t.Fatalf("live external effect reached authorization=%d launch=%d, want zero", len(probe.authorizations), probe.launches)
	}
}

func TestMockOnlyPostureRejectsLiveStartupProbeBeforeAuthorization(t *testing.T) {
	probe := &effectStoreProbe{}
	probeID := uuid.NewString()
	executionAuthorityID := uuid.NewString()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: effectLifecycleToken(t, 1, "startup-agent", 1).Identity,
		RuntimeMode:   "task", Provider: "claude_cli", Transport: "cli", ProviderContract: "claude_cli",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: executionAuthorityID,
			StartupOwnerID: "startup-owner", StartupGeneration: 1,
		},
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("build startup capability surface: %v", err)
	}
	authority := Authority{
		Kind: AuthorityStartupProbe, ID: probeID, ExecutionOwner: "startup-owner",
		LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1, ExecutionMode: ExecutionModeLive,
		StartupProbe: StartupProbeAuthority{
			ProbeID: probeID, StartupAuthorityID: uuid.NewString(), StartupStateVersion: 1,
			ActorID: surface.ActorID, ExecutionKind: string(managedcapabilities.ExecutionNormalAgent), ExecutionAuthorityID: executionAuthorityID,
		},
	}
	ctx := WithAuthority(context.Background(), authority)
	ctx = WithController(ctx, NewController(probe).WithExecutionPosture(executionposture.MockOnly))
	ctx = managedcapabilities.WithContext(ctx, surface)
	if _, err := BeginStartupProbe(ctx, "claude_cli_startup_probe", []byte("probe"), nil); err == nil {
		t.Fatal("mock_only posture admitted a live startup probe")
	}
	if len(probe.authorizations) != 0 || probe.launches != 0 {
		t.Fatalf("live startup probe reached authorization=%d launch=%d, want zero", len(probe.authorizations), probe.launches)
	}
}

func managedEffectTestContext(t testing.TB, ctx context.Context, agentID string) context.Context {
	t.Helper()
	ctx = WithExecutionMode(ctx, ExecutionModeLive)
	admission, err := managedexecution.New(managedexecution.KindNormalRuntime, "test-execution-authority", 1, "", "test-actors", "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", nil)
	if err != nil {
		t.Fatalf("build managed execution test admission: %v", err)
	}
	target := UsageTarget{
		Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: uuid.NewString(), AgentID: agentID,
		AgentIdentity: agentidentitytest.Runtime(t, agentID, "effects-test", "effects-test", "instance", "effects-test/instance"),
		SessionID:     uuid.NewString(), Memory: agentmemory.PlatformDefault(), FlowInstance: "effects-test/instance",
	}
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = WithUsageTarget(ctx, target)
	return managedcapabilities.WithContext(ctx, normalManagedEffectSurface(t, admission, target, agentID))
}

func normalManagedEffectSurface(t testing.TB, admission managedexecution.Admission, target UsageTarget, actorID string) managedcapabilities.Surface {
	t.Helper()
	actorIdentity := target.AgentIdentity
	if actorID != target.AgentIdentity.AgentID() {
		actorIdentity = agentidentitytest.Runtime(t, actorID, "effects-test", "effects-test", "instance", "effects-test/instance")
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: actorIdentity, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "test-contract",
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
	actorIdentity := target.AgentIdentity
	if actorID != target.AgentIdentity.AgentID() {
		actorIdentity = agentidentitytest.Runtime(t, actorID, "effects-test", "effects-test", "instance", "effects-test/instance")
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: actorIdentity, RuntimeMode: "task", Provider: "test", Transport: "api", ProviderContract: "test-contract",
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
	firstToken := effectLifecycleToken(t, 7, "agent-a", 3)
	first, err := canonicalOperationID(ctx, testAuthority(firstToken), "authored_http_tool", map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := canonicalOperationID(ctx, testAuthority(effectLifecycleToken(t, 8, "agent-a", 4)), "authored_http_tool", map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("operation identity changed across lifecycle generation: %s != %s", first, second)
	}
	siblingToken := firstToken
	siblingToken.Identity = agentidentitytest.Runtime(
		t, firstToken.AgentID, "effects-test", "effects-test", "sibling", "effects-test/sibling",
	)
	sibling, err := canonicalOperationID(ctx, testAuthority(siblingToken), "authored_http_tool", map[string]string{"tool": "lookup"})
	if err != nil {
		t.Fatal(err)
	}
	if first == sibling {
		t.Fatalf("same-slug sibling identities shared operation identity %s", first)
	}
}

func TestLogicalOperationIdentitySegmentsSeparateSiblingEffectsAndRemainStable(t *testing.T) {
	base := WithLogicalOperationIdentity(context.Background(), "event-123")
	first := WithLogicalOperationIdentitySegment(base, "provider_turn:1")
	replay := WithLogicalOperationIdentitySegment(base, "provider_turn:1")
	second := WithLogicalOperationIdentitySegment(base, "provider_turn:2")
	token := effectLifecycleToken(t, 7, "agent-a", 3)
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
