package eventreceiver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

const receiverTestBundleHash = "bundle-v2:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

type receiverContextKey struct{}

type receiverEffectStore struct{}

func (receiverEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	return true, nil
}

func (receiverEffectStore) AuthorizeExternalAttempt(context.Context, runtimeeffects.Authority, runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	return runtimeeffects.Attempt{}, nil
}

func (receiverEffectStore) MarkExternalAttemptLaunched(context.Context, runtimeeffects.Attempt, time.Time) error {
	return nil
}

func (receiverEffectStore) MarkExternalAttemptResponseObserved(context.Context, runtimeeffects.Attempt, map[string]any, time.Time) error {
	return nil
}

func (receiverEffectStore) SettleExternalAttempt(context.Context, runtimeeffects.Settlement) error {
	return nil
}

func TestNewContextStartsWithEmptyValuesAndFollowsLifetime(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.WithValue(context.Background(), receiverContextKey{}, "publisher-secret"))
	ctx, cleanup := NewContext(parent)
	defer cleanup()

	if got := ctx.Value(receiverContextKey{}); got != nil {
		t.Fatalf("receiver context inherited publisher value %#v", got)
	}
	cancelParent()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("receiver context did not follow its explicit lifetime owner")
	}
}

func TestUnconfiguredExecutionCannotBindReceiver(t *testing.T) {
	var variant ExecutionVariant
	if variant.Configured() {
		t.Fatal("zero receiver execution unexpectedly reports an owner")
	}
	if kind := variant.Kind(); kind != "" {
		t.Fatalf("zero receiver execution kind = %q, want empty", kind)
	}
	if err := variant.Validate(); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unconfigured receiver validation = %v", err)
	}
	if _, err := variant.Bind(context.Background(), executionmode.Live); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unconfigured receiver bind = %v", err)
	}
}

func TestNormalExecutionRejectsInheritedSelectedState(t *testing.T) {
	variant := NormalExecution()
	authority, admission, controller := selectedReceiverState(t, executionmode.Live)
	hostile := runtimeeffects.WithAuthority(context.Background(), authority)
	hostile = managedexecution.WithAdmission(hostile, admission)
	hostile = runtimeeffects.WithController(hostile, controller)

	bound, err := variant.Bind(hostile, executionmode.Live)
	if err != nil {
		t.Fatalf("bind normal receiver: %v", err)
	}
	if err := variant.ValidateBound(bound, executionmode.Live); err == nil {
		t.Fatal("normal receiver admitted inherited selected execution state")
	}
}

func TestSelectedExecutionBindsExactOwnerAndCausalMode(t *testing.T) {
	authority, admission, controller := selectedReceiverState(t, executionmode.Mock)
	lineage := runtimecorrelation.RuntimeLineage{
		Owner: "selected-receiver-test", RunID: authority.SelectedFork.ForkRunID,
		Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
		SelectedForkContext: true,
	}
	variant, err := SelectedContractForkExecution(authority, admission, controller, lineage)
	if err != nil {
		t.Fatalf("construct selected receiver execution: %v", err)
	}
	ctx, cleanup := NewContext(context.Background())
	defer cleanup()
	ctx, err = variant.Bind(ctx, executionmode.Mock)
	if err != nil {
		t.Fatalf("bind selected receiver: %v", err)
	}
	if err := variant.ValidateBound(ctx, executionmode.Mock); err != nil {
		t.Fatalf("validate selected receiver: %v", err)
	}
	if got, ok := runtimeeffects.AuthorityFromContext(ctx); !ok || got.ID != authority.ID {
		t.Fatalf("selected receiver authority = %#v, %v; want %q", got, ok, authority.ID)
	}
	if got, ok := managedexecution.FromContext(ctx); !ok || got.ID != admission.ID {
		t.Fatalf("selected receiver admission = %#v, %v; want %q", got, ok, admission.ID)
	}
	if got, ok := runtimeeffects.ControllerFromContext(ctx); !ok || got != controller {
		t.Fatalf("selected receiver controller = %p, %v; want %p", got, ok, controller)
	}
	if got, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); !ok || got.Owner != lineage.Owner {
		t.Fatalf("selected receiver lineage = %#v, %v; want owner %q", got, ok, lineage.Owner)
	}
}

func TestSelectedExecutionBindsEachCausalModeAndRejectsOwnerDrift(t *testing.T) {
	authority, admission, controller := selectedReceiverState(t, executionmode.Live)
	variant, err := SelectedContractForkExecution(authority, admission, controller, runtimecorrelation.RuntimeLineage{})
	if err != nil {
		t.Fatalf("construct selected receiver execution: %v", err)
	}
	ctx, err := variant.Bind(context.Background(), executionmode.Mock)
	if err != nil {
		t.Fatalf("bind mock-causal selected receiver: %v", err)
	}
	if err := variant.ValidateBound(ctx, executionmode.Mock); err != nil {
		t.Fatalf("validate mock-causal selected receiver: %v", err)
	}
	boundAuthority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || boundAuthority.ExecutionMode != executionmode.Mock {
		t.Fatalf("bound selected authority = %#v, %v; want mock mode", boundAuthority, ok)
	}
	if authority.ExecutionMode != executionmode.Live {
		t.Fatalf("container authority mode mutated to %q, want live default", authority.ExecutionMode)
	}
	foreign := authority
	foreign.ID = uuid.NewString()
	foreign.SelectedFork.ExecutionID = foreign.ID
	foreign.ExecutionMode = executionmode.Mock
	ctx = runtimeeffects.WithAuthority(ctx, foreign)
	if err := variant.ValidateBound(ctx, executionmode.Mock); err == nil || !strings.Contains(err.Error(), "authority") {
		t.Fatalf("selected receiver owner drift = %v", err)
	}
}

func TestSelectedExecutionFinalizesAdmissionBeforeReceiverStart(t *testing.T) {
	authority, admission, controller := selectedReceiverState(t, executionmode.Live)
	variant, err := SelectedContractForkExecution(authority, admission, controller, runtimecorrelation.RuntimeLineage{})
	if err != nil {
		t.Fatalf("construct selected receiver execution: %v", err)
	}
	finalAdmission, err := admission.WithCapabilitySurfaces([]string{uuid.NewString()})
	if err != nil {
		t.Fatalf("finalize selected admission: %v", err)
	}
	variant, err = variant.WithSelectedAdmission(finalAdmission)
	if err != nil {
		t.Fatalf("finalize selected receiver execution: %v", err)
	}
	ctx, err := variant.Bind(context.Background(), executionmode.Live)
	if err != nil {
		t.Fatalf("bind finalized selected receiver: %v", err)
	}
	got, ok := managedexecution.FromContext(ctx)
	if !ok || got.ID != finalAdmission.ID || len(got.CapabilitySurfaceIDs) != 1 {
		t.Fatalf("final selected receiver admission = %#v, %v", got, ok)
	}
}

func selectedReceiverState(t testing.TB, mode executionmode.Mode) (runtimeeffects.Authority, managedexecution.Admission, *runtimeeffects.Controller) {
	t.Helper()
	executionID := uuid.NewString()
	runID := uuid.NewString()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthoritySelectedContractFork,
		ID:   executionID,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{
			ExecutionID: executionID, ForkRunID: runID, Generation: 1,
			AdmissionFingerprint: "test-admission", ContainerPlanFingerprint: "test-container",
			ActorCensusFingerprint: "test-actors", EffectiveConfigFingerprint: "test-config",
		},
		ExecutionOwner: "selected-receiver-test", LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
		FenceGeneration: 1, ExecutionMode: mode,
	}
	admission, err := managedexecution.New(
		managedexecution.KindSelectedContractFork, executionID, 1, runID,
		"test-actors", receiverTestBundleHash, nil,
	)
	if err != nil {
		t.Fatalf("construct selected admission: %v", err)
	}
	return authority, admission, liveTestEffectController(receiverEffectStore{})
}
