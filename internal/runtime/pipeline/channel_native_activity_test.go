package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	sessionexecution "github.com/division-sh/swarm/internal/sessionprovider/execution"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

func nativeActivityHandoffFixture(t *testing.T) (channelonboarding.CompiledActivation, contracts.ToolSchemaEntry, engine.ActivityIntent, ActivityAttemptRecord, map[string]any) {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	body, err := os.ReadFile(filepath.Join(root, "packs/provider-triggers/whatsapp/trigger.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := providertriggers.ParseManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
		Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.input", Version: "0.1.0", ManifestHash: packs.ManifestHash(body), Provenance: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	plan := packfixture.WhatsAppSessionChannel(t, filepath.Join(root, "platform-spec.yaml"), catalog)
	binding, err := packs.NewOutboundBindingPlanWithRegistration("native-activity", plan, "15551234568@s.whatsapp.net", nil, nil, "ingress:.:whatsapp")
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	activation := channelonboarding.CompiledActivation{Source: channelonboarding.ActivationSourceLearned,
		OnboardingOperationID: uuid.NewString(), OnboardingRevision: 1, ActivationRevision: 1, Plan: binding,
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
			BundleIdentity: "native-activity", PackInventoryGeneration: "sha256:inventory", RuntimeInstanceID: uuid.NewString(),
			ContextPublicationGeneration: 1, PlanGeneration: generation, TargetGeneration: 1},
		SessionAccount: operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
			AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}}
	tool, err := binding.OperationTool("deliver")
	if err != nil {
		t.Fatal(err)
	}
	private, err := binding.RuntimeActivityTarget("deliver")
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"destination": "15551234568@s.whatsapp.net", "text": "exact activity input"}
	value, err := canonicaljson.FromGo(input)
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	intent := engine.ActivityIntent{ActivityID: "native-activity", Tool: private.ToolID(), Input: value,
		BundleHash: activation.Coordinate.BundleHash, PlanGeneration: private.Generation(),
		EffectClass: contracts.ActivityEffectClassNonIdempotentWrite, SuccessEvent: "channel.deliver.succeeded", FailureEvent: "channel.deliver.failed",
		SourceRunID: runID, SourceEventID: uuid.NewString(), ParentEventID: uuid.NewString(), EntityID: identity.NormalizeEntityID(uuid.NewString()),
		ExecutionFlowID: identity.NormalizeFlowID("."), FlowInstance: runID, Owner: activityidentity.MustAgentOwner("agent"),
		HandlerEventKey: "channel.deliver", Attempt: 1, ExecutionMode: effects.ExecutionModeLive}
	started := activityAttemptStartRecord(intent, activityInputHash(value))
	started.StartedAt = time.Now().UTC()
	return activation, tool, intent, started, input
}

func TestNativeActivityHandoffCannotBeReconstructedOrConsumedTwice(t *testing.T) {
	activation, tool, intent, started, input := nativeActivityHandoffFixture(t)
	ctx, close, err := withNativeChannelActivityLaunch(context.Background(), started, intent, activation, "deliver", tool, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	var successes atomic.Int32
	var workers sync.WaitGroup
	for range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			permit, err := ConsumeNativeChannelActivity(ctx, activation.OnboardingOperationID, activation.SessionAccount, tool, input)
			if err == nil {
				successes.Add(1)
				if permit.MessageID() != strings.ReplaceAll(started.RequestEventID, "-", "") {
					t.Error("handoff lost original journal identity")
				}
			}
		}()
	}
	workers.Wait()
	if successes.Load() != 1 {
		t.Fatal("one journal attempt issued multiple native launches", successes.Load())
	}
	if err := (NativeChannelActivityPermit{}).Validate(ctx); err == nil {
		t.Fatal("reconstructed permit became executable")
	}
	if _, err := ConsumeNativeChannelActivity(context.Background(), activation.OnboardingOperationID, activation.SessionAccount, tool, input); err == nil {
		t.Fatal("persisted fields manufactured a process-local launch")
	}
}

func TestNativeActivityDispatchRefusesAbsentInstalledOwners(t *testing.T) {
	_, tool, intent, started, _ := nativeActivityHandoffFixture(t)
	for _, options := range []PipelineCoordinatorOptions{{}, {
		NativeChannelExecution: func(context.Context, channelonboarding.CompiledActivation) (sessionexecution.Channel, error) {
			t.Fatal("absent selected activation reached the SDK owner")
			return sessionexecution.Channel{}, nil
		},
	}} {
		pc := &PipelineCoordinator{nativeChannelExecution: options.NativeChannelExecution}
		if result, err := (pipelineActivityDispatcher{coordinator: pc}).executeNativeChannelActivity(context.Background(), intent, tool, started); err == nil || result != nil {
			t.Fatal("native declaration executed without its selected original owners", result, err)
		}
	}
}

func TestNativeActivityHandoffRefusesScopeMutationAndFreezesInput(t *testing.T) {
	activation, tool, intent, started, input := nativeActivityHandoffFixture(t)
	var fenced atomic.Bool
	cause := errors.New("original activation fenced")
	ctx, close, err := withNativeChannelActivityLaunch(context.Background(), started, intent, activation, "deliver", tool, func(context.Context) error {
		if fenced.Load() {
			return cause
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer close()
	if _, err := ConsumeNativeChannelActivity(ctx, uuid.NewString(), activation.SessionAccount, tool, input); err == nil {
		t.Fatal("foreign parent consumed launch")
	}
	changed := activation.SessionAccount
	changed.AdmissionID = uuid.NewString()
	if _, err := ConsumeNativeChannelActivity(ctx, activation.OnboardingOperationID, changed, tool, input); err == nil {
		t.Fatal("foreign account consumed launch")
	}
	input["text"] = "mutated input"
	if _, err := ConsumeNativeChannelActivity(ctx, activation.OnboardingOperationID, activation.SessionAccount, tool, input); err == nil {
		t.Fatal("changed input consumed launch")
	}
	input["text"] = "exact activity input"
	permit, err := ConsumeNativeChannelActivity(ctx, activation.OnboardingOperationID, activation.SessionAccount, tool, input)
	if err != nil {
		t.Fatal(err)
	}
	input["text"] = "mutation after consumption"
	if activityInputHash(permit.Input()) != started.InputHash {
		t.Fatal("SDK handoff retained the caller's mutable input")
	}
	fenced.Store(true)
	if err := permit.Validate(ctx); !errors.Is(err, cause) {
		t.Fatal("retained handoff lost its original fence", err)
	}
	fenced.Store(false)
	close()
	if err := permit.Validate(ctx); err == nil {
		t.Fatal("released handoff remained executable")
	}
}

func TestNativeActivityHandoffIssuanceRequiresOriginalClaimAndTarget(t *testing.T) {
	activation, tool, intent, started, _ := nativeActivityHandoffFixture(t)
	for name, mutate := range map[string]func(*ActivityAttemptRecord, *engine.ActivityIntent, *channelonboarding.CompiledActivation){
		"uncertain attempt": func(r *ActivityAttemptRecord, _ *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			r.Status = ActivityAttemptStatusUncertain
		},
		"settled attempt": func(r *ActivityAttemptRecord, _ *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			r.Status = ActivityAttemptStatusSucceeded
		},
		"missing claim time": func(r *ActivityAttemptRecord, _ *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			r.StartedAt = time.Time{}
		},
		"different run": func(r *ActivityAttemptRecord, _ *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			r.RunID = uuid.NewString()
		},
		"different input": func(r *ActivityAttemptRecord, _ *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			r.InputHash = "different"
		},
		"public alias": func(_ *ActivityAttemptRecord, i *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			i.Tool = "whatsapp.send_text"
		},
		"other private target": func(_ *ActivityAttemptRecord, i *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			i.Tool += "other"
		},
		"different source": func(_ *ActivityAttemptRecord, i *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			i.BundleHash = "different"
		},
		"mock": func(_ *ActivityAttemptRecord, i *engine.ActivityIntent, _ *channelonboarding.CompiledActivation) {
			i.ExecutionMode = effects.ExecutionModeMock
		},
		"missing account": func(_ *ActivityAttemptRecord, _ *engine.ActivityIntent, a *channelonboarding.CompiledActivation) {
			a.SessionAccount = operatorchannel.SessionAccountAdmission{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, i, a := started, intent, activation
			mutate(&r, &i, &a)
			ctx, release, err := withNativeChannelActivityLaunch(context.Background(), r, i, a, "deliver", tool, func(context.Context) error { return nil })
			if err == nil || ctx != nil || release != nil {
				t.Fatal("foreign/replayed attempt issued native authority", err)
			}
		})
	}
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, release, err := withNativeChannelActivityLaunch(caller, started, intent, activation, "deliver", tool, func(context.Context) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || ctx != nil || release != nil {
		t.Fatal("lookup cancellation issued native authority", err)
	}
}
