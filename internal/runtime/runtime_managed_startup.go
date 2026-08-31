package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type managedExecutionActivation struct {
	Admission     managedexecution.Admission
	ReplaySummary runtimemanager.StartupReplaySummary
	ReplayErr     error
}

func (rt *Runtime) currentStartupProbeAuthority() (runtimestartupownership.GrantEvidence, error) {
	if rt == nil {
		return runtimestartupownership.GrantEvidence{}, fmt.Errorf("runtime is nil")
	}
	if rt.startupGrant == nil {
		return runtimestartupownership.GrantEvidence{}, fmt.Errorf("runtime generation grant is missing")
	}
	return rt.startupGrant.Evidence()
}

func (rt *Runtime) managedProviderPreflightAuthority(authority runtimestartupownership.GrantEvidence) (ManagedProviderPreflightAuthority, error) {
	effectStore := rt.effectsStore
	if effectStore == nil {
		return ManagedProviderPreflightAuthority{}, fmt.Errorf("runtime store does not implement managed external-effect persistence")
	}
	capabilityStore := rt.managedCapabilitiesStore
	if capabilityStore == nil {
		return ManagedProviderPreflightAuthority{}, fmt.Errorf("runtime store does not implement managed capability persistence")
	}
	return ManagedProviderPreflightAuthority{
		ExecutionKind:        managedcapabilities.ExecutionNormalAgent,
		ExecutionAuthorityID: authority.GrantID,
		StartupOwnerID:       authority.ProcessOwnerID,
		StartupGeneration:    authority.RuntimeGeneration,
		EffectController:     runtimeeffects.NewController(effectStore).WithExecutionPosture(rt.ExecutionPosture),
		CapabilityStore:      capabilityStore,
		EffectAuthority: func(probeID, actorID string) (runtimeeffects.Authority, error) {
			effectAuthority := runtimeeffects.Authority{
				Kind: runtimeeffects.AuthorityStartupProbe, ID: strings.TrimSpace(probeID),
				ExecutionOwner: authority.ProcessOwnerID, LeaseExpiresAt: time.Now().UTC().Add(15 * time.Minute), FenceGeneration: authority.RuntimeGeneration,
				ExecutionMode: runtimeeffects.ExecutionMode(rt.ExecutionPosture.RootMode()),
				StartupProbe: runtimeeffects.StartupProbeAuthority{
					ProbeID: probeID, StartupAuthorityID: authority.GrantID, StartupStateVersion: authority.StateVersion,
					ActorID: actorID, ExecutionKind: string(managedcapabilities.ExecutionNormalAgent), ExecutionAuthorityID: authority.GrantID,
				},
			}
			if !effectAuthority.Valid() {
				return runtimeeffects.Authority{}, fmt.Errorf("startup probe effect authority is invalid")
			}
			return effectAuthority, nil
		},
	}, nil
}

func (rt *Runtime) settleManagedStartupPreflight(ctx context.Context, surfaceIDs []string) (runtimestartupownership.GrantEvidence, error) {
	if rt.startupGrant == nil {
		return runtimestartupownership.GrantEvidence{}, fmt.Errorf("runtime generation grant is missing")
	}
	if _, err := rt.startupGrant.MarkProbesSettled(ctx, surfaceIDs); err != nil {
		return runtimestartupownership.GrantEvidence{}, err
	}
	return rt.startupGrant.AdmitExecution(ctx)
}

func (rt *Runtime) admitManagedExecution(ctx context.Context, authority runtimestartupownership.GrantEvidence) (managedExecutionActivation, error) {
	actorFingerprint, err := rt.managedActorCensusFingerprint()
	if err != nil {
		return managedExecutionActivation{}, err
	}
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		authority.GrantID,
		authority.RuntimeGeneration,
		"",
		actorFingerprint,
		rt.Options.BundleSourceFact.BundleHash(),
		authority.ProbeSurfaceIDs,
	)
	if err != nil {
		return managedExecutionActivation{}, err
	}
	deliveryAuthority, err := runtimedelivery.NewExecutionAuthority(rt.Options.BundleSourceFact, admission)
	if err != nil {
		return managedExecutionActivation{}, err
	}
	ctx = managedexecution.WithAdmission(ctx, admission)
	if rt.Bus == nil {
		return managedExecutionActivation{}, fmt.Errorf("runtime delivery authority requires event bus")
	}
	if err := rt.Bus.SetDeliveryAuthority(deliveryAuthority); err != nil {
		return managedExecutionActivation{}, err
	}
	if rt.deliveryStore != nil {
		if err := rt.deliveryStore.ActivateDeliveryAuthority(ctx, deliveryAuthority); err != nil {
			return managedExecutionActivation{}, fmt.Errorf("activate delivery execution authority: %w", err)
		}
		coordinator, err := runtimedeliverycontinuation.New(
			rt.deliveryStore,
			rt.Pipeline,
			deliveryAuthority,
			rt.workOccurrence,
			rt.Bus,
			func(reportCtx context.Context, reportErr error) {
				if reportErr == nil {
					return
				}
				rt.failDeliveryContinuation(reportErr)
				if rt.Logger != nil {
					handleRuntimeLogPersistenceError("delivery-continuation", "continuation_failed", rt.Logger.Error(
						reportCtx, "delivery-continuation", "continuation_failed", nil, reportErr,
					))
				}
			},
		)
		if err != nil {
			return managedExecutionActivation{}, err
		}
		if err := rt.Bus.SetDeliveryContinuationOwner(coordinator); err != nil {
			return managedExecutionActivation{}, err
		}
		if rt.Pipeline != nil {
			registration, err := rt.Pipeline.RegisterDeliveryContinuationSignal(deliveryAuthority, rt.Bus.SignalDeliveryContinuations)
			if err != nil {
				return managedExecutionActivation{}, fmt.Errorf("register delivery continuation signal owner: %w", err)
			}
			rt.deliverySignalRegistration = registration
		}
		rt.deliveryContinuations = coordinator
	}
	result := managedExecutionActivation{Admission: admission}
	rt.lifecycleMu.Lock()
	rt.startupAdmission = admission
	if rt.startCtx != nil {
		rt.startCtx = managedexecution.WithAdmission(rt.startCtx, admission)
		ctx = rt.startCtx
	}
	rt.lifecycleMu.Unlock()
	return result, nil
}

func (rt *Runtime) recoverManagedExecution(ctx context.Context, replay bool) managedExecutionActivation {
	result := managedExecutionActivation{}
	if !replay {
		return result
	}
	if rt.deliveryStore != nil {
		result.ReplayErr = runtimepipeline.NewRecoveryManagerWith(rt.Bus).RecoverToExhaustion(ctx)
		return result
	}
	if rt.Manager != nil {
		result.ReplaySummary, result.ReplayErr = rt.Manager.RecoverAfterStartupAdmission(ctx)
	}
	return result
}

func (rt *Runtime) startAndSynchronizeDeliveryContinuations(ctx context.Context) error {
	if rt == nil || rt.deliveryContinuations == nil {
		return nil
	}
	if err := rt.deliveryContinuations.Start(ctx); err != nil {
		return err
	}
	return rt.deliveryContinuations.Synchronize(ctx)
}

func (rt *Runtime) failDeliveryContinuation(err error) {
	if rt == nil || err == nil {
		return
	}
	rt.CloseAdmission()
	rt.lifecycleMu.Lock()
	cancel := rt.cancelStart
	rt.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (rt *Runtime) managedActorCensusFingerprint() (string, error) {
	actors := []any{}
	if rt != nil && rt.Manager != nil {
		configs := rt.Manager.ListAgentConfigs()
		actors = make([]any, 0, len(configs))
		for _, cfg := range configs {
			actors = append(actors, cfg)
		}
	}
	raw, err := json.Marshal(actors)
	if err != nil {
		return "", fmt.Errorf("marshal managed actor census: %w", err)
	}
	return runtimeeffects.Fingerprint(raw), nil
}
