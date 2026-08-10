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

func (rt *Runtime) currentStartupProbeAuthority() (runtimestartupownership.Authority, error) {
	if rt == nil {
		return runtimestartupownership.Authority{}, fmt.Errorf("runtime is nil")
	}
	if rt.pendingOwnershipHandoff != nil {
		return rt.pendingOwnershipHandoff.Authority()
	}
	if rt.ownershipLease == nil {
		return runtimestartupownership.Authority{}, fmt.Errorf("runtime startup ownership authority is missing")
	}
	return rt.ownershipLease.Authority()
}

func (rt *Runtime) managedProviderPreflightAuthority(authority runtimestartupownership.Authority) (ManagedProviderPreflightAuthority, error) {
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
		ExecutionAuthorityID: authority.AuthorityID,
		StartupOwnerID:       authority.OwnerID,
		StartupGeneration:    authority.Generation,
		EffectController:     runtimeeffects.NewController(effectStore).WithExecutionPosture(rt.ExecutionPosture),
		CapabilityStore:      capabilityStore,
		EffectAuthority: func(probeID, actorID string) (runtimeeffects.Authority, error) {
			effectAuthority := runtimeeffects.Authority{
				Kind: runtimeeffects.AuthorityStartupProbe, ID: strings.TrimSpace(probeID),
				ExecutionOwner: authority.OwnerID, LeaseExpiresAt: time.Now().UTC().Add(15 * time.Minute), FenceGeneration: authority.Generation,
				ExecutionMode: runtimeeffects.ExecutionMode(rt.ExecutionPosture.RootMode()),
				StartupProbe: runtimeeffects.StartupProbeAuthority{
					ProbeID: probeID, StartupAuthorityID: authority.AuthorityID, StartupStateVersion: authority.StateVersion,
					ActorID: actorID, ExecutionKind: string(managedcapabilities.ExecutionNormalAgent), ExecutionAuthorityID: authority.AuthorityID,
				},
			}
			if !effectAuthority.Valid() {
				return runtimeeffects.Authority{}, fmt.Errorf("startup probe effect authority is invalid")
			}
			return effectAuthority, nil
		},
	}, nil
}

func (rt *Runtime) settleManagedStartupPreflight(ctx context.Context, surfaceIDs []string) (runtimestartupownership.Authority, bool, error) {
	if rt.pendingOwnershipHandoff != nil {
		authority, err := rt.pendingOwnershipHandoff.MarkProbesSettled(ctx, surfaceIDs)
		return authority, true, err
	}
	if rt.ownershipLease == nil {
		return runtimestartupownership.Authority{}, false, fmt.Errorf("runtime startup ownership lease is missing")
	}
	if _, err := rt.ownershipLease.MarkProbesSettled(ctx, surfaceIDs); err != nil {
		return runtimestartupownership.Authority{}, false, err
	}
	authority, err := rt.ownershipLease.AdmitExecution(ctx)
	return authority, false, err
}

func (rt *Runtime) admitManagedExecution(ctx context.Context, authority runtimestartupownership.Authority, replay bool) (managedExecutionActivation, error) {
	actorFingerprint, err := rt.managedActorCensusFingerprint()
	if err != nil {
		return managedExecutionActivation{}, err
	}
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		authority.AuthorityID,
		authority.Generation,
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
	var deliveryCoordinator *runtimedeliverycontinuation.Coordinator
	if rt.deliveryStore != nil {
		if err := rt.deliveryStore.ActivateDeliveryAuthority(ctx, deliveryAuthority); err != nil {
			return managedExecutionActivation{}, fmt.Errorf("activate delivery execution authority: %w", err)
		}
		coordinator, err := runtimedeliverycontinuation.New(
			rt.deliveryStore,
			deliveryAuthority,
			rt.workOccurrence,
			rt.Bus,
			func(reportCtx context.Context, reportErr error) {
				if reportErr == nil {
					return
				}
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
		deliveryCoordinator = coordinator
	}
	result := managedExecutionActivation{Admission: admission}
	rt.lifecycleMu.Lock()
	rt.startupAdmission = admission
	if rt.startCtx != nil {
		rt.startCtx = managedexecution.WithAdmission(rt.startCtx, admission)
		ctx = rt.startCtx
	}
	rt.lifecycleMu.Unlock()
	if replay {
		if rt.deliveryStore != nil {
			result.ReplayErr = runtimepipeline.NewRecoveryManagerWith(rt.Bus).RecoverToExhaustion(ctx)
			if result.ReplayErr != nil {
				return result, fmt.Errorf("recover pipeline obligations before delivery enumeration: %w", result.ReplayErr)
			}
		} else if rt.Manager != nil {
			result.ReplaySummary, result.ReplayErr = rt.Manager.RecoverAfterStartupAdmission(ctx)
		}
	}
	if deliveryCoordinator != nil {
		if err := deliveryCoordinator.Start(ctx); err != nil {
			return managedExecutionActivation{}, err
		}
	}
	return result, nil
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
