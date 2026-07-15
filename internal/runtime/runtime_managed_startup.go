package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
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
	effectStore, ok := rt.Stores.ManagerStore.(runtimeeffects.Store)
	if !ok || effectStore == nil {
		return ManagedProviderPreflightAuthority{}, fmt.Errorf("runtime store does not implement managed external-effect persistence")
	}
	capabilityStore, ok := rt.Stores.ManagerStore.(managedcapabilities.Persistence)
	if !ok || capabilityStore == nil {
		return ManagedProviderPreflightAuthority{}, fmt.Errorf("runtime store does not implement managed capability persistence")
	}
	return ManagedProviderPreflightAuthority{
		ExecutionKind:        managedcapabilities.ExecutionNormalAgent,
		ExecutionAuthorityID: authority.AuthorityID,
		StartupOwnerID:       authority.OwnerID,
		StartupGeneration:    authority.Generation,
		EffectController:     runtimeeffects.NewController(effectStore),
		CapabilityStore:      capabilityStore,
		EffectAuthority: func(probeID, actorID string) (runtimeeffects.Authority, error) {
			effectAuthority := runtimeeffects.Authority{
				Kind: runtimeeffects.AuthorityStartupProbe, ID: strings.TrimSpace(probeID),
				ExecutionOwner: authority.OwnerID, LeaseExpiresAt: time.Now().UTC().Add(15 * time.Minute), FenceGeneration: authority.Generation,
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
		coalesceRuntimeIdentity(rt.Options.BundleFingerprint),
		authority.ProbeSurfaceIDs,
	)
	if err != nil {
		return managedExecutionActivation{}, err
	}
	result := managedExecutionActivation{Admission: admission}
	ctx = managedexecution.WithAdmission(ctx, admission)
	rt.lifecycleMu.Lock()
	rt.startupAdmission = admission
	if rt.startCtx != nil {
		rt.startCtx = managedexecution.WithAdmission(rt.startCtx, admission)
		ctx = rt.startCtx
	}
	rt.lifecycleMu.Unlock()
	if rt.Manager == nil {
		return result, nil
	}
	if replay {
		result.ReplaySummary, result.ReplayErr = rt.Manager.ReplayAfterStartupAdmission(ctx, true)
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
