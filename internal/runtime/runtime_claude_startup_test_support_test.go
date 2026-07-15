package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

type startupCapabilityStore struct {
	mu       sync.Mutex
	surfaces map[string]managedcapabilities.Surface
}

func (s *startupCapabilityStore) SaveManagedCapabilitySurface(_ context.Context, surface managedcapabilities.Surface) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.surfaces == nil {
		s.surfaces = map[string]managedcapabilities.Surface{}
	}
	if previous, ok := s.surfaces[surface.ID]; ok {
		if err := surface.CanAdvanceFrom(previous); err != nil {
			return err
		}
	}
	s.surfaces[surface.ID] = surface.Clone()
	return nil
}

type startupEffectStore struct{}

func (*startupEffectStore) IsExternalEffectAuthorityCurrent(context.Context, runtimeeffects.Authority) (bool, error) {
	return true, nil
}

func (*startupEffectStore) AuthorizeExternalAttempt(_ context.Context, authority runtimeeffects.Authority, req runtimeeffects.AuthorizeRequest) (runtimeeffects.Attempt, error) {
	return runtimeeffects.Attempt{
		OperationID: req.OperationID, AttemptID: req.AttemptID, Authority: authority,
		Kind: req.Kind, Class: req.Class, Adapter: req.Adapter, Transport: req.Transport,
		Ordinal: 1, AuthorizedAt: req.Now,
	}, nil
}

func (*startupEffectStore) MarkExternalAttemptLaunched(context.Context, runtimeeffects.Attempt, time.Time) error {
	return nil
}

func (*startupEffectStore) MarkExternalAttemptResponseObserved(context.Context, runtimeeffects.Attempt, map[string]any, time.Time) error {
	return nil
}

func (*startupEffectStore) SettleExternalAttempt(context.Context, runtimeeffects.Settlement) error {
	return nil
}

// This test-only adapter keeps the existing startup proof matrix focused on
// behavior while routing every case through the canonical typed preflight.
func validateClaudeMCPToolsForManagedAgents(ctx context.Context, cfg *config.Config, source semanticview.Source, binding toolgateway.Binding, probe llm.StartupVisibleToolSurfaceProber, turns llm.MCPTurnContextStore, tools claudeStartupToolSource, manager *runtimemanager.AgentManager) error {
	startupAuthorityID := uuid.NewString()
	store := &startupEffectStore{}
	_, err := ValidateManagedProviderPreflight(ctx, cfg, source, binding, &llm.ClaudeCLIRuntime{}, probe, turns, tools, manager, ManagedProviderPreflightAuthority{
		ExecutionKind:        managedcapabilities.ExecutionNormalAgent,
		ExecutionAuthorityID: startupAuthorityID,
		StartupOwnerID:       "startup-test-owner",
		StartupGeneration:    1,
		EffectController:     runtimeeffects.NewController(store),
		CapabilityStore:      &startupCapabilityStore{},
		EffectAuthority: func(probeID, actorID string) (runtimeeffects.Authority, error) {
			return runtimeeffects.Authority{
				Kind: runtimeeffects.AuthorityStartupProbe, ID: probeID,
				ExecutionOwner: "startup-test-owner", LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1,
				StartupProbe: runtimeeffects.StartupProbeAuthority{
					ProbeID: probeID, StartupAuthorityID: startupAuthorityID, StartupStateVersion: 1,
					ActorID: actorID, ExecutionKind: string(managedcapabilities.ExecutionNormalAgent), ExecutionAuthorityID: startupAuthorityID,
				},
			}, nil
		},
	})
	return err
}
