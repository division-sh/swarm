package deliverylifecycle

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/google/uuid"
)

// AgentClaimProfile is immutable first-claim evidence, not current execution or
// capacity permission. Selected history uses its existing preparation binding.
type AgentClaimProfile struct {
	DeliveryID            string                 `json:"delivery_id"`
	EventID               string                 `json:"event_id"`
	Identity              agentidentity.Identity `json:"identity"`
	ProfileID             string                 `json:"profile_id"`
	ConfigurationRevision string                 `json:"configuration_revision"`
	BundleHash            string                 `json:"bundle_hash"`
	ExecutionID           string                 `json:"execution_id"`
	ExecutionGeneration   uint64                 `json:"execution_generation"`
	AgentRuntimeEpoch     int64                  `json:"agent_runtime_epoch"`
	AgentGeneration       uint64                 `json:"agent_generation"`
}

func (p AgentClaimProfile) Validate() error {
	for _, id := range []string{p.DeliveryID, p.EventID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return fmt.Errorf("agent claim profile requires canonical delivery/event identity")
		}
	}
	if p.Identity != p.Identity.Normalize() || p.Identity.Validate() != nil {
		return fmt.Errorf("agent claim profile requires exact concrete agent identity")
	}
	profile, err := selection.ResolveActiveBackend(p.ProfileID)
	if err != nil || profile.ID != p.ProfileID {
		return fmt.Errorf("agent claim profile requires a canonical admitted profile")
	}
	revision, err := hex.DecodeString(p.ConfigurationRevision)
	if err != nil || len(revision) != 32 || strings.ToLower(p.ConfigurationRevision) != p.ConfigurationRevision {
		return fmt.Errorf("agent claim profile requires its immutable configuration revision")
	}
	source, err := correlation.NewSourceArtifactFact(p.BundleHash)
	if err != nil {
		return err
	}
	if strings.TrimSpace(p.ExecutionID) != p.ExecutionID {
		return fmt.Errorf("agent claim profile execution identity is noncanonical")
	}
	if _, err := NewNormalExecutionAuthority(source, p.ExecutionID, p.ExecutionGeneration); err != nil {
		return err
	}
	if p.AgentRuntimeEpoch <= 0 || p.AgentGeneration == 0 {
		return fmt.Errorf("agent claim profile requires its admitted agent epoch/generation")
	}
	return nil
}

func (p AgentClaimProfile) MatchesDelivery(snapshot Snapshot) bool {
	if p.Validate() != nil || snapshot.Authority.Validate() != nil ||
		snapshot.Authority.Kind() != ExecutionAuthorityNormalRuntime ||
		p.DeliveryID != snapshot.DeliveryID || p.EventID != snapshot.EventID ||
		p.Identity != snapshot.Route.AgentIdentity || p.Identity.RunID != snapshot.RunID ||
		p.BundleHash != snapshot.Authority.SourceArtifact().BundleHash() ||
		snapshot.SubscriberClass != SubscriberAgent || p.Identity.AgentID() != snapshot.SubscriberID {
		return false
	}
	id, err := DeliveryID(snapshot.EventID, snapshot.Route)
	return err == nil && id == p.DeliveryID
}

func (p AgentClaimProfile) MatchesFirstAdmission(event events.Event, route events.DeliveryRoute, authority ExecutionAuthority) bool {
	id, err := DeliveryID(event.ID(), route)
	return err == nil && p.Validate() == nil && authority.Validate() == nil &&
		authority.Kind() == ExecutionAuthorityNormalRuntime && p.DeliveryID == id &&
		p.EventID == event.ID() && p.Identity == route.AgentIdentity && p.Identity.RunID == event.RunID() &&
		route.Recipient.IsAgent() && route.Recipient.ID() == p.Identity.AgentID() &&
		p.BundleHash == authority.SourceArtifact().BundleHash() &&
		p.ExecutionID == authority.ExecutionID() && p.ExecutionGeneration == authority.Generation()
}
