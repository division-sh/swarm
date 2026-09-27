package effects

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

type AuthorityKind string

const (
	AuthorityNormalAgent          AuthorityKind = "normal_agent"
	AuthoritySelectedContractFork AuthorityKind = "selected_contract_fork"
	AuthorityConversationForkChat AuthorityKind = "conversation_fork_chat"
	AuthorityStartupProbe         AuthorityKind = "startup_probe"
	AuthorityServeRegistration    AuthorityKind = "serve_registration"
	AuthorityChannelConfirmation  AuthorityKind = "channel_confirmation"
	AuthorityChannelDelivery      AuthorityKind = "channel_delivery"
	AuthorityChannelNativeSetting AuthorityKind = "channel_native_setting"
)

type UsageTargetKind string

const (
	UsageTargetAgentTurn                  UsageTargetKind = "agent_turn"
	UsageTargetConversationForkCompletion UsageTargetKind = "conversation_fork_turn_completion"
)

type UsageTarget struct {
	Kind          UsageTargetKind
	ID            string
	Ordinal       int
	RunID         string
	AgentID       string
	AgentIdentity agentidentity.Identity
	SessionID     string
	Memory        agentmemory.Plan
	FlowInstance  string
	EntityID      string
}

type BudgetAdmissionScope struct {
	Kind   string
	Key    string
	CapUSD float64
}

func (t UsageTarget) Valid() bool {
	if _, err := uuid.Parse(strings.TrimSpace(t.ID)); err != nil {
		return false
	}
	switch t.Kind {
	case UsageTargetAgentTurn:
		if t.Ordinal != 0 || !nonEmpty(t.RunID, t.AgentID, t.SessionID) {
			return false
		}
		identity := t.AgentIdentity.Normalize()
		if err := identity.Validate(); err != nil ||
			identity.RunID != strings.TrimSpace(t.RunID) ||
			identity.AgentID() != strings.TrimSpace(t.AgentID) {
			return false
		}
		memory, err := t.Memory.Normalize()
		return err == nil &&
			(!memory.Enabled || strings.TrimSpace(t.FlowInstance) != "") &&
			identity.FlowInstance() == strings.Trim(strings.TrimSpace(t.FlowInstance), "/")
	case UsageTargetConversationForkCompletion:
		return t.Ordinal > 0 && validUUIDs(t.RunID)
	default:
		return false
	}
}

func ProviderTurnTargetMatchesCapabilitySurface(target UsageTarget, surface managedcapabilities.Surface) bool {
	sameActor, err := agentidentity.Equal(target.AgentIdentity, surface.ActorIdentity)
	return err == nil && sameActor &&
		target.Kind == UsageTargetAgentTurn && target.Valid() &&
		surface.Authority.Kind == managedcapabilities.AuthorityProviderTurn &&
		surface.Authority.ID == target.ID && surface.ActorID == target.AgentID &&
		surface.Authority.SessionID == target.SessionID && surface.Authority.RunID == target.RunID
}

func ValidateManagedAgentFrame(frame agentframe.Frame, authority Authority, surface managedcapabilities.Surface) error {
	if authority.Target.Kind != UsageTargetAgentTurn || !ProviderTurnTargetMatchesCapabilitySurface(authority.Target, surface) {
		return fmt.Errorf("managed execution frame requires an exact agent-turn target and capability surface")
	}
	if !frame.MatchesSurface(surface) {
		return fmt.Errorf("managed execution frame does not match capability surface")
	}
	authorityID, err := frame.ProviderTurnAuthorityID()
	if err != nil || authorityID != authority.Target.ID {
		return fmt.Errorf("managed execution frame does not match provider-turn authority")
	}
	sameActor, err := agentidentity.Equal(frame.Session.AgentIdentity, authority.Target.AgentIdentity)
	if err != nil || !sameActor || frame.Session.AgentIdentity.AgentID() != authority.Target.AgentID {
		return fmt.Errorf("managed execution frame does not match target actor")
	}
	if frame.Turn.Event.RunID != authority.Target.RunID {
		return fmt.Errorf("managed execution frame does not match target run")
	}
	// Event mode is causal input truth; authority mode is the provider-effect
	// posture and may legitimately narrow a live event to a mock agent.
	if frame.Session.Provider.RuntimeMode != surface.RuntimeMode || frame.Session.Provider.Provider != surface.Provider || frame.Session.Provider.Transport != surface.Transport {
		return fmt.Errorf("managed execution frame does not match provider contract")
	}
	return nil
}

func validateManagedAgentFramePrelaunch(ctx context.Context, frame agentframe.Frame, authority Authority, surface managedcapabilities.Surface) error {
	if err := ValidateManagedAgentFrame(frame, authority, surface); err != nil {
		return err
	}
	admission, ok := managedexecution.FromContext(ctx)
	if !ok {
		return fmt.Errorf("managed execution frame requires execution admission")
	}
	bundleSource, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok {
		return fmt.Errorf("managed execution frame requires authoritative bundle source")
	}
	bundleHash := bundleSource.BundleHash()
	if admission.BundleHash != bundleHash || frame.Session.SourceArtifact.Hash != bundleHash {
		return fmt.Errorf("managed execution frame does not match admitted bundle source")
	}
	causalEvent, ok := correlation.InboundEventFromContext(ctx)
	if !ok {
		return fmt.Errorf("managed execution frame requires causal event")
	}
	if err := frame.ValidateCausalEvent(causalEvent); err != nil {
		return err
	}
	return nil
}

type SelectedContractForkAuthority struct {
	ExecutionID                string
	ForkRunID                  string
	Generation                 uint64
	AdmissionFingerprint       string
	ContainerPlanFingerprint   string
	ActorCensusFingerprint     string
	EffectiveConfigFingerprint string
}

type ConversationForkChatAuthority struct {
	ForkTurnID          string
	ForkID              string
	SourceRunID         string
	BundleHash          string
	ActorTokenID        string
	RequestOccurrenceID string
	RequestHash         string
}

type StartupProbeAuthority struct {
	ProbeID              string
	StartupAuthorityID   string
	StartupStateVersion  uint64
	ActorID              string
	ExecutionKind        string
	ExecutionAuthorityID string
	Preparation          *managedcapabilities.PreparedSelectedForkProbeAuthority
}

type ServeRegistrationAuthority struct {
	IntentID                     string
	StartupAuthorityID           string
	StartupStateVersion          uint64
	OnboardingOperationID        string
	OnboardingRevision           int64
	BundleHash                   string
	BundleIdentity               string
	PackInventoryGeneration      string
	RuntimeInstanceID            string
	ContextPublicationGeneration uint64
	PlanGeneration               plangeneration.Generation
	TargetGeneration             uint64
}

type ChannelConfirmationAuthority struct {
	EffectOperationID            string
	OnboardingOperationID        string
	OnboardingRevision           int64
	ActivationID                 string
	ActivationRevision           int64
	BindingRevision              int64
	PrincipalID                  string
	BundleHash                   string
	BundleIdentity               string
	PackInventoryGeneration      string
	RuntimeInstanceID            string
	ContextPublicationGeneration uint64
	PlanGeneration               plangeneration.Generation
	TargetGeneration             uint64
}

type ChannelDeliveryAuthority struct {
	EffectOperationID            string
	DeliveryID                   string
	RenderID                     string
	RenderHash                   string
	PrincipalID                  string
	InterfaceKey                 string
	DeliveryEpoch                int64
	BindingRevision              int64
	ExternalAccountRef           string
	ConversationRef              string
	ActivationID                 string
	ActivationRevision           int64
	BundleHash                   string
	BundleIdentity               string
	PackInventoryGeneration      string
	RuntimeInstanceID            string
	ContextPublicationGeneration uint64
	PlanGeneration               plangeneration.Generation
	TargetGeneration             uint64
}

type ChannelNativeSettingAuthority struct {
	EffectOperationID            string
	SettingID                    string
	SettingGeneration            int64
	Provider                     string
	ResourceSlotID               string
	ConversationRef              string
	ScopeKind                    string
	MemberReference              string
	PrincipalID                  string
	EntryContractHash            string
	EntryCommand                 string
	PackID                       string
	PackVersion                  string
	PackManifestHash             string
	ActivationID                 string
	ActivationRevision           int64
	BindingRevision              int64
	BundleHash                   string
	BundleIdentity               string
	PackInventoryGeneration      string
	RuntimeInstanceID            string
	ContextPublicationGeneration uint64
	PlanGeneration               plangeneration.Generation
	TargetGeneration             uint64
}

func ChannelDeliveryOperationID(deliveryID, renderID string) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(deliveryID))
	if err != nil || !validUUIDs(renderID) {
		return "", fmt.Errorf("channel delivery operation requires exact delivery and render ids")
	}
	return uuid.NewSHA1(parsed, []byte("deliver:"+strings.TrimSpace(renderID))).String(), nil
}

type Authority struct {
	Kind                 AuthorityKind
	ID                   string
	Normal               LifecycleToken
	SelectedFork         SelectedContractForkAuthority
	ForkChat             ConversationForkChatAuthority
	StartupProbe         StartupProbeAuthority
	ServeRegistration    ServeRegistrationAuthority
	ChannelConfirmation  ChannelConfirmationAuthority
	ChannelDelivery      ChannelDeliveryAuthority
	ChannelNativeSetting ChannelNativeSettingAuthority
	ExecutionOwner       string
	LeaseExpiresAt       time.Time
	FenceGeneration      uint64
	Target               UsageTarget
	BudgetScopes         []BudgetAdmissionScope
	ExecutionMode        ExecutionMode
}

type ExecutionMode = executionmode.Mode

const (
	ExecutionModeLive = executionmode.Live
	ExecutionModeMock = executionmode.Mock
)

func NormalAgentAuthority(token LifecycleToken, executionOwner string, leaseExpiresAt time.Time) Authority {
	return Authority{
		Kind:            AuthorityNormalAgent,
		ID:              strings.TrimSpace(token.AgentID),
		Normal:          token,
		ExecutionOwner:  strings.TrimSpace(executionOwner),
		LeaseExpiresAt:  leaseExpiresAt.UTC(),
		FenceGeneration: token.Generation,
		ExecutionMode:   ExecutionModeLive,
	}
}

func (a Authority) Valid() bool {
	if a.StartupProbe.Preparation != nil && a.Kind != AuthorityStartupProbe {
		return false
	}
	if strings.TrimSpace(a.ID) == "" || strings.TrimSpace(a.ExecutionOwner) == "" || a.LeaseExpiresAt.IsZero() || a.FenceGeneration == 0 || !a.ExecutionMode.Valid() {
		return false
	}
	switch a.Kind {
	case AuthorityNormalAgent:
		return a.Normal.Valid() && a.ID == strings.TrimSpace(a.Normal.AgentID)
	case AuthoritySelectedContractFork:
		return validUUIDs(a.SelectedFork.ExecutionID, a.SelectedFork.ForkRunID) &&
			a.ID == strings.TrimSpace(a.SelectedFork.ExecutionID) && a.SelectedFork.Generation > 0 &&
			nonEmpty(a.SelectedFork.AdmissionFingerprint, a.SelectedFork.ContainerPlanFingerprint, a.SelectedFork.ActorCensusFingerprint, a.SelectedFork.EffectiveConfigFingerprint)
	case AuthorityConversationForkChat:
		return validUUIDs(a.ForkChat.ForkTurnID, a.ForkChat.ForkID, a.ForkChat.SourceRunID, a.ForkChat.RequestOccurrenceID) &&
			a.ID == strings.TrimSpace(a.ForkChat.ForkTurnID) && nonEmpty(a.ForkChat.BundleHash, a.ForkChat.ActorTokenID, a.ForkChat.RequestHash)
	case AuthorityStartupProbe:
		if p := a.StartupProbe.Preparation; p != nil {
			return p.Validate() == nil && validUUIDs(a.StartupProbe.ProbeID, a.StartupProbe.ExecutionAuthorityID) &&
				a.ID == a.StartupProbe.ProbeID && a.ExecutionOwner == p.ProcessOwnerID &&
				a.StartupProbe.ExecutionKind == string(managedcapabilities.ExecutionSelectedForkPreparation) &&
				a.StartupProbe.StartupAuthorityID == "" && a.StartupProbe.StartupStateVersion == 0 &&
				nonEmpty(a.StartupProbe.ActorID) && a.Target == (UsageTarget{}) && len(a.BudgetScopes) == 0 &&
				a.Normal == (LifecycleToken{}) && a.SelectedFork == (SelectedContractForkAuthority{}) &&
				a.ForkChat == (ConversationForkChatAuthority{}) && a.ServeRegistration == (ServeRegistrationAuthority{}) &&
				a.ChannelConfirmation == (ChannelConfirmationAuthority{}) && a.ChannelDelivery == (ChannelDeliveryAuthority{}) &&
				a.ChannelNativeSetting == (ChannelNativeSettingAuthority{})
		}
		return validUUIDs(a.StartupProbe.ProbeID, a.StartupProbe.StartupAuthorityID) &&
			a.ID == strings.TrimSpace(a.StartupProbe.ProbeID) && a.StartupProbe.StartupStateVersion > 0 &&
			a.StartupProbe.ExecutionKind != string(managedcapabilities.ExecutionSelectedForkPreparation) &&
			nonEmpty(a.StartupProbe.ActorID, a.StartupProbe.ExecutionKind, a.StartupProbe.ExecutionAuthorityID)
	case AuthorityServeRegistration:
		registration := a.ServeRegistration
		if !validUUIDs(registration.IntentID, registration.StartupAuthorityID) ||
			a.ID != strings.TrimSpace(registration.IntentID) || registration.StartupStateVersion == 0 {
			return false
		}
		if strings.TrimSpace(registration.OnboardingOperationID) == "" {
			return registration.OnboardingRevision == 0 && registration.BundleHash == "" &&
				registration.BundleIdentity == "" && registration.PackInventoryGeneration == "" &&
				registration.RuntimeInstanceID == "" && registration.ContextPublicationGeneration == 0 &&
				!registration.PlanGeneration.Valid() && registration.TargetGeneration == 0
		}
		return validUUIDs(registration.OnboardingOperationID) && registration.OnboardingRevision > 0 &&
			nonEmpty(registration.BundleHash, registration.BundleIdentity,
				registration.PackInventoryGeneration, registration.RuntimeInstanceID) &&
			registration.ContextPublicationGeneration > 0 && registration.PlanGeneration.Valid() && registration.TargetGeneration > 0
	case AuthorityChannelConfirmation:
		confirmation := a.ChannelConfirmation
		return validUUIDs(confirmation.EffectOperationID, confirmation.OnboardingOperationID, confirmation.ActivationID, confirmation.PrincipalID, confirmation.RuntimeInstanceID) &&
			a.ID == strings.TrimSpace(confirmation.EffectOperationID) && confirmation.OnboardingRevision > 0 &&
			confirmation.ActivationRevision > 0 && confirmation.BindingRevision > 0 &&
			confirmation.ContextPublicationGeneration == a.FenceGeneration &&
			nonEmpty(confirmation.BundleHash, confirmation.BundleIdentity,
				confirmation.PackInventoryGeneration) && confirmation.PlanGeneration.Valid() && confirmation.TargetGeneration > 0
	case AuthorityChannelDelivery:
		delivery := a.ChannelDelivery
		operationID, err := ChannelDeliveryOperationID(delivery.DeliveryID, delivery.RenderID)
		return err == nil && delivery.EffectOperationID == operationID && a.ID == operationID &&
			validUUIDs(delivery.PrincipalID, delivery.ActivationID, delivery.RuntimeInstanceID) &&
			delivery.DeliveryEpoch > 0 && delivery.BindingRevision > 0 && delivery.ActivationRevision > 0 &&
			delivery.ContextPublicationGeneration == a.FenceGeneration && delivery.TargetGeneration > 0 &&
			nonEmpty(delivery.InterfaceKey, delivery.ExternalAccountRef, delivery.ConversationRef, delivery.RenderHash,
				delivery.BundleHash, delivery.BundleIdentity, delivery.PackInventoryGeneration) && delivery.PlanGeneration.Valid()
	case AuthorityChannelNativeSetting:
		setting := a.ChannelNativeSetting
		operationID, err := channelnative.InstallOperationID(setting.SettingID, setting.SettingGeneration)
		entryCommand, commandErr := channelnative.EntryCommand(setting.SettingID, setting.SettingGeneration)
		return err == nil && setting.EffectOperationID == operationID && a.ID == operationID &&
			commandErr == nil && setting.EntryCommand == entryCommand &&
			((setting.ScopeKind == "chat" && setting.MemberReference == "") ||
				(setting.ScopeKind == "chat_member" && setting.MemberReference != "")) &&
			setting.SettingGeneration == int64(a.FenceGeneration) &&
			validUUIDs(setting.PrincipalID, setting.ActivationID, setting.RuntimeInstanceID) &&
			setting.ActivationRevision > 0 && setting.BindingRevision > 0 &&
			setting.ContextPublicationGeneration > 0 && setting.TargetGeneration > 0 &&
			strings.HasPrefix(setting.ResourceSlotID, setting.Provider+":") &&
			nonEmpty(setting.Provider, setting.ConversationRef, setting.EntryContractHash,
				setting.PackID, setting.PackVersion, setting.PackManifestHash,
				setting.BundleHash, setting.BundleIdentity, setting.PackInventoryGeneration) && setting.PlanGeneration.Valid()
	default:
		return false
	}
}

func (a Authority) Generation() uint64 {
	switch a.Kind {
	case AuthorityNormalAgent:
		return a.Normal.Generation
	case AuthoritySelectedContractFork:
		return a.SelectedFork.Generation
	case AuthorityConversationForkChat, AuthorityServeRegistration, AuthorityChannelConfirmation, AuthorityChannelDelivery, AuthorityChannelNativeSetting:
		return a.FenceGeneration
	case AuthorityStartupProbe:
		return a.FenceGeneration
	default:
		return 0
	}
}

func (a Authority) RuntimeEpoch() int64 {
	if a.Kind == AuthorityNormalAgent {
		return a.Normal.RuntimeEpoch
	}
	return 0
}

func (a Authority) Evidence() map[string]any {
	evidence := map[string]any{
		"authority_kind":   string(a.Kind),
		"authority_id":     strings.TrimSpace(a.ID),
		"execution_owner":  strings.TrimSpace(a.ExecutionOwner),
		"fence_generation": a.FenceGeneration,
		"execution_mode":   a.ExecutionMode,
	}
	if a.Target.Valid() {
		evidence["usage_target"] = map[string]any{
			"kind": a.Target.Kind, "id": a.Target.ID, "ordinal": a.Target.Ordinal,
			"run_id": a.Target.RunID, "agent_id": a.Target.AgentID, "session_id": a.Target.SessionID,
			"agent_identity": a.Target.AgentIdentity,
			"memory_enabled": a.Target.Memory.Enabled, "memory_source": a.Target.Memory.Source,
			"flow_instance": a.Target.FlowInstance, "entity_id": a.Target.EntityID,
		}
	}
	switch a.Kind {
	case AuthorityNormalAgent:
		evidence["agent_id"] = a.Normal.AgentID
		evidence["runtime_epoch"] = a.Normal.RuntimeEpoch
		evidence["generation"] = a.Normal.Generation
	case AuthoritySelectedContractFork:
		evidence["execution_id"] = a.SelectedFork.ExecutionID
		evidence["fork_run_id"] = a.SelectedFork.ForkRunID
		evidence["generation"] = a.SelectedFork.Generation
		evidence["admission_fingerprint"] = a.SelectedFork.AdmissionFingerprint
		evidence["container_plan_fingerprint"] = a.SelectedFork.ContainerPlanFingerprint
		evidence["actor_census_fingerprint"] = a.SelectedFork.ActorCensusFingerprint
		evidence["effective_config_fingerprint"] = a.SelectedFork.EffectiveConfigFingerprint
	case AuthorityConversationForkChat:
		evidence["fork_turn_id"] = a.ForkChat.ForkTurnID
		evidence["fork_id"] = a.ForkChat.ForkID
		evidence["source_run_id"] = a.ForkChat.SourceRunID
		evidence["bundle_hash"] = a.ForkChat.BundleHash
		evidence["actor_token_id"] = a.ForkChat.ActorTokenID
		evidence["request_occurrence_id"] = a.ForkChat.RequestOccurrenceID
		evidence["request_hash"] = a.ForkChat.RequestHash
	case AuthorityStartupProbe:
		evidence["probe_id"] = a.StartupProbe.ProbeID
		evidence["startup_authority_id"] = a.StartupProbe.StartupAuthorityID
		evidence["startup_state_version"] = a.StartupProbe.StartupStateVersion
		evidence["actor_id"] = a.StartupProbe.ActorID
		evidence["execution_kind"] = a.StartupProbe.ExecutionKind
		evidence["execution_authority_id"] = a.StartupProbe.ExecutionAuthorityID
		if a.StartupProbe.Preparation != nil {
			evidence["preparation"] = *a.StartupProbe.Preparation
		}
	case AuthorityServeRegistration:
		evidence["intent_id"] = a.ServeRegistration.IntentID
		evidence["startup_authority_id"] = a.ServeRegistration.StartupAuthorityID
		evidence["startup_state_version"] = a.ServeRegistration.StartupStateVersion
		if strings.TrimSpace(a.ServeRegistration.OnboardingOperationID) != "" {
			evidence["onboarding_operation_id"] = a.ServeRegistration.OnboardingOperationID
			evidence["onboarding_revision"] = a.ServeRegistration.OnboardingRevision
			evidence["bundle_hash"] = a.ServeRegistration.BundleHash
			evidence["bundle_identity"] = a.ServeRegistration.BundleIdentity
			evidence["pack_inventory_generation"] = a.ServeRegistration.PackInventoryGeneration
			evidence["runtime_instance_id"] = a.ServeRegistration.RuntimeInstanceID
			evidence["context_publication_generation"] = a.ServeRegistration.ContextPublicationGeneration
			evidence["plan_generation"] = a.ServeRegistration.PlanGeneration.Diagnostic()
			evidence["target_generation"] = a.ServeRegistration.TargetGeneration
		}
	case AuthorityChannelConfirmation:
		confirmation := a.ChannelConfirmation
		evidence["effect_operation_id"] = confirmation.EffectOperationID
		evidence["onboarding_operation_id"] = confirmation.OnboardingOperationID
		evidence["onboarding_revision"] = confirmation.OnboardingRevision
		evidence["activation_id"] = confirmation.ActivationID
		evidence["activation_revision"] = confirmation.ActivationRevision
		evidence["binding_revision"] = confirmation.BindingRevision
		evidence["principal_id"] = confirmation.PrincipalID
		evidence["bundle_hash"] = confirmation.BundleHash
		evidence["bundle_identity"] = confirmation.BundleIdentity
		evidence["pack_inventory_generation"] = confirmation.PackInventoryGeneration
		evidence["runtime_instance_id"] = confirmation.RuntimeInstanceID
		evidence["context_publication_generation"] = confirmation.ContextPublicationGeneration
		evidence["plan_generation"] = confirmation.PlanGeneration.Diagnostic()
		evidence["target_generation"] = confirmation.TargetGeneration
	case AuthorityChannelDelivery:
		delivery := a.ChannelDelivery
		evidence["effect_operation_id"] = delivery.EffectOperationID
		evidence["delivery_id"] = delivery.DeliveryID
		evidence["render_id"] = delivery.RenderID
		evidence["render_hash"] = delivery.RenderHash
		evidence["principal_id"] = delivery.PrincipalID
		evidence["interface_key"] = delivery.InterfaceKey
		evidence["delivery_epoch"] = delivery.DeliveryEpoch
		evidence["binding_revision"] = delivery.BindingRevision
		evidence["external_account_reference"] = delivery.ExternalAccountRef
		evidence["conversation_reference"] = delivery.ConversationRef
		evidence["activation_id"] = delivery.ActivationID
		evidence["activation_revision"] = delivery.ActivationRevision
		evidence["bundle_hash"] = delivery.BundleHash
		evidence["bundle_identity"] = delivery.BundleIdentity
		evidence["pack_inventory_generation"] = delivery.PackInventoryGeneration
		evidence["runtime_instance_id"] = delivery.RuntimeInstanceID
		evidence["context_publication_generation"] = delivery.ContextPublicationGeneration
		evidence["plan_generation"] = delivery.PlanGeneration.Diagnostic()
		evidence["target_generation"] = delivery.TargetGeneration
	case AuthorityChannelNativeSetting:
		setting := a.ChannelNativeSetting
		evidence["effect_operation_id"] = setting.EffectOperationID
		evidence["setting_id"] = setting.SettingID
		evidence["setting_generation"] = setting.SettingGeneration
		evidence["provider"] = setting.Provider
		evidence["resource_slot_id"] = setting.ResourceSlotID
		evidence["conversation_reference"] = setting.ConversationRef
		evidence["scope_kind"] = setting.ScopeKind
		evidence["member_reference"] = setting.MemberReference
		evidence["principal_id"] = setting.PrincipalID
		evidence["entry_contract_hash"] = setting.EntryContractHash
		evidence["entry_command"] = setting.EntryCommand
		evidence["pack_id"] = setting.PackID
		evidence["pack_version"] = setting.PackVersion
		evidence["pack_manifest_hash"] = setting.PackManifestHash
		evidence["activation_id"] = setting.ActivationID
		evidence["activation_revision"] = setting.ActivationRevision
		evidence["binding_revision"] = setting.BindingRevision
		evidence["bundle_hash"] = setting.BundleHash
		evidence["bundle_identity"] = setting.BundleIdentity
		evidence["pack_inventory_generation"] = setting.PackInventoryGeneration
		evidence["runtime_instance_id"] = setting.RuntimeInstanceID
		evidence["context_publication_generation"] = setting.ContextPublicationGeneration
		evidence["plan_generation"] = setting.PlanGeneration.Diagnostic()
		evidence["target_generation"] = setting.TargetGeneration
	}
	return evidence
}

func (a Authority) ValidateCompletionAdapter(adapter string) error {
	if !a.Valid() {
		return fmt.Errorf("completion execution authority is invalid")
	}
	registration, ok := RegistrationFor(strings.TrimSpace(adapter))
	if !ok || registration.Kind != KindProviderTurn {
		return fmt.Errorf("completion execution authority rejects non-provider adapter %q", strings.TrimSpace(adapter))
	}
	if !a.Target.Valid() {
		return fmt.Errorf("completion execution authority requires a valid preallocated usage target")
	}
	if a.Kind == AuthorityConversationForkChat && strings.TrimSpace(a.Target.RunID) != strings.TrimSpace(a.ForkChat.SourceRunID) {
		return fmt.Errorf("conversation fork completion target run_id does not match source run authority")
	}
	seen := map[string]struct{}{}
	for _, scope := range a.BudgetScopes {
		kind := strings.TrimSpace(scope.Kind)
		key := strings.TrimSpace(scope.Key)
		if scope.CapUSD <= 0 {
			return fmt.Errorf("completion budget admission scope %s has a non-positive cap", kind)
		}
		if kind != "system" && kind != "global" && kind != "entity" {
			return fmt.Errorf("completion budget admission scope %q is invalid", kind)
		}
		if (kind == "system" || kind == "global") && key != "" {
			return fmt.Errorf("completion budget admission scope %s must have an empty key", kind)
		}
		if kind == "entity" && key == "" {
			return fmt.Errorf("completion entity budget scope requires a key")
		}
		identity := kind + "\x00" + key
		if _, duplicate := seen[identity]; duplicate {
			return fmt.Errorf("completion budget admission scope %s is duplicated", kind)
		}
		seen[identity] = struct{}{}
	}
	return nil
}

type authorityContextKey struct{}
type executionModeContextKey struct{}

func WithExecutionMode(ctx context.Context, mode ExecutionMode) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, executionModeContextKey{}, mode)
	if authority, ok := AuthorityFromContext(ctx); ok {
		authority.ExecutionMode = mode
		ctx = context.WithValue(ctx, authorityContextKey{}, authority)
	}
	return ctx
}

func ExecutionModeFromContext(ctx context.Context) (ExecutionMode, bool) {
	if ctx == nil {
		return "", false
	}
	mode, ok := ctx.Value(executionModeContextKey{}).(ExecutionMode)
	return mode, ok && mode.Valid()
}

func WithAuthority(ctx context.Context, authority Authority) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, authorityContextKey{}, authority.clonePreparation())
}

func AuthorityFromContext(ctx context.Context) (Authority, bool) {
	if ctx == nil {
		return Authority{}, false
	}
	authority, ok := ctx.Value(authorityContextKey{}).(Authority)
	return authority.clonePreparation(), ok && authority.Valid()
}

func (a Authority) clonePreparation() Authority {
	if a.StartupProbe.Preparation != nil {
		preparation := *a.StartupProbe.Preparation
		a.StartupProbe.Preparation = &preparation
	}
	return a
}

func (a Authority) ValidatePreparedProbeRequest(req AuthorizeRequest) error {
	registration, registered := RegistrationFor(req.Adapter)
	if !a.Valid() || a.StartupProbe.Preparation == nil || !registered ||
		registration.Kind != KindProviderStartupProbe || req.Kind != registration.Kind ||
		req.Class != registration.Class || req.Transport != registration.Transport ||
		req.AgentFrame != nil || req.Origin != (CompletionOrigin{}) || req.CapabilitySurface == nil ||
		!StartupProbeSurfaceMatchesAuthority(*req.CapabilitySurface, a) || req.Lineage["run_id"] != "" {
		return fmt.Errorf("selected preparation authorizes only its exact startup probe")
	}
	return nil
}

func completionAuthorityFromContext(ctx context.Context) (Authority, bool) {
	if authority, ok := AuthorityFromContext(ctx); ok {
		return authority, true
	}
	token, ok := LifecycleTokenFromContext(ctx)
	if !ok {
		return Authority{}, false
	}
	owner := fmt.Sprintf("agent:%s:%d:%d", token.AgentID, token.RuntimeEpoch, token.Generation)
	authority := NormalAgentAuthority(token, owner, time.Now().UTC().Add(5*time.Minute))
	if mode, found := ExecutionModeFromContext(ctx); found {
		authority.ExecutionMode = mode
	}
	return authority, authority.Valid()
}

func CompletionAuthorityFromContext(ctx context.Context) (Authority, bool) {
	return completionAuthorityFromContext(ctx)
}

func WithUsageTarget(ctx context.Context, target UsageTarget) context.Context {
	authority, ok := completionAuthorityFromContext(ctx)
	if !ok {
		return ctx
	}
	authority.Target = target
	return WithAuthority(ctx, authority)
}

func WithBudgetAdmissionScopes(ctx context.Context, scopes []BudgetAdmissionScope) context.Context {
	authority, ok := completionAuthorityFromContext(ctx)
	if !ok {
		return ctx
	}
	authority.BudgetScopes = append([]BudgetAdmissionScope(nil), scopes...)
	return WithAuthority(ctx, authority)
}

func validUUIDs(values ...string) bool {
	for _, value := range values {
		if _, err := uuid.Parse(strings.TrimSpace(value)); err != nil {
			return false
		}
	}
	return true
}

func nonEmpty(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	return true
}
