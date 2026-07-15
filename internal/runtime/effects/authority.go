package effects

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type AuthorityKind string

const (
	AuthorityNormalAgent          AuthorityKind = "normal_agent"
	AuthoritySelectedContractFork AuthorityKind = "selected_contract_fork"
	AuthorityConversationForkChat AuthorityKind = "conversation_fork_chat"
	AuthorityStartupProbe         AuthorityKind = "startup_probe"
)

type UsageTargetKind string

const (
	UsageTargetAgentTurn                  UsageTargetKind = "agent_turn"
	UsageTargetConversationForkCompletion UsageTargetKind = "conversation_fork_turn_completion"
)

type UsageTarget struct {
	Kind         UsageTargetKind
	ID           string
	Ordinal      int
	RunID        string
	AgentID      string
	SessionID    string
	Memory       agentmemory.Plan
	FlowInstance string
	EntityID     string
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
		memory, err := t.Memory.Normalize()
		return err == nil && (!memory.Enabled || strings.TrimSpace(t.FlowInstance) != "")
	case UsageTargetConversationForkCompletion:
		return t.Ordinal > 0
	default:
		return false
	}
}

func ProviderTurnTargetMatchesCapabilitySurface(target UsageTarget, surface managedcapabilities.Surface) bool {
	return target.Kind == UsageTargetAgentTurn && target.Valid() &&
		surface.Authority.Kind == managedcapabilities.AuthorityProviderTurn &&
		surface.Authority.ID == target.ID && surface.ActorID == target.AgentID &&
		surface.Authority.SessionID == target.SessionID && surface.Authority.RunID == target.RunID
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
}

type Authority struct {
	Kind            AuthorityKind
	ID              string
	Normal          LifecycleToken
	SelectedFork    SelectedContractForkAuthority
	ForkChat        ConversationForkChatAuthority
	StartupProbe    StartupProbeAuthority
	ExecutionOwner  string
	LeaseExpiresAt  time.Time
	FenceGeneration uint64
	Target          UsageTarget
	BudgetScopes    []BudgetAdmissionScope
	ExecutionMode   ExecutionMode
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
		return validUUIDs(a.ForkChat.ForkTurnID, a.ForkChat.ForkID, a.ForkChat.RequestOccurrenceID) &&
			a.ID == strings.TrimSpace(a.ForkChat.ForkTurnID) && nonEmpty(a.ForkChat.BundleHash, a.ForkChat.ActorTokenID, a.ForkChat.RequestHash)
	case AuthorityStartupProbe:
		return validUUIDs(a.StartupProbe.ProbeID, a.StartupProbe.StartupAuthorityID) &&
			a.ID == strings.TrimSpace(a.StartupProbe.ProbeID) && a.StartupProbe.StartupStateVersion > 0 &&
			nonEmpty(a.StartupProbe.ActorID, a.StartupProbe.ExecutionKind, a.StartupProbe.ExecutionAuthorityID)
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
	case AuthorityConversationForkChat:
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
	return context.WithValue(ctx, authorityContextKey{}, authority)
}

func AuthorityFromContext(ctx context.Context) (Authority, bool) {
	if ctx == nil {
		return Authority{}, false
	}
	authority, ok := ctx.Value(authorityContextKey{}).(Authority)
	return authority, ok && authority.Valid()
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
