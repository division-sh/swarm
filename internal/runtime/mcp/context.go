package mcp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

type actorResolverFn func(context.Context) (models.AgentConfig, bool)

type TurnContext struct {
	Actor                  models.AgentConfig
	Inbound                events.Event
	HasInbound             bool
	RuntimeLineage         runtimecorrelation.RuntimeLineage
	HasRuntimeLineage      bool
	AuthorActivityScope    runtimeauthoractivity.Scope
	HasAuthorActivityScope bool
	BundleSourceFact       runtimecorrelation.BundleSourceFact
	HasBundleSourceFact    bool
	LifecycleToken         runtimeeffects.LifecycleToken
	HasLifecycleToken      bool
	EffectController       *runtimeeffects.Controller
	EffectAuthority        runtimeeffects.Authority
	HasEffectAuthority     bool
	DifferentOwner         runtimeeffects.DifferentOwner
	LogicalIdentity        string
	HasLogicalIdentity     bool
	CapabilitySurface      *managedcapabilities.Surface
	ExecutionAdmission     managedexecution.Admission
	HasExecutionAdmission  bool
	ForkSandboxAllowed     map[string]struct{}
	Recorder               *runtimebus.EmittedEventsRecorder
	Emitted                map[string]struct{}
	MCPCallOccurrences     map[string]struct{}
	CreatedAt              time.Time
	ExpiresAt              time.Time
}

type TurnContextRegistry struct {
	mu   sync.RWMutex
	data map[string]TurnContext

	actorResolver actorResolverFn
	defaultTTL    time.Duration
}

func NewTurnContextRegistry(resolve actorResolverFn) *TurnContextRegistry {
	return &TurnContextRegistry{
		data:          make(map[string]TurnContext),
		actorResolver: resolve,
		defaultTTL:    2 * time.Hour,
	}
}

func (r *TurnContextRegistry) RegisterTurnContext(ctx context.Context) string {
	if r == nil {
		return ""
	}
	return r.RegisterTurnContextWithTTL(ctx, r.defaultTTL)
}

func (r *TurnContextRegistry) RegisterTurnContextWithTTL(ctx context.Context, ttl time.Duration) string {
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return ""
	}
	return r.RegisterTurnContextWithCapabilitySurface(ctx, ttl, surface)
}

func (r *TurnContextRegistry) RegisterTurnContextWithCapabilitySurface(ctx context.Context, ttl time.Duration, surface managedcapabilities.Surface) string {
	if r == nil || r.actorResolver == nil {
		return ""
	}
	if err := surface.Validate(); err != nil {
		return ""
	}
	actor, ok := r.actorResolver(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.ID) != surface.ActorID {
		return ""
	}
	effectAuthority, hasEffectAuthority := runtimeeffects.AuthorityFromContext(ctx)
	if surface.Authority.Kind == managedcapabilities.AuthorityProviderTurn &&
		(!hasEffectAuthority || !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(effectAuthority.Target, surface)) {
		return ""
	}
	if ttl <= 0 {
		ttl = r.defaultTTL
	}
	now := time.Now().UTC()
	token := uuid.NewString()
	recorder, _ := runtimebus.EmittedEventsRecorderFromContext(ctx)
	inbound, hasInbound := runtimebus.InboundEventFromContext(ctx)
	lineage, hasLineage := runtimecorrelation.RuntimeLineageFromContext(ctx)
	authorActivityScope, hasAuthorActivityScope := runtimeauthoractivity.ScopeFromContext(ctx)
	bundleSourceFact, hasBundleSourceFact := runtimecorrelation.BundleSourceFactFromContext(ctx)
	lifecycleToken, hasLifecycleToken := runtimeeffects.LifecycleTokenFromContext(ctx)
	effectController, _ := runtimeeffects.ControllerFromContext(ctx)
	executionAdmission, hasExecutionAdmission := managedexecution.FromContext(ctx)
	differentOwner, _ := runtimeeffects.DifferentOwnerFromContext(ctx)
	logicalIdentity, hasLogicalIdentity := runtimeeffects.LogicalOperationIdentityFromContext(ctx)
	r.put(token, TurnContext{
		Actor:                  actor,
		Inbound:                inbound,
		HasInbound:             hasInbound,
		RuntimeLineage:         lineage,
		HasRuntimeLineage:      hasLineage,
		AuthorActivityScope:    authorActivityScope,
		HasAuthorActivityScope: hasAuthorActivityScope,
		BundleSourceFact:       bundleSourceFact,
		HasBundleSourceFact:    hasBundleSourceFact,
		LifecycleToken:         lifecycleToken,
		HasLifecycleToken:      hasLifecycleToken,
		EffectController:       effectController,
		EffectAuthority:        effectAuthority,
		HasEffectAuthority:     hasEffectAuthority,
		DifferentOwner:         differentOwner,
		LogicalIdentity:        logicalIdentity,
		HasLogicalIdentity:     hasLogicalIdentity,
		CapabilitySurface:      capabilitySurfacePointer(surface),
		ExecutionAdmission:     executionAdmission,
		HasExecutionAdmission:  hasExecutionAdmission,
		Recorder:               recorder,
		CreatedAt:              now,
		ExpiresAt:              now.Add(ttl),
	})
	return token
}

func (r *TurnContextRegistry) RegisterConversationForkSandboxTurnContext(ctx context.Context, ttl time.Duration, allowedTools []string) string {
	if r == nil || r.actorResolver == nil {
		return ""
	}
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || authority.Kind != runtimeeffects.AuthorityConversationForkChat || !authority.Valid() {
		return ""
	}
	actor, ok := r.actorResolver(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return ""
	}
	if ttl <= 0 {
		ttl = r.defaultTTL
	}
	now := time.Now().UTC()
	token := uuid.NewString()
	controller, _ := runtimeeffects.ControllerFromContext(ctx)
	logicalIdentity, hasLogicalIdentity := runtimeeffects.LogicalOperationIdentityFromContext(ctx)
	r.put(token, TurnContext{
		Actor: actor, EffectController: controller, EffectAuthority: authority, HasEffectAuthority: true,
		LogicalIdentity: logicalIdentity, HasLogicalIdentity: hasLogicalIdentity,
		ForkSandboxAllowed: normalizeForkSandboxTools(allowedTools), CreatedAt: now, ExpiresAt: now.Add(ttl),
	})
	return token
}

func (r *TurnContextRegistry) ObserveCapabilityEvidence(token string, evidence ...managedcapabilities.DeliveryEvidence) (managedcapabilities.Surface, bool) {
	if r == nil || strings.TrimSpace(token) == "" || len(evidence) == 0 {
		return managedcapabilities.Surface{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(time.Now().UTC())
	turn, ok := r.data[strings.TrimSpace(token)]
	if !ok || turn.CapabilitySurface == nil {
		return managedcapabilities.Surface{}, false
	}
	updated, err := turn.CapabilitySurface.Observe(evidence...)
	if err != nil {
		return managedcapabilities.Surface{}, false
	}
	turn.CapabilitySurface = capabilitySurfacePointer(updated)
	r.data[strings.TrimSpace(token)] = turn
	return updated, true
}

func (r *TurnContextRegistry) ObserveCapabilityMismatch(token string, mismatches ...managedcapabilities.DeliveryMismatch) (managedcapabilities.Surface, bool) {
	if r == nil || strings.TrimSpace(token) == "" || len(mismatches) == 0 {
		return managedcapabilities.Surface{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(time.Now().UTC())
	turn, ok := r.data[strings.TrimSpace(token)]
	if !ok || turn.CapabilitySurface == nil {
		return managedcapabilities.Surface{}, false
	}
	updated, err := turn.CapabilitySurface.ObserveMismatch(mismatches...)
	if err != nil {
		return managedcapabilities.Surface{}, false
	}
	turn.CapabilitySurface = capabilitySurfacePointer(updated)
	r.data[strings.TrimSpace(token)] = turn
	return updated, true
}

func (r *TurnContextRegistry) ObserveMCPProviderCall(token, toolName, occurrence string) (managedcapabilities.Surface, error) {
	if r == nil || strings.TrimSpace(token) == "" {
		return managedcapabilities.Surface{}, fmt.Errorf("MCP provider call requires turn context token")
	}
	canonical := toolidentity.CanonicalName(toolName)
	occurrence = strings.TrimSpace(occurrence)
	if canonical == "" || occurrence == "" {
		return managedcapabilities.Surface{}, fmt.Errorf("MCP provider call requires canonical tool and provider occurrence")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(time.Now().UTC())
	token = strings.TrimSpace(token)
	turn, ok := r.data[token]
	if !ok || turn.CapabilitySurface == nil {
		return managedcapabilities.Surface{}, fmt.Errorf("MCP provider call turn context is unavailable")
	}
	surface := turn.CapabilitySurface.Clone()
	if surface.HasMismatch() {
		return managedcapabilities.Surface{}, fmt.Errorf("MCP provider call capability surface is mismatched")
	}
	if surface.Authority.Kind == managedcapabilities.AuthorityProviderTurn &&
		(!turn.HasEffectAuthority || !runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(turn.EffectAuthority.Target, surface)) {
		updated, err := surface.ObserveMismatch(managedcapabilities.DeliveryMismatch{
			BindingKind: managedcapabilities.BindingMCPProvider, ExactName: toolidentity.RuntimeToolsMCPPrefix + canonical,
			Kind: "mcp_provider_call_authority_mismatch", Detail: "provider call authority does not match the planned turn surface",
		})
		if err != nil {
			return managedcapabilities.Surface{}, err
		}
		turn.CapabilitySurface = capabilitySurfacePointer(updated)
		r.data[token] = turn
		return updated, fmt.Errorf("MCP provider call authority does not match capability surface")
	}
	exactName := toolidentity.RuntimeToolsMCPPrefix + canonical
	planned := false
	for _, tool := range surface.Tools {
		if tool.Name != canonical || !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == managedcapabilities.BindingMCPProvider && binding.ExactName == exactName {
				planned = true
				break
			}
		}
	}
	if !planned {
		updated, err := surface.ObserveMismatch(managedcapabilities.DeliveryMismatch{
			BindingKind: managedcapabilities.BindingMCPProvider, ExactName: exactName,
			Kind: "unplanned_mcp_provider_call", Detail: "provider called a tool outside the planned callable surface",
		})
		if err != nil {
			return managedcapabilities.Surface{}, err
		}
		turn.CapabilitySurface = capabilitySurfacePointer(updated)
		r.data[token] = turn
		return updated, fmt.Errorf("MCP provider call tool %q is not planned", canonical)
	}
	occurrenceKey := runtimeeffects.Fingerprint([]byte(occurrence))
	if _, replay := turn.MCPCallOccurrences[occurrenceKey]; replay {
		updated, err := surface.ObserveMismatch(managedcapabilities.DeliveryMismatch{
			BindingKind: managedcapabilities.BindingMCPProvider, ExactName: exactName,
			Kind: "replayed_mcp_provider_call", Detail: "provider-owned tools/call occurrence was already consumed",
		})
		if err != nil {
			return managedcapabilities.Surface{}, err
		}
		turn.CapabilitySurface = capabilitySurfacePointer(updated)
		r.data[token] = turn
		return updated, fmt.Errorf("MCP provider call occurrence was already consumed")
	}
	updated, err := surface.Observe(managedcapabilities.DeliveryEvidence{
		BindingKind: managedcapabilities.BindingMCPProvider, ExactName: exactName,
		Kind: "mcp_visible", Status: managedcapabilities.EvidenceConfirmed,
	})
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	if turn.MCPCallOccurrences == nil {
		turn.MCPCallOccurrences = map[string]struct{}{}
	}
	turn.MCPCallOccurrences[occurrenceKey] = struct{}{}
	turn.CapabilitySurface = capabilitySurfacePointer(updated)
	r.data[token] = turn
	return updated, nil
}

func (r *TurnContextRegistry) ResolveTurnContext(token string) (TurnContext, bool) {
	if r == nil {
		return TurnContext{}, false
	}
	return r.get(strings.TrimSpace(token))
}

func (r *TurnContextRegistry) ResolveManagedCapabilitySurface(token string) (managedcapabilities.Surface, bool) {
	turn, ok := r.ResolveTurnContext(token)
	if !ok || turn.CapabilitySurface == nil {
		return managedcapabilities.Surface{}, false
	}
	return turn.CapabilitySurface.Clone(), true
}

func (r *TurnContextRegistry) MarkEmitUsed(token string) bool {
	if r == nil {
		return false
	}
	return r.markEmitKeyUsed(strings.TrimSpace(token), "__default__")
}

func (r *TurnContextRegistry) MarkEmitKeyUsed(token, key string) bool {
	if r == nil {
		return false
	}
	return r.markEmitKeyUsed(strings.TrimSpace(token), strings.TrimSpace(key))
}

func (r *TurnContextRegistry) UnregisterTurnContext(token string) {
	if r == nil {
		return
	}
	r.delete(strings.TrimSpace(token))
}

func (r *TurnContextRegistry) Reset() {
	if r == nil {
		return
	}
	r.reset()
}

func (r *TurnContextRegistry) PutTurnContextForTest(token string, data TurnContext) {
	if r == nil {
		return
	}
	r.put(strings.TrimSpace(token), data)
}

func (r *TurnContextRegistry) PruneTurnContextsBefore(now time.Time) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
}

func (r *TurnContextRegistry) put(token string, data TurnContext) {
	if strings.TrimSpace(token) == "" {
		return
	}
	now := time.Now().UTC()
	if data.CreatedAt.IsZero() {
		data.CreatedAt = now
	}
	if data.ExpiresAt.IsZero() {
		data.ExpiresAt = data.CreatedAt.Add(r.defaultTTL)
	}
	if data.CapabilitySurface != nil {
		data.CapabilitySurface = capabilitySurfacePointer(data.CapabilitySurface.Clone())
	}
	data.ForkSandboxAllowed = copyForkSandboxTools(data.ForkSandboxAllowed)
	if data.Emitted != nil {
		cloned := make(map[string]struct{}, len(data.Emitted))
		for key := range data.Emitted {
			cloned[key] = struct{}{}
		}
		data.Emitted = cloned
	}
	if data.MCPCallOccurrences != nil {
		cloned := make(map[string]struct{}, len(data.MCPCallOccurrences))
		for key := range data.MCPCallOccurrences {
			cloned[key] = struct{}{}
		}
		data.MCPCallOccurrences = cloned
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	r.data[token] = data
}

func (r *TurnContextRegistry) get(token string) (TurnContext, bool) {
	if strings.TrimSpace(token) == "" {
		return TurnContext{}, false
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	v, ok := r.data[token]
	if v.CapabilitySurface != nil {
		v.CapabilitySurface = capabilitySurfacePointer(v.CapabilitySurface.Clone())
	}
	v.ForkSandboxAllowed = copyForkSandboxTools(v.ForkSandboxAllowed)
	if v.Emitted != nil {
		cloned := make(map[string]struct{}, len(v.Emitted))
		for key := range v.Emitted {
			cloned[key] = struct{}{}
		}
		v.Emitted = cloned
	}
	if v.MCPCallOccurrences != nil {
		cloned := make(map[string]struct{}, len(v.MCPCallOccurrences))
		for key := range v.MCPCallOccurrences {
			cloned[key] = struct{}{}
		}
		v.MCPCallOccurrences = cloned
	}
	return v, ok
}

func (r *TurnContextRegistry) markEmitKeyUsed(token, key string) bool {
	if strings.TrimSpace(token) == "" {
		return false
	}
	if strings.TrimSpace(key) == "" {
		key = "__default__"
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneLocked(now)
	v, ok := r.data[token]
	if !ok {
		return false
	}
	if v.Emitted == nil {
		v.Emitted = map[string]struct{}{}
	}
	if _, ok := v.Emitted[key]; ok {
		return true
	}
	v.Emitted[key] = struct{}{}
	r.data[token] = v
	return false
}

func (r *TurnContextRegistry) delete(token string) {
	if strings.TrimSpace(token) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.data, token)
}

func (r *TurnContextRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = make(map[string]TurnContext)
}

func (r *TurnContextRegistry) pruneLocked(now time.Time) {
	for k, v := range r.data {
		if !v.ExpiresAt.IsZero() {
			if !v.ExpiresAt.After(now) {
				delete(r.data, k)
			}
			continue
		}
		if v.CreatedAt.Before(now.Add(-r.defaultTTL)) {
			delete(r.data, k)
		}
	}
}

func capabilitySurfacePointer(surface managedcapabilities.Surface) *managedcapabilities.Surface {
	copy := surface.Clone()
	return &copy
}

func normalizeForkSandboxTools(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if name := strings.TrimSpace(value); name != "" {
			out[name] = struct{}{}
		}
	}
	return out
}

func copyForkSandboxTools(values map[string]struct{}) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(values))
	for name := range values {
		out[name] = struct{}{}
	}
	return out
}
