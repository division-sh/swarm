package managedcapabilities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/google/uuid"
)

const SurfaceVersion = "managed-agent-capability-surface.v1"

type AuthorityKind string

const (
	AuthorityProviderTurn AuthorityKind = "provider_turn"
	AuthorityStartupProbe AuthorityKind = "startup_probe"
)

type ExecutionKind string

const (
	ExecutionNormalAgent          ExecutionKind = "normal_agent"
	ExecutionSelectedContractFork ExecutionKind = "selected_contract_fork"
)

type Authority struct {
	Kind                 AuthorityKind `json:"kind"`
	ID                   string        `json:"id"`
	ExecutionKind        ExecutionKind `json:"execution_kind"`
	ExecutionAuthorityID string        `json:"execution_authority_id"`
	RunID                string        `json:"run_id,omitempty"`
	SessionID            string        `json:"session_id,omitempty"`
	TurnOrdinal          int           `json:"turn_ordinal,omitempty"`
	StartupOwnerID       string        `json:"startup_owner_id,omitempty"`
	StartupGeneration    uint64        `json:"startup_generation,omitempty"`
}

func (a Authority) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(a.ID)); err != nil {
		return fmt.Errorf("managed capability authority id is invalid: %w", err)
	}
	if strings.TrimSpace(a.ExecutionAuthorityID) == "" {
		return fmt.Errorf("managed capability execution authority id is required")
	}
	switch a.ExecutionKind {
	case ExecutionNormalAgent, ExecutionSelectedContractFork:
	default:
		return fmt.Errorf("managed capability execution kind %q is invalid", a.ExecutionKind)
	}
	switch a.Kind {
	case AuthorityProviderTurn:
		if _, err := uuid.Parse(strings.TrimSpace(a.SessionID)); err != nil {
			return fmt.Errorf("managed capability provider-turn session id is invalid: %w", err)
		}
		if a.TurnOrdinal <= 0 || a.StartupOwnerID != "" || a.StartupGeneration != 0 {
			return fmt.Errorf("managed capability provider-turn authority is malformed")
		}
	case AuthorityStartupProbe:
		if strings.TrimSpace(a.StartupOwnerID) == "" || a.StartupGeneration == 0 || a.SessionID != "" || a.TurnOrdinal != 0 {
			return fmt.Errorf("managed capability startup-probe authority is malformed")
		}
	default:
		return fmt.Errorf("managed capability authority kind %q is invalid", a.Kind)
	}
	if a.ExecutionKind == ExecutionSelectedContractFork {
		if _, err := uuid.Parse(strings.TrimSpace(a.ExecutionAuthorityID)); err != nil {
			return fmt.Errorf("managed capability selected-fork execution authority id is invalid: %w", err)
		}
		if _, err := uuid.Parse(strings.TrimSpace(a.RunID)); err != nil {
			return fmt.Errorf("managed capability selected-fork run id is invalid: %w", err)
		}
	}
	return nil
}

type BindingKind string

const (
	BindingAPIDefinition   BindingKind = "api_definition"
	BindingProviderBuiltin BindingKind = "provider_builtin"
	BindingMCPTool         BindingKind = "mcp_tool"
	BindingMCPProvider     BindingKind = "mcp_provider_visible"
	BindingLocalRuntime    BindingKind = "local_runtime"
)

type EvidenceStatus string

const (
	EvidenceConfirmed   EvidenceStatus = "confirmed"
	EvidenceUnavailable EvidenceStatus = "unavailable"
	EvidenceMismatch    EvidenceStatus = "mismatch"
)

type DeliveryBinding struct {
	Kind                 BindingKind `json:"kind"`
	ExactName            string      `json:"exact_name"`
	RequiredEvidenceKind string      `json:"required_evidence_kind"`
}

type DeliveryEvidence struct {
	BindingKind BindingKind    `json:"binding_kind"`
	ExactName   string         `json:"exact_name"`
	Kind        string         `json:"kind"`
	Status      EvidenceStatus `json:"status"`
	Detail      string         `json:"detail,omitempty"`
}

type DeliveryMismatch struct {
	BindingKind BindingKind `json:"binding_kind"`
	ExactName   string      `json:"exact_name"`
	Kind        string      `json:"kind"`
	Detail      string      `json:"detail,omitempty"`
}

type Tool struct {
	Name              string                      `json:"name"`
	DefinitionHash    string                      `json:"definition_hash"`
	Capability        toolcapabilities.Capability `json:"capability"`
	Bindings          []DeliveryBinding           `json:"bindings"`
	Evidence          []DeliveryEvidence          `json:"evidence"`
	EffectiveVisible  bool                        `json:"effective_visible"`
	EffectiveCallable bool                        `json:"effective_callable"`
	EffectiveDenial   string                      `json:"effective_denial,omitempty"`
}

type Surface struct {
	Version          string             `json:"version"`
	ID               string             `json:"id"`
	IntegrityHash    string             `json:"integrity_hash"`
	ActorID          string             `json:"actor_id"`
	RuntimeMode      string             `json:"runtime_mode"`
	Provider         string             `json:"provider"`
	Transport        string             `json:"transport"`
	ProviderContract string             `json:"provider_contract"`
	Authority        Authority          `json:"authority"`
	Tools            []Tool             `json:"tools"`
	Mismatches       []DeliveryMismatch `json:"mismatches,omitempty"`
	CreatedAt        time.Time          `json:"created_at"`
}

type Persistence interface {
	SaveManagedCapabilitySurface(context.Context, Surface) error
}

type PlannedTool struct {
	Name           string
	DefinitionHash string
	Capability     toolcapabilities.Capability
	Bindings       []DeliveryBinding
}

type Plan struct {
	ActorID          string
	RuntimeMode      string
	Provider         string
	Transport        string
	ProviderContract string
	Authority        Authority
	Tools            []PlannedTool
	CreatedAt        time.Time
}

// PlanFingerprint identifies the exact callable plan independently of the
// preallocated per-attempt authority and provider-observation evidence.
func (s Surface) PlanFingerprint() (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	type plannedTool struct {
		Name           string
		DefinitionHash string
		Capability     toolcapabilities.Capability
		Bindings       []DeliveryBinding
	}
	tools := make([]plannedTool, 0, len(s.Tools))
	for _, tool := range s.Tools {
		tools = append(tools, plannedTool{
			Name: tool.Name, DefinitionHash: tool.DefinitionHash,
			Capability: tool.Capability, Bindings: append([]DeliveryBinding(nil), tool.Bindings...),
		})
	}
	authority := s.Authority
	authority.ID = ""
	return hashValue(struct {
		Version          string
		ActorID          string
		RuntimeMode      string
		Provider         string
		Transport        string
		ProviderContract string
		Authority        Authority
		Tools            []plannedTool
	}{s.Version, s.ActorID, s.RuntimeMode, s.Provider, s.Transport, s.ProviderContract, authority, tools})
}

func New(plan Plan) (Surface, error) {
	if err := plan.Authority.Validate(); err != nil {
		return Surface{}, err
	}
	if strings.TrimSpace(plan.ActorID) == "" || strings.TrimSpace(plan.Provider) == "" || strings.TrimSpace(plan.Transport) == "" || strings.TrimSpace(plan.ProviderContract) == "" {
		return Surface{}, fmt.Errorf("managed capability surface requires actor, provider, transport, and provider contract")
	}
	s := Surface{
		Version:          SurfaceVersion,
		ActorID:          strings.TrimSpace(plan.ActorID),
		RuntimeMode:      strings.TrimSpace(plan.RuntimeMode),
		Provider:         strings.TrimSpace(plan.Provider),
		Transport:        strings.TrimSpace(plan.Transport),
		ProviderContract: strings.TrimSpace(plan.ProviderContract),
		Authority:        plan.Authority,
		CreatedAt:        plan.CreatedAt.UTC(),
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	seen := make(map[string]struct{}, len(plan.Tools))
	for _, planned := range plan.Tools {
		name := toolidentity.CanonicalName(planned.Name)
		if name == "" {
			return Surface{}, fmt.Errorf("managed capability planned tool name is required")
		}
		if _, duplicate := seen[name]; duplicate {
			return Surface{}, fmt.Errorf("managed capability planned tool %s is duplicated", name)
		}
		seen[name] = struct{}{}
		capability := planned.Capability
		capability.Name = name
		bindings, err := normalizeBindings(planned.Bindings)
		if err != nil {
			return Surface{}, fmt.Errorf("managed capability planned tool %s: %w", name, err)
		}
		tool := Tool{
			Name:           name,
			DefinitionHash: strings.TrimSpace(planned.DefinitionHash),
			Capability:     capability,
			Bindings:       bindings,
		}
		if tool.DefinitionHash == "" {
			return Surface{}, fmt.Errorf("managed capability planned tool %s requires definition identity", name)
		}
		resolveTool(&tool)
		s.Tools = append(s.Tools, tool)
	}
	slices.SortFunc(s.Tools, func(a, b Tool) int { return strings.Compare(a.Name, b.Name) })
	planHash, err := hashValue(struct {
		Version          string
		ActorID          string
		RuntimeMode      string
		Provider         string
		Transport        string
		ProviderContract string
		Authority        Authority
		Tools            []Tool
	}{s.Version, s.ActorID, s.RuntimeMode, s.Provider, s.Transport, s.ProviderContract, s.Authority, s.Tools})
	if err != nil {
		return Surface{}, err
	}
	s.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(planHash)).String()
	if err := s.refreshIntegrityHash(); err != nil {
		return Surface{}, err
	}
	return s, s.Validate()
}

func (s Surface) Validate() error {
	if s.Version != SurfaceVersion {
		return fmt.Errorf("managed capability surface version %q is unsupported", s.Version)
	}
	if _, err := uuid.Parse(strings.TrimSpace(s.ID)); err != nil {
		return fmt.Errorf("managed capability surface id is invalid: %w", err)
	}
	if err := s.Authority.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(s.IntegrityHash) == "" || strings.TrimSpace(s.ActorID) == "" || strings.TrimSpace(s.RuntimeMode) == "" || strings.TrimSpace(s.Provider) == "" || strings.TrimSpace(s.Transport) == "" || strings.TrimSpace(s.ProviderContract) == "" || s.CreatedAt.IsZero() {
		return fmt.Errorf("managed capability surface identity is incomplete")
	}
	switch s.Transport {
	case "api", "cli", "in_process":
	default:
		return fmt.Errorf("managed capability surface transport %q is invalid", s.Transport)
	}
	seen := make(map[string]struct{}, len(s.Tools))
	previousToolName := ""
	for _, tool := range s.Tools {
		if tool.Name == "" || tool.Name != toolidentity.CanonicalName(tool.Name) || tool.DefinitionHash == "" || tool.Capability.Name != tool.Name {
			return fmt.Errorf("managed capability tool identity is incomplete")
		}
		if _, duplicate := seen[tool.Name]; duplicate {
			return fmt.Errorf("managed capability tool %s is duplicated", tool.Name)
		}
		if previousToolName != "" && strings.Compare(previousToolName, tool.Name) >= 0 {
			return fmt.Errorf("managed capability tools are not canonically ordered")
		}
		seen[tool.Name] = struct{}{}
		previousToolName = tool.Name
		previousBindingKey := ""
		bindingKeys := make(map[string]struct{}, len(tool.Bindings))
		for _, binding := range tool.Bindings {
			if err := validateBinding(binding); err != nil {
				return fmt.Errorf("managed capability tool %s: %w", tool.Name, err)
			}
			key := deliveryBindingKey(binding)
			if _, duplicate := bindingKeys[key]; duplicate {
				return fmt.Errorf("managed capability tool %s binding %s is duplicated", tool.Name, key)
			}
			if previousBindingKey != "" && strings.Compare(previousBindingKey, key) >= 0 {
				return fmt.Errorf("managed capability tool %s bindings are not canonically ordered", tool.Name)
			}
			bindingKeys[key] = struct{}{}
			previousBindingKey = key
		}
		previousEvidenceKey := ""
		evidenceKeys := make(map[string]struct{}, len(tool.Evidence))
		for _, evidence := range tool.Evidence {
			if err := validateEvidence(evidence, tool.Bindings); err != nil {
				return fmt.Errorf("managed capability tool %s: %w", tool.Name, err)
			}
			key := deliveryEvidenceKey(evidence)
			if _, duplicate := evidenceKeys[key]; duplicate {
				return fmt.Errorf("managed capability tool %s evidence %s is duplicated", tool.Name, key)
			}
			if previousEvidenceKey != "" && strings.Compare(previousEvidenceKey, key) >= 0 {
				return fmt.Errorf("managed capability tool %s evidence is not canonically ordered", tool.Name)
			}
			evidenceKeys[key] = struct{}{}
			previousEvidenceKey = key
		}
		resolved := tool
		resolveTool(&resolved)
		if len(s.Mismatches) > 0 {
			resolved.EffectiveVisible = false
			resolved.EffectiveCallable = false
			resolved.EffectiveDenial = "delivery_mismatch"
		}
		if resolved.EffectiveVisible != tool.EffectiveVisible || resolved.EffectiveCallable != tool.EffectiveCallable || resolved.EffectiveDenial != tool.EffectiveDenial {
			return fmt.Errorf("managed capability tool %s effective state is not derived from its plan and evidence", tool.Name)
		}
	}
	seenMismatch := make(map[string]struct{}, len(s.Mismatches))
	previousMismatchKey := ""
	for _, mismatch := range s.Mismatches {
		if !validBindingKind(mismatch.BindingKind) || strings.TrimSpace(mismatch.ExactName) == "" || strings.TrimSpace(mismatch.Kind) == "" {
			return fmt.Errorf("managed capability mismatch identity is incomplete")
		}
		key := deliveryMismatchKey(mismatch)
		if _, duplicate := seenMismatch[key]; duplicate {
			return fmt.Errorf("managed capability mismatch %s is duplicated", key)
		}
		if previousMismatchKey != "" && strings.Compare(previousMismatchKey, key) >= 0 {
			return fmt.Errorf("managed capability mismatches are not canonically ordered")
		}
		seenMismatch[key] = struct{}{}
		previousMismatchKey = key
	}
	wantID, err := s.planID()
	if err != nil {
		return err
	}
	if wantID != s.ID {
		return fmt.Errorf("managed capability surface plan identity mismatch")
	}
	copy := s
	copy.IntegrityHash = ""
	want, err := hashValue(copy)
	if err != nil {
		return err
	}
	if want != s.IntegrityHash {
		return fmt.Errorf("managed capability surface integrity hash mismatch")
	}
	return nil
}

// CanAdvanceFrom accepts only additional evidence or explicit narrowing of a
// previously confirmed observation. Planned authority and prior denial facts
// are immutable for the life of a surface.
func (s Surface) CanAdvanceFrom(previous Surface) error {
	if err := previous.Validate(); err != nil {
		return fmt.Errorf("validate previous managed capability surface: %w", err)
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if s.ID != previous.ID {
		return fmt.Errorf("managed capability surface plan identity changed")
	}
	for _, prior := range previous.Mismatches {
		if !slices.Contains(s.Mismatches, prior) {
			return fmt.Errorf("managed capability mismatch %s/%s/%s was removed", prior.BindingKind, prior.ExactName, prior.Kind)
		}
	}
	for _, priorTool := range previous.Tools {
		var nextTool *Tool
		for i := range s.Tools {
			if s.Tools[i].Name == priorTool.Name {
				nextTool = &s.Tools[i]
				break
			}
		}
		if nextTool == nil {
			return fmt.Errorf("managed capability tool %s was removed", priorTool.Name)
		}
		for _, prior := range priorTool.Evidence {
			next, ok := matchingEvidence(nextTool.Evidence, prior)
			if !ok {
				return fmt.Errorf("managed capability evidence %s/%s/%s was removed", prior.BindingKind, prior.ExactName, prior.Kind)
			}
			if prior.Status == EvidenceUnavailable || prior.Status == EvidenceMismatch {
				if next != prior {
					return fmt.Errorf("managed capability denial evidence %s/%s/%s was widened", prior.BindingKind, prior.ExactName, prior.Kind)
				}
			} else if next.Status == EvidenceConfirmed && next != prior {
				return fmt.Errorf("managed capability confirmed evidence %s/%s/%s was rewritten", prior.BindingKind, prior.ExactName, prior.Kind)
			} else if next.Status != EvidenceConfirmed && next.Status != EvidenceUnavailable && next.Status != EvidenceMismatch {
				return fmt.Errorf("managed capability evidence %s/%s/%s has invalid transition", prior.BindingKind, prior.ExactName, prior.Kind)
			}
		}
	}
	return nil
}

func (s Surface) Observe(evidence ...DeliveryEvidence) (Surface, error) {
	out := s.Clone()
	for _, observed := range evidence {
		observed.ExactName = strings.TrimSpace(observed.ExactName)
		observed.Kind = strings.TrimSpace(observed.Kind)
		if observed.ExactName == "" || observed.Kind == "" {
			return Surface{}, fmt.Errorf("managed capability delivery evidence requires exact binding name and kind")
		}
		switch observed.Status {
		case EvidenceConfirmed, EvidenceUnavailable, EvidenceMismatch:
		default:
			return Surface{}, fmt.Errorf("managed capability evidence status %q is invalid", observed.Status)
		}
		matched := false
		for i := range out.Tools {
			for _, binding := range out.Tools[i].Bindings {
				if binding.Kind == observed.BindingKind && binding.ExactName == observed.ExactName {
					if prior, ok := matchingEvidence(out.Tools[i].Evidence, observed); ok {
						if (prior.Status == EvidenceUnavailable || prior.Status == EvidenceMismatch) && prior != observed {
							return Surface{}, fmt.Errorf("managed capability denial evidence %s/%s/%s cannot be rewritten or widened", prior.BindingKind, prior.ExactName, prior.Kind)
						}
						if prior.Status == EvidenceConfirmed && observed.Status == EvidenceConfirmed && prior != observed {
							return Surface{}, fmt.Errorf("managed capability confirmed evidence %s/%s/%s cannot be rewritten", prior.BindingKind, prior.ExactName, prior.Kind)
						}
					}
					out.Tools[i].Evidence = replaceEvidence(out.Tools[i].Evidence, observed)
					resolveTool(&out.Tools[i])
					matched = true
				}
			}
		}
		if !matched {
			return Surface{}, fmt.Errorf("managed capability evidence names unplanned binding %s/%s", observed.BindingKind, observed.ExactName)
		}
	}
	if out.HasMismatch() {
		denyAllToolsForMismatch(&out)
	}
	if err := out.refreshIntegrityHash(); err != nil {
		return Surface{}, err
	}
	return out, out.Validate()
}

func (s Surface) ObserveMismatch(mismatches ...DeliveryMismatch) (Surface, error) {
	out := s.Clone()
	for _, mismatch := range mismatches {
		mismatch.ExactName = strings.TrimSpace(mismatch.ExactName)
		mismatch.Kind = strings.TrimSpace(mismatch.Kind)
		mismatch.Detail = strings.TrimSpace(mismatch.Detail)
		if mismatch.ExactName == "" || mismatch.Kind == "" {
			return Surface{}, fmt.Errorf("managed capability mismatch requires exact name and kind")
		}
		if prior, ok := matchingMismatch(out.Mismatches, mismatch); ok {
			if prior != mismatch {
				return Surface{}, fmt.Errorf("managed capability mismatch %s cannot be rewritten", deliveryMismatchKey(mismatch))
			}
			continue
		}
		out.Mismatches = append(out.Mismatches, mismatch)
	}
	slices.SortFunc(out.Mismatches, func(a, b DeliveryMismatch) int {
		return strings.Compare(string(a.BindingKind)+"\x00"+a.ExactName+"\x00"+a.Kind, string(b.BindingKind)+"\x00"+b.ExactName+"\x00"+b.Kind)
	})
	denyAllToolsForMismatch(&out)
	if err := out.refreshIntegrityHash(); err != nil {
		return Surface{}, err
	}
	return out, out.Validate()
}

func denyAllToolsForMismatch(surface *Surface) {
	if surface == nil {
		return
	}
	for i := range surface.Tools {
		surface.Tools[i].EffectiveVisible = false
		surface.Tools[i].EffectiveCallable = false
		surface.Tools[i].EffectiveDenial = "delivery_mismatch"
	}
}

func (s Surface) HasMismatch() bool {
	return len(s.Mismatches) > 0
}

func (s Surface) Capability(name string) (toolcapabilities.Capability, bool) {
	name = toolidentity.CanonicalName(name)
	for _, tool := range s.Tools {
		if tool.Name != name {
			continue
		}
		capability := tool.Capability
		capability.Visible = tool.EffectiveVisible
		capability.Callable = tool.EffectiveCallable
		capability.DenialReason = tool.EffectiveDenial
		return capability, true
	}
	return toolcapabilities.Capability{}, false
}

func (s Surface) CapabilitySet() toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(s.Tools))
	for _, tool := range s.Tools {
		capability, _ := s.Capability(tool.Name)
		caps = append(caps, capability)
	}
	return toolcapabilities.NewSet(caps)
}

func (s Surface) EffectiveNames() []string {
	out := make([]string, 0, len(s.Tools))
	for _, tool := range s.Tools {
		if tool.EffectiveVisible && tool.EffectiveCallable {
			out = append(out, tool.Name)
		}
	}
	return out
}

func (s Surface) BindingNames(kind BindingKind) []string {
	var out []string
	for _, tool := range s.Tools {
		for _, binding := range tool.Bindings {
			if binding.Kind == kind {
				out = append(out, binding.ExactName)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func (s Surface) PlannedBindingNames(kind BindingKind) []string {
	var out []string
	for _, tool := range s.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == kind {
				out = append(out, binding.ExactName)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func (s Surface) Clone() Surface {
	out := s
	out.Tools = append([]Tool(nil), s.Tools...)
	for i := range out.Tools {
		out.Tools[i].Bindings = append([]DeliveryBinding(nil), s.Tools[i].Bindings...)
		out.Tools[i].Evidence = append([]DeliveryEvidence(nil), s.Tools[i].Evidence...)
	}
	out.Mismatches = append([]DeliveryMismatch(nil), s.Mismatches...)
	return out
}

func (s *Surface) refreshIntegrityHash() error {
	s.IntegrityHash = ""
	hash, err := hashValue(*s)
	if err != nil {
		return err
	}
	s.IntegrityHash = hash
	return nil
}

func (s Surface) planID() (string, error) {
	planned := s.Clone()
	planned.ID = ""
	planned.IntegrityHash = ""
	planned.Mismatches = nil
	for i := range planned.Tools {
		planned.Tools[i].Evidence = nil
		resolveTool(&planned.Tools[i])
	}
	planHash, err := hashValue(struct {
		Version          string
		ActorID          string
		RuntimeMode      string
		Provider         string
		Transport        string
		ProviderContract string
		Authority        Authority
		Tools            []Tool
	}{planned.Version, planned.ActorID, planned.RuntimeMode, planned.Provider, planned.Transport, planned.ProviderContract, planned.Authority, planned.Tools})
	if err != nil {
		return "", err
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(planHash)).String(), nil
}

func matchingEvidence(evidence []DeliveryEvidence, target DeliveryEvidence) (DeliveryEvidence, bool) {
	for _, item := range evidence {
		if item.BindingKind == target.BindingKind && item.ExactName == target.ExactName && item.Kind == target.Kind {
			return item, true
		}
	}
	return DeliveryEvidence{}, false
}

func matchingMismatch(mismatches []DeliveryMismatch, target DeliveryMismatch) (DeliveryMismatch, bool) {
	key := deliveryMismatchKey(target)
	for _, mismatch := range mismatches {
		if deliveryMismatchKey(mismatch) == key {
			return mismatch, true
		}
	}
	return DeliveryMismatch{}, false
}

func deliveryMismatchKey(mismatch DeliveryMismatch) string {
	return string(mismatch.BindingKind) + "\x00" + strings.TrimSpace(mismatch.ExactName) + "\x00" + strings.TrimSpace(mismatch.Kind)
}

func resolveTool(tool *Tool) {
	tool.EffectiveVisible = false
	tool.EffectiveCallable = false
	tool.EffectiveDenial = strings.TrimSpace(tool.Capability.DenialReason)
	if !tool.Capability.Visible || !tool.Capability.Callable {
		if tool.EffectiveDenial == "" {
			tool.EffectiveDenial = "planned_not_callable"
		}
		return
	}
	if len(tool.Bindings) == 0 {
		tool.EffectiveDenial = "delivery_binding_missing"
		return
	}
	for _, evidence := range tool.Evidence {
		if evidence.Status == EvidenceMismatch {
			tool.EffectiveDenial = "delivery_mismatch"
			return
		}
		if evidence.Status == EvidenceUnavailable {
			tool.EffectiveDenial = "delivery_unavailable"
			return
		}
	}
	for _, binding := range tool.Bindings {
		confirmed := false
		for _, evidence := range tool.Evidence {
			if evidence.BindingKind != binding.Kind || evidence.ExactName != binding.ExactName || evidence.Kind != binding.RequiredEvidenceKind {
				continue
			}
			if evidence.Status == EvidenceConfirmed {
				confirmed = true
				break
			}
		}
		if !confirmed {
			tool.EffectiveDenial = "required_delivery_evidence_missing"
			return
		}
	}
	tool.EffectiveVisible = true
	tool.EffectiveCallable = true
	tool.EffectiveDenial = ""
}

func normalizeBindings(bindings []DeliveryBinding) ([]DeliveryBinding, error) {
	out := make([]DeliveryBinding, 0, len(bindings))
	seen := map[string]struct{}{}
	for _, binding := range bindings {
		binding.ExactName = strings.TrimSpace(binding.ExactName)
		binding.RequiredEvidenceKind = strings.TrimSpace(binding.RequiredEvidenceKind)
		if err := validateBinding(binding); err != nil {
			return nil, err
		}
		key := deliveryBindingKey(binding)
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("delivery binding %s is duplicated", key)
		}
		seen[key] = struct{}{}
		out = append(out, binding)
	}
	slices.SortFunc(out, func(a, b DeliveryBinding) int {
		return strings.Compare(deliveryBindingKey(a), deliveryBindingKey(b))
	})
	return out, nil
}

func replaceEvidence(existing []DeliveryEvidence, observed DeliveryEvidence) []DeliveryEvidence {
	out := append([]DeliveryEvidence(nil), existing...)
	for i := range out {
		if out[i].BindingKind == observed.BindingKind && out[i].ExactName == observed.ExactName && out[i].Kind == observed.Kind {
			out[i] = observed
			return out
		}
	}
	out = append(out, observed)
	slices.SortFunc(out, func(a, b DeliveryEvidence) int {
		return strings.Compare(deliveryEvidenceKey(a), deliveryEvidenceKey(b))
	})
	return out
}

func validateBinding(binding DeliveryBinding) error {
	if !validBindingKind(binding.Kind) || strings.TrimSpace(binding.ExactName) == "" || strings.TrimSpace(binding.RequiredEvidenceKind) == "" {
		return fmt.Errorf("delivery binding identity is incomplete")
	}
	return nil
}

func validateEvidence(evidence DeliveryEvidence, bindings []DeliveryBinding) error {
	if !validBindingKind(evidence.BindingKind) || strings.TrimSpace(evidence.ExactName) == "" || strings.TrimSpace(evidence.Kind) == "" {
		return fmt.Errorf("delivery evidence identity is incomplete")
	}
	switch evidence.Status {
	case EvidenceConfirmed, EvidenceUnavailable, EvidenceMismatch:
	default:
		return fmt.Errorf("delivery evidence status %q is invalid", evidence.Status)
	}
	for _, binding := range bindings {
		if binding.Kind == evidence.BindingKind && binding.ExactName == evidence.ExactName && binding.RequiredEvidenceKind == evidence.Kind {
			return nil
		}
	}
	return fmt.Errorf("delivery evidence names no exact planned binding")
}

func validBindingKind(kind BindingKind) bool {
	switch kind {
	case BindingAPIDefinition, BindingProviderBuiltin, BindingMCPTool, BindingMCPProvider, BindingLocalRuntime:
		return true
	default:
		return false
	}
}

func deliveryBindingKey(binding DeliveryBinding) string {
	return string(binding.Kind) + "\x00" + strings.TrimSpace(binding.ExactName) + "\x00" + strings.TrimSpace(binding.RequiredEvidenceKind)
}

func deliveryEvidenceKey(evidence DeliveryEvidence) string {
	return string(evidence.BindingKind) + "\x00" + strings.TrimSpace(evidence.ExactName) + "\x00" + strings.TrimSpace(evidence.Kind)
}

func hashValue(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal managed capability identity: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

type contextKey struct{}

func WithContext(ctx context.Context, surface Surface) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, surface.Clone())
}

func FromContext(ctx context.Context) (Surface, bool) {
	if ctx == nil {
		return Surface{}, false
	}
	surface, ok := ctx.Value(contextKey{}).(Surface)
	if !ok {
		return Surface{}, false
	}
	return surface.Clone(), true
}
