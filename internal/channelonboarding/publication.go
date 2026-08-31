package channelonboarding

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
)

// ChannelActivationGeneration identifies one complete immutable publication
// of executable channel deployment state. It is deliberately a distinct type
// from structural channel plan and effective-source generations.
type ChannelActivationGeneration struct {
	generation plangeneration.Generation
}

type ChannelActivationPublicationMode string

const (
	ChannelActivationPublicationExecutable   ChannelActivationPublicationMode = "executable"
	ChannelActivationPublicationDeclaredOnly ChannelActivationPublicationMode = "declared_only"
)

func (m ChannelActivationPublicationMode) Valid() bool {
	return m == ChannelActivationPublicationExecutable || m == ChannelActivationPublicationDeclaredOnly
}

func (g ChannelActivationGeneration) Valid() bool {
	return g.generation.Valid()
}

func (g ChannelActivationGeneration) Equal(other ChannelActivationGeneration) bool {
	return g.generation.Equal(other.generation)
}

func (g ChannelActivationGeneration) Diagnostic() string {
	return g.generation.Diagnostic()
}

func (g ChannelActivationGeneration) MarshalJSON() ([]byte, error) {
	if !g.Valid() {
		return nil, fmt.Errorf("channel activation generation is missing")
	}
	return json.Marshal(g.Diagnostic())
}

func (g *ChannelActivationGeneration) UnmarshalJSON(raw []byte) error {
	if g == nil {
		return fmt.Errorf("channel activation generation destination is nil")
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return fmt.Errorf("decode channel activation generation: %w", err)
	}
	parsed, err := plangeneration.Parse(encoded)
	if err != nil {
		return err
	}
	g.generation = parsed
	return nil
}

// ChannelActivationPublication is the canonical process projection of all
// declared and learned activations in one runtime context. Records are sorted,
// validated, and copied before their generation is minted.
type ChannelActivationPublication struct {
	generation  ChannelActivationGeneration
	mode        ChannelActivationPublicationMode
	activations []CompiledActivation
	bindings    []packs.OutboundBindingPlan
}

func NewChannelActivationPublication(activations []CompiledActivation) (ChannelActivationPublication, error) {
	canonical, err := canonicalActivationPublication(activations)
	if err != nil {
		return ChannelActivationPublication{}, err
	}
	generation, err := plangeneration.FromCanonicalValue(canonical.value)
	if err != nil {
		return ChannelActivationPublication{}, fmt.Errorf("channel activation publication generation: %w", err)
	}
	return ChannelActivationPublication{
		generation:  ChannelActivationGeneration{generation: generation},
		mode:        ChannelActivationPublicationExecutable,
		activations: canonical.activations,
	}, nil
}

// NewDeclaredOnlyChannelActivationPublication admits configured channel
// bindings for validation before a runtime-context occurrence exists. This
// mode cannot be installed as executable authority and never synthesizes
// learned activation state.
func NewDeclaredOnlyChannelActivationPublication(bindings []packs.OutboundBindingPlan) (ChannelActivationPublication, error) {
	canonical, ordered, err := canonicalDeclaredOnlyPublication(bindings)
	if err != nil {
		return ChannelActivationPublication{}, err
	}
	generation, err := plangeneration.FromCanonicalValue(canonical)
	if err != nil {
		return ChannelActivationPublication{}, fmt.Errorf("declared-only channel activation publication generation: %w", err)
	}
	return ChannelActivationPublication{
		generation: ChannelActivationGeneration{generation: generation},
		mode:       ChannelActivationPublicationDeclaredOnly,
		bindings:   ordered,
	}, nil
}

func (p ChannelActivationPublication) Validate() error {
	if !p.generation.Valid() || !p.mode.Valid() {
		return fmt.Errorf("channel activation publication generation is missing")
	}
	var canonical ChannelActivationPublication
	var err error
	switch p.mode {
	case ChannelActivationPublicationExecutable:
		if len(p.bindings) != 0 {
			return fmt.Errorf("executable channel activation publication contains declared-only bindings")
		}
		canonical, err = NewChannelActivationPublication(p.activations)
	case ChannelActivationPublicationDeclaredOnly:
		if len(p.activations) != 0 {
			return fmt.Errorf("declared-only channel activation publication contains executable activations")
		}
		canonical, err = NewDeclaredOnlyChannelActivationPublication(p.bindings)
	}
	if err != nil {
		return err
	}
	if !p.generation.Equal(canonical.generation) {
		return fmt.Errorf("channel activation publication generation contradicts its records")
	}
	return nil
}

func (p ChannelActivationPublication) Generation() ChannelActivationGeneration {
	return p.generation
}

func (p ChannelActivationPublication) Mode() ChannelActivationPublicationMode {
	return p.mode
}

func (p ChannelActivationPublication) Executable() bool {
	return p.mode == ChannelActivationPublicationExecutable
}

func (p ChannelActivationPublication) Activations() []CompiledActivation {
	return cloneCompiledActivations(p.activations)
}

func (p ChannelActivationPublication) Bindings() []packs.OutboundBindingPlan {
	if p.mode == ChannelActivationPublicationDeclaredOnly {
		return append([]packs.OutboundBindingPlan(nil), p.bindings...)
	}
	out := make([]packs.OutboundBindingPlan, 0, len(p.activations))
	for _, activation := range p.activations {
		out = append(out, activation.Plan)
	}
	return out
}

type canonicalPublication struct {
	activations []CompiledActivation
	value       []any
}

func canonicalActivationPublication(activations []CompiledActivation) (canonicalPublication, error) {
	ordered := cloneCompiledActivations(activations)
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if left.Coordinate.BundleHash != right.Coordinate.BundleHash {
			return left.Coordinate.BundleHash < right.Coordinate.BundleHash
		}
		if left.Coordinate.RuntimeInstanceID != right.Coordinate.RuntimeInstanceID {
			return left.Coordinate.RuntimeInstanceID < right.Coordinate.RuntimeInstanceID
		}
		if left.Coordinate.ContextPublicationGeneration != right.Coordinate.ContextPublicationGeneration {
			return left.Coordinate.ContextPublicationGeneration < right.Coordinate.ContextPublicationGeneration
		}
		return left.Plan.BindingID() < right.Plan.BindingID()
	})
	values := make([]any, 0, len(ordered))
	seenBindings := map[string]struct{}{}
	var contextKey string
	for _, activation := range ordered {
		if err := activation.Validate(); err != nil {
			return canonicalPublication{}, err
		}
		bindingID := activation.Plan.BindingID()
		if _, duplicate := seenBindings[bindingID]; duplicate {
			return canonicalPublication{}, fmt.Errorf("duplicate channel activation binding %q", bindingID)
		}
		seenBindings[bindingID] = struct{}{}
		coordinate := activation.Coordinate.Normalized()
		key := strings.Join([]string{
			coordinate.BundleHash, coordinate.BundleIdentity,
			coordinate.PackInventoryGeneration, coordinate.RuntimeInstanceID, fmt.Sprint(coordinate.ContextPublicationGeneration),
		}, "\x00")
		if contextKey == "" {
			contextKey = key
		} else if contextKey != key {
			return canonicalPublication{}, fmt.Errorf("channel activation publication spans multiple runtime contexts")
		}
		planValue, err := activation.Plan.ActivationCanonicalValue()
		if err != nil {
			return canonicalPublication{}, err
		}
		admissions := append([]CredentialAdmission(nil), activation.CredentialAdmissions...)
		sort.Slice(admissions, func(i, j int) bool {
			if admissions[i].Role != admissions[j].Role {
				return admissions[i].Role < admissions[j].Role
			}
			return admissions[i].StoreKey < admissions[j].StoreKey
		})
		values = append(values, map[string]any{
			"source":                  activation.Source,
			"onboarding_operation_id": activation.OnboardingOperationID,
			"onboarding_revision":     activation.OnboardingRevision,
			"coordinate":              coordinate,
			"activation_revision":     activation.ActivationRevision,
			"plan":                    planValue,
			"credential_admissions":   admissions,
		})
	}
	return canonicalPublication{activations: ordered, value: []any{string(ChannelActivationPublicationExecutable), values}}, nil
}

func canonicalDeclaredOnlyPublication(bindings []packs.OutboundBindingPlan) ([]any, []packs.OutboundBindingPlan, error) {
	ordered := append([]packs.OutboundBindingPlan(nil), bindings...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].BindingID() < ordered[j].BindingID() })
	values := make([]any, 0, len(ordered))
	seen := map[string]struct{}{}
	for _, binding := range ordered {
		bindingID := binding.BindingID()
		if strings.TrimSpace(bindingID) == "" {
			return nil, nil, fmt.Errorf("declared-only channel activation binding identity is missing")
		}
		if _, duplicate := seen[bindingID]; duplicate {
			return nil, nil, fmt.Errorf("duplicate declared-only channel activation binding %q", bindingID)
		}
		seen[bindingID] = struct{}{}
		value, err := binding.ActivationCanonicalValue()
		if err != nil {
			return nil, nil, err
		}
		values = append(values, value)
	}
	return []any{string(ChannelActivationPublicationDeclaredOnly), values}, ordered, nil
}

func cloneCompiledActivations(values []CompiledActivation) []CompiledActivation {
	out := append([]CompiledActivation(nil), values...)
	for index := range out {
		out[index].CredentialAdmissions = append([]CredentialAdmission(nil), out[index].CredentialAdmissions...)
	}
	return out
}
