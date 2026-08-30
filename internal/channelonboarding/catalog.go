package channelonboarding

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type CandidateTarget struct {
	Selector             string                       `json:"selector"`
	ServiceID            string                       `json:"service_id"`
	FlowPath             string                       `json:"flow_path"`
	Alias                string                       `json:"alias"`
	Provider             string                       `json:"provider"`
	Generation           uint64                       `json:"generation"`
	PublicationSequence  int64                        `json:"publication_sequence"`
	AdmissionGeneration  triggergeneration.Generation `json:"admission_generation"`
	SigningCredentialKey string                       `json:"signing_credential_key,omitempty"`
}

func (t CandidateTarget) Validate() error {
	parsed, err := packs.ParseChannelRegistrationTarget(t.Selector)
	if err != nil {
		return fmt.Errorf("channel onboarding target selector %q: %w", strings.TrimSpace(t.Selector), err)
	}
	if strings.TrimSpace(t.ServiceID) == "" || strings.TrimSpace(t.FlowPath) != parsed.FlowPath || strings.TrimSpace(t.Provider) != parsed.Provider || strings.TrimSpace(t.Alias) == "" || t.Generation == 0 || t.PublicationSequence < 1 || !t.AdmissionGeneration.Valid() {
		return fmt.Errorf("channel onboarding target contradicts its exact selector")
	}
	return nil
}

type Candidate struct {
	Provider               string                            `json:"provider"`
	Interface              operatorchannel.InterfaceIdentity `json:"interface"`
	Coordinate             ChannelRuntimeContextCoordinate   `json:"coordinate"`
	Target                 CandidateTarget                   `json:"target"`
	Posture                ActivationPosture                 `json:"activation_posture"`
	Ceremony               IdentityCeremony                  `json:"identity_ceremony"`
	ProviderCredentialRole string                            `json:"provider_credential_role"`
	SigningCredentialRole  string                            `json:"signing_credential_role,omitempty"`
	ConfirmationOperation  string                            `json:"confirmation_operation"`
	ConnectionHealth       string                            `json:"connection_health,omitempty"`
	Plan                   packs.SatisfactionPlan            `json:"-"`
}

func (c Candidate) Validate() error {
	if strings.TrimSpace(c.Provider) == "" || strings.TrimSpace(c.ProviderCredentialRole) == "" || strings.TrimSpace(c.ConfirmationOperation) == "" {
		return fmt.Errorf("channel onboarding candidate requires provider, credential role, and confirmation operation")
	}
	if err := c.Interface.Validate(); err != nil {
		return err
	}
	if err := c.Coordinate.Validate(); err != nil {
		return err
	}
	if !c.Posture.Valid() || !c.Ceremony.Valid() {
		return fmt.Errorf("channel onboarding candidate posture and ceremony are invalid")
	}
	switch c.Posture {
	case ActivationWebhookRegistration:
		if strings.TrimSpace(c.SigningCredentialRole) == "" || strings.TrimSpace(c.ConnectionHealth) != "" {
			return fmt.Errorf("webhook channel onboarding candidate requires signing credential and forbids connection health")
		}
		if err := c.Target.Validate(); err != nil {
			return err
		}
	case ActivationSessionConnection:
		if strings.TrimSpace(c.SigningCredentialRole) != "" || strings.TrimSpace(c.ConnectionHealth) == "" {
			return fmt.Errorf("session channel onboarding candidate requires connection health and forbids signing credential")
		}
	}
	return nil
}

type CandidateSelection struct {
	Provider          string
	BundleHash        string
	InterfaceSelector string
	TargetSelector    string
}

type CandidateCatalog struct {
	candidates []Candidate
}

func NewCandidateCatalog(candidates []Candidate) (*CandidateCatalog, error) {
	projected := append([]Candidate(nil), candidates...)
	seen := map[string]struct{}{}
	for _, candidate := range projected {
		if err := candidate.Validate(); err != nil {
			return nil, err
		}
		key := candidate.Coordinate.BundleHash + "\x00" + candidate.Interface.Key() + "\x00" + candidate.Target.Selector
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("duplicate channel onboarding candidate %s", candidateDiagnostic(candidate))
		}
		seen[key] = struct{}{}
	}
	sort.Slice(projected, func(i, j int) bool { return candidateDiagnostic(projected[i]) < candidateDiagnostic(projected[j]) })
	return &CandidateCatalog{candidates: projected}, nil
}

func (c *CandidateCatalog) Candidates() []Candidate {
	if c == nil {
		return nil
	}
	return append([]Candidate(nil), c.candidates...)
}

func (c *CandidateCatalog) Resolve(selection CandidateSelection) (Candidate, error) {
	if c == nil {
		return Candidate{}, fmt.Errorf("channel onboarding candidate catalog is required")
	}
	selection.Provider = strings.TrimSpace(selection.Provider)
	selection.BundleHash = strings.TrimSpace(selection.BundleHash)
	selection.InterfaceSelector = strings.TrimSpace(selection.InterfaceSelector)
	selection.TargetSelector = strings.TrimSpace(selection.TargetSelector)
	if selection.Provider == "" {
		return Candidate{}, fmt.Errorf("%w: provider is required", ErrInvalidRequest)
	}
	matches := make([]Candidate, 0, len(c.candidates))
	for _, candidate := range c.candidates {
		if candidate.Provider != selection.Provider || selection.BundleHash != "" && candidate.Coordinate.BundleHash != selection.BundleHash || selection.InterfaceSelector != "" && candidate.Interface.Selector != selection.InterfaceSelector || selection.TargetSelector != "" && candidate.Target.Selector != selection.TargetSelector {
			continue
		}
		matches = append(matches, candidate)
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	options := make([]string, 0, len(matches))
	if len(matches) == 0 {
		for _, candidate := range c.candidates {
			if candidate.Provider == selection.Provider {
				options = append(options, candidateDiagnostic(candidate))
			}
		}
		return Candidate{}, fmt.Errorf("%w: no channel onboarding candidate matches provider %q and exact selectors; eligible: %s", ErrNotFound, selection.Provider, strings.Join(options, "; "))
	}
	for _, candidate := range matches {
		options = append(options, candidateDiagnostic(candidate))
	}
	return Candidate{}, fmt.Errorf("%w: provider %q is ambiguous; select exactly one with --bundle, --interface, and --target: %s", ErrConflict, selection.Provider, strings.Join(options, "; "))
}

// FindExact returns only a candidate owned by the same current runtime
// occurrence.
func (c *CandidateCatalog) FindExact(provider string, identity operatorchannel.InterfaceIdentity, coordinate ChannelRuntimeContextCoordinate, targetSelector string) (Candidate, bool) {
	if c == nil {
		return Candidate{}, false
	}
	provider = strings.TrimSpace(provider)
	identity = identity.Normalized()
	targetSelector = strings.TrimSpace(targetSelector)
	for _, candidate := range c.candidates {
		if candidate.Provider == provider && candidate.Interface.Normalized() == identity && candidate.Target.Selector == targetSelector && candidate.Coordinate.Matches(coordinate) {
			return candidate, true
		}
	}
	return Candidate{}, false
}

// FindDurableSuccessor resolves the current live occurrence for one exact
// restart-stable onboarding responsibility. Every behavior-bearing semantic
// field remains exact; only publication and target generations may change.
func (c *CandidateCatalog) FindDurableSuccessor(provider string, identity operatorchannel.InterfaceIdentity, coordinate ChannelRuntimeContextCoordinate, targetSelector string, posture ActivationPosture, ceremony IdentityCeremony) (Candidate, bool) {
	if c == nil {
		return Candidate{}, false
	}
	provider = strings.TrimSpace(provider)
	identity = identity.Normalized()
	targetSelector = strings.TrimSpace(targetSelector)
	for _, candidate := range c.candidates {
		if candidate.Provider == provider && candidate.Interface.Normalized() == identity &&
			candidate.Target.Selector == targetSelector && candidate.Posture == posture && candidate.Ceremony == ceremony &&
			candidate.Coordinate.MatchesDurableIdentity(coordinate) {
			return candidate, true
		}
	}
	return Candidate{}, false
}

func candidateDiagnostic(candidate Candidate) string {
	return fmt.Sprintf("--bundle %s --interface %s --target %s", candidate.Coordinate.BundleHash, candidate.Interface.Selector, candidate.Target.Selector)
}
