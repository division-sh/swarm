package channelonboarding

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

// StartSelection is non-executable declaration readback. Its transient fence
// detects selection changes before Start, never grants credential write authority.
type StartSelection struct {
	Candidate      Candidate `json:"candidate"`
	SelectionFence string    `json:"selection_fence"`
}

func (r StartSelection) ProviderCredentialRequirement() (string, error) {
	if r.Candidate.ValidateDeclaration() != nil || r.SelectionFence == "" ||
		r.Candidate.ProviderCredentialRole != strings.TrimSpace(r.Candidate.ProviderCredentialRole) {
		return "", fmt.Errorf("%w: selected credential declaration is incomplete", ErrInvalidRequest)
	}
	return r.Candidate.ProviderCredentialRole, nil
}

// Select resolves the same owner as Start without reserving an operation,
// reading/writing credentials, opening SDK state or launching provider effects.
func (s *Service) Select(ctx context.Context, input StartInput) (StartSelection, error) {
	if input.ProviderCredential != "" || input.ExpectedSelection != "" {
		return StartSelection{}, fmt.Errorf("%w: selection readback does not admit credentials or a caller selection fence", ErrInvalidRequest)
	}
	principal, candidate, err := s.resolveStartCandidate(ctx, input)
	if err != nil {
		return StartSelection{}, err
	}
	return selectedStartRequirement(principal, candidate, input)
}

func (s *Service) resolveStartCandidate(ctx context.Context, input StartInput) (operatorchannel.Principal, Candidate, error) {
	if s == nil || !input.Verb.Valid() {
		return operatorchannel.Principal{}, Candidate{}, fmt.Errorf("%w: an explicit connect, reconnect, or rebind verb is required", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return operatorchannel.Principal{}, Candidate{}, err
	}
	principal, err := s.identities.Principal()
	if err != nil {
		return principal, Candidate{}, err
	}
	catalog, err := s.catalog()
	if err != nil {
		return principal, Candidate{}, err
	}
	candidate, err := catalog.Resolve(input.Selection)
	if err != nil {
		return principal, candidate, err
	}
	if err := s.qualifyProvider(candidate); err != nil {
		return principal, candidate, err
	}
	if input.ClientLanguage != "" {
		profile, err := candidate.Plan.NativeInboxProfile()
		if err != nil {
			return principal, candidate, fmt.Errorf("%w: client_language requires the native_inbox extension: %w", ErrInvalidRequest, err)
		}
		if err := profile.ValidateLanguage(input.ClientLanguage); err != nil {
			return principal, candidate, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}
	}
	return principal, candidate, ctx.Err()
}

func selectedStartRequirement(principal operatorchannel.Principal, candidate Candidate, input StartInput) (StartSelection, error) {
	// Display labels do not affect admission. The existing plan/context/target
	// generations and all declaration fields do; no secret enters this fence.
	exact := candidate
	exact.SourceLabel = ""
	declaration, err := json.Marshal(exact)
	if err != nil {
		return StartSelection{}, err
	}
	return StartSelection{Candidate: candidate, SelectionFence: operatorchannel.Hash(
		"channel-onboarding-start-selection-v1", principal.ID, string(input.Verb), string(declaration),
		fmt.Sprint(input.SaveProof), input.ClientLanguage,
	)}, nil
}
