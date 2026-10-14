package channelonboarding

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"
)

func (p *PairingReadback) UnmarshalJSON(raw []byte) error {
	type readback PairingReadback
	var decoded struct {
		readback
		Status    *string `json:"status"`
		Paired    *bool   `json:"paired"`
		Connected *bool   `json:"connected"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if decoded.Status == nil || decoded.Paired == nil || decoded.Connected == nil {
		return fmt.Errorf("%w: pairing readback requires explicit status, paired and connected evidence", ErrInvalidRequest)
	}
	*p = PairingReadback(decoded.readback)
	p.Status, p.Paired, p.Connected = *decoded.Status, *decoded.Paired, *decoded.Connected
	return nil
}

// ProviderCredentialRequirement consumes the selected declaration, never a
// provider name or an absent field. The role may be explicitly empty.
func (r Result) ProviderCredentialRequirement() (string, error) {
	c := r.Candidate
	o := r.Operation
	if c == nil || c.ValidateDeclaration() != nil || o.OperationID == "" || o.Revision < 1 ||
		!o.Phase.Valid() || c.Provider != o.Provider || c.Interface != o.Interface ||
		!c.Coordinate.MatchesDeclaration(o.Coordinate) || c.Target.Selector != o.TargetSelector ||
		c.Posture != o.Posture || c.Ceremony != o.Ceremony ||
		c.ProviderCredentialRole != strings.TrimSpace(c.ProviderCredentialRole) {
		return "", fmt.Errorf("%w: onboarding readback lacks its selected credential declaration", ErrInvalidRequest)
	}
	return c.ProviderCredentialRole, nil
}

// SessionPreparationPending validates the owner's explicit non-executable
// progress projection. Missing pairing material or unchanged revisions do not
// establish pending preparation.
func (r Result) SessionPreparationPending() (bool, error) {
	p := r.Pairing
	if p == nil || p.Status != PairingStatusPreparing {
		return false, nil
	}
	if _, err := r.ProviderCredentialRequirement(); err != nil {
		return false, err
	}
	o := r.Operation
	if o.Posture != ActivationSessionConnection || o.Phase != PhaseActivatingProvider ||
		o.IdentityOperationID != "" || o.ActivationRevision != 0 || o.FailureCode != "" || o.FailureMessage != "" ||
		r.IdentityOperation != nil || r.Logout != nil || r.Readiness != nil && r.Readiness.Ready ||
		p.Code != "" || !p.ExpiresAt.IsZero() || p.Paired || p.Connected {
		return false, fmt.Errorf("%w: session preparation readback contradicts its retained responsibility", ErrInvalidRequest)
	}
	return true, nil
}

// SessionPairingPending consumes authenticated pairing progress, not account,
// principal, executable readiness or permission to reconnect/poll automatically.
func (r Result) SessionPairingPending(now time.Time) (bool, error) {
	p := r.Pairing
	if p == nil || p.Status == PairingStatusPreparing {
		return r.SessionPreparationPending()
	}
	if p.Status != "awaiting_qr" && p.Status != "paired" && p.Status != "connected" {
		return false, nil
	}
	if _, err := r.ProviderCredentialRequirement(); err != nil {
		return false, err
	}
	o := r.Operation
	if o.Posture != ActivationSessionConnection || o.Phase != PhaseActivatingProvider ||
		o.IdentityOperationID != "" || o.ActivationRevision != 0 || o.FailureCode != "" || o.FailureMessage != "" ||
		r.IdentityOperation != nil || r.Logout != nil || r.Readiness != nil && r.Readiness.Ready {
		return false, fmt.Errorf("%w: pairing progress contradicts its retained responsibility", ErrInvalidRequest)
	}
	if p.Status == "awaiting_qr" {
		if p.Code == "" || len(p.Code) > 4096 || strings.ContainsFunc(p.Code, unsafePairingCharacter) || p.Paired || !p.ExpiresAt.After(now) {
			return false, fmt.Errorf("%w: pairing material is absent, expired or contradictory", ErrInvalidRequest)
		}
	} else if p.Code != "" || !p.ExpiresAt.IsZero() || p.Status == "paired" && !p.Paired || p.Status == "connected" && !p.Connected {
		return false, fmt.Errorf("%w: paired progress contradicts its session evidence", ErrInvalidRequest)
	}
	return true, nil
}

func unsafePairingCharacter(r rune) bool {
	return unicode.IsControl(r) || unicode.Is(unicode.Cf, r)
}
