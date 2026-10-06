package operatorchannel

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

type ProviderAuthorityKind string

const (
	ProviderAuthorityCredential ProviderAuthorityKind = "credential"
	ProviderAuthoritySession    ProviderAuthorityKind = "session_account"
)

// SessionAccountAdmission identifies the admitted provider account, not the
// human operator, provider-private keys, or a current connection occurrence.
type SessionAccountAdmission struct {
	Provider     string `json:"provider"`
	ConnectionID string `json:"connection_id"`
	AccountRef   string `json:"account_reference"`
	AdmissionID  string `json:"admission_id"`
	Revision     int64  `json:"admission_revision"`
}

func (s SessionAccountAdmission) Validate() error {
	if strings.TrimSpace(s.Provider) == "" || strings.TrimSpace(s.AccountRef) == "" ||
		uuid.Validate(s.ConnectionID) != nil || uuid.Validate(s.AdmissionID) != nil || s.Revision < 1 {
		return fmt.Errorf("session account admission requires provider, stable connection, exact account and admission revision")
	}
	return nil
}

// ProviderAuthority is one closed private identity authority. A session may
// additionally require a real credential (Discord), but never a dummy token.
type ProviderAuthority struct {
	Kind       ProviderAuthorityKind
	Credential runtimecredentials.ValueEvidence
	Session    SessionAccountAdmission
}

type SessionConnectionObservation struct {
	Admission    SessionAccountAdmission
	OccurrenceID string
	Connected    bool
	ObservedAt   time.Time
}

func (o SessionConnectionObservation) Validate() error {
	if err := o.Admission.Validate(); err != nil {
		return err
	}
	if uuid.Validate(o.OccurrenceID) != nil || o.ObservedAt.IsZero() {
		return fmt.Errorf("session observation requires an exact connection occurrence and observation time")
	}
	return nil
}

func CredentialProviderAuthority(evidence runtimecredentials.ValueEvidence) (ProviderAuthority, error) {
	authority := ProviderAuthority{Kind: ProviderAuthorityCredential, Credential: evidence}
	return authority, authority.Validate()
}

func (a ProviderAuthority) Validate() error {
	switch a.Kind {
	case ProviderAuthorityCredential:
		if a.Session != (SessionAccountAdmission{}) {
			return fmt.Errorf("credential provider authority cannot carry an inactive session admission")
		}
		return a.Credential.Validate()
	case ProviderAuthoritySession:
		if err := a.Session.Validate(); err != nil {
			return err
		}
		if a.Credential != (runtimecredentials.ValueEvidence{}) {
			return a.Credential.Validate()
		}
		return nil
	default:
		return fmt.Errorf("provider authority kind must be credential or session_account")
	}
}

type SessionProviderUnavailableError struct {
	Provider string
}

func (e *SessionProviderUnavailableError) Error() string {
	return fmt.Sprintf("session channel provider %q has no installed connection implementation", e.Provider)
}

func (a ProviderAuthority) RequireExecutable() error {
	if err := a.Validate(); err != nil {
		return err
	}
	if a.Kind == ProviderAuthoritySession {
		return &SessionProviderUnavailableError{Provider: a.Session.Provider}
	}
	return nil
}

func (a ProviderAuthority) Current(ctx context.Context, owner interface {
	CurrentValueMatchesSeal(context.Context, runtimecredentials.ValueEvidence) (bool, error)
}) (bool, error) {
	if err := a.RequireExecutable(); err != nil {
		return false, err
	}
	if owner == nil {
		return false, fmt.Errorf("provider credential snapshot owner is required")
	}
	return owner.CurrentValueMatchesSeal(ctx, a.Credential)
}

// ProviderAuthorityRecord is the private durable codec, never a public DTO.
// Record decoding is structural; executable admission remains a separate gate.
type ProviderAuthorityRecord struct {
	Kind           ProviderAuthorityKind   `json:"kind"`
	CredentialKey  string                  `json:"credential_key,omitempty"`
	CredentialSeal string                  `json:"credential_value_seal,omitempty"`
	Session        SessionAccountAdmission `json:"session_account,omitzero"`
}

func (a ProviderAuthority) PrivateRecord() (ProviderAuthorityRecord, error) {
	if err := a.Validate(); err != nil {
		return ProviderAuthorityRecord{}, err
	}
	return ProviderAuthorityRecord{Kind: a.Kind, CredentialKey: a.Credential.Key,
		CredentialSeal: a.Credential.Seal.String(), Session: a.Session}, nil
}

func (r ProviderAuthorityRecord) Admit() (ProviderAuthority, error) {
	authority := ProviderAuthority{Kind: r.Kind, Session: r.Session}
	if r.CredentialKey != "" || r.CredentialSeal != "" {
		seal, err := runtimecredentials.ParseValueSeal(r.CredentialSeal)
		if err != nil {
			return ProviderAuthority{}, err
		}
		authority.Credential = runtimecredentials.ValueEvidence{Key: r.CredentialKey, Seal: seal}
	}
	return authority, authority.Validate()
}
