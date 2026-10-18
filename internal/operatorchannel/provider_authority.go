package operatorchannel

import (
	"context"
	"fmt"
	"time"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/google/uuid"
)

type ProviderAuthorityKind string

const (
	ProviderAuthorityCredential ProviderAuthorityKind = "credential"
	ProviderAuthoritySession    ProviderAuthorityKind = "session_account"
)

// SessionAccountAdmission identifies the admitted provider account, not the
// human operator, provider-private keys, or a current connection occurrence.
type SessionAccountAdmission = authority.Account

// ProviderAuthority is one closed private identity authority. A session may
// additionally require a real credential (Discord), but never a dummy token.
type ProviderAuthority struct {
	Kind       ProviderAuthorityKind
	Credential runtimecredentials.ValueEvidence
	Session    SessionAccountAdmission
	session    authority.Admission
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
		if a.Session != (SessionAccountAdmission{}) || !a.session.Empty() {
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
		if err := a.session.Validate(context.Background(), a.Session); err != nil {
			return &SessionProviderUnavailableError{Provider: a.Session.Provider}
		}
	}
	return nil
}

type SessionAdmissionOwner interface {
	AdmitSessionAccount(context.Context, SessionAccountAdmission) (authority.Admission, error)
}

// AdmitExecution re-admits exact retained provenance, never replacement values.
// Session issuance is restricted to the owned SDK subtree, not this interface.
func (a ProviderAuthority) AdmitExecution(ctx context.Context, owner CredentialCurrentness) (ProviderAuthority, bool, error) {
	if err := a.Validate(); err != nil {
		return ProviderAuthority{}, false, err
	}
	if ctx == nil || ctx.Err() != nil {
		return ProviderAuthority{}, false, fmt.Errorf("current provider admission context is required")
	}
	if a.Kind == ProviderAuthoritySession {
		native, ok := owner.(SessionAdmissionOwner)
		if !ok {
			return ProviderAuthority{}, false, &SessionProviderUnavailableError{Provider: a.Session.Provider}
		}
		admission, err := native.AdmitSessionAccount(ctx, a.Session)
		if err != nil {
			return ProviderAuthority{}, false, err
		}
		if err := admission.Validate(ctx, a.Session); err != nil {
			admission.Close()
			return ProviderAuthority{}, false, err
		}
		a.session = admission
		if a.Credential == (runtimecredentials.ValueEvidence{}) {
			return a, true, nil
		}
	}
	if owner == nil {
		a.CloseExecution()
		return ProviderAuthority{}, false, fmt.Errorf("provider credential snapshot owner is required")
	}
	current, err := owner.CurrentValueMatchesSeal(ctx, a.Credential)
	if err != nil || !current || ctx.Err() != nil {
		a.CloseExecution()
		if err == nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		return ProviderAuthority{}, false, err
	}
	return a, true, nil
}

func (a ProviderAuthority) Current(ctx context.Context, owner CredentialCurrentness) (bool, error) {
	admitted, current, err := a.AdmitExecution(ctx, owner)
	defer admitted.CloseExecution()
	return current, err
}

func (a ProviderAuthority) CloseExecution() { a.session.Close() }

func (a ProviderAuthority) SessionParent() (string, int64) { return a.session.Parent() }

func (a ProviderAuthority) SameProvenance(other ProviderAuthority) bool {
	return a.Kind == other.Kind && a.Credential == other.Credential && a.Session == other.Session
}

func (a ProviderAuthority) RequireExecutableFor(expected ProviderAuthority) error {
	if !a.SameProvenance(expected) {
		return fmt.Errorf("provider admission contradicts its retained authority")
	}
	return a.RequireExecutable()
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
