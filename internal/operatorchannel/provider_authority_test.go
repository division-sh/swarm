package operatorchannel

import (
	"context"
	"errors"
	"strings"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/google/uuid"
)

func TestProviderAuthorityDistinguishesRealCredentialsAndSessionAccounts(t *testing.T) {
	evidence := runtimecredentials.ValueEvidence{Key: "discord.bot",
		Seal: runtimecredentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat("a", 64))}
	credential, err := CredentialProviderAuthority(evidence)
	if err != nil {
		t.Fatal(err)
	}
	account := SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
		AccountRef: "exact-provider-account", AdmissionID: uuid.NewString(), Revision: 1}
	session := ProviderAuthority{Kind: ProviderAuthoritySession, Session: account}
	discord := session
	discord.Session.Provider, discord.Credential = "discord", evidence
	for name, authority := range map[string]ProviderAuthority{"credential": credential, "zero-token session": session, "real-token session": discord} {
		t.Run(name, func(t *testing.T) {
			if err := authority.Validate(); err != nil {
				t.Fatal(err)
			}
			record, err := authority.PrivateRecord()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := record.Admit()
			if err != nil || decoded != authority {
				t.Fatalf("private authority round trip = %#v, %v", decoded, err)
			}
			if authority.Kind == ProviderAuthoritySession {
				var unavailable *SessionProviderUnavailableError
				if err := authority.RequireExecutable(); !errors.As(err, &unavailable) || unavailable.Provider != authority.Session.Provider {
					t.Fatalf("uninstalled session execution = %v", err)
				}
				if current, err := authority.Current(context.Background(), nil); current || !errors.As(err, &unavailable) {
					t.Fatalf("uninstalled session currentness = %t, %v", current, err)
				}
			}
		})
	}
	for _, test := range []struct {
		name string
		base ProviderAuthority
		edit func(*ProviderAuthority)
	}{
		{"missing kind", credential, func(a *ProviderAuthority) { a.Kind = "" }},
		{"unknown kind", session, func(a *ProviderAuthority) { a.Kind = "pairing" }},
		{"credential without seal", credential, func(a *ProviderAuthority) { a.Credential.Seal = "" }},
		{"credential with inactive session", credential, func(a *ProviderAuthority) { a.Session = account }},
		{"session without admission", session, func(a *ProviderAuthority) { a.Session = SessionAccountAdmission{} }},
		{"session without provider account", session, func(a *ProviderAuthority) { a.Session.AccountRef = "" }},
		{"session without stable connection", session, func(a *ProviderAuthority) { a.Session.ConnectionID = "" }},
		{"session without admission revision", session, func(a *ProviderAuthority) { a.Session.Revision = 0 }},
		{"session with dummy token", session, func(a *ProviderAuthority) { a.Credential.Key = "fake-session-token" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			authority := test.base
			test.edit(&authority)
			if err := authority.Validate(); err == nil {
				t.Fatal("contradictory provider authority was admitted")
			}
		})
	}
}

type inventedSessionObserver struct{}

func (inventedSessionObserver) CurrentValueMatchesSeal(context.Context, runtimecredentials.ValueEvidence) (bool, error) {
	return true, nil
}
func (inventedSessionObserver) AdmitSessionAccount(context.Context, SessionAccountAdmission) (authority.Admission, error) {
	return authority.Admission{}, nil
}

func TestProviderAuthorityRefusesFabricatedSessionObservation(t *testing.T) {
	retained := ProviderAuthority{Kind: ProviderAuthoritySession, Session: SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
		AccountRef: "invented@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}}
	if current, err := retained.Current(context.Background(), inventedSessionObserver{}); current || err == nil {
		t.Fatal("caller observation minted native execution authority", err)
	}
}
