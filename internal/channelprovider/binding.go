// Package channelprovider defines immutable declaration and presentation facts,
// never installation, identity or execution authority.
package channelprovider

import (
	"fmt"
	"time"
)

// Definition is immutable provider data supplied by the executable composition.
// It grants no registration, identity, storage or execution authority.
type Definition struct {
	ID                           string
	DisplayName                  string
	Session                      bool
	Webhook                      bool
	PollClaims                   bool
	Credential                   bool
	SigningCredential            bool
	CanonicalAccount             func(string) (string, error)
	ClaimInstructions            func(ClaimPrompt) (string, error)
	ReadyTeaching                string
	RegistrationTeaching         string
	OmitBoundRegistrationReceipt bool
	NotLinked                    string
	PairingPrompt                string
	LanguageHelp                 func(string) string
	NativeRead                   string
	NativeWrite                  string
	CaptureKey                   string
}

type FactsLookup func(string) (Binding, bool)

func Define(definition Definition) Binding {
	return Binding{definition: &bindingDefinition{
		id: definition.ID, displayName: definition.DisplayName,
		session: definition.Session, webhook: definition.Webhook, pollClaims: definition.PollClaims,
		credential: definition.Credential, signingCredential: definition.SigningCredential,
		canonicalAccount: definition.CanonicalAccount, claimInstructions: definition.ClaimInstructions,
		readyTeaching: definition.ReadyTeaching, registrationTeaching: definition.RegistrationTeaching,
		omitBoundRegistrationReceipt: definition.OmitBoundRegistrationReceipt,
		notLinked:                    definition.NotLinked, pairingPrompt: definition.PairingPrompt, languageHelp: definition.LanguageHelp,
		nativeRead: definition.NativeRead, nativeWrite: definition.NativeWrite, captureKey: definition.CaptureKey,
	}}
}

type Binding struct {
	definition *bindingDefinition
}

type bindingDefinition struct {
	id                           string
	displayName                  string
	session                      bool
	webhook                      bool
	pollClaims                   bool
	credential                   bool
	signingCredential            bool
	canonicalAccount             func(string) (string, error)
	claimInstructions            func(ClaimPrompt) (string, error)
	readyTeaching                string
	registrationTeaching         string
	omitBoundRegistrationReceipt bool
	notLinked                    string
	pairingPrompt                string
	languageHelp                 func(string) string
	nativeRead                   string
	nativeWrite                  string
	captureKey                   string
}

func (b Binding) value() bindingDefinition {
	if b.definition == nil {
		return bindingDefinition{}
	}
	return *b.definition
}

type AccountPresentation struct {
	Account string
	Name    string
}

// ClaimPrompt carries existing presentation facts, never claim authority.
type ClaimPrompt struct {
	Account    string
	Code       string
	ExpiresAt  time.Time
	Deployment bool
	Connection *AccountPresentation
}

func (b Binding) Provider() string { return b.value().id }
func (b Binding) MatchesProvider(provider string) bool {
	return b.value().id != "" && b.value().id == provider
}

func (b Binding) ForProvider(provider string) Binding {
	if !b.MatchesProvider(provider) {
		return Binding{}
	}
	return b
}
func (b Binding) DisplayName() string   { return b.value().displayName }
func (b Binding) ReadyTeaching() string { return b.value().readyTeaching }
func (b Binding) RegistrationTeaching() string {
	if b.value().registrationTeaching != "" {
		return b.value().registrationTeaching
	}
	return b.value().readyTeaching
}
func (b Binding) ShowRegistrationReceipt(bound bool) bool {
	return !bound || !b.value().omitBoundRegistrationReceipt
}
func (b Binding) NotLinked() string { return b.value().notLinked }
func (b Binding) Session() bool     { return b.value().id != "" && b.value().session }
func (b Binding) Webhook() bool     { return b.value().id != "" && b.value().webhook }
func (b Binding) PollsClaim() bool  { return b.value().id != "" && b.value().pollClaims }

func (b Binding) CaptureProvenanceKey() string { return b.value().captureKey }

func (b Binding) AcceptsCaptureKind(kind string) bool {
	return b.value().captureKey != "" && (kind == "message" || kind == "edit" || kind == "revoke")
}

func (b Binding) DeploymentPosture() string {
	if b.value().id == "" {
		return ""
	}
	if b.value().session {
		return "session_connection"
	}
	if b.value().webhook {
		return "webhook_registration"
	}
	return ""
}

func (b Binding) MatchesDeployment(posture, credential, signing string) bool {
	return b.value().id != "" && posture == b.DeploymentPosture() &&
		(credential != "") == b.value().credential &&
		(signing == "" || b.value().signingCredential && signing != credential)
}

func (b Binding) CanonicalAccount(account string) (string, error) {
	if b.value().canonicalAccount == nil {
		return "", fmt.Errorf("provider must be whatsapp or telegram")
	}
	return b.value().canonicalAccount(account)
}

func (b Binding) ClaimInstructions(prompt ClaimPrompt) (string, error) {
	if b.value().claimInstructions == nil {
		return "", fmt.Errorf("provider must be whatsapp or telegram")
	}
	return b.value().claimInstructions(prompt)
}

func ClaimDeadline(expires time.Time) string {
	return expires.Local().Format("2006-01-02 15:04:05 MST (-07:00)")
}

func (b Binding) PairingInstructions(expires time.Time, qr string) string {
	if b.value().pairingPrompt == "" {
		return ""
	}
	return fmt.Sprintf(b.value().pairingPrompt, expires.UTC().Format(time.RFC3339), qr)
}

func (b Binding) LanguageHelp(operation string) string {
	if b.value().languageHelp == nil {
		return ""
	}
	return b.value().languageHelp(operation)
}

func (b Binding) NativeRead(target string) bool {
	return b.value().id != "" && b.value().nativeRead != "" && target == b.value().nativeRead
}

func (b Binding) NativeWrite(target string) bool {
	return b.value().id != "" && b.value().nativeWrite != "" && target == b.value().nativeWrite
}
