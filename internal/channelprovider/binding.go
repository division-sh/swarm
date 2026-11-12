// Package channelprovider binds the compiled embedded adapters. A binding is
// declaration and presentation data, never identity or execution authority.
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

func Define(definition Definition) Binding {
	return Binding{
		id: definition.ID, displayName: definition.DisplayName,
		session: definition.Session, webhook: definition.Webhook, pollClaims: definition.PollClaims,
		credential: definition.Credential, signingCredential: definition.SigningCredential,
		canonicalAccount: definition.CanonicalAccount, claimInstructions: definition.ClaimInstructions,
		readyTeaching: definition.ReadyTeaching, registrationTeaching: definition.RegistrationTeaching,
		omitBoundRegistrationReceipt: definition.OmitBoundRegistrationReceipt,
		notLinked:                    definition.NotLinked, pairingPrompt: definition.PairingPrompt, languageHelp: definition.LanguageHelp,
		nativeRead: definition.NativeRead, nativeWrite: definition.NativeWrite, captureKey: definition.CaptureKey,
	}
}

type Binding struct {
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

// Keep installation in one compiled seat. Packs cannot register callbacks or
// construct bindings; a new native operation still needs the finite catalog.
var installed = [...]Binding{telegram, whatsapp}

func Lookup(provider string) (Binding, bool) {
	for _, binding := range installed {
		if binding.id == provider {
			return binding, true
		}
	}
	return Binding{}, false
}

func ForNativeTarget(target string) (Binding, bool) {
	for _, binding := range installed {
		if binding.NativeRead(target) || binding.NativeWrite(target) {
			return binding, true
		}
	}
	return Binding{}, false
}

func Installed() []Binding { return append([]Binding(nil), installed[:]...) }

func (b Binding) Provider() string { return b.id }
func (b Binding) MatchesProvider(provider string) bool {
	return b.id != "" && b.id == provider
}
func (b Binding) DisplayName() string   { return b.displayName }
func (b Binding) ReadyTeaching() string { return b.readyTeaching }
func (b Binding) RegistrationTeaching() string {
	if b.registrationTeaching != "" {
		return b.registrationTeaching
	}
	return b.readyTeaching
}
func (b Binding) ShowRegistrationReceipt(bound bool) bool {
	return !bound || !b.omitBoundRegistrationReceipt
}
func (b Binding) NotLinked() string { return b.notLinked }
func (b Binding) Session() bool     { return b.id != "" && b.session }
func (b Binding) Webhook() bool     { return b.id != "" && b.webhook }
func (b Binding) PollsClaim() bool  { return b.id != "" && b.pollClaims }

func (b Binding) CaptureProvenanceKey() string { return b.captureKey }

func (b Binding) AcceptsCaptureKind(kind string) bool {
	return b.captureKey != "" && (kind == "message" || kind == "edit" || kind == "revoke")
}

func (b Binding) DeploymentPosture() string {
	if b.id == "" {
		return ""
	}
	if b.session {
		return "session_connection"
	}
	if b.webhook {
		return "webhook_registration"
	}
	return ""
}

func (b Binding) MatchesDeployment(posture, credential, signing string) bool {
	return b.id != "" && posture == b.DeploymentPosture() &&
		(credential != "") == b.credential &&
		(signing == "" || b.signingCredential && signing != credential)
}

func (b Binding) CanonicalAccount(account string) (string, error) {
	if b.canonicalAccount == nil {
		return "", fmt.Errorf("provider must be whatsapp or telegram")
	}
	return b.canonicalAccount(account)
}

func (b Binding) ClaimInstructions(prompt ClaimPrompt) (string, error) {
	if b.claimInstructions == nil {
		return "", fmt.Errorf("provider must be whatsapp or telegram")
	}
	return b.claimInstructions(prompt)
}

func ClaimDeadline(expires time.Time) string {
	return expires.Local().Format("2006-01-02 15:04:05 MST (-07:00)")
}

func (b Binding) PairingInstructions(expires time.Time, qr string) string {
	if b.pairingPrompt == "" {
		return ""
	}
	return fmt.Sprintf(b.pairingPrompt, expires.UTC().Format(time.RFC3339), qr)
}

func (b Binding) LanguageHelp(operation string) string {
	if b.languageHelp == nil {
		return ""
	}
	return b.languageHelp(operation)
}

func (b Binding) NativeRead(target string) bool {
	return b.id != "" && b.nativeRead != "" && target == b.nativeRead
}

func (b Binding) NativeWrite(target string) bool {
	return b.id != "" && b.nativeWrite != "" && target == b.nativeWrite
}

func Session(provider string) bool {
	binding, ok := Lookup(provider)
	return ok && binding.Session()
}
