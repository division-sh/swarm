// Package channelprovider binds the compiled embedded adapters. A binding is
// declaration and presentation data, never identity or execution authority.
package channelprovider

import (
	"fmt"
	"time"
)

type Binding struct {
	id                string
	displayName       string
	session           bool
	webhook           bool
	pollClaims        bool
	credential        bool
	signingCredential bool
	canonicalAccount  func(string) (string, error)
	claimInstructions func(string, string, time.Time) (string, error)
	readyTeaching     string
	notLinked         string
	pairingPrompt     string
	languageHelp      func(string) string
	claimantFormat    string
	sanitizeClaimant  bool
	nativeRead        string
	nativeWrite       string
	captureKey        string
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

func Installed() []Binding { return append([]Binding(nil), installed[:]...) }

func (b Binding) Provider() string      { return b.id }
func (b Binding) DisplayName() string   { return b.displayName }
func (b Binding) ReadyTeaching() string { return b.readyTeaching }
func (b Binding) NotLinked() string     { return b.notLinked }
func (b Binding) Session() bool         { return b.id != "" && b.session }
func (b Binding) Webhook() bool         { return b.id != "" && b.webhook }
func (b Binding) PollsClaim() bool      { return b.id != "" && b.pollClaims }

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

func (b Binding) ClaimInstructions(account, code string, expires time.Time) (string, error) {
	if b.claimInstructions == nil {
		return "", fmt.Errorf("provider must be whatsapp or telegram")
	}
	return b.claimInstructions(account, code, expires)
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

func (b Binding) ClaimantObservation(account, oneLineAccount, scope string) string {
	if b.sanitizeClaimant {
		account = oneLineAccount
	}
	if b.claimantFormat == "" {
		return ""
	}
	return fmt.Sprintf(b.claimantFormat, account, scope)
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
