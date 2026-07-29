package contracts

import (
	"fmt"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
)

type toolCredentialKey struct {
	value string
}

func admitToolCredentialKeys(raw []string) ([]toolCredentialKey, error) {
	out := make([]toolCredentialKey, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for index, value := range raw {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("credentials[%d] is empty", index)
		}
		if !utf8.ValidString(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return nil, fmt.Errorf("credentials[%d] is not a valid credential key", index)
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf("credential %q is duplicated", value)
		}
		seen[value] = struct{}{}
		out = append(out, toolCredentialKey{value: value})
	}
	return out, nil
}

func toolCredentialKeyStrings(keys []toolCredentialKey) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key.value)
	}
	return out
}

type admittedManagedCredentialValue struct {
	key                 toolCredentialKey
	header              string
	prefix              string
	grantType           managedcredentialmodel.GrantTypeKind
	scopes              []string
	grantModel          managedcredentialmodel.GrantModelKind
	tokenRequest        admittedManagedTokenRequest
	installationIDInput string
}

type admittedManagedTokenRequest struct {
	clientAuth    managedcredentialmodel.TokenClientAuthKind
	body          managedcredentialmodel.TokenBodyKind
	staticHeaders map[string]string
}

// ToolManagedCredential is the immutable managed-credential execution policy
// admitted with the tool.
type ToolManagedCredential struct {
	value *admittedManagedCredentialValue
}

func admitManagedCredential(ref ManagedCredentialRef) (ToolManagedCredential, error) {
	if strings.TrimSpace(ref.Key) == "" {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.key is required")
	}
	keys, err := admitToolCredentialKeys([]string{ref.Key})
	if err != nil {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.key: %w", err)
	}
	header := strings.TrimSpace(ref.Header)
	if header != "" && !httpHeaderNamePattern.MatchString(header) {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.header %q is invalid", ref.Header)
	}
	header = http.CanonicalHeaderKey(header)
	prefix := strings.TrimSpace(ref.Prefix)
	if prefix != "" && header == "" {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.header is required when prefix is set")
	}
	if strings.ContainsAny(prefix, "\r\n") {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.prefix must not contain a line break")
	}
	grantType, err := managedcredentialmodel.ParseGrantTypeKind(ref.GrantType)
	if err != nil {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.%w", err)
	}
	scopes, err := admitManagedCredentialScopes(ref.Scopes)
	if err != nil {
		return ToolManagedCredential{}, err
	}
	grantModel, err := managedcredentialmodel.ParseGrantModelKind(ref.GrantModel)
	if err != nil {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.%w", err)
	}
	if err := managedcredentialmodel.ValidateTokenRequestProfile(ref.TokenRequest); err != nil {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.%w", err)
	}
	tokenRequestSyntax := managedcredentialmodel.NormalizeTokenRequestProfile(ref.TokenRequest)
	tokenClientAuth, err := managedcredentialmodel.ParseTokenClientAuthKind(tokenRequestSyntax.ClientAuth)
	if err != nil {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.%w", err)
	}
	tokenBody, err := managedcredentialmodel.ParseTokenBodyKind(tokenRequestSyntax.Body)
	if err != nil {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.%w", err)
	}
	tokenRequest := admittedManagedTokenRequest{
		clientAuth: tokenClientAuth,
		body:       tokenBody,
	}
	if len(tokenRequestSyntax.StaticHeaders) > 0 {
		tokenRequest.staticHeaders = make(map[string]string, len(tokenRequestSyntax.StaticHeaders))
		for key, value := range tokenRequestSyntax.StaticHeaders {
			tokenRequest.staticHeaders[key] = value
		}
	}
	installationInput := strings.TrimSpace(ref.InstallationIDInput)
	if installationInput != "" && !toolPathNamePattern.MatchString(installationInput) {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.installation_id_input %q is invalid", ref.InstallationIDInput)
	}
	if grantType == managedcredentialmodel.GrantTypeGitHubAppInstallation && installationInput == "" {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.installation_id_input is required for grant_type %s", grantType.String())
	}
	if installationInput != "" && grantType != managedcredentialmodel.GrantTypeGitHubAppInstallation {
		return ToolManagedCredential{}, fmt.Errorf("managed_credential.installation_id_input requires grant_type %s", managedcredentialmodel.GrantGitHubAppInstallation)
	}
	return ToolManagedCredential{value: &admittedManagedCredentialValue{
		key: keys[0], header: header, prefix: prefix, grantType: grantType, scopes: scopes,
		grantModel: grantModel, tokenRequest: tokenRequest, installationIDInput: installationInput,
	}}, nil
}

func admitManagedCredentialScopes(raw []string) ([]string, error) {
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for index, value := range raw {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("managed_credential.scopes[%d] is empty", index)
		}
		if !utf8.ValidString(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return nil, fmt.Errorf("managed_credential.scopes[%d] is invalid", index)
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf("managed_credential scope %q is duplicated", value)
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func (m ToolManagedCredential) Key() string {
	if m.value == nil {
		return ""
	}
	return m.value.key.value
}

func (m ToolManagedCredential) Header() string {
	if m.value == nil {
		return ""
	}
	return m.value.header
}

func (m ToolManagedCredential) Prefix() string {
	if m.value == nil {
		return ""
	}
	return m.value.prefix
}

func (m ToolManagedCredential) GrantType() string {
	if m.value == nil {
		return ""
	}
	return m.value.grantType.String()
}

func (m ToolManagedCredential) Scopes() []string {
	if m.value == nil {
		return nil
	}
	return append([]string(nil), m.value.scopes...)
}

func (m ToolManagedCredential) GrantModel() string {
	if m.value == nil {
		return ""
	}
	return m.value.grantModel.String()
}

func (m ToolManagedCredential) TokenRequest() managedcredentialmodel.TokenRequestProfile {
	if m.value == nil {
		return managedcredentialmodel.TokenRequestProfile{}
	}
	out := managedcredentialmodel.TokenRequestProfile{
		ClientAuth: m.value.tokenRequest.clientAuth.String(),
		Body:       m.value.tokenRequest.body.String(),
	}
	out.StaticHeaders = make(map[string]string, len(m.value.tokenRequest.staticHeaders))
	for key, value := range m.value.tokenRequest.staticHeaders {
		out.StaticHeaders[key] = value
	}
	return out
}

func (m ToolManagedCredential) InstallationIDInput() string {
	if m.value == nil {
		return ""
	}
	return m.value.installationIDInput
}

func (m ToolManagedCredential) syntax() ManagedCredentialRef {
	if m.value == nil {
		return ManagedCredentialRef{}
	}
	return ManagedCredentialRef{
		Key: m.Key(), Header: m.Header(), Prefix: m.Prefix(), GrantType: m.GrantType(),
		Scopes: m.Scopes(), GrantModel: m.GrantModel(), TokenRequest: m.TokenRequest(),
		InstallationIDInput: m.InstallationIDInput(),
	}
}

func (m ToolManagedCredential) Readback() ManagedCredentialRef {
	return m.syntax()
}
