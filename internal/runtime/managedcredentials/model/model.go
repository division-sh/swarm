package model

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

const (
	TokenClientAuthPost  = "post"
	TokenClientAuthBasic = "basic"

	TokenBodyForm = "form"
	TokenBodyJSON = "json"

	GrantModelScope     = "scope_grant"
	GrantModelWorkspace = "workspace_grant"
)

type TokenRequestProfile struct {
	ClientAuth    string            `yaml:"client_auth" json:"client_auth"`
	Body          string            `yaml:"body" json:"body"`
	StaticHeaders map[string]string `yaml:"static_headers,omitempty" json:"static_headers,omitempty"`
}

func DefaultTokenRequestProfile() TokenRequestProfile {
	return TokenRequestProfile{ClientAuth: TokenClientAuthPost, Body: TokenBodyForm}
}

func NormalizeTokenRequestProfile(profile TokenRequestProfile) TokenRequestProfile {
	profile.ClientAuth = strings.TrimSpace(strings.ToLower(profile.ClientAuth))
	if profile.ClientAuth == "" {
		profile.ClientAuth = TokenClientAuthPost
	}
	profile.Body = strings.TrimSpace(strings.ToLower(profile.Body))
	if profile.Body == "" {
		profile.Body = TokenBodyForm
	}
	if len(profile.StaticHeaders) > 0 {
		headers := make(map[string]string, len(profile.StaticHeaders))
		for key, value := range profile.StaticHeaders {
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			if key != "" {
				key = http.CanonicalHeaderKey(key)
			}
			headers[key] = value
		}
		if len(headers) > 0 {
			profile.StaticHeaders = headers
		} else {
			profile.StaticHeaders = nil
		}
	}
	return profile
}

func ValidateTokenRequestProfile(profile TokenRequestProfile) error {
	profile = NormalizeTokenRequestProfile(profile)
	switch profile.ClientAuth {
	case TokenClientAuthPost, TokenClientAuthBasic:
	default:
		return fmt.Errorf("token_request.client_auth %q is not supported; want %s or %s", profile.ClientAuth, TokenClientAuthPost, TokenClientAuthBasic)
	}
	switch profile.Body {
	case TokenBodyForm, TokenBodyJSON:
	default:
		return fmt.Errorf("token_request.body %q is not supported; want %s or %s", profile.Body, TokenBodyForm, TokenBodyJSON)
	}
	seen := map[string]string{}
	for key, value := range profile.StaticHeaders {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return fmt.Errorf("token_request.static_headers must not contain empty names or values")
		}
		canonical := http.CanonicalHeaderKey(key)
		lower := strings.ToLower(canonical)
		if previous, exists := seen[lower]; exists && previous != canonical {
			return fmt.Errorf("token_request.static_headers contains duplicate header %q", canonical)
		}
		seen[lower] = canonical
		switch lower {
		case "authorization", "content-type":
			return fmt.Errorf("token_request.static_headers must not declare %s; token client auth and body encoding own that header", canonical)
		}
	}
	return nil
}

func TokenRequestProfileEqual(left, right TokenRequestProfile) bool {
	left = NormalizeTokenRequestProfile(left)
	right = NormalizeTokenRequestProfile(right)
	if left.ClientAuth != right.ClientAuth || left.Body != right.Body {
		return false
	}
	if len(left.StaticHeaders) != len(right.StaticHeaders) {
		return false
	}
	for key, value := range left.StaticHeaders {
		if right.StaticHeaders[key] != value {
			return false
		}
	}
	return true
}

func TokenRequestProfileCovers(actual, required TokenRequestProfile) error {
	if !TokenRequestProfileEqual(actual, required) {
		return fmt.Errorf("record token_request %s does not satisfy required token_request %s", TokenRequestProfileSummary(actual), TokenRequestProfileSummary(required))
	}
	return nil
}

func TokenRequestProfileSummary(profile TokenRequestProfile) string {
	profile = NormalizeTokenRequestProfile(profile)
	parts := []string{
		"client_auth=" + profile.ClientAuth,
		"body=" + profile.Body,
	}
	if len(profile.StaticHeaders) > 0 {
		names := make([]string, 0, len(profile.StaticHeaders))
		for key := range profile.StaticHeaders {
			names = append(names, key)
		}
		sort.Strings(names)
		parts = append(parts, "static_headers="+strings.Join(names, ","))
	}
	return strings.Join(parts, ",")
}

func NormalizeGrantModel(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return GrantModelScope
	}
	return raw
}

func ValidateGrantModel(raw string) error {
	normalized := NormalizeGrantModel(raw)
	switch normalized {
	case GrantModelScope, GrantModelWorkspace:
		return nil
	default:
		return fmt.Errorf("grant_model %q is not supported; want %s or %s", normalized, GrantModelScope, GrantModelWorkspace)
	}
}

func GrantModelCovers(actual, required string) error {
	required = strings.TrimSpace(strings.ToLower(required))
	if required == "" {
		return nil
	}
	actual = NormalizeGrantModel(actual)
	if actual != required {
		return fmt.Errorf("record grant_model %s does not satisfy required grant_model %s", actual, required)
	}
	return nil
}
