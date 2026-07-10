package managedcredentials

import (
	"fmt"
	"net/http"
	"strings"
)

type HTTPAuthorization struct {
	CredentialKey string
	AccessToken   string
	Header        string
	Prefix        string
}

func ApplyHTTPAuthorization(headers http.Header, auth HTTPAuthorization, replace bool) error {
	if headers == nil {
		return fmt.Errorf("managed credential headers are unavailable")
	}
	header := strings.TrimSpace(auth.Header)
	if header == "" {
		header = "Authorization"
	}
	if existing := strings.TrimSpace(headers.Get(header)); existing != "" && !replace {
		return fmt.Errorf("managed credential cannot set %s because the header is already configured", header)
	}
	value := strings.TrimSpace(auth.AccessToken)
	if value == "" {
		return fmt.Errorf("managed credential %q did not provide an access token", strings.TrimSpace(auth.CredentialKey))
	}
	prefix := strings.TrimSpace(auth.Prefix)
	if prefix == "" && strings.EqualFold(header, "Authorization") {
		prefix = "Bearer"
	}
	if prefix != "" {
		value = prefix + " " + value
	}
	headers.Set(header, value)
	return nil
}
