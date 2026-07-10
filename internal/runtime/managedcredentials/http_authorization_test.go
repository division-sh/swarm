package managedcredentials

import (
	"net/http"
	"strings"
	"testing"
)

func TestApplyHTTPAuthorizationOwnsDefaultsAndReplacement(t *testing.T) {
	tests := []struct {
		name    string
		auth    HTTPAuthorization
		initial http.Header
		replace bool
		wantKey string
		want    string
		wantErr string
	}{
		{
			name:    "default authorization bearer",
			auth:    HTTPAuthorization{CredentialKey: "github_app", AccessToken: "fixture-token"},
			initial: make(http.Header),
			wantKey: "Authorization",
			want:    "Bearer fixture-token",
		},
		{
			name:    "custom header has no implicit prefix",
			auth:    HTTPAuthorization{CredentialKey: "custom", AccessToken: "fixture-token", Header: "X-Api-Key"},
			initial: make(http.Header),
			wantKey: "X-Api-Key",
			want:    "fixture-token",
		},
		{
			name:    "explicit prefix",
			auth:    HTTPAuthorization{CredentialKey: "custom", AccessToken: "fixture-token", Header: "X-Api-Key", Prefix: "Token"},
			initial: make(http.Header),
			wantKey: "X-Api-Key",
			want:    "Token fixture-token",
		},
		{
			name:    "collision fails closed",
			auth:    HTTPAuthorization{CredentialKey: "github_app", AccessToken: "fixture-token"},
			initial: http.Header{"Authorization": []string{"existing"}},
			wantErr: "already configured",
		},
		{
			name:    "replacement refreshes token",
			auth:    HTTPAuthorization{CredentialKey: "github_app", AccessToken: "refreshed-token"},
			initial: http.Header{"Authorization": []string{"Bearer stale-token"}},
			replace: true,
			wantKey: "Authorization",
			want:    "Bearer refreshed-token",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ApplyHTTPAuthorization(tc.initial, tc.auth, tc.replace)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ApplyHTTPAuthorization error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ApplyHTTPAuthorization: %v", err)
			}
			if got := tc.initial.Get(tc.wantKey); got != tc.want {
				t.Fatalf("header %s = %q, want %q", tc.wantKey, got, tc.want)
			}
		})
	}
}

func TestApplyHTTPAuthorizationRejectsMissingToken(t *testing.T) {
	err := ApplyHTTPAuthorization(make(http.Header), HTTPAuthorization{CredentialKey: "github_app"}, false)
	if err == nil || !strings.Contains(err.Error(), "did not provide an access token") {
		t.Fatalf("ApplyHTTPAuthorization error = %v", err)
	}
}

func TestApplyHTTPAuthorizationRejectsMissingHeaders(t *testing.T) {
	err := ApplyHTTPAuthorization(nil, HTTPAuthorization{CredentialKey: "github_app", AccessToken: "fixture-token"}, false)
	if err == nil || !strings.Contains(err.Error(), "headers are unavailable") {
		t.Fatalf("ApplyHTTPAuthorization error = %v", err)
	}
}
