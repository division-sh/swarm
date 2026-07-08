package model

import (
	"strings"
	"testing"
)

func TestValidateTokenRequestProfileRejectsDuplicateStaticHeadersBeforeCanonicalizing(t *testing.T) {
	err := ValidateTokenRequestProfile(TokenRequestProfile{
		StaticHeaders: map[string]string{
			"X-Provider-Version": "2026-03-11",
			"x-provider-version": "2026-04-01",
		},
	})
	if err == nil {
		t.Fatal("ValidateTokenRequestProfile error = nil, want duplicate static header failure")
	}
	if !strings.Contains(err.Error(), "duplicate header") {
		t.Fatalf("ValidateTokenRequestProfile error = %v, want duplicate header failure", err)
	}
}
