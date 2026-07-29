package provideroutput

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestAuthorizationRejectsIncompleteValuesAndRoundTripsTypedGeneration(t *testing.T) {
	generation := triggergeneration.FromCanonicalBytes([]byte("provider-output-generation"))
	if _, err := NewAuthorization("telegram", "event", "pack", "1.0.0", "", generation); err == nil {
		t.Fatal("incomplete authorization acquired authority")
	}
	if _, err := NewAuthorization("telegram", "event", "Pack/Invalid", "1.0.0", "sha256:"+strings.Repeat("a", 64), generation); err == nil {
		t.Fatal("invalid pack id acquired authorization")
	}
	if _, err := NewAuthorization("telegram", "event", "provider.telegram", "latest", "sha256:"+strings.Repeat("a", 64), generation); err == nil {
		t.Fatal("invalid pack version acquired authorization")
	}
	want := MustAuthorization(
		"telegram", "inbound.telegram.message", "provider.telegram", "1.0.0",
		"sha256:"+strings.Repeat("a", 64), generation,
	)
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Authorization
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !want.Matches(got) || !got.Generation().Equal(generation) {
		t.Fatalf("authorization round trip = %#v, want exact admitted value", got)
	}
}

func TestAuthorizationCodecRejectsNonCanonicalGeneration(t *testing.T) {
	generation := triggergeneration.FromCanonicalBytes([]byte("catalog"))
	_, err := ParseAuthorization(
		"provider", "event", "pack.id", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		" "+generation.Diagnostic(),
	)
	if err == nil {
		t.Fatal("generation with leading whitespace was admitted")
	}
}
