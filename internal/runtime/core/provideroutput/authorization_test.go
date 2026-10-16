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

func TestS1AuthorizationRefusesProviderKeyRepair(t *testing.T) {
	generation := triggergeneration.FromCanonicalBytes([]byte("exact-provider-key"))
	for _, key := range []string{"telegram-sales", "telegram_sales"} {
		authorization, err := NewAuthorization(key, "inbound."+key+".message", "test.trigger", "1.0.0", "sha256:"+strings.Repeat("a", 64), generation)
		if err != nil || authorization.Provider() != key {
			t.Fatalf("authorization changed %q: %+v err=%v", key, authorization, err)
		}
		raw, err := json.Marshal(authorization)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Authorization
		if err := json.Unmarshal(raw, &decoded); err != nil || !authorization.Matches(decoded) {
			t.Fatalf("exact key round trip changed authority: %v", err)
		}
	}
	for _, key := range []string{"", "_team", "con", "lpt9", "Telegram", "telegram.bot", " telegram_sales", "telegram_sales ", strings.Repeat("a", 101)} {
		if authorization, err := NewAuthorization(key, "event", "test.trigger", "1.0.0", "sha256:"+strings.Repeat("a", 64), generation); err == nil || authorization.Valid() {
			t.Errorf("authorization repaired nonportable key %q", key)
		}
		wire, err := json.Marshal(map[string]string{"Provider": key, "Event": "event", "PackID": "test.trigger", "PackVersion": "1.0.0", "ManifestHash": "sha256:" + strings.Repeat("a", 64), "GenerationID": generation.Diagnostic()})
		if err != nil {
			t.Fatal(err)
		}
		var decoded Authorization
		if err := json.Unmarshal(wire, &decoded); err == nil || decoded.Valid() {
			t.Errorf("codec repaired nonportable key %q", key)
		}
	}
}
