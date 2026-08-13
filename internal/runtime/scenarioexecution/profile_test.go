package scenarioexecution

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

func TestProfileCanonicalBytesAreStableAcrossResponseOrder(t *testing.T) {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	left, err := NewProfile(identity, "derived/chat", []ConnectorResponse{
		{ToolID: "z.send", OutputSchemaDigest: "sha256:" + strings.Repeat("c", 64), Response: json.RawMessage(`{"ok":true}`)},
		{ToolID: "a.send", OutputSchemaDigest: "sha256:" + strings.Repeat("d", 64), Response: json.RawMessage(`{"id":1}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewProfile(identity, "derived/chat", []ConnectorResponse{
		{ToolID: "a.send", OutputSchemaDigest: "sha256:" + strings.Repeat("d", 64), Response: json.RawMessage(`{ "id" : 1 }`)},
		{ToolID: "z.send", OutputSchemaDigest: "sha256:" + strings.Repeat("c", 64), Response: json.RawMessage(`{ "ok" : true }`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if left.Digest() != right.Digest() || !bytes.Equal(left.CanonicalBytes(), right.CanonicalBytes()) {
		t.Fatalf("canonical profiles differ:\n%s\n%s", left.CanonicalBytes(), right.CanonicalBytes())
	}
	decoded, err := DecodeProfile(left.CanonicalBytes(), left.Digest())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID() != left.ID() || !decoded.EffectiveSourceIdentity().Equal(identity) {
		t.Fatalf("decoded profile identity mismatch: %#v", decoded)
	}
}

func TestDecodeProfileRejectsNonCanonicalOrTamperedBytes(t *testing.T) {
	fact, _ := runtimecorrelation.NewPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("a", 64))
	identity, _ := NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("b", 64))
	profile, err := NewProfile(identity, "empty", nil)
	if err != nil {
		t.Fatal(err)
	}
	nonCanonical := append([]byte(" "), profile.CanonicalBytes()...)
	if _, err := DecodeProfile(nonCanonical, profile.Digest()); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("non-canonical decode error = %v", err)
	}
	tampered := append([]byte(nil), profile.CanonicalBytes()...)
	tampered[len(tampered)-2] = 'x'
	if _, err := DecodeProfile(tampered, profile.Digest()); err == nil {
		t.Fatal("tampered profile decode succeeded")
	}
}
