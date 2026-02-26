package runtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
)

func TestNormalizeEventTokenAndResolveProviderEventType(t *testing.T) {
	if normalizeEventToken("  Payment.Succeeded ") != "payment_succeeded" {
		t.Fatal("unexpected normalization")
	}
	if normalizeEventToken("") != "event" {
		t.Fatal("expected default event")
	}

	if resolveProviderEventType("domain", map[string]any{"status": "Confirmed-OK"}) != "confirmed_ok" {
		t.Fatal("expected domain status token")
	}
	if resolveProviderEventType("stripe", map[string]any{"type": "invoice.paid"}) != "invoice_paid" {
		t.Fatal("expected stripe type token")
	}
	if resolveProviderEventType("stripe", map[string]any{}) != "payment_event" {
		t.Fatal("expected stripe default")
	}
}

func TestVerifyProviderSignature_StripeAndDefaultAndNoSecret(t *testing.T) {
	body := []byte(`{"ok":true}`)

	// No secret only accepts email.
	if !verifyProviderSignature("email", "", body, http.Header{}) {
		t.Fatal("expected email unsigned accepted")
	}
	if verifyProviderSignature("stripe", "", body, http.Header{}) {
		t.Fatal("expected unsigned stripe rejected")
	}

	// Stripe signature: t=timestamp,v1=hmac(secret, t+"."+body)
	secret := "whsec_test"
	ts := "1700000000"
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(ts))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	h := http.Header{}
	h.Set("Stripe-Signature", "t="+ts+",v1="+expected)
	if !verifyProviderSignature("stripe", secret, body, h) {
		t.Fatal("expected stripe signature to verify")
	}
	h.Set("Stripe-Signature", "t="+ts+",v1=deadbeef")
	if verifyProviderSignature("stripe", secret, body, h) {
		t.Fatal("expected stripe signature mismatch")
	}

	// Default provider uses X-Webhook-Token or Authorization bearer.
	h = http.Header{}
	h.Set("X-Webhook-Token", "tok")
	if !verifyProviderSignature("domain", "tok", body, h) {
		t.Fatal("expected token header to verify")
	}
	h = http.Header{}
	h.Set("Authorization", "Bearer tok")
	if !verifyProviderSignature("domain", "tok", body, h) {
		t.Fatal("expected bearer token to verify")
	}
}

