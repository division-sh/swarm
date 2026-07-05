package providertriggers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestManifestInterpreterHostileProviderRejectsBoundaryAttacks(t *testing.T) {
	registry := newHostileRegistry(t)
	now := time.Unix(1710000000, 0).UTC()

	t.Run("smuggled fields do not override manifest sources", func(t *testing.T) {
		body := []byte(`{"event_type":"safe.event","event_id":"payload-evil","provider_event_type":"payload-evil"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Delivery", "delivery-header")
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))

		delivery, err := registry.Accept(req)
		if err != nil {
			t.Fatalf("Accept hostile valid request: %v", err)
		}
		if delivery.ProviderEventID != "delivery-header" {
			t.Fatalf("provider event id = %q, want manifest header source", delivery.ProviderEventID)
		}
		if delivery.ProviderEventType != "safe_event" {
			t.Fatalf("provider event type = %q, want manifest json path source", delivery.ProviderEventType)
		}
		if delivery.Payload["provider_event_id"] != "delivery-header" || delivery.Payload["provider_event_type"] != "safe_event" {
			t.Fatalf("payload identity = %+v, want manifest-derived fields", delivery.Payload)
		}
	})

	t.Run("signature confusion fails closed", func(t *testing.T) {
		body := []byte(`{"event_type":"safe.event"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Delivery", "delivery-header")
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Other-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))
		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusUnauthorized)
	})

	t.Run("replayed timestamp fails closed", func(t *testing.T) {
		old := now.Add(-10 * time.Minute)
		body := []byte(`{"event_type":"safe.event"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Delivery", "delivery-header")
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(old))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(old), body))
		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusUnauthorized)
	})

	t.Run("raw-byte unicode canonicalization mismatch fails closed", func(t *testing.T) {
		signedBody := []byte(`{"event_type":"café","amount":1}`)
		deliveredBody := []byte(`{"amount":1,"event_type":"café"}`)
		req := hostileRequest(now, deliveredBody)
		req.Headers.Set("X-Hostile-Delivery", "delivery-header")
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), signedBody))
		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusUnauthorized)
	})

	t.Run("object event type fails closed", func(t *testing.T) {
		body := []byte(`{"event_type":"safe.event"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Delivery", "delivery-header")
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))
		req.Payload = map[string]any{
			"event_type": map[string]any{"nested": "safe.event"},
		}
		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusBadRequest)
	})

	t.Run("list event type fails closed", func(t *testing.T) {
		body := []byte(`{"event_type":"safe.event"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Delivery", "delivery-header")
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))
		req.Payload = map[string]any{
			"event_type": []any{"safe.event"},
		}
		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusBadRequest)
	})

	t.Run("bool event type fails closed", func(t *testing.T) {
		body := []byte(`{"event_type":"safe.event"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Delivery", "delivery-header")
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))
		req.Payload = map[string]any{
			"event_type": true,
		}
		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusBadRequest)
	})
}

func TestManifestInterpreterRejectsMalformedJSONPathIdentityPayloads(t *testing.T) {
	registry := newHostilePayloadIdentityRegistry(t)
	now := time.Unix(1710000000, 0).UTC()

	t.Run("object delivery id fails closed", func(t *testing.T) {
		body := []byte(`{"event_id":{"nested":"delivery"},"event_type":"safe.event"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))
		req.Payload = map[string]any{
			"event_id":   map[string]any{"nested": "delivery"},
			"event_type": "safe.event",
		}

		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusBadRequest)
	})

	t.Run("list delivery id fails closed", func(t *testing.T) {
		body := []byte(`{"event_id":["delivery"],"event_type":"safe.event"}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))
		req.Payload = map[string]any{
			"event_id":   []any{"delivery"},
			"event_type": "safe.event",
		}

		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusBadRequest)
	})

	t.Run("bool event type fails closed", func(t *testing.T) {
		body := []byte(`{"event_id":"delivery","event_type":true}`)
		req := hostileRequest(now, body)
		req.Headers.Set("X-Hostile-Timestamp", strconvFormatUnix(now))
		req.Headers.Set("X-Hostile-Signature", hostileSignature("hostile-secret", strconvFormatUnix(now), body))
		req.Payload = map[string]any{
			"event_id":   "delivery",
			"event_type": true,
		}

		_, err := registry.Accept(req)
		requireProviderTriggerError(t, err, http.StatusBadRequest)
	})
}

func TestStripeManifestRejectsMalformedSignatureParams(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	body := []byte(`{"id":"evt_123","type":"invoice.paid"}`)
	req := Request{
		Provider: "stripe",
		Target: Target{
			EntityID:      "entity-1",
			WebhookSecret: "stripe-secret",
		},
		Body:     body,
		Headers:  make(http.Header),
		Payload:  map[string]any{"id": "evt_123", "type": "invoice.paid"},
		Received: now,
	}
	req.Headers.Set("Stripe-Signature", "t="+strconvFormatUnix(now)+",v0="+stripeSignatureHex("stripe-secret", strconvFormatUnix(now), body))

	_, err := DefaultRegistry().Accept(req)
	requireProviderTriggerError(t, err, http.StatusUnauthorized)

	req.Headers.Set("Stripe-Signature", "t="+strconvFormatUnix(now)+",broken,v1="+stripeSignatureHex("stripe-secret", strconvFormatUnix(now), body))
	_, err = DefaultRegistry().Accept(req)
	requireProviderTriggerError(t, err, http.StatusUnauthorized)

	req.Headers.Set("Stripe-Signature", "t="+strconvFormatUnix(now)+",t="+strconvFormatUnix(now)+",v1="+stripeSignatureHex("stripe-secret", strconvFormatUnix(now), body))
	_, err = DefaultRegistry().Accept(req)
	requireProviderTriggerError(t, err, http.StatusUnauthorized)
}

func TestStripeManifestAcceptsLiteralEventType(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	body := []byte(`{"id":"evt_123","type":"event"}`)
	req := Request{
		Provider: "stripe",
		Target: Target{
			EntityID:      "entity-1",
			WebhookSecret: "stripe-secret",
		},
		Body:     body,
		Headers:  make(http.Header),
		Payload:  map[string]any{"id": "evt_123", "type": "event"},
		Received: now,
	}
	req.Headers.Set("Stripe-Signature", "t="+strconvFormatUnix(now)+",v1="+stripeSignatureHex("stripe-secret", strconvFormatUnix(now), body))

	delivery, err := DefaultRegistry().Accept(req)
	if err != nil {
		t.Fatalf("Accept Stripe literal event type: %v", err)
	}
	if delivery.ProviderEventType != "event" {
		t.Fatalf("ProviderEventType = %q, want event", delivery.ProviderEventType)
	}
	if delivery.EventName != "inbound.stripe" {
		t.Fatalf("EventName = %q, want inbound.stripe", delivery.EventName)
	}
}

func TestManifestValidationRejectsAuthoringErrors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		manifest Manifest
	}{
		{
			name: "invalid json path syntax",
			manifest: Manifest{
				Provider:   "badpath",
				DeliveryID: ValueSource{JSONPath: "$..id", Required: true},
				EventType:  ValueSource{JSONPath: "$.type", Required: true},
				EventName:  EventNameManifest{Literal: "inbound.badpath"},
			},
		},
		{
			name: "unknown metadata source",
			manifest: Manifest{
				Provider:   "badmetadata",
				DeliveryID: ValueSource{JSONPath: "$.id", Required: true},
				EventType:  ValueSource{JSONPath: "$.type", Required: true},
				EventName:  EventNameManifest{Literal: "inbound.badmetadata"},
				Metadata:   map[string]string{"bad": "unknown_source"},
			},
		},
		{
			name: "new provider event name template",
			manifest: Manifest{
				Provider:   "badtemplate",
				DeliveryID: ValueSource{JSONPath: "$.id", Required: true},
				EventType:  ValueSource{JSONPath: "$.type", Required: true},
				EventName:  EventNameManifest{Template: "inbound.badtemplate.{event_type}"},
			},
		},
		{
			name: "literal and template both set",
			manifest: Manifest{
				Provider:   "ambiguousname",
				DeliveryID: ValueSource{JSONPath: "$.id", Required: true},
				EventType:  ValueSource{JSONPath: "$.type", Required: true},
				EventName: EventNameManifest{
					Literal:  "inbound.ambiguousname",
					Template: "inbound.ambiguousname.{event_type}",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRegistry(tc.manifest); err == nil {
				t.Fatal("NewRegistry succeeded, want validation error")
			}
		})
	}
}

func TestRegistryRejectsEmptyProvider(t *testing.T) {
	_, err := DefaultRegistry().Accept(Request{
		Target:  Target{EntityID: "entity-1"},
		Body:    []byte(`{}`),
		Headers: make(http.Header),
		Payload: map[string]any{},
	})
	requireProviderTriggerError(t, err, http.StatusBadRequest)
}

func newHostileRegistry(t *testing.T) *Registry {
	t.Helper()
	manifest := Manifest{
		Provider:              "hostile",
		PayloadObjectRequired: true,
		PayloadObjectError:    "hostile payload object is required",
		Secret:                SecretManifest{Required: true},
		Signature: SignatureManifest{
			Type:          "hmac_sha256",
			Header:        "X-Hostile-Signature",
			Prefix:        "v1=",
			SignedPayload: "timestamp_dot_raw_body",
			MissingError:  "hostile signature is required",
			InvalidError:  "invalid hostile signature",
			Timestamp: &TimestampManifest{
				Header:       "X-Hostile-Timestamp",
				Tolerance:    "5m",
				MissingError: "hostile timestamp is required",
				InvalidError: "invalid hostile timestamp",
				StaleError:   "stale hostile timestamp",
			},
		},
		DeliveryID: ValueSource{
			Header:       "X-Hostile-Delivery",
			Required:     true,
			MissingError: "hostile delivery id is required",
		},
		EventType: ValueSource{
			JSONPath:     "$.event_type",
			Required:     true,
			MissingError: "hostile event type is required",
		},
		EventName: EventNameManifest{Literal: "inbound.hostile"},
		Ack:       AckManifest{Mode: "durable_before_dispatch"},
		Metadata: map[string]string{
			"hostile_delivery": "delivery_id",
			"hostile_event":    "event_type",
		},
	}
	registry, err := NewRegistry(manifest)
	if err != nil {
		t.Fatalf("NewRegistry hostile: %v", err)
	}
	return registry
}

func newHostilePayloadIdentityRegistry(t *testing.T) *Registry {
	t.Helper()
	manifest := Manifest{
		Provider:              "hostile",
		PayloadObjectRequired: true,
		PayloadObjectError:    "hostile payload object is required",
		Secret:                SecretManifest{Required: true},
		Signature: SignatureManifest{
			Type:          "hmac_sha256",
			Header:        "X-Hostile-Signature",
			Prefix:        "v1=",
			SignedPayload: "timestamp_dot_raw_body",
			MissingError:  "hostile signature is required",
			InvalidError:  "invalid hostile signature",
			Timestamp: &TimestampManifest{
				Header:       "X-Hostile-Timestamp",
				Tolerance:    "5m",
				MissingError: "hostile timestamp is required",
				InvalidError: "invalid hostile timestamp",
				StaleError:   "stale hostile timestamp",
			},
		},
		DeliveryID: ValueSource{
			JSONPath:     "$.event_id",
			Required:     true,
			MissingError: "hostile delivery id is required",
		},
		EventType: ValueSource{
			JSONPath:     "$.event_type",
			Required:     true,
			MissingError: "hostile event type is required",
		},
		EventName: EventNameManifest{Literal: "inbound.hostile"},
		Ack:       AckManifest{Mode: "durable_before_dispatch"},
	}
	registry, err := NewRegistry(manifest)
	if err != nil {
		t.Fatalf("NewRegistry hostile payload identity: %v", err)
	}
	return registry
}

func hostileRequest(now time.Time, body []byte) Request {
	return Request{
		Provider: "hostile",
		Target: Target{
			EntityID:      "entity-1",
			WebhookSecret: "hostile-secret",
		},
		Body:     body,
		Headers:  make(http.Header),
		Payload:  mustPayload(body),
		Received: now,
	}
}

func mustPayload(body []byte) map[string]any {
	out := make(map[string]any)
	for _, pair := range strings.Split(strings.Trim(strings.TrimSpace(string(body)), "{}"), ",") {
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.Trim(strings.TrimSpace(kv[0]), `"`)
		value := strings.Trim(strings.TrimSpace(kv[1]), `"`)
		out[key] = value
	}
	return out
}

func requireProviderTriggerError(t *testing.T, err error, status int) {
	t.Helper()
	if err == nil {
		t.Fatal("err = nil, want provider trigger error")
	}
	providerErr, ok := err.(Error)
	if !ok {
		t.Fatalf("err type = %T, want providertriggers.Error", err)
	}
	if providerErr.Status != status {
		t.Fatalf("status = %d, want %d (%v)", providerErr.Status, status, err)
	}
}

func hostileSignature(secret, timestamp string, body []byte) string {
	return "v1=" + stripeSignatureHex(secret, timestamp, body)
}

func stripeSignatureHex(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + string(body)))
	return hex.EncodeToString(mac.Sum(nil))
}

func strconvFormatUnix(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}
