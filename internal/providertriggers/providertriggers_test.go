package providertriggers

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
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

func TestTwilioManifestAcceptsSignedFormWebhook(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":          {"hello from twilio"},
		"From":          {"+15551234567"},
		"MessageSid":    {"SM1234567890abcdef"},
		"To":            {"+15557654321"},
		"UnexpectedNew": {"still signed"},
	}
	req := Request{
		Provider: "twilio",
		Target: Target{
			EntityID:      "entity-1",
			WebhookSecret: "twilio-secret",
		},
		Method:      http.MethodPost,
		URL:         requestURL,
		Body:        []byte(form.Encode()),
		Headers:     make(http.Header),
		Payload:     map[string]any{"raw": form.Encode()},
		ContentType: "application/x-www-form-urlencoded",
		Query:       url.Values{"tenant": {"alpha"}},
		Form:        form,
		FormParsed:  true,
		Received:    now,
		UserAgent:   "twilio-test",
	}
	req.Headers.Set("X-Twilio-Signature", twilioSignatureBase64("twilio-secret", requestURL, form))

	delivery, err := DefaultRegistry().Accept(req)
	if err != nil {
		t.Fatalf("Accept Twilio form webhook: %v", err)
	}
	if delivery.ProviderEventID != "SM1234567890abcdef" {
		t.Fatalf("ProviderEventID = %q, want MessageSid", delivery.ProviderEventID)
	}
	if delivery.ProviderEventType != "message_received" {
		t.Fatalf("ProviderEventType = %q, want message_received", delivery.ProviderEventType)
	}
	if delivery.EventName != "inbound.twilio" {
		t.Fatalf("EventName = %q, want inbound.twilio", delivery.EventName)
	}
	if !delivery.AcknowledgeBeforeDispatch {
		t.Fatal("Twilio delivery did not request durable_before_dispatch acknowledgement")
	}
	payload, ok := delivery.Payload["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload = %T, want form payload map", delivery.Payload["payload"])
	}
	if payload["Body"] != "hello from twilio" || payload["UnexpectedNew"] != "still signed" {
		t.Fatalf("form payload = %+v, want Twilio form parameters without allowlist loss", payload)
	}
	headers, ok := delivery.Payload["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %T, want metadata map", delivery.Payload["headers"])
	}
	if headers["twilio_message_sid"] != "SM1234567890abcdef" || headers["twilio_event_type"] != "message_received" {
		t.Fatalf("headers = %+v, want Twilio manifest metadata", headers)
	}
	encoded := fmtPayload(delivery.Payload)
	if strings.Contains(encoded, "twilio-secret") {
		t.Fatal("Twilio signing secret leaked into delivery payload")
	}
}

func TestTwilioManifestRejectsAmbiguousOrUnsupportedCallbacks(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	validForm := url.Values{
		"Body":       {"hello"},
		"MessageSid": {"SM1234567890abcdef"},
	}
	validRequest := func() Request {
		req := Request{
			Provider: "twilio",
			Target: Target{
				EntityID:      "entity-1",
				WebhookSecret: "twilio-secret",
			},
			Method:      http.MethodPost,
			URL:         requestURL,
			Body:        []byte(validForm.Encode()),
			Headers:     make(http.Header),
			Payload:     map[string]any{"raw": validForm.Encode()},
			ContentType: "application/x-www-form-urlencoded",
			Query:       url.Values{"tenant": {"alpha"}},
			Form:        cloneValues(validForm),
			FormParsed:  true,
			Received:    now,
		}
		req.Headers.Set("X-Twilio-Signature", twilioSignatureBase64("twilio-secret", requestURL, validForm))
		return req
	}
	for _, tc := range []struct {
		name       string
		configure  func(Request) Request
		wantStatus int
	}{
		{
			name: "missing signature",
			configure: func(req Request) Request {
				req.Headers.Del("X-Twilio-Signature")
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "url mismatch",
			configure: func(req Request) Request {
				req.URL = "https://example.com/webhooks/customer-a/twilio?tenant=beta"
				req.Query = url.Values{"tenant": {"beta"}}
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "duplicate query params",
			configure: func(req Request) Request {
				req.URL = "https://example.com/webhooks/customer-a/twilio?tenant=alpha&tenant=beta"
				req.Query = url.Values{"tenant": {"alpha", "beta"}}
				req.Headers.Set("X-Twilio-Signature", twilioSignatureBase64("twilio-secret", req.URL, validForm))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "duplicate form params",
			configure: func(req Request) Request {
				req.Form = cloneValues(validForm)
				req.Form["Body"] = []string{"hello", "tampered"}
				req.Body = []byte(req.Form.Encode())
				req.Headers.Set("X-Twilio-Signature", twilioSignatureBase64("twilio-secret", req.URL, req.Form))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing message sid",
			configure: func(req Request) Request {
				req.Form = url.Values{"Body": {"hello"}}
				req.Body = []byte(req.Form.Encode())
				req.Headers.Set("X-Twilio-Signature", twilioSignatureBase64("twilio-secret", req.URL, req.Form))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "json body sha256 mode unsupported in this slice",
			configure: func(req Request) Request {
				req.URL = "https://example.com/webhooks/customer-a/twilio?bodySHA256=abc123"
				req.Query = url.Values{"bodySHA256": {"abc123"}}
				req.Body = []byte(`{"MessageSid":"SM1234567890abcdef"}`)
				req.Payload = map[string]any{"MessageSid": "SM1234567890abcdef"}
				req.ContentType = "application/json"
				req.Form = nil
				req.FormParsed = false
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DefaultRegistry().Accept(tc.configure(validRequest()))
			requireProviderTriggerError(t, err, tc.wantStatus)
		})
	}
}

func TestShopifyManifestAcceptsRawBodyBase64Signature(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := Request{
		Provider: "shopify",
		Target: Target{
			EntityID:      "entity-1",
			WebhookSecret: "shopify-secret",
		},
		Body:      body,
		Headers:   make(http.Header),
		Payload:   map[string]any{"id": float64(123), "line_items": []any{map[string]any{"sku": "abc"}}},
		Received:  now,
		UserAgent: "shopify-test",
	}
	req.Headers.Set("X-Shopify-Hmac-Sha256", shopifySignatureBase64("shopify-secret", body))
	req.Headers.Set("X-Shopify-Webhook-Id", "webhook-123")
	req.Headers.Set("X-Shopify-Topic", "orders/create")

	delivery, err := DefaultRegistry().Accept(req)
	if err != nil {
		t.Fatalf("Accept Shopify webhook: %v", err)
	}
	if delivery.ProviderEventID != "webhook-123" {
		t.Fatalf("ProviderEventID = %q, want webhook-123", delivery.ProviderEventID)
	}
	if delivery.ProviderEventType != "orders_create" {
		t.Fatalf("ProviderEventType = %q, want orders_create", delivery.ProviderEventType)
	}
	if delivery.EventName != "inbound.shopify" {
		t.Fatalf("EventName = %q, want inbound.shopify", delivery.EventName)
	}
	if !delivery.AcknowledgeBeforeDispatch {
		t.Fatal("Shopify delivery did not request durable_before_dispatch acknowledgement")
	}
	headers, ok := delivery.Payload["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %T, want metadata map", delivery.Payload["headers"])
	}
	if headers["shopify_webhook_id"] != "webhook-123" || headers["shopify_topic"] != "orders_create" {
		t.Fatalf("headers = %+v, want Shopify manifest metadata", headers)
	}
	encoded := fmtPayload(delivery.Payload)
	if strings.Contains(encoded, "shopify-secret") {
		t.Fatal("Shopify signing secret leaked into delivery payload")
	}
}

func TestShopifyManifestRejectsInvalidInputsBeforeDelivery(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	validBody := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	validRequest := func() Request {
		req := Request{
			Provider: "shopify",
			Target: Target{
				EntityID:      "entity-1",
				WebhookSecret: "shopify-secret",
			},
			Body:     validBody,
			Headers:  make(http.Header),
			Payload:  map[string]any{"id": float64(123), "line_items": []any{map[string]any{"sku": "abc"}}},
			Received: now,
		}
		req.Headers.Set("X-Shopify-Hmac-Sha256", shopifySignatureBase64("shopify-secret", validBody))
		req.Headers.Set("X-Shopify-Webhook-Id", "webhook-123")
		req.Headers.Set("X-Shopify-Topic", "orders/create")
		return req
	}
	for _, tc := range []struct {
		name       string
		configure  func(Request) Request
		wantStatus int
	}{
		{
			name: "missing signature",
			configure: func(req Request) Request {
				req.Headers.Del("X-Shopify-Hmac-Sha256")
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid signature",
			configure: func(req Request) Request {
				req.Headers.Set("X-Shopify-Hmac-Sha256", shopifySignatureBase64("wrong-secret", req.Body))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "raw body mutation",
			configure: func(req Request) Request {
				signedBody := []byte(`{"line_items":[{"sku":"abc"}],"id":123}`)
				req.Headers.Set("X-Shopify-Hmac-Sha256", shopifySignatureBase64("shopify-secret", signedBody))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing webhook id",
			configure: func(req Request) Request {
				req.Headers.Del("X-Shopify-Webhook-Id")
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing topic",
			configure: func(req Request) Request {
				req.Headers.Del("X-Shopify-Topic")
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non object payload",
			configure: func(req Request) Request {
				req.Body = []byte(`[{"id":123}]`)
				req.Payload = []any{map[string]any{"id": float64(123)}}
				req.Headers.Set("X-Shopify-Hmac-Sha256", shopifySignatureBase64("shopify-secret", req.Body))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DefaultRegistry().Accept(tc.configure(validRequest()))
			requireProviderTriggerError(t, err, tc.wantStatus)
		})
	}
}

func TestTypeformManifestAcceptsRawBodyBase64Signature(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	body := []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`)
	req := Request{
		Provider: "typeform",
		Target: Target{
			EntityID:      "entity-1",
			WebhookSecret: "typeform-secret",
		},
		Body:      body,
		Headers:   make(http.Header),
		Payload:   map[string]any{"event_id": "tf-evt-123", "event_type": "form_response", "form_response": map[string]any{"token": "abc"}},
		Received:  now,
		UserAgent: "typeform-test",
	}
	req.Headers.Set("Typeform-Signature", typeformSignatureBase64("typeform-secret", body))

	delivery, err := DefaultRegistry().Accept(req)
	if err != nil {
		t.Fatalf("Accept Typeform webhook: %v", err)
	}
	if delivery.ProviderEventID != "tf-evt-123" {
		t.Fatalf("ProviderEventID = %q, want tf-evt-123", delivery.ProviderEventID)
	}
	if delivery.ProviderEventType != "form_response" {
		t.Fatalf("ProviderEventType = %q, want form_response", delivery.ProviderEventType)
	}
	if delivery.EventName != "inbound.typeform" {
		t.Fatalf("EventName = %q, want inbound.typeform", delivery.EventName)
	}
	if !delivery.AcknowledgeBeforeDispatch {
		t.Fatal("Typeform delivery did not request durable_before_dispatch acknowledgement")
	}
	headers, ok := delivery.Payload["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %T, want metadata map", delivery.Payload["headers"])
	}
	if headers["typeform_event_id"] != "tf-evt-123" || headers["typeform_event_type"] != "form_response" {
		t.Fatalf("headers = %+v, want Typeform manifest metadata", headers)
	}
	encoded := fmtPayload(delivery.Payload)
	if strings.Contains(encoded, "typeform-secret") {
		t.Fatal("Typeform signing secret leaked into delivery payload")
	}
}

func TestTypeformManifestRejectsInvalidInputsBeforeDelivery(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	validBody := []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`)
	validPayload := map[string]any{"event_id": "tf-evt-123", "event_type": "form_response", "form_response": map[string]any{"token": "abc"}}
	validRequest := func() Request {
		req := Request{
			Provider: "typeform",
			Target: Target{
				EntityID:      "entity-1",
				WebhookSecret: "typeform-secret",
			},
			Body:     validBody,
			Headers:  make(http.Header),
			Payload:  validPayload,
			Received: now,
		}
		req.Headers.Set("Typeform-Signature", typeformSignatureBase64("typeform-secret", validBody))
		return req
	}
	for _, tc := range []struct {
		name       string
		configure  func(Request) Request
		wantStatus int
	}{
		{
			name: "missing signature",
			configure: func(req Request) Request {
				req.Headers.Del("Typeform-Signature")
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid signature",
			configure: func(req Request) Request {
				req.Headers.Set("Typeform-Signature", typeformSignatureBase64("wrong-secret", req.Body))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "raw body mutation",
			configure: func(req Request) Request {
				signedBody := []byte(`{"event_type":"form_response","event_id":"tf-evt-123","form_response":{"token":"abc"}}`)
				req.Headers.Set("Typeform-Signature", typeformSignatureBase64("typeform-secret", signedBody))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing event id",
			configure: func(req Request) Request {
				req.Body = []byte(`{"event_type":"form_response","form_response":{"token":"abc"}}`)
				req.Payload = map[string]any{"event_type": "form_response", "form_response": map[string]any{"token": "abc"}}
				req.Headers.Set("Typeform-Signature", typeformSignatureBase64("typeform-secret", req.Body))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing event type",
			configure: func(req Request) Request {
				req.Body = []byte(`{"event_id":"tf-evt-123","form_response":{"token":"abc"}}`)
				req.Payload = map[string]any{"event_id": "tf-evt-123", "form_response": map[string]any{"token": "abc"}}
				req.Headers.Set("Typeform-Signature", typeformSignatureBase64("typeform-secret", req.Body))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non object payload",
			configure: func(req Request) Request {
				req.Body = []byte(`[{"event_id":"tf-evt-123","event_type":"form_response"}]`)
				req.Payload = []any{map[string]any{"event_id": "tf-evt-123", "event_type": "form_response"}}
				req.Headers.Set("Typeform-Signature", typeformSignatureBase64("typeform-secret", req.Body))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DefaultRegistry().Accept(tc.configure(validRequest()))
			requireProviderTriggerError(t, err, tc.wantStatus)
		})
	}
}

func TestIntercomManifestAcceptsRawBodySHA1Signature(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	body := []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`)
	req := Request{
		Provider: "intercom",
		Target: Target{
			EntityID:      "entity-1",
			WebhookSecret: "intercom-secret",
		},
		Body:      body,
		Headers:   make(http.Header),
		Payload:   map[string]any{"id": "notif_123", "topic": "conversation.user.created", "data": map[string]any{"item": map[string]any{"id": "conv_1"}}},
		Received:  now,
		UserAgent: "intercom-test",
	}
	req.Headers.Set("X-Hub-Signature", intercomSignatureHex("intercom-secret", body))

	delivery, err := DefaultRegistry().Accept(req)
	if err != nil {
		t.Fatalf("Accept Intercom webhook: %v", err)
	}
	if delivery.ProviderEventID != "notif_123" {
		t.Fatalf("ProviderEventID = %q, want notif_123", delivery.ProviderEventID)
	}
	if delivery.ProviderEventType != "conversation_user_created" {
		t.Fatalf("ProviderEventType = %q, want conversation_user_created", delivery.ProviderEventType)
	}
	if delivery.EventName != "inbound.intercom" {
		t.Fatalf("EventName = %q, want inbound.intercom", delivery.EventName)
	}
	if !delivery.AcknowledgeBeforeDispatch {
		t.Fatal("Intercom delivery did not request durable_before_dispatch acknowledgement")
	}
	headers, ok := delivery.Payload["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %T, want metadata map", delivery.Payload["headers"])
	}
	if headers["intercom_notification_id"] != "notif_123" || headers["intercom_topic"] != "conversation_user_created" {
		t.Fatalf("headers = %+v, want Intercom manifest metadata", headers)
	}
	encoded := fmtPayload(delivery.Payload)
	if strings.Contains(encoded, "intercom-secret") {
		t.Fatal("Intercom signing secret leaked into delivery payload")
	}
}

func TestIntercomManifestRejectsInvalidInputsBeforeDelivery(t *testing.T) {
	now := time.Unix(1710000000, 0).UTC()
	validBody := []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`)
	validPayload := map[string]any{"id": "notif_123", "topic": "conversation.user.created", "data": map[string]any{"item": map[string]any{"id": "conv_1"}}}
	validRequest := func() Request {
		req := Request{
			Provider: "intercom",
			Target: Target{
				EntityID:      "entity-1",
				WebhookSecret: "intercom-secret",
			},
			Body:     validBody,
			Headers:  make(http.Header),
			Payload:  validPayload,
			Received: now,
		}
		req.Headers.Set("X-Hub-Signature", intercomSignatureHex("intercom-secret", validBody))
		return req
	}
	for _, tc := range []struct {
		name       string
		configure  func(Request) Request
		wantStatus int
	}{
		{
			name: "missing signature",
			configure: func(req Request) Request {
				req.Headers.Del("X-Hub-Signature")
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid signature",
			configure: func(req Request) Request {
				req.Headers.Set("X-Hub-Signature", intercomSignatureHex("wrong-secret", req.Body))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "raw body mutation",
			configure: func(req Request) Request {
				signedBody := []byte(`{"topic":"conversation.user.created","id":"notif_123","data":{"item":{"id":"conv_1"}}}`)
				req.Headers.Set("X-Hub-Signature", intercomSignatureHex("intercom-secret", signedBody))
				return req
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing notification id",
			configure: func(req Request) Request {
				req.Body = []byte(`{"topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`)
				req.Payload = map[string]any{"topic": "conversation.user.created", "data": map[string]any{"item": map[string]any{"id": "conv_1"}}}
				req.Headers.Set("X-Hub-Signature", intercomSignatureHex("intercom-secret", req.Body))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing topic",
			configure: func(req Request) Request {
				req.Body = []byte(`{"id":"notif_123","data":{"item":{"id":"conv_1"}}}`)
				req.Payload = map[string]any{"id": "notif_123", "data": map[string]any{"item": map[string]any{"id": "conv_1"}}}
				req.Headers.Set("X-Hub-Signature", intercomSignatureHex("intercom-secret", req.Body))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non object payload",
			configure: func(req Request) Request {
				req.Body = []byte(`[{"id":"notif_123","topic":"conversation.user.created"}]`)
				req.Payload = []any{map[string]any{"id": "notif_123", "topic": "conversation.user.created"}}
				req.Headers.Set("X-Hub-Signature", intercomSignatureHex("intercom-secret", req.Body))
				return req
			},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DefaultRegistry().Accept(tc.configure(validRequest()))
			requireProviderTriggerError(t, err, tc.wantStatus)
		})
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
		{
			name: "url sorted form requires hmac sha1",
			manifest: Manifest{
				Provider: "badformsignature",
				Secret:   SecretManifest{Required: true},
				Signature: SignatureManifest{
					Type:          "hmac_sha256",
					Encoding:      "base64",
					Header:        "X-Signature",
					SignedPayload: "url_plus_sorted_form",
				},
				DeliveryID: ValueSource{FormParam: "MessageSid", Required: true},
				EventType:  ValueSource{Literal: "message_received", Required: true},
				EventName:  EventNameManifest{Literal: "inbound.badformsignature"},
			},
		},
		{
			name: "url sorted form requires base64",
			manifest: Manifest{
				Provider: "badformencoding",
				Secret:   SecretManifest{Required: true},
				Signature: SignatureManifest{
					Type:          "hmac_sha1",
					Encoding:      "hex",
					Header:        "X-Signature",
					SignedPayload: "url_plus_sorted_form",
				},
				DeliveryID: ValueSource{FormParam: "MessageSid", Required: true},
				EventType:  ValueSource{Literal: "message_received", Required: true},
				EventName:  EventNameManifest{Literal: "inbound.badformencoding"},
			},
		},
		{
			name: "ambiguous value source",
			manifest: Manifest{
				Provider:   "badsource",
				DeliveryID: ValueSource{Header: "X-Delivery", FormParam: "MessageSid", Required: true},
				EventType:  ValueSource{Literal: "message_received", Required: true},
				EventName:  EventNameManifest{Literal: "inbound.badsource"},
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

func TestNormalizeEventTokenNormalizesProviderTopicSeparators(t *testing.T) {
	if got := NormalizeEventToken("orders/create"); got != "orders_create" {
		t.Fatalf("NormalizeEventToken = %q, want orders_create", got)
	}
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

func twilioSignatureBase64(secret, requestURL string, form url.Values) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(twilioSignedPayload(requestURL, form)))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func twilioSignedPayload(requestURL string, form url.Values) string {
	keys := make([]string, 0, len(form))
	for key := range form {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(requestURL)
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString(form.Get(key))
	}
	return b.String()
}

func shopifySignatureBase64(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func typeformSignatureBase64(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func intercomSignatureHex(secret string, body []byte) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha1=" + hex.EncodeToString(mac.Sum(nil))
}

func cloneValues(values url.Values) url.Values {
	cloned := make(url.Values, len(values))
	for key, items := range values {
		cloned[key] = append([]string(nil), items...)
	}
	return cloned
}

func fmtPayload(payload map[string]any) string {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Sprintf("%+v", payload)
	}
	return string(body)
}

func strconvFormatUnix(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10)
}
