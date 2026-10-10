package providertriggers

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestManifestNormalizedPayloadProjectionSharesWebhookOwner(t *testing.T) {
	manifest := normalizedEventTestManifest().mustAdmit()
	body := []byte(`{"update_id":123,"message":{"message_id":7,"chat":{"id":42},"text":"hello"}}`)
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	delivery, err := manifest.Accept(Request{Target: Target{}, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := manifest.ProjectNormalizedPayload(body)
	if err != nil || len(delivery.Events) != 2 || !reflect.DeepEqual(projected, delivery.Events[1:]) {
		t.Fatalf("pure and admitted projection differ: %+v vs %+v, %v", projected, delivery.Events, err)
	}
	if !projected[0].Authorization.Empty() {
		t.Fatal("payload projection minted output authorization")
	}
}

func TestManifestNormalizedPayloadUsesAuthenticatedObjectGate(t *testing.T) {
	for _, message := range []string{"", "exact authored object failure"} {
		for _, body := range []string{`[]`, `"text"`, `17`, `true`, `null`} {
			t.Run(message+"/"+body, func(t *testing.T) {
				fixture := normalizedEventTestManifest()
				fixture.PayloadObjectRequired = true
				fixture.PayloadObjectError = message
				fixture.Secret.Required = true
				fixture.Signature = SignatureManifest{Type: signatureTypeHMACSHA256, Header: "X-Signature", SignedPayload: "raw_body"}
				manifest := fixture.mustAdmit()
				var payload any
				decoder := json.NewDecoder(bytes.NewBufferString(body))
				decoder.UseNumber()
				if err := decoder.Decode(&payload); err != nil {
					t.Fatal(err)
				}
				mac := hmac.New(sha256.New, []byte("object-gate-proof"))
				_, _ = mac.Write([]byte(body))
				request := Request{Target: Target{WebhookSecret: "object-gate-proof"}, Body: []byte(body), Payload: payload,
					Headers: http.Header{"X-Signature": {hex.EncodeToString(mac.Sum(nil))}}}
				_, authenticatedErr := manifest.Accept(request)
				_, projectedErr := manifest.ProjectNormalizedPayload([]byte(body))
				if authenticatedErr == nil || projectedErr == nil || authenticatedErr.Error() != projectedErr.Error() {
					t.Fatalf("object gate drift: authenticated=%v pure=%v", authenticatedErr, projectedErr)
				}
				request.Headers.Set("X-Signature", "invalid")
				if _, err := manifest.Accept(request); err == nil || err.Error() == projectedErr.Error() {
					t.Fatalf("payload gate bypassed signature refusal: %v", err)
				}
			})
		}
	}
}
