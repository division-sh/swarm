package providertriggers

import (
	"bytes"
	"encoding/json"
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
