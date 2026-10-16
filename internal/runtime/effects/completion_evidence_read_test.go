package effects

import (
	"encoding/json"
	"testing"
)

func TestReadCompletionContinuationPayloadIsDetachedEvidence(t *testing.T) {
	request := []byte("exact request")
	payload := json.RawMessage(`{"response":{"content":"done"}}`)
	document := map[string]any{}
	if err := AttachCompletionContinuationEvidence(document, request, payload); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	read, err := ReadCompletionContinuationPayload(raw, Fingerprint(request))
	if err != nil || string(read) != string(payload) {
		t.Fatalf("immutable continuation read=%s error=%v", read, err)
	}
	read[0] = '['
	if again, err := ReadCompletionContinuationPayload(raw, Fingerprint(request)); err != nil || string(again) != string(payload) {
		t.Fatalf("read evidence was mutable: %s %v", again, err)
	}
	for _, bad := range []struct {
		evidence json.RawMessage
		request  string
	}{
		{raw, Fingerprint([]byte("different request"))},
		{json.RawMessage(`{}`), Fingerprint(request)},
		{json.RawMessage(`null`), Fingerprint(request)},
		{json.RawMessage(`{`), Fingerprint(request)},
	} {
		if read, err := ReadCompletionContinuationPayload(bad.evidence, bad.request); err == nil || read != nil {
			t.Fatalf("invalid immutable evidence admitted: %s %v", read, err)
		}
	}
}
