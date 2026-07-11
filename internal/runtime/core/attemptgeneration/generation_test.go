package attemptgeneration

import "testing"

func TestGenerationPayloadRoundTripAndIdentity(t *testing.T) {
	want := Generation{FlowID: "flow", LoopID: "revision", ActivationID: "activation", RevisionField: "revision_id", RevisionID: "rev-2", Attempt: 2}
	payload := map[string]any{PayloadKey: want.PayloadValue()}
	got, ok := FromPayload(payload)
	if !ok || !got.Equal(want) || got.KeySuffix() == "" {
		t.Fatalf("round trip = %#v ok=%v", got, ok)
	}
}
