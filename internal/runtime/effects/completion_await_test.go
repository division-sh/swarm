package effects

import (
	"context"
	"encoding/json"
	"testing"
)

// The private carrier scopes data to one event; it does not manufacture native authority.
func TestCompletionAwaitPublicationCarrierIsExactAndCopyIsolated(t *testing.T) {
	value := CompletionAwaitPublication{Attempt: Attempt{OperationID: "original", AttemptID: "attempt"}, CompletedResults: json.RawMessage(`[]`)}
	ctx := WithCompletionAwaitPublication(context.Background(), "request", value)
	value.CompletedResults[0] = 'x'
	first, found := CompletionAwaitPublicationForEvent(ctx, "request")
	if !found || first.Attempt.OperationID != "original" || string(first.CompletedResults) != "[]" || first.Attempt.Authority.Valid() {
		t.Fatalf("carrier aliased inputs or minted authority: value=%+v found=%t", first, found)
	}
	first.CompletedResults[0] = 'y'
	second, found := CompletionAwaitPublicationForEvent(ctx, "request")
	if !found || string(second.CompletedResults) != "[]" {
		t.Fatal("carrier returned mutable backing storage")
	}
	for _, event := range []string{"", "other-request"} {
		if _, found := CompletionAwaitPublicationForEvent(ctx, event); found {
			t.Fatal("unrelated publication inherited await metadata")
		}
	}
	if _, found := CompletionAwaitPublicationForEvent(context.Background(), "request"); found {
		t.Fatal("ordinary publication acquired await metadata")
	}
}
