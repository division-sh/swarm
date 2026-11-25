package effects

import (
	"context"
	"encoding/json"

	"github.com/division-sh/swarm/internal/runtime/forkcancelcapture"
)

// CompletionAwaitPublication is private native composition, not authored syntax
// or permission to end, recover, or resume an invocation.
type CompletionAwaitPublication struct {
	Attempt          Attempt
	CallIndex        int
	CompletedResults json.RawMessage
}

func (p CompletionAwaitPublication) Clone() CompletionAwaitPublication {
	p.CompletedResults = append(json.RawMessage(nil), p.CompletedResults...)
	return p
}

type completionAwaitPublicationKey struct{}
type completionAwaitPublicationContext struct {
	eventID     string
	publication CompletionAwaitPublication
}

// This is scoped publication input, not admission or a replacement work context.
// The native owners must still prove the original authority and exact emit.
func WithCompletionAwaitPublication(ctx context.Context, eventID string, publication CompletionAwaitPublication) context.Context {
	return forkcancelcapture.WithValue(ctx, completionAwaitPublicationKey{}, completionAwaitPublicationContext{eventID, publication.Clone()})
}

func CompletionAwaitPublicationForEvent(ctx context.Context, eventID string) (CompletionAwaitPublication, bool) {
	value, ok := ctx.Value(completionAwaitPublicationKey{}).(completionAwaitPublicationContext)
	if !ok || eventID == "" || value.eventID != eventID {
		return CompletionAwaitPublication{}, false
	}
	return value.publication.Clone(), true
}
