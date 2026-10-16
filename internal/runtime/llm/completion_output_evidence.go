package llm

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
)

// CompletionOutputCandidate describes a proposed tool output, not an accepted
// publication. The read owner must join EventID to committed publication facts.
type CompletionOutputCandidate struct {
	EventID   string
	ToolName  string
	CreatedAt time.Time
}

// ReadCompletionOutputCandidates shares the executor's complete call coordinate
// and output identity owner without returning execution or recovery authority.
func ReadCompletionOutputCandidates(payload json.RawMessage, adapter, operationID string, surface managedcapabilities.Surface) ([]CompletionOutputCandidate, error) {
	if err := surface.Validate(); err != nil {
		return nil, err
	}
	continuation, err := completionContinuationForPayload(payload, adapter)
	if err != nil {
		return nil, err
	}
	authority := continuation.Response.ToolOutputAuthority
	if authority.ProviderOperationID != operationID || !surface.MatchesActor(continuation.Projection.Identity) {
		return nil, fmt.Errorf("completion output evidence contradicts its operation or concrete actor")
	}
	out := make([]CompletionOutputCandidate, 0)
	for i, call := range continuation.Response.ToolCalls {
		capability, ok := surface.Capability(call.Name)
		if !ok || !capability.Callable {
			// A provider may propose a denied tool; the executor returns a tool
			// error. Such a proposal is not an authorized output candidate.
			continue
		}
		if capability.Kind != toolcapabilities.KindEmit {
			continue
		}
		identity, err := authority.eventIdentity(completionToolCallIdentity(continuation.Projection.TurnCount, i, call))
		if err != nil {
			return nil, err
		}
		out = append(out, CompletionOutputCandidate{EventID: identity.EventID(), ToolName: call.Name, CreatedAt: identity.CreatedAt()})
	}
	return out, nil
}
