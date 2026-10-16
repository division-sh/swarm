package effectpersistence

import (
	"context"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
)

func encodeProviderRequestJournal(ctx context.Context, authority runtimeeffects.Authority, request runtimeeffects.AuthorizeRequest) (any, error) {
	if request.ProviderRequestJournal == nil {
		return nil, nil
	}
	if err := runtimeeffects.ValidateProviderRequestJournalAdmission(ctx, authority, request); err != nil {
		return nil, err
	}
	journal := *request.ProviderRequestJournal
	if _, err := runtimellm.RebuildProviderRequestJournal(journal, *request.AgentFrame, *request.CapabilitySurface, request.RequestFingerprint); err != nil {
		return nil, err
	}
	raw, err := runtimeeffects.EncodeProviderRequestJournal(journal)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}
