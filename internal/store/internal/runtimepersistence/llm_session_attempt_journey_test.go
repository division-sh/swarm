package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"

	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type llmSessionAttemptJourneyOwner interface {
	AcquireLiveSession(context.Context, agentmemory.Identity, string) (*sessions.Lease, runtimellm.ConversationRecord, error)
	Rotate(context.Context, *sessions.Lease, sessions.RotationMetadata) (*sessions.Lease, error)
	ReleaseOutcome(context.Context, *sessions.Lease) (sessions.ReleaseResult, error)
}
