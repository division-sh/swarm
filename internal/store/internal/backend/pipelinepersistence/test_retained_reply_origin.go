package pipelinepersistence

import (
	"context"
	"encoding/json"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	replycontextstore "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
)

func (s *PipelinePostgresOwner) CorruptRetainedReplyOriginForTest(ctx context.Context, runID, contextID string, origin json.RawMessage) error {
	return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, replycontextstore.CorruptRetainedReplyOriginTxForTest(ctx, attempt, true, runID, contextID, origin)
	}).Err()
}

func (s *PipelineSQLiteOwner) CorruptRetainedReplyOriginForTest(ctx context.Context, runID, contextID string, origin json.RawMessage) error {
	return mutationprotocol.RunSQLite(ctx, s.backend, "canonical event fixture", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, replycontextstore.CorruptRetainedReplyOriginTxForTest(ctx, attempt, false, runID, contextID, origin)
	}).Err()
}
