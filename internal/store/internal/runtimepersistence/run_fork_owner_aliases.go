package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/operatorread"
)

func (s *PostgresStore) ListOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	return s.runForkPostgresOwner.ListOperatorConversationTurns(ctx, opts)
}

func (s *SQLiteRuntimeStore) ListOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	return s.runForkSQLiteOwner.ListOperatorConversationTurns(ctx, opts)
}

func (s *PostgresStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	return s.runForkPostgresOwner.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}

func (s *SQLiteRuntimeStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	return s.runForkSQLiteOwner.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}
