package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/operatorread"
)

func (s *PostgresStore) ListOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	return s.operatorConversationPostgres.ListOperatorConversationTurns(ctx, opts)
}

func (s *SQLiteRuntimeStore) ListOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	return s.operatorConversationSQLite.ListOperatorConversationTurns(ctx, opts)
}

func (s *PostgresStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	return s.operatorConversationPostgres.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}

func (s *SQLiteRuntimeStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	return s.operatorConversationSQLite.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}
