package runtimepersistence

import (
	"context"

	storerunfork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type ConversationForkChatExecution = storerunfork.ConversationForkChatExecution
type ConversationForkChatFailureRequest = storerunfork.ConversationForkChatFailureRequest
type ConversationForkChatPrepareRequest = storerunfork.ConversationForkChatPrepareRequest
type ConversationForkChatPrepared = storerunfork.ConversationForkChatPrepared
type ConversationForkChatRecordRequest = storerunfork.ConversationForkChatRecordRequest
type ConversationForkChatReplayStateError = storerunfork.ConversationForkChatReplayStateError
type ConversationForkChatResult = storerunfork.ConversationForkChatResult
type ConversationForkCreateRequest = storerunfork.ConversationForkCreateRequest
type ConversationForkDeleteResult = storerunfork.ConversationForkDeleteResult
type ConversationForkEntitySnapshot = storerunfork.ConversationForkEntitySnapshot
type ConversationForkListOptions = storerunfork.ConversationForkListOptions
type ConversationForkListResult = storerunfork.ConversationForkListResult
type ConversationForkPointDescriptor = storerunfork.ConversationForkPointDescriptor
type ConversationForkPointSelector = storerunfork.ConversationForkPointSelector
type ConversationForkSandboxPolicy = storerunfork.ConversationForkSandboxPolicy
type ConversationForkSnapshot = storerunfork.ConversationForkSnapshot
type ConversationForkSourceTurn = storerunfork.ConversationForkSourceTurn
type OperatorConversationForkSession = storerunfork.OperatorConversationForkSession
type activeRunSourceOwnerFunc = storerunfork.ActiveRunSourceOwnerFunc

const ConversationForkChatSnapshotOwner = storerunfork.ConversationForkChatSnapshotOwner
const ConversationForkChatSandboxOwner = storerunfork.ConversationForkChatSandboxOwner
const ConversationForkLifecycleTTL = storerunfork.ConversationForkLifecycleTTL

var CanonicalConversationForkSandboxPolicy = storerunfork.CanonicalConversationForkSandboxPolicy

func (s *PostgresStore) ListOperatorConversationTurns(ctx context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	return s.runForkPostgresOwner.ListOperatorConversationTurns(ctx, opts)
}

func (s *SQLiteRuntimeStore) ListOperatorConversationTurns(ctx context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	return s.runForkSQLiteOwner.ListOperatorConversationTurns(ctx, opts)
}

func (s *PostgresStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	return s.runForkPostgresOwner.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}

func (s *SQLiteRuntimeStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	return s.runForkSQLiteOwner.LoadOperatorPublicConversationTurn(ctx, sessionID, turnID)
}
