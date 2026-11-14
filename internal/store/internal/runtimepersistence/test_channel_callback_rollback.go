package runtimepersistence

import (
	"context"
	"encoding/hex"
	"fmt"

	storeoperatorchannel "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

type ChannelCallbackRollbackEvidence = storeoperatorchannel.CallbackRollbackEvidence

type ChannelCallbackRollbackFault interface {
	ReadEvidence(context.Context) (ChannelCallbackRollbackEvidence, error)
	CardState(context.Context, string) (string, error)
	Close() error
}

// Only this native SQLite fixture owns the fixed write fault; an independent
// read-only inspection can never install it or receive its connection.
func OpenChannelCallbackRollbackFaultForTest(ctx context.Context, selected any, event string) (ChannelCallbackRollbackFault, error) {
	decoded, err := hex.DecodeString(event)
	owner, ok := selected.(*SQLiteRuntimeStore)
	if !ok || owner == nil || owner.backend == nil || !owner.backend.Valid() || err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != event {
		return nil, fmt.Errorf("callback rollback fault requires its original SQLite owner and exact provider event")
	}
	return storeoperatorchannel.OpenCallbackRollbackFaultForTest(ctx, owner.operatorChannelSQLiteOwner, event)
}
