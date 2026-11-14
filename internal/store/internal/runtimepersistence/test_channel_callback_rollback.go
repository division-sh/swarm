package runtimepersistence

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	storeoperatorchannel "github.com/division-sh/swarm/internal/store/internal/backend/operatorchannel"
)

type ChannelCallbackRollbackEvidence = storeoperatorchannel.CallbackRollbackEvidence

type ChannelCallbackRollbackFault interface {
	ReadEvidence(context.Context) (ChannelCallbackRollbackEvidence, error)
	CardState(context.Context, string) (string, error)
	Close() error
}

type channelCallbackRollbackFault struct {
	owner *storeoperatorchannel.SQLiteOwner
	event string
	ctx   context.Context
}

// Only this native SQLite fixture owns the fixed write fault; an independent
// read-only inspection can never install it or receive its connection.
func OpenChannelCallbackRollbackFaultForTest(ctx context.Context, selected any, event string) (ChannelCallbackRollbackFault, error) {
	decoded, err := hex.DecodeString(event)
	owner, ok := selected.(*SQLiteRuntimeStore)
	if !ok || owner == nil || owner.backend == nil || !owner.backend.Valid() || err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != event {
		return nil, fmt.Errorf("callback rollback fault requires its original SQLite owner and exact provider event")
	}
	fault := &channelCallbackRollbackFault{owner: owner.operatorChannelSQLiteOwner, event: event, ctx: ctx}
	if err := fault.set(true); err != nil {
		return nil, err
	}
	return fault, nil
}

func (f *channelCallbackRollbackFault) set(enabled bool) error {
	return f.owner.SetCallbackRollbackFixtureFault(f.ctx, f.event, enabled)
}

func (f *channelCallbackRollbackFault) ReadEvidence(ctx context.Context) (ChannelCallbackRollbackEvidence, error) {
	if f.owner == nil {
		return ChannelCallbackRollbackEvidence{}, errors.New("callback rollback fault is closed")
	}
	return f.owner.ReadCallbackRollbackFixtureEvidence(ctx, f.event)
}

func (f *channelCallbackRollbackFault) CardState(ctx context.Context, token string) (string, error) {
	if f.owner == nil {
		return "", errors.New("callback rollback fault is closed")
	}
	return f.owner.ReadCallbackRollbackFixtureCardState(ctx, token)
}

func (f *channelCallbackRollbackFault) Close() error {
	if f.owner == nil {
		return errors.New("callback rollback fault is closed")
	}
	err := f.set(false)
	f.owner = nil
	return err
}
