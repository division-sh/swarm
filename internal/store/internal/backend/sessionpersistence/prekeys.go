package sessionpersistence

import (
	"context"
	"errors"
	"sync/atomic"

	"go.mau.fi/whatsmeow/util/keys"
)

var errSDKTransactionScope = errors.New("private SDK transaction scope is absent, foreign or expired")

type sdkTransactionKey struct{}

type sdkTransactionScope struct {
	owner  *Owner
	active atomic.Bool
}

// Acquire before the SDK transaction and prekey mutex on every generation
// path. The same private owner spans freshly loaded handles for its one account.
func (s *sdkStorage) sdkTransaction(ctx context.Context, fn func(context.Context) error) error {
	if ctx == nil || s.owner == nil || s.owner.sdkTxn == nil {
		return errSDKTransactionScope
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if parent, _ := ctx.Value(sdkTransactionKey{}).(*sdkTransactionScope); parent != nil {
		if parent.owner != s.owner || !parent.active.Load() {
			return errSDKTransactionScope
		}
		return fn(ctx)
	}
	select {
	case s.owner.sdkTxn <- struct{}{}:
		defer func() { <-s.owner.sdkTxn }()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return s.session.DoDecryptionTxn(ctx, func(txctx context.Context) error {
		scope := &sdkTransactionScope{owner: s.owner}
		scope.active.Store(true)
		defer scope.active.Store(false)
		return fn(context.WithValue(txctx, sdkTransactionKey{}, scope))
	})
}

func (s *sdkStorage) DoDecryptionTxn(ctx context.Context, fn func(context.Context) error) error {
	return s.sdkTransaction(ctx, fn)
}

func (s *sdkStorage) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	var prepared []*keys.PreKey
	err := s.sdkTransaction(ctx, func(txctx context.Context) error {
		var err error
		prepared, err = s.session.GetOrGenPreKeys(txctx, count)
		return err
	})
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

func (s *sdkStorage) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	var prepared *keys.PreKey
	err := s.sdkTransaction(ctx, func(txctx context.Context) error {
		var err error
		prepared, err = s.session.GenOnePreKey(txctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return prepared, nil
}
