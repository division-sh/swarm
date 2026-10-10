package sessionpersistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

type prekeyCommitBarrier struct {
	store.AllSessionSpecificStores
	entered chan struct{}
	release chan struct{}
}

func (b prekeyCommitBarrier) DoDecryptionTxn(ctx context.Context, fn func(context.Context) error) error {
	return b.AllSessionSpecificStores.DoDecryptionTxn(ctx, func(txctx context.Context) error {
		if err := fn(txctx); err != nil {
			return err
		}
		close(b.entered)
		select {
		case <-b.release:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
}

func TestSDKPreKeyPreparationDoesNotReturnBeforeActualCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "provider.db")
	owner, err := OpenSDKFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	device := owner.NewDevice()
	account := types.NewJID("15551234567", types.DefaultUserServer)
	device.ID = &account
	device.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{1}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	if err := device.Save(ctx); err != nil {
		t.Fatal(err)
	}
	observer, err := OpenSDKFixture(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	storage := device.PreKeys.(*sdkStorage)
	barrier := prekeyCommitBarrier{AllSessionSpecificStores: storage.session, entered: make(chan struct{}), release: make(chan struct{})}
	storage.session = barrier
	t.Cleanup(func() {
		select {
		case <-barrier.release:
		default:
			close(barrier.release)
		}
	})
	type outcome struct {
		keys []*keys.PreKey
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		prepared, err := device.PreKeys.GetOrGenPreKeys(ctx, 812)
		done <- outcome{prepared, err}
	}()
	select {
	case <-barrier.entered:
	case <-ctx.Done():
		t.Fatal("genuine preparation did not reach its pre-commit boundary", context.Cause(ctx))
	}
	select {
	case result := <-done:
		t.Fatal("preparation returned before SQLite committed", len(result.keys), result.err)
	default:
	}
	before, err := observer.SDKPreKeyInventory(ctx, account.String())
	if err != nil || len(before) != 0 {
		t.Fatal("independent read observed unfinished keys", len(before), err)
	}
	close(barrier.release)
	result := <-done
	if result.err != nil || len(result.keys) != 812 {
		t.Fatal("committed preparation did not return exact keys", len(result.keys), result.err)
	}
	after, err := observer.SDKPreKeyInventory(ctx, account.String())
	if err != nil || len(after) != 812 {
		t.Fatal("preparation returned before durable inventory became visible", len(after), err)
	}
}
