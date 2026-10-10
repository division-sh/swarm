package sessionprovider

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/util/keys"
)

func TestSDKPreKeyPreparationRollsBackFailedBatch(t *testing.T) {
	ctx := context.Background()
	fixture, owner := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, owner)
	before, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil || len(before) == 0 {
		t.Fatal("original SDK key inventory", before, err)
	}
	if err := fixture.SetSDKPreKeyInsertFailure(ctx, before[len(before)-1].ID+5); err != nil {
		t.Fatal(err)
	}
	keys, err := device.PreKeys.GetOrGenPreKeys(ctx, 812)
	if err == nil || len(keys) != 0 {
		t.Fatal("failed preparation returned usable keys", len(keys), err)
	}
	after, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("failed preparation durably exposed a partial key batch", len(before), len(after), err)
	}
}

func TestSDKPreKeyPreparationExactInventoryAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "provider.db")
	fixture, owner := openSDKStoreFixture(t, path)
	device := newSDKDeviceFixture(t, owner)
	prepared, err := device.PreKeys.GetOrGenPreKeys(ctx, 812)
	if err != nil || len(prepared) != 812 {
		t.Fatal("genuine SDK batch", len(prepared), err)
	}
	before, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range prepared {
		stored, err := device.PreKeys.GetPreKey(ctx, key.KeyID)
		if err != nil || stored == nil || !reflect.DeepEqual(key, stored) {
			t.Fatal("returned SDK key was not committed in full", key.KeyID, err)
		}
	}
	if err := fixture.Close(); err != nil {
		t.Fatal(err)
	}
	second, reopened := openSDKStoreFixture(t, path)
	after, err := second.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("FULL key inventory changed after reopen", len(before), len(after), err)
	}
	retained, err := reopened.GetDevice(ctx, *device.ID)
	if err != nil || retained == nil {
		t.Fatal(err)
	}
	again, err := retained.PreKeys.GetOrGenPreKeys(ctx, 812)
	if err != nil || !reflect.DeepEqual(prepared, again) {
		t.Fatal("reopen generated replacements for the original SDK keys", len(again), err)
	}
}

func TestSDKPreKeyPreparationSerializesLoadedHandlesAndSingleGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture, owner := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, owner)
	before, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	failures := make(chan error, 16)
	for index := range 16 {
		loaded, err := owner.GetDevice(ctx, *device.ID)
		if err != nil {
			t.Fatal(err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			if index%2 == 0 {
				_, err := loaded.PreKeys.GetOrGenPreKeys(ctx, 812)
				failures <- err
			} else {
				_, err := loaded.PreKeys.GenOnePreKey(ctx)
				failures <- err
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal("shared private owner key generation conflicted", err)
		}
	}
	after, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil || len(after) != len(before)+812+8 {
		t.Fatal("concurrent SDK key inventory", len(before), len(after), err)
	}
	for index, key := range after {
		if key.ID != uint32(index+1) {
			t.Fatal("key generation reused or skipped an original identifier", key.ID)
		}
	}
}

func TestSDKPreKeyPreparationCancellationAndNestedRollback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fixture, owner := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, owner)
	before, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	err = device.EventBuffer.DoDecryptionTxn(ctx, func(txctx context.Context) error {
		if _, err := device.PreKeys.GetOrGenPreKeys(txctx, 8); err != nil {
			return err
		}
		if _, err := device.PreKeys.GenOnePreKey(txctx); err != nil {
			return err
		}
		cancel()
		return context.Cause(ctx)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("interrupted SDK transaction lost cancellation", err)
	}
	after, err := fixture.SDKPreKeyInventory(context.Background(), device.ID.String())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("cancellation committed a nested key preparation", len(before), len(after), err)
	}
	if generated, err := device.PreKeys.GetOrGenPreKeys(ctx, 8); !errors.Is(err, context.Canceled) || generated != nil {
		t.Fatal("canceled standalone preparation returned keys", len(generated), err)
	}
}

func TestSDKPreKeyPreparationLockOrderingCommitBoundaryAndClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture, owner := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, owner)
	guard, err := guardSDKStores(device, owner.LIDMap())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	done := make(chan error, 1)
	var prepared []*keys.PreKey
	go func() {
		done <- device.EventBuffer.DoDecryptionTxn(ctx, func(txctx context.Context) error {
			close(entered)
			<-release
			var err error
			prepared, err = device.PreKeys.GetOrGenPreKeys(txctx, 8)
			return err
		})
	}()
	<-entered
	waitCtx, cancelWait := context.WithCancel(ctx)
	blocked := make(chan error, 1)
	go func() { _, err := device.PreKeys.GenOnePreKey(waitCtx); blocked <- err }()
	for {
		guard.fence.mu.Lock()
		inFlight := guard.fence.inFlight
		guard.fence.mu.Unlock()
		if inFlight == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("generation did not reach the held transaction", context.Cause(ctx))
		case <-time.After(time.Millisecond):
		}
	}
	cancelWait()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatal("queued key preparation ignored cancellation", err)
	}
	short, cancelShort := context.WithCancel(ctx)
	cancelShort()
	if err := guard.fence.join(short); !errors.Is(err, context.Canceled) {
		t.Fatal("close abandoned the original unfinished transaction", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal("nested generation inverted transaction/key lock order", err)
	}
	if err := guard.fence.join(ctx); err != nil {
		t.Fatal(err)
	}
	inventory, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range prepared {
		found := false
		for _, stored := range inventory {
			found = found || stored.ID == key.KeyID && stored.Digest == sha256.Sum256(key.Priv[:]) && !stored.Uploaded
		}
		if !found {
			t.Fatal("joined generation returned an uncommitted or altered key", key.KeyID)
		}
	}
	if keys, err := device.PreKeys.GetOrGenPreKeys(ctx, 8); !errors.Is(err, errSDKStoreFenced) || keys != nil {
		t.Fatal("late generation escaped the complete store join", len(keys), err)
	}
}

func TestSDKPreKeyPreparationReturnsNoKeysAfterCommitFailure(t *testing.T) {
	ctx := context.Background()
	fixture, owner := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, owner)
	before, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.SetSDKPreKeyCommitFailure(ctx, true); err != nil {
		t.Fatal(err)
	}
	keys, err := device.PreKeys.GetOrGenPreKeys(ctx, 8)
	if err == nil || len(keys) != 0 {
		t.Fatal("failed commit returned usable keys", len(keys), err)
	}
	after, err := fixture.SDKPreKeyInventory(ctx, device.ID.String())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("failed commit exposed an unfinished key batch", len(before), len(after), err)
	}
}
