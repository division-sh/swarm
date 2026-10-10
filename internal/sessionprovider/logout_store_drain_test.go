package sessionprovider

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWhatsAppLogoutStoreQuiescenceKeepsOnlyCurrentDeletionWork(t *testing.T) {
	fence := newSDKStoreFence()
	parent, release, err := fence.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { done <- fence.quiesceForLogout(context.Background()) }()
	waitLogoutStoreQuiescence(t, fence)
	if _, finish, err := fence.acquire(context.Background()); err == nil {
		finish()
		t.Fatal("quiescing SDK store admitted fresh ordinary work")
	}
	if _, finish, err := fence.acquireCurrent(context.Background()); err == nil {
		finish()
		t.Fatal("SDK deletion work entered before the old transaction drained")
	}
	_, finish, err := fence.acquire(parent)
	if err != nil {
		t.Fatal("quiescence interrupted an admitted transaction", err)
	}
	finish()
	release()
	if err := awaitLogoutStoreQuiescence(t, done); err != nil {
		t.Fatal(err)
	}
	if _, finish, err := fence.acquire(parent); err == nil {
		finish()
		t.Fatal("ended transaction context granted fresh work")
	}
	_, finish, err = fence.acquireCurrent(context.Background())
	if err != nil {
		t.Fatal("quiescence prematurely retired current explicit deletion work", err)
	}
	finish()
	if err := fence.join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, finish, err := fence.acquireCurrent(context.Background()); err == nil {
		finish()
		t.Fatal("ordinary retirement retained deletion work")
	}
}

func TestWhatsAppLogoutStoreQuiescenceCancellationAndRetirement(t *testing.T) {
	for _, boundary := range []string{"cancellation", "retirement"} {
		t.Run(boundary, func(t *testing.T) {
			fence := newSDKStoreFence()
			_, release, err := fence.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- fence.quiesceForLogout(ctx) }()
			waitLogoutStoreQuiescence(t, fence)
			want := errSDKStoreFenced
			if boundary == "cancellation" {
				cancel()
				want = context.Canceled
			} else {
				fence.fence()
			}
			release()
			if err := awaitLogoutStoreQuiescence(t, done); !errors.Is(err, want) {
				t.Fatal("SDK quiescence lost its precise refusal", boundary, err)
			}
			if err := fence.join(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func waitLogoutStoreQuiescence(t *testing.T, fence *sdkStoreFence) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		fence.mu.Lock()
		quiescing := fence.quiescing
		fence.mu.Unlock()
		if quiescing {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("SDK private store did not fence fresh ordinary work")
		case <-time.After(time.Millisecond):
		}
	}
}

func awaitLogoutStoreQuiescence(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("SDK private-store drain did not finish")
		return nil
	}
}
