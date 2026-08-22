package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestSessionAuthorityBeginTxInstallsCancellationBeforeDetachedBegin(t *testing.T) {
	authority := newSessionAuthority(&sql.Conn{})
	beginEntered := make(chan struct{})
	backendCancelled := make(chan struct{})
	authority.mu.Lock()
	authority.cancelCurrentOperation = func() error {
		close(backendCancelled)
		return nil
	}
	authority.testBeginTx = func(beginCtx context.Context, _ *sql.Conn) (*sql.Tx, error) {
		if beginCtx.Done() == nil {
			return nil, errors.New("retained transaction begin context cannot cancel stalled startup")
		}
		close(beginEntered)
		<-beginCtx.Done()
		return nil, beginCtx.Err()
	}
	authority.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	beginDone := make(chan error, 1)
	go func() {
		_, err := authority.beginTx(ctx)
		beginDone <- err
	}()
	select {
	case <-beginEntered:
	case <-time.After(time.Second):
		t.Fatal("transaction start did not reach the backend")
	}
	cancel()
	select {
	case err := <-beginDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("beginTx error = %v, want caller cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not interrupt transaction start")
	}
	select {
	case <-backendCancelled:
	default:
		t.Fatal("caller cancellation did not invoke retained backend cancellation")
	}

	endOperation, err := authority.beginOperation()
	if err != nil {
		t.Fatalf("operation serialization remained held after canceled transaction start: %v", err)
	}
	endOperation()
}
