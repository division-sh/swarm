package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func TestSessionAuthorityBeginTxRetainsNativeCancellationWithoutSQLAutoRollback(t *testing.T) {
	authority := newSessionAuthority(&sql.Conn{})
	beginEntered := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	authority.mu.Lock()
	authority.testBeginTx = func(beginCtx context.Context, _ *sql.Conn) (*sql.Tx, error) {
		if beginCtx.Done() != nil {
			return nil, errors.New("retained begin must not authorize database/sql automatic rollback")
		}
		close(beginEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	authority.mu.Unlock()

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
	endOperation, err := authority.beginOperation()
	if err != nil {
		t.Fatalf("operation serialization remained held after canceled transaction start: %v", err)
	}
	endOperation()
}
