package deliverycontinuation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestChannelShutdownProbeNetCanceledDialIsOwnedCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1:1")
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected real owned canceled dial: %T %v", err, err)
	}
	wrapped := fmt.Errorf("dispatch delivery continuation: load runtime ingress state: %w", err)
	if !ordinaryCoordinatorStop(ctx, wrapped, true) {
		t.Fatalf("base coordinator rejects owned canceled dial: type=%T err=%v errors.Is(context.Canceled)=%t", err, wrapped, errors.Is(wrapped, context.Canceled))
	}
}
