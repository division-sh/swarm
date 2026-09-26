package runforkexecution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSelectedGatewayStandingWorkDoesNotMaskReceiverQuiescence(t *testing.T) {
	owner := testGatewayWorkOwner(t)
	gateway, err := owner.BeginStanding(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gateway.Done() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("standing gateway blocked idle runtime: %v", err)
	}
	receiver, err := owner.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = receiver.Done() }()
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := owner.WaitForQuiescence(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unfinished receiver was not counted: %v", err)
	}
	if err := receiver.Done(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.WaitForQuiescence(ctx); err != nil {
		t.Fatalf("settled receiver did not drain while gateway remained live: %v", err)
	}
}
