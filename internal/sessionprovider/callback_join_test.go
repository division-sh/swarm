package sessionprovider

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWhatsAppCallbackCancellationAfterCaptureReturnsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	guard, err := newCallbackGuard(ctx, uuid.NewString(), uuid.NewString(),
		func(context.Context, any) error { cancel(); return nil },
		func(context.Context, callbackFailure) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if guard.receive("committed capture from retired occurrence") {
		t.Fatal("capture completion after cancellation granted successful SDK acknowledgment")
	}
}

func TestWhatsAppCallbackJoinIncludesCaptureAndFailureEvidence(t *testing.T) {
	for _, boundary := range []string{"capture", "failure_evidence"} {
		t.Run(boundary, func(t *testing.T) {
			entered, finish := make(chan struct{}), make(chan struct{})
			var finishOnce sync.Once
			defer finishOnce.Do(func() { close(finish) })
			guard, err := newCallbackGuard(context.Background(), uuid.NewString(), uuid.NewString(),
				func(context.Context, any) error {
					if boundary == "failure_evidence" {
						return errors.New("capture commit refused")
					}
					close(entered)
					<-finish
					return nil
				}, func(context.Context, callbackFailure) error {
					close(entered)
					<-finish
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan bool, 1)
			go func() { done <- guard.receive("owned capture") }()
			<-entered
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := guard.join(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("unfinished callback joined: %v", err)
			}
			if guard.receive("unowned late capture") {
				t.Fatal("fenced callback accepted more work")
			}
			select {
			case <-guard.drained:
				t.Fatal("unfinished capture or failure evidence released its join")
			default:
			}
			finishOnce.Do(func() { close(finish) })
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if err := guard.join(ctx); err != nil {
				t.Fatal(err)
			}
			if <-done {
				t.Fatal("retired callback granted successful acknowledgment")
			}
		})
	}
}
