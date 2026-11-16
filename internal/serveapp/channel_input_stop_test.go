package serveapp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestChannelActionWorkerSealsOrdinaryInputsBeforeStoredIntake(t *testing.T) {
	for _, kind := range []string{"text", "action"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			manager := &runtime.RuntimeContextManager{}
			entered, release := make(chan struct{}), make(chan struct{})
			finish := sync.OnceFunc(func() { close(release) })
			var reads atomic.Int32
			read := func(ctx context.Context) error {
				if reads.Add(1) != 1 {
					return errors.New("fresh input selection reached the store after sealing")
				}
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			store := &channelWorkerControlStore{actions: read}
			dispatcher := channelWorkerControlDispatcher(store)
			dispatcher.manager = manager
			selectInput := dispatcher.reconcileCardActions
			if kind == "text" {
				store.texts = func() error { return read(ctx) }
				selectInput = dispatcher.reconcileNativeInboxEntries
			}
			active := make(chan error, 1)
			go func() { active <- selectInput(ctx) }()
			sealed := make(chan error, 1)
			defer func() {
				finish()
				if err := <-active; err != nil {
					t.Errorf("selected input failed: %v", err)
				}
				if err := <-sealed; err != nil {
					t.Errorf("input selection join failed: %v", err)
				}
				if !t.Failed() {
					if err := manager.OpenStoredInbound(); err != nil {
						t.Errorf("joined input did not permit stored intake: %v", err)
					}
				}
			}()
			<-entered
			joinCtx := &observedTerminalJoinContext{Context: ctx, entered: make(chan struct{})}
			go func() { sealed <- manager.SealInboundExecution(joinCtx) }()
			select {
			case <-joinCtx.entered:
			case err := <-sealed:
				sealed <- err
				t.Fatal("stored intake became ready before selected input joined")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := manager.OpenStoredInbound(); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Fatalf("unjoined input opened stored intake: %v", err)
			}
			if err := selectInput(ctx); err != nil {
				t.Fatal(err)
			}
			if reads.Load() != 1 {
				t.Fatal("fresh pending input was processed during the stop")
			}
		})
	}
}
