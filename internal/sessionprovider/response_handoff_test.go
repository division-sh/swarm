//go:build linux || darwin

package sessionprovider

import (
	"context"
	"sync"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
)

type sdkResponseHandoffContext struct {
	context.Context
	selected chan struct{}
	resume   chan struct{}
	once     sync.Once
}

func (ctx *sdkResponseHandoffContext) Done() <-chan struct{} {
	close(ctx.selected)
	<-ctx.resume
	return ctx.Context.Done()
}

func (ctx *sdkResponseHandoffContext) release() {
	ctx.once.Do(func() { close(ctx.resume) })
}

func TestWhatsAppSDKResponseCancelAfterReceiverSelectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			connection := openRuntimeConnectionFixture(t, f)
			sdk := connection.state.currentOccurrence().client.DangerousInternals()
			waiter := sdk.WaitResponse("owned-request")
			node := &waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": "owned-request", "type": "result"}}
			ctx := &sdkResponseHandoffContext{
				Context: f.ctx, selected: make(chan struct{}), resume: make(chan struct{}),
			}
			defer ctx.release()
			result := make(chan any, 1)
			// Pause select-operand evaluation after the SDK reader owns the waiter.
			go func() {
				defer func() { result <- recover() }()
				if !sdk.ReceiveResponse(ctx, node) {
					panic("registered response was not selected")
				}
			}()
			select {
			case <-ctx.selected:
			case <-time.After(time.Second):
				t.Fatal("SDK response did not reach its handoff boundary")
			}
			sdk.CancelResponse("owned-request", waiter)
			ctx.release()
			select {
			case failure := <-result:
				if failure != nil {
					t.Fatalf("SDK response handoff panicked: %v", failure)
				}
			case <-time.After(time.Second):
				t.Fatal("SDK response handoff did not finish")
			}
			select {
			case actual, open := <-waiter:
				if !open || actual != node {
					t.Fatal("late cancellation lost the receiver-owned response")
				}
			case <-time.After(time.Second):
				t.Fatal("receiver-owned response was not delivered")
			}
		})
	}
}
