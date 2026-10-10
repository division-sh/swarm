//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
)

type sdkResponseHandoffContext struct {
	context.Context
	selected chan struct{}
	resume   chan struct{}
	once     sync.Once
}

func TestWhatsAppSDKIQResultErrorAndCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			connection := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, connection)
			sdk := connection.state.currentOccurrence().client.DangerousInternals()
			for _, outcome := range []string{"result", "error", "canceled"} {
				t.Run(outcome, func(t *testing.T) {
					ctx, cancel := context.WithCancel(f.ctx)
					defer cancel()
					completed := make(chan error, 1)
					go func() {
						_, err := sdk.SendIQ(ctx, whatsmeow.DangerousInfoQuery{ID: "owned-" + outcome,
							Namespace: "md", Type: "get", NoRetry: true})
						completed <- err
					}()
					frame := f.peer.next(t)
					if frame.node.Attrs["id"] != "owned-"+outcome {
						t.Fatal("IQ peer received a foreign request", frame.node)
					}
					switch outcome {
					case "result":
						f.peer.acknowledge(t, frame)
					case "error":
						if err := frame.peer.send(f.ctx, waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{
							"id": frame.node.Attrs["id"], "type": "error",
						}, Content: []waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"code": "403", "text": "forbidden"}}}}); err != nil {
							t.Fatal(err)
						}
					case "canceled":
						cancel()
					}
					err := awaitOccurrenceProbe(t, f.ctx, completed)
					if outcome == "result" && err != nil || outcome == "error" && !errors.Is(err, whatsmeow.ErrIQForbidden) ||
						outcome == "canceled" && !errors.Is(err, context.Canceled) {
						t.Fatalf("IQ %s lost its precise outcome: %v", outcome, err)
					}
					if outcome == "canceled" {
						// A late genuine result may settle its waiter, not resurrect the canceled caller.
						f.peer.acknowledge(t, frame)
					}
				})
			}
			if err := connection.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case frame := <-f.peer.frames:
				t.Fatal("IQ completion or close replayed a request", frame.node)
			default:
			}
		})
	}
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
