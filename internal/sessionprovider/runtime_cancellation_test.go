//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.mau.fi/whatsmeow"
)

func waitRuntimeRetirementFixture(t *testing.T, c *RuntimeConnection) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		c.state.mu.Lock()
		retiring := c.state.retiring
		c.state.mu.Unlock()
		if retiring {
			return
		}
		select {
		case <-deadline:
			t.Fatal("connection cleanup did not begin")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestWhatsAppRuntimeConcurrentCloseHonorsCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			admitted, err := c.AdmitSessionAccount(f.ctx, f.operation.SessionAccount)
			if err != nil {
				t.Fatal(err)
			}
			defer admitted.Close()
			f.workOwner.Retire()
			waitRuntimeRetirementFixture(t, c)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.Close(ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("waiting cleanup ignored its expired deadline: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("waiting cleanup blocked behind parent retirement")
				admitted.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("connection cleanup remained stuck")
				}
			}
		})
	}
}

type runtimeBlockedConnectTransport struct {
	entered chan context.Context
	exited  chan struct{}
}

func (r runtimeBlockedConnectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.entered <- req.Context()
	defer close(r.exited)
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestWhatsAppRuntimeConnectHonorsCallerCancellationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			entered, exited := make(chan context.Context, 1), make(chan struct{})
			client := &http.Client{Transport: runtimeBlockedConnectTransport{entered: entered, exited: exited}}
			occurrence := c.state.currentOccurrence()
			occurrence.client.SetPreLoginHTTPClient(client)
			occurrence.client.SetWebsocketHTTPClient(client)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.Connect(ctx) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("SDK connection request did not enter the transport")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("canceled connect returned %v", err)
				}
			case <-time.After(time.Second):
				t.Error("Connect ignored its caller cancellation")
				if err := c.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("SDK connection request remained stuck")
				}
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("connect returned with an abandoned dial request")
			}
		})
	}
}

func TestWhatsAppRuntimeCloseWaitersRetainOwnedCleanupBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, owner := range []string{"explicit", "parent_retirement"} {
			t.Run(backend+"/"+owner, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				c := openRuntimeConnectionFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				admitted, err := c.AdmitSessionAccount(f.ctx, f.operation.SessionAccount)
				if err != nil {
					t.Fatal(err)
				}
				defer admitted.Close()
				cleanup := make(chan error, 1)
				if owner == "explicit" {
					go func() { cleanup <- c.Close(context.Background()) }()
				} else {
					f.workOwner.Retire()
				}
				waitRuntimeRetirementFixture(t, c)
				// These are waiters behind a real held native admission, not an
				// artificial mutex. Every wait remains independently cancelable.
				type waiter struct {
					done   <-chan error
					cancel context.CancelFunc
					want   error
				}
				var waiters []waiter
				for _, canceled := range []bool{true, false, false} {
					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
					want := error(context.DeadlineExceeded)
					if canceled {
						cancel()
						want = context.Canceled
					}
					done := make(chan error, 1)
					go func() { done <- c.Close(ctx) }()
					waiters = append(waiters, waiter{done: done, cancel: cancel, want: want})
				}
				for _, waiter := range waiters {
					select {
					case err := <-waiter.done:
						if !errors.Is(err, waiter.want) {
							t.Error("waiter cancellation was not preserved", err)
						}
					case <-time.After(time.Second):
						waiter.cancel()
						admitted.Close()
						<-waiter.done
						t.Fatal("cleanup waiter ignored cancellation")
					}
					waiter.cancel()
				}
				c.state.mu.Lock()
				closed := c.state.closed
				c.state.mu.Unlock()
				if closed || f.workOwner.ActiveCount() != 2 {
					t.Fatal("canceled waiter dropped held admission, work or possession")
				}
				other, err := openSessionState(context.Background(), f.basePath, f.operation.SessionAccount.ConnectionID, f.operation.SessionAccount.AccountRef)
				if !errors.Is(err, errSessionPossession) {
					if other != nil {
						_ = other.close(context.Background())
					}
					t.Fatal("canceled waiter allowed premature successor possession", err)
				}
				admitted.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if owner == "explicit" {
					select {
					case err := <-cleanup:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal("explicit cleanup did not finish after release")
					}
				}
				for range 3 {
					if err := c.Close(ctx); err != nil {
						t.Fatal("complete cleanup replay", err)
					}
				}
				if f.workOwner.ActiveCount() != 0 {
					t.Fatal("complete cleanup did not release exactly its owned work")
				}
				other, err = openSessionState(ctx, f.basePath, f.operation.SessionAccount.ConnectionID, f.operation.SessionAccount.AccountRef)
				if err != nil {
					t.Fatal("joined cleanup did not release pairing possession", err)
				}
				if err := other.close(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestWhatsAppRuntimeConnectCancellationBoundariesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cell := range []string{"before_io", "parent_retirement", "successful_call_lifetime"} {
			t.Run(backend+"/"+cell, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				c := openRuntimeConnectionFixture(t, f)
				occurrence := c.state.currentOccurrence()
				ctx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				switch cell {
				case "before_io":
					cancel()
					if err := c.Connect(ctx); err == nil || occurrence.started {
						t.Fatal("canceled caller started SDK I/O", err)
					}
				case "parent_retirement":
					entered, exited := make(chan context.Context, 1), make(chan struct{})
					client := &http.Client{Transport: runtimeBlockedConnectTransport{entered: entered, exited: exited}}
					occurrence.client.SetWebsocketHTTPClient(client)
					done := make(chan error, 1)
					go func() { done <- c.Connect(ctx) }()
					select {
					case <-entered:
					case <-f.peer.ctx.Done():
						t.Fatal("pending SDK request did not start")
					}
					cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					if _, err := f.workOwner.RetireAndWait(cleanup); err != nil {
						t.Fatal("parent did not join pending SDK connection", err)
					}
					if err := <-done; err == nil {
						t.Fatal("parent retirement fabricated successful connection")
					}
					select {
					case <-exited:
					default:
						t.Fatal("parent completed with a pending SDK dial")
					}
					if f.workOwner.ActiveCount() != 0 {
						t.Fatal("parent retirement left connection work unjoined")
					}
				case "successful_call_lifetime":
					f.peer.attach(t, occurrence.client)
					if err := c.Connect(ctx); err != nil || !occurrence.client.WaitForConnection(5*time.Second) {
						t.Fatal("successful caller did not connect", err)
					}
					cancel()
					provider, observed, err := c.ObserveSession(f.ctx)
					if err != nil || !observed.Connected || provider.RequireExecutable() != nil {
						t.Fatal("completed Connect caller owned the durable socket", err)
					}
					provider.CloseExecution()
					if err := c.Connect(f.ctx); !errors.Is(err, errClientOccurrenceUsed) {
						t.Fatal("successful connection accepted reconnect", err)
					}
				}
				quiescence, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if err := f.workOwner.WaitForQuiescence(quiescence); err != nil {
					t.Fatal("caller completion leaked transient runtime work", err)
				}
			})
		}
	}
}

func TestWhatsAppRuntimeConnectCanceledHandshakeJoinsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			entered, exited := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(exited)
				socket, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					t.Error(err)
					return
				}
				defer socket.CloseNow()
				if _, _, err := socket.Read(r.Context()); err != nil {
					t.Error("missing Noise client hello", err)
					return
				}
				close(entered)
				// No server hello: cancellation must stop the socket and join the
				// real SDK handshake, not return with a background connect attempt.
				_, _, _ = socket.Read(r.Context())
			}))
			defer server.Close()
			target, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			client := &http.Client{Transport: sdkPeerTransport{target: target, base: http.DefaultTransport}}
			c.state.currentOccurrence().client.SetWebsocketHTTPClient(client)
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.Connect(ctx) }()
			select {
			case <-entered:
			case <-f.peer.ctx.Done():
				t.Fatal("SDK handshake did not enter its external wait")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal("canceled handshake lost its cause", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled SDK handshake caller remained pending")
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("connect returned before its handshake socket stopped")
			}
			occurrence := c.state.currentOccurrence()
			if err := c.Connect(f.ctx); !errors.Is(err, errClientOccurrenceFenced) {
				t.Fatal("canceled attempt accepted reconnect", err)
			}
			c.state.mu.Lock()
			closed := c.state.closed
			c.state.mu.Unlock()
			if closed || f.workOwner.ActiveCount() != 2 {
				t.Fatal("canceled wait abandoned pending attempt or durable connection work")
			}
			canceled, stop := context.WithCancel(context.Background())
			stop()
			if err := f.workOwner.WaitForQuiescence(canceled); !errors.Is(err, context.Canceled) {
				t.Fatal("quiescence lost its still-owned SDK handshake", err)
			}
			if err := c.Close(canceled); !errors.Is(err, context.Canceled) {
				t.Fatal("canceled cleanup claimed complete SDK join", err)
			}
			other, err := openSessionState(context.Background(), f.basePath, f.operation.SessionAccount.ConnectionID, f.operation.SessionAccount.AccountRef)
			if !errors.Is(err, errSessionPossession) {
				if other != nil {
					_ = other.close(context.Background())
				}
				t.Fatal("pending handshake released provider possession", err)
			}
			// The pinned SDK's handshake select does not consume cancellation.
			// Its existing finite bound is not changed; runtime ownership remains
			// held until the SDK exits and the occurrence is actually joined.
			joined, release := context.WithTimeout(context.Background(), whatsmeow.NoiseHandshakeResponseTimeout+time.Second)
			defer release()
			if err := f.workOwner.WaitForQuiescence(joined); err != nil {
				t.Fatal("pending SDK handshake was not completely joined", err)
			}
			select {
			case <-occurrence.stopDone:
			default:
				t.Fatal("canceled handshake did not join SDK stop")
			}
			if err := c.Close(joined); err != nil || f.workOwner.ActiveCount() != 0 {
				t.Fatal("completed SDK join did not release exactly its durable work", err)
			}
		})
	}
}
