package publicingress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func awaitExposure[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("public listener proof did not reach its next boundary")
		var zero T
		return zero
	}
}

func TestExposureReportsListenerLossAndRefusesStaleProbe(t *testing.T) {
	for _, mode := range []string{ModeExternalOrigin, ModeNativeTLS} {
		t.Run(mode, func(t *testing.T) {
			listener := reserveTestListener(t)
			ready := NewReadinessOwner(true)
			ready.SetRuntimeReady(true)
			fatal := make(chan error, 2)
			probing, release := make(chan struct{}), make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			var probes atomic.Int32
			opts := Options{Mode: mode, PublicOrigin: "https://hooks.example.test", ListenAddress: listener.Addr().String(),
				Listener: listener, Readiness: ready, Handler: http.NotFoundHandler(),
				OnFatal: func(err error) { fatal <- err }}
			transport := &http.Transport{}
			t.Cleanup(transport.CloseIdleConnections)
			if mode == ModeNativeTLS {
				root := t.TempDir()
				settings, leaf, _, _ := webhookCertificateFixture(t, root, "hooks.example.test", time.Now().Add(time.Hour))
				config, err := NativeTLSConfig(settings, root, root)
				if err != nil {
					t.Fatal(err)
				}
				opts.TLSConfig = config
				roots := x509.NewCertPool()
				roots.AddCert(leaf)
				transport.TLSClientConfig = &tls.Config{RootCAs: roots}
				transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
				}
			}
			opts.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if probes.Add(1) == 2 {
					defer func() { close(probing); <-release }()
				}
				if mode == ModeExternalOrigin {
					req = req.Clone(req.Context())
					req.URL.Scheme, req.URL.Host = "http", listener.Addr().String()
				}
				return transport.RoundTrip(req)
			})}
			// Native TLS requires a concrete transport with fresh verified handshakes.
			if mode == ModeNativeTLS {
				opts.HTTPClient.Transport = transport
			}
			c, err := NewController(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { resume(); _ = c.Stop(context.Background()) })
			if err := c.Start(t.Context()); err != nil || !ready.Load() {
				t.Fatalf("initial public route not ready: %v", err)
			}
			var renewed chan error
			if mode == ModeExternalOrigin {
				renewed = make(chan error, 1)
				go func() { renewed <- c.Renew(t.Context()) }()
				awaitExposure(t, probing)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			awaitExposure(t, c.serverDone)
			select {
			case got := <-fatal:
				if !errors.Is(got, net.ErrClosed) {
					t.Errorf("listener cause lost: %v", got)
				}
			default:
				t.Error("unexpected Serve return was not reported")
			}
			if ready.Load() {
				t.Error("dead listener still advertised ready")
			}
			if renewed != nil {
				stopped := make(chan error, 1)
				go func() { stopped <- c.Stop(t.Context()) }()
				awaitExposure(t, c.UpdateAdmissionClosed())
				resume()
				if got := awaitExposure(t, renewed); !errors.Is(got, net.ErrClosed) {
					t.Errorf("stale successful probe did not observe listener loss: %v", got)
				}
				if got := awaitExposure(t, stopped); got != nil {
					t.Errorf("shutdown repeated the serving cause already delivered to OnFatal: %v", got)
				}
			} else {
				_ = c.Stop(context.Background())
			}
			if ready.Snapshot(time.Now()).Exposure != nil {
				t.Error("listener outage left exposure after stop")
			}
			select {
			case duplicate := <-fatal:
				t.Errorf("listener failure reported twice: %v", duplicate)
			default:
			}
		})
	}
}

func TestExposureListenerLossDuringStartupReturnsServeCause(t *testing.T) {
	listener := reserveTestListener(t)
	probing := make(chan struct{})
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	canceled := make(chan error, 1)
	c, err := NewController(Options{Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test",
		ListenAddress: listener.Addr().String(), Listener: listener,
		Handler: http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
			close(entered)
			select {
			case <-req.Context().Done():
				canceled <- req.Context().Err()
			case <-release:
			}
			<-release
		}),
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			close(probing)
			<-req.Context().Done()
			return nil, req.Context().Err()
		})}, OnFatal: func(err error) { t.Errorf("startup failure also reported as running: %v", err) }})
	if err != nil {
		t.Fatal(err)
	}
	c.waitRetry = func(ctx context.Context, _ time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("startup proof retried after listener loss")
	}
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan error, 1)
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		started <- c.Start(ctx)
	}()
	t.Cleanup(func() {
		resume()
		cancel()
		_ = listener.Close()
		awaitExposure(t, startDone)
		_ = c.Stop(context.Background())
	})
	awaitExposure(t, probing)
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := fmt.Fprint(conn, "POST /webhooks/support/telegram HTTP/1.1\r\nHost: hooks.example.test\r\nContent-Length: 5\r\n\r\na"); err != nil {
		t.Fatal(err)
	}
	awaitExposure(t, entered)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	awaitExposure(t, c.serverDone)
	if got := awaitExposure(t, started); got != c.ListenerFailure() || !errors.Is(got, net.ErrClosed) {
		t.Fatalf("startup failed without its exact listener cause: %v", got)
	}
	select {
	case got := <-canceled:
		t.Fatalf("startup canceled its admitted handler before cutoff: %v", got)
	default:
	}
	deadline, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop(deadline) }()
	if got := awaitExposure(t, canceled); !errors.Is(got, context.Canceled) || deadline.Err() != context.DeadlineExceeded {
		t.Errorf("handler canceled outside original shutdown deadline: delegate=%v owner=%v", got, deadline.Err())
	}
	select {
	case got := <-stopped:
		t.Fatalf("cutoff reported %v before the admitted handler returned", got)
	default:
	}
	resume()
	if got := awaitExposure(t, stopped); !errors.Is(got, context.DeadlineExceeded) || errors.Is(got, net.ErrClosed) {
		t.Errorf("shutdown lost deadline or repeated startup error: %v", got)
	}
}

type exposureLifetimeLauncher struct {
	fakeManagedLauncher
	launches chan context.Context
}

func (l *exposureLifetimeLauncher) Launch(ctx context.Context, local, metrics, binary string) (Process, error) {
	l.launches <- ctx
	return l.fakeManagedLauncher.Launch(ctx, local, metrics, binary)
}

func TestExposureCutoffCancelsAndJoinsUnreadHandler(t *testing.T) {
	for _, mode := range []string{"external", "selected_renewal", "published_replacement"} {
		t.Run(mode, func(t *testing.T) { proveExposureCutoff(t, mode) })
	}
}

func proveExposureCutoff(t *testing.T, mode string) {
	release := make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	entered := make(chan struct{})
	requestCanceled := make(chan error, 1)
	writerCanceled := make(chan struct{})
	selected := make(chan struct{})
	writerErr := errors.New("independent admitted registration failure")
	launcher := &exposureLifetimeLauncher{launches: make(chan context.Context, 2)}
	ready := NewReadinessOwner(true)
	var c *Controller
	var active atomic.Bool
	opts := Options{Mode: ModeManagedQuickTunnel, Launcher: launcher, Readiness: ready,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !active.CompareAndSwap(false, true) {
				w.WriteHeader(http.StatusAccepted)
				return
			}
			close(entered)
			select {
			case <-r.Context().Done():
				requestCanceled <- r.Context().Err()
			case <-release:
			}
			<-release
		}), HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/ready":
				return testHTTPResponse(http.StatusOK, ""), nil
			case "/quicktunnel":
				return testHTTPResponse(http.StatusOK, `{"hostname":"unit.trycloudflare.com"}`), nil
			}
			req = req.Clone(req.Context())
			req.URL.Scheme, req.URL.Host = "http", c.listener.Addr().String()
			return http.DefaultTransport.RoundTrip(req)
		})}}
	if mode == "external" {
		opts.Mode, opts.PublicOrigin = ModeExternalOrigin, "https://hooks.example.test"
		opts.Listener = reserveTestListener(t)
		opts.ListenAddress = opts.Listener.Addr().String()
	}
	var calls atomic.Int32
	opts.OnGeneration = func(ctx context.Context, _ Generation) error {
		if calls.Add(1) == 2 {
			close(selected)
			<-ctx.Done()
			close(writerCanceled)
			return writerErr
		}
		return nil
	}
	var err error
	c, err = NewController(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	var stopStarted bool
	stopDone := make(chan struct{})
	t.Cleanup(func() {
		resume()
		if stopStarted {
			awaitExposure(t, stopDone)
		} else {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = c.Stop(ctx)
		}
	})
	var transportCtx context.Context
	child := 0
	if mode != "external" {
		transportCtx = awaitExposure(t, launcher.launches)
		if mode == "published_replacement" {
			launcher.process(0).fail(errors.New("replace initial tunnel"))
			awaitExposure(t, selected)
			transportCtx = awaitExposure(t, launcher.launches)
			child = 1
		} else {
			go func() { _, _, _ = c.RenewRegistrations(t.Context()) }()
			awaitExposure(t, selected)
		}
	}
	conn, err := net.DialTimeout("tcp", c.listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprint(conn, "POST /webhooks/support/telegram HTTP/1.1\r\nHost: hooks.example.test\r\nContent-Length: 5\r\n\r\na"); err != nil {
		t.Fatal(err)
	}
	awaitExposure(t, entered)
	deadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	stopped := make(chan error, 1)
	stopStarted = true
	go func() {
		defer close(stopDone)
		stopped <- c.Stop(deadline)
	}()
	if transportCtx != nil {
		awaitExposure(t, writerCanceled)
		if err := transportCtx.Err(); err != nil {
			t.Errorf("selected writer cancellation canceled its retained transport: %v", err)
		}
	}
	select {
	case got := <-stopped:
		t.Errorf("stop returned before the unread-body handler joined: %v", got)
		return
	case got := <-requestCanceled:
		if !errors.Is(got, context.Canceled) {
			t.Errorf("cutoff did not cancel delegation: %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Error("unread-body handler not canceled by cutoff")
		resume()
		awaitExposure(t, stopped)
		return
	}
	if ready.Snapshot(time.Now()).Exposure == nil {
		t.Error("cutoff revoked exposure before delegate joined")
	}
	if transportCtx != nil {
		select {
		case <-launcher.process(child).Done():
			t.Error("transport stopped before delegate joined")
		default:
		}
	}
	late := httptest.NewRecorder()
	c.servePublicRoute(late, httptest.NewRequest(http.MethodPost, "/webhooks/support/telegram", nil))
	if late.Code != http.StatusServiceUnavailable {
		t.Errorf("fresh delegate after cutoff: %d", late.Code)
	}
	resume()
	if got := awaitExposure(t, stopped); !errors.Is(got, context.DeadlineExceeded) ||
		(transportCtx != nil && !errors.Is(got, writerErr)) {
		t.Fatalf("shutdown lost deadline or admitted writer error: %v", got)
	}
	if got := c.CloseListener(context.Background()); !errors.Is(got, context.DeadlineExceeded) {
		t.Errorf("repeated cutoff erased original deadline: %v", got)
	}
	if transportCtx != nil {
		awaitExposure(t, launcher.process(child).Done())
		if transportCtx.Err() == nil {
			t.Error("joined transport lifetime was not canceled")
		}
	}
}
