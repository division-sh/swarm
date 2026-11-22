package publicingress

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestPublicRouteProbeRejectsWrongListener(t *testing.T) {
	for _, mode := range []string{ModeNativeTLS, ModeExternalOrigin, ModeManagedQuickTunnel} {
		for _, kind := range []string{"bare_204", "redirect_to_204"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				host := "hooks.example.test"
				if mode == ModeManagedQuickTunnel {
					host = "unit.trycloudflare.com"
				}
				root := t.TempDir()
				settings, leaf, _, _ := webhookCertificateFixture(t, root, host, time.Now().Add(time.Hour))
				config, err := NativeTLSConfig(settings, root, root)
				if err != nil {
					t.Fatal(err)
				}
				var wrong, target atomic.Int64
				peer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/final" {
						target.Add(1)
					} else {
						wrong.Add(1)
						if kind == "redirect_to_204" {
							http.Redirect(w, r, "/final", http.StatusFound)
							return
						}
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				peer.TLS = config
				peer.StartTLS()
				t.Cleanup(peer.Close)
				discovery := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/quicktunnel" {
						_, _ = w.Write([]byte(`{"hostname":"unit.trycloudflare.com"}`))
					}
				}))
				t.Cleanup(discovery.Close)
				transport := peer.Client().Transport.(*http.Transport).Clone()
				roots := x509.NewCertPool()
				roots.AddCert(leaf)
				transport.TLSClientConfig.RootCAs = roots
				transport.TLSClientConfig.ServerName = host
				transport.DialContext = func(ctx context.Context, _, addr string) (net.Conn, error) {
					if addr == host+":443" {
						addr = peer.Listener.Addr().String()
					} else {
						addr = discovery.Listener.Addr().String()
					}
					return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				}
				t.Cleanup(transport.CloseIdleConnections)
				ready := NewReadinessOwner(true)
				var calls int
				opts := Options{Mode: mode, Handler: http.NotFoundHandler(), Readiness: ready,
					HTTPClient: &http.Client{Transport: transport}, Launcher: &fakeManagedLauncher{},
					OnGeneration: func(context.Context, Generation) error { calls++; return nil }}
				if mode != ModeManagedQuickTunnel {
					opts.Listener = reserveTestListener(t)
					opts.PublicOrigin, opts.ListenAddress = "https://"+host, opts.Listener.Addr().String()
					if mode == ModeNativeTLS {
						opts.TLSConfig = config
					}
				}
				c, err := NewController(opts)
				if err != nil {
					t.Fatal(err)
				}
				c.waitRetry = func(context.Context, time.Duration) error { return nil }
				defer func() { _ = c.Stop(context.Background()) }()
				startErr := c.Start(t.Context())
				if wrong.Load() == 0 {
					t.Fatalf("probe never reached the wrong responder: %v", startErr)
				}
				if startErr == nil || calls != 0 || ready.Snapshot(time.Now()).Exposure != nil {
					t.Errorf("wrong route admitted: Start=%v registration calls=%d", startErr, calls)
				}
				if target.Load() != 0 {
					t.Errorf("probe contacted redirect target %d time(s)", target.Load())
				}
			})
		}
	}
}

func TestQuickTunnelRenewalCannotCrossReplacement(t *testing.T) {
	for _, cut := range []string{"revoked_before_install", "late_success", "late_failure", "selected_callback"} {
		t.Run(cut, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				launcher, ready := &fakeManagedLauncher{}, NewReadinessOwner(true)
				a, b := make(chan struct{}), make(chan struct{}, 1)
				releaseA, releaseB := make(chan struct{}), make(chan struct{})
				openA, openB := sync.OnceFunc(func() { close(releaseA) }), sync.OnceFunc(func() { close(releaseB) })
				callbacks := make(chan Generation, 4)
				var hold atomic.Bool
				var c *Controller
				transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
					switch r.URL.Path {
					case "/ready":
						return testHTTPResponse(http.StatusOK, ""), nil
					case "/quicktunnel":
						host := "a.trycloudflare.com"
						if launcher.process(1) != nil {
							host = "b.trycloudflare.com"
						}
						return testHTTPResponse(http.StatusOK, `{"hostname":"`+host+`"}`), nil
					}
					w := httptest.NewRecorder()
					c.servePublicRoute(w, r)
					if strings.HasPrefix(r.URL.Host, "b.") && hold.Load() {
						b <- struct{}{}
						if cut == "revoked_before_install" {
							<-releaseB
						}
					} else if hold.Load() && cut != "selected_callback" {
						close(a)
						<-releaseA
						if cut == "late_failure" {
							return nil, errors.New("old tunnel route failed")
						}
					}
					return w.Result(), nil
				})
				var err error
				c, err = NewController(Options{Mode: ModeManagedQuickTunnel, Handler: http.NotFoundHandler(),
					Launcher: launcher, Readiness: ready, HTTPClient: &http.Client{Transport: transport},
					OnGeneration: func(_ context.Context, g Generation) error {
						if hold.Load() && cut == "selected_callback" && strings.Contains(g.PublicOrigin, "a.trycloudflare") {
							close(a)
							<-releaseA
						}
						callbacks <- g
						return nil
					}})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { openA(); openB(); cancel(); _ = c.Stop(context.Background()) }()
				c.listener, c.nonce = reserveTestListener(t), "probe"
				if err := c.establish(ctx); err != nil {
					t.Fatal(err)
				}
				first := <-callbacks
				c.supervising = true
				go c.supervise(ctx)
				hold.Store(true)
				renewed := make(chan struct{})
				go func() { c.RenewRegistrations(ctx); close(renewed) }()
				<-a
				launcher.process(0).fail(errors.New("old child exited"))
				<-b
				synctest.Wait()
				if cut == "revoked_before_install" || cut == "selected_callback" {
					if got := ready.Snapshot(time.Now()).Exposure; got != nil {
						t.Errorf("exposure advanced before replacement could apply: %s", got.GenerationID)
					}
					openA()
					<-renewed
					if cut == "revoked_before_install" {
						if got := ready.Snapshot(time.Now()).Exposure; got != nil {
							t.Errorf("late renewal restored the revoked generation: %s", got.GenerationID)
						}
					}
					openB()
				} else {
					openA()
					<-renewed
				}
				synctest.Wait()
				second := c.Generation()
				got := ready.Snapshot(time.Now()).Exposure
				if second.ID == first.ID || got == nil || got.GenerationID != second.ID {
					t.Errorf("stale renewal corrupted replacement: exposure=%+v generation=%s", got, second.ID)
				}
				if cut == "selected_callback" {
					if got := <-callbacks; got.ID != first.ID {
						t.Errorf("replacement callback overtook selected renewal: %s", got.ID)
					}
				}
				if len(callbacks) != 1 {
					t.Errorf("expected just the replacement callback, got %d", len(callbacks))
				}
				hold.Store(false)
				if selected, probeErr, regErr := c.RenewRegistrations(ctx); !selected || probeErr != nil || regErr != nil {
					t.Errorf("current B renewal failed: %t %v %v", selected, probeErr, regErr)
				}
			})
		})
	}
}

func TestQuickTunnelCanceledCommitKeepsCurrentProbeError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		independent := errors.New("current tunnel probe transport failure")
		entered, release := make(chan struct{}), make(chan struct{})
		resume := sync.OnceFunc(func() { close(release) })
		defer resume()
		ready := NewReadinessOwner(true)
		var c *Controller
		var calls atomic.Int32
		var failed atomic.Bool
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/ready":
				return testHTTPResponse(http.StatusOK, ""), nil
			case "/quicktunnel":
				return testHTTPResponse(http.StatusOK, `{"hostname":"unit.trycloudflare.com"}`), nil
			}
			if calls.Load() != 0 {
				failed.Store(true)
				return nil, independent
			}
			w := httptest.NewRecorder()
			c.servePublicRoute(w, r)
			return w.Result(), nil
		})}
		var err error
		c, err = NewController(Options{Mode: ModeManagedQuickTunnel, Handler: http.NotFoundHandler(),
			Readiness: ready, HTTPClient: client, Launcher: &fakeManagedLauncher{},
			OnGeneration: func(context.Context, Generation) error {
				if calls.Add(1) == 1 {
					close(entered)
					<-release
				}
				return nil
			}})
		if err != nil {
			t.Fatal(err)
		}
		c.listener, c.nonce = reserveTestListener(t), "probe"
		c.updateCtx, c.cancel = context.WithCancel(t.Context())
		established, renewed, stopped := make(chan error, 1), make(chan error, 1), make(chan error, 1)
		go func() { established <- c.establish(c.updateCtx) }()
		<-entered
		generation, before := ready.registration.captureExposure()
		go func() {
			selected, probeErr, regErr := c.RenewRegistrations(t.Context())
			if !selected || regErr != nil {
				t.Errorf("renewal did not remain a selected probe: %t, %v", selected, regErr)
			}
			renewed <- probeErr
		}()
		synctest.Wait()
		if !failed.Load() || len(renewed) != 0 || generation == nil {
			t.Fatal("failed current probe did not wait behind the admitted callback")
		}
		stopCtx, cancel := context.WithCancel(t.Context())
		go func() { stopped <- c.Stop(stopCtx) }()
		<-c.UpdateAdmissionClosed()
		cancel()
		if err := <-renewed; !errors.Is(err, context.Canceled) || !errors.Is(err, independent) {
			t.Errorf("canceled renewal lost one of its failures: %v", err)
		}
		if !ready.registration.currentExposureRevision(before) || calls.Load() != 1 || len(stopped) != 0 {
			t.Error("canceled renewal changed exposure, reconciled, or bypassed the selected callback join")
		}
		resume()
		if err := <-established; err != nil {
			t.Fatal(err)
		}
		if err := <-stopped; !errors.Is(err, context.Canceled) || !errors.Is(err, independent) {
			t.Errorf("stop lost one of the renewal failures: %v", err)
		}
	})
}
