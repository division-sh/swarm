package publicingress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func reserveTestListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("reserve listener: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestPublicIngressListenerProvesNonceAndExposesOnlyWebhookRoutes(t *testing.T) {
	listener := reserveTestListener(t)
	var controller *Controller
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		clone := request.Clone(request.Context())
		clone.URL.Scheme = "http"
		clone.URL.Host = controller.listener.Addr().String()
		return http.DefaultTransport.RoundTrip(clone)
	})}
	var admitted atomic.Int64
	var err error
	controller, err = NewController(Options{
		Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: listener.Addr().String(), Listener: listener,
		HTTPClient: client,
		Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			admitted.Add(1)
			response.WriteHeader(http.StatusAccepted)
		}),
	})
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := controller.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		if err := controller.Stop(stopCtx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	for _, route := range []string{
		"/v1/health", "/readyz", "/mcp", "/rpc", "/tools", "/webhooks/_swarm_probe/not-the-nonce",
		"/webhooks/support%2ftelegram", "/webhooks/support%2Ftelegram", "/webhooks/support/telegram/",
		"//webhooks/support/telegram", "/webhooks/../v1/rpc", "/webhooks/support/../telegram",
		"/webhooks/support/.", "/webhooks/support/..", "/webhooks//telegram", "/webhooks/support/",
	} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			request, err := http.NewRequest(method, "https://hooks.example.test"+route, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, requestErr := client.Do(request)
			if requestErr != nil {
				t.Fatalf("%s %s: %v", method, route, requestErr)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusNotFound || response.Header.Get("Location") != "" {
				t.Fatalf("%s %s status = %d, want 404 without redirect", method, route, response.StatusCode)
			}
		}
	}
	response, err := client.Post("https://hooks.example.test/webhooks/support/telegram", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST webhook: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || admitted.Load() != 1 {
		t.Fatalf("webhook status=%d admitted=%d, want 202/1", response.StatusCode, admitted.Load())
	}
}

type fakeManagedProcess struct {
	done chan error
	once sync.Once
}

func newFakeManagedProcess() *fakeManagedProcess {
	return &fakeManagedProcess{done: make(chan error, 1)}
}

func (p *fakeManagedProcess) Done() <-chan error { return p.done }
func (p *fakeManagedProcess) Stop() error {
	p.once.Do(func() {
		p.done <- context.Canceled
		close(p.done)
	})
	return nil
}
func (p *fakeManagedProcess) fail(err error) { p.once.Do(func() { p.done <- err; close(p.done) }) }

type fakeManagedLauncher struct {
	mu        sync.Mutex
	processes []*fakeManagedProcess
}

func (l *fakeManagedLauncher) Launch(context.Context, string, string, string) (Process, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	process := newFakeManagedProcess()
	l.processes = append(l.processes, process)
	return process, nil
}

func (l *fakeManagedLauncher) process(index int) *fakeManagedProcess {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index >= len(l.processes) {
		return nil
	}
	return l.processes[index]
}

func TestManagedTunnelFailureRevokesThenMintsFreshGeneration(t *testing.T) {
	launcher := &fakeManagedLauncher{}
	readiness := NewReadinessOwner(true)
	var controller *Controller
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/ready":
			return testHTTPResponse(http.StatusOK, ""), nil
		case "/quicktunnel":
			return testHTTPResponse(http.StatusOK, `{"hostname":"unit.trycloudflare.com"}`), nil
		default:
			clone := request.Clone(request.Context())
			clone.URL.Scheme = "http"
			clone.URL.Host = controller.listener.Addr().String()
			return http.DefaultTransport.RoundTrip(clone)
		}
	})}
	var err error
	controller, err = NewController(Options{
		Mode: ModeManagedQuickTunnel, Handler: http.NotFoundHandler(), HTTPClient: client,
		Launcher: launcher, Readiness: readiness, StartupAuthority: func() string { return "startup-current" },
	})
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := controller.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		if err := controller.Stop(stopCtx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	first := controller.Generation()
	if first.ID == "" {
		t.Fatal("initial exposure generation is missing")
	}
	launcher.process(0).fail(errors.New("injected tunnel loss"))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		generation := controller.Generation()
		if generation.ID != "" && generation.ID != first.ID {
			snapshot := readiness.Snapshot(time.Now().UTC())
			if snapshot.Exposure == nil || snapshot.Exposure.GenerationID != generation.ID {
				t.Fatalf("readiness generation = %#v, want %s", snapshot.Exposure, generation.ID)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("managed tunnel did not mint a fresh generation after bounded restart")
}

func TestManagedGenerationFailureStopsChildBeforeRetry(t *testing.T) {
	launcher := &fakeManagedLauncher{}
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/ready":
			return testHTTPResponse(http.StatusOK, ""), nil
		case "/quicktunnel":
			return testHTTPResponse(http.StatusOK, `{"hostname":"unit.trycloudflare.com"}`), nil
		default:
			return testHTTPResponse(http.StatusBadGateway, "unreachable"), nil
		}
	})}
	controller, err := NewController(Options{
		Mode: ModeManagedQuickTunnel, Handler: http.NotFoundHandler(), HTTPClient: client, Launcher: launcher,
	})
	if err != nil {
		t.Fatalf("NewController: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer listener.Close()
	controller.listener = listener
	controller.nonce = "probe"
	controller.waitRetry = func(context.Context, time.Duration) error { return nil }

	if err := controller.establish(context.Background()); err == nil {
		t.Fatal("establish succeeded despite failed public-route proof")
	}
	process := launcher.process(0)
	if process == nil {
		t.Fatal("managed child was not launched")
	}
	select {
	case <-process.Done():
	case <-time.After(time.Second):
		t.Fatal("failed generation left its managed child running")
	}
}

func TestManagedQuickTunnelRetriesPublicRouteWhileHostnamePropagates(t *testing.T) {
	for _, mode := range []string{ModeManagedQuickTunnel, ModeExternalOrigin} {
		t.Run(mode, func(t *testing.T) {
			launcher := &fakeManagedLauncher{}
			var routeAttempts atomic.Int64
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch request.URL.Path {
				case "/ready":
					return testHTTPResponse(http.StatusOK, ""), nil
				case "/quicktunnel":
					return testHTTPResponse(http.StatusOK, `{"hostname":"unit.trycloudflare.com"}`), nil
				default:
					if routeAttempts.Add(1) < 3 {
						return nil, &net.DNSError{Err: "no such host", Name: "unit.trycloudflare.com", IsNotFound: true}
					}
					return testHTTPResponse(http.StatusNoContent, ""), nil
				}
			})}
			opts := Options{Mode: mode, Handler: http.NotFoundHandler(), HTTPClient: client, Launcher: launcher}
			if mode == ModeExternalOrigin {
				opts.PublicOrigin, opts.ListenAddress = "https://hooks.example.test", "127.0.0.1:8443"
			}
			controller, err := NewController(opts)
			if err != nil {
				t.Fatal(err)
			}
			controller.waitRetry = func(context.Context, time.Duration) error { return nil }
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			controller.listener = listener
			controller.nonce = "probe"
			if err := controller.establish(context.Background()); err != nil {
				t.Fatalf("establish after propagation retries: %v", err)
			}
			if routeAttempts.Load() != 3 || controller.Generation().ID == "" {
				t.Fatalf("route attempts=%d generation=%#v", routeAttempts.Load(), controller.Generation())
			}
		})
	}
}

func TestPublicRouteProbeRetainsDualStackFailuresWithinRetryBound(t *testing.T) {
	ipv6Err, ipv4Err := errors.New("IPv6: no route to host"), errors.New("IPv4: connection refused")
	var attempts int
	controller := &Controller{opts: Options{HTTPClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		attempts++
		trace := httptrace.ContextClientTrace(request.Context())
		if trace != nil {
			trace.ConnectStart("tcp", "[2001:db8::1]:443")
			trace.ConnectStart("tcp", "192.0.2.1:443")
			trace.ConnectDone("tcp", "[2001:db8::1]:443", ipv6Err)
			trace.ConnectDone("tcp", "192.0.2.1:443", ipv4Err)
		}
		return nil, ipv6Err // net.Dialer's returned error hides the fallback's error.
	})}}}
	var waits []time.Duration
	controller.waitRetry = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	err := controller.probePublicRouteWithRetry(context.Background(), "https://hooks.example.test")
	if err == nil || !errors.Is(err, ipv6Err) || attempts != 4 || len(waits) != 3 || waits[0] != time.Second || waits[1] != 2*time.Second || waits[2] != 4*time.Second {
		t.Fatalf("bounded retries: attempts=%d waits=%v error=%v", attempts, waits, err)
	}
	for _, detail := range []string{"[2001:db8::1]:443: " + ipv6Err.Error(), "192.0.2.1:443: " + ipv4Err.Error(), "attempt 1", "attempt 4", "Check the origin's A/AAAA records"} {
		if !strings.Contains(err.Error(), detail) {
			t.Errorf("missing %q in diagnostic: %v", detail, err)
		}
	}
	if strings.Count(err.Error(), "Check the origin's A/AAAA records") != 1 {
		t.Fatalf("expected one remediation line: %v", err)
	}
	controller.waitRetry = func(context.Context, time.Duration) error { return context.Canceled }
	attempts = 0
	if err := controller.probePublicRouteWithRetry(context.Background(), "https://hooks.example.test"); !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("cancellation did not stop retries: attempts=%d error=%v", attempts, err)
	}
}

func TestQuickTunnelEndpointContractFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		ready    int
		payload  string
		wantOK   bool
		wantHost string
	}{
		{name: "exact", ready: http.StatusOK, payload: `{"hostname":"abc.trycloudflare.com"}`, wantOK: true, wantHost: "abc.trycloudflare.com"},
		{name: "not ready", ready: http.StatusServiceUnavailable, payload: `{"hostname":"abc.trycloudflare.com"}`},
		{name: "unknown field", ready: http.StatusOK, payload: `{"hostname":"abc.trycloudflare.com","extra":true}`},
		{name: "wrong suffix", ready: http.StatusOK, payload: `{"hostname":"example.com"}`},
		{name: "trailing document", ready: http.StatusOK, payload: `{"hostname":"abc.trycloudflare.com"}{}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == "/ready" {
					return testHTTPResponse(tc.ready, ""), nil
				}
				return testHTTPResponse(http.StatusOK, tc.payload), nil
			})}
			ok, host := readQuickTunnelEndpoints(context.Background(), client, "127.0.0.1:1")
			if ok != tc.wantOK || host != tc.wantHost {
				t.Fatalf("readQuickTunnelEndpoints = %v,%q want %v,%q", ok, host, tc.wantOK, tc.wantHost)
			}
		})
	}
}

func TestPublicIngressConfigurationAdmission(t *testing.T) {
	tests := []struct {
		name   string
		mode   string
		origin string
		listen string
		ok     bool
	}{
		{name: "managed", mode: ModeManagedQuickTunnel, ok: true},
		{name: "external", mode: ModeExternalOrigin, origin: "https://hooks.example.test", listen: "127.0.0.1:8443", ok: true},
		{name: "external root slash", mode: ModeExternalOrigin, origin: "https://hooks.example.test/", listen: "127.0.0.1:8443", ok: true},
		{name: "external http", mode: ModeExternalOrigin, origin: "http://hooks.example.test", listen: "127.0.0.1:8443"},
		{name: "external path", mode: ModeExternalOrigin, origin: "https://hooks.example.test/path", listen: "127.0.0.1:8443"},
		{name: "external nonloopback", mode: ModeExternalOrigin, origin: "https://hooks.example.test", listen: "0.0.0.0:8443"},
		{name: "external ephemeral", mode: ModeExternalOrigin, origin: "https://hooks.example.test", listen: "127.0.0.1:0"},
		{name: "managed override", mode: ModeManagedQuickTunnel, origin: "https://hooks.example.test"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfiguration(tc.mode, tc.origin, tc.listen)
			if (err == nil) != tc.ok {
				t.Fatalf("ValidateConfiguration error=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestCloudflaredPreflightVersionContract(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		ok   bool
	}{
		{name: "minimum", raw: "cloudflared version 2021.7.0 (built 2021-07-15)", ok: true},
		{name: "current", raw: "cloudflared version 2026.7.1", ok: true},
		{name: "below minimum", raw: "cloudflared version 2021.6.9"},
		{name: "old year", raw: "cloudflared version 2020.12.3"},
		{name: "missing semver", raw: "cloudflared development build"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := admitCloudflaredVersion([]byte(tc.raw))
			if (err == nil) != tc.ok {
				t.Fatalf("admitCloudflaredVersion(%q) error=%v, want ok=%v", tc.raw, err, tc.ok)
			}
		})
	}
}

func TestCallbackURLReservesProbeNamespaceAndRequiresOrigin(t *testing.T) {
	generation := Generation{PublicOrigin: "https://hooks.example.test"}
	if _, err := CallbackURL(generation, "_swarm_probe", "telegram", "token"); err == nil {
		t.Fatal("reserved probe alias was accepted")
	}
	if _, err := CallbackURL(generation, "support", "telegram/path", "token"); err == nil {
		t.Fatal("provider path segment was accepted")
	}
	if _, err := CallbackURL(generation, "..", "telegram", "token"); err == nil {
		t.Fatal("callback path traversal segment was accepted")
	}
	got, err := CallbackURL(generation, "support", "telegram", "opaque")
	if err != nil {
		t.Fatalf("CallbackURL: %v", err)
	}
	if got != "https://hooks.example.test/webhooks/support/telegram?swarm_callback_generation=opaque" {
		t.Fatalf("CallbackURL = %q", got)
	}
}

func TestA9CallbackURLUsesExactAlias(t *testing.T) {
	generation := Generation{PublicOrigin: "https://hooks.example.test"}
	for _, alias := range []string{" support", "support ", "/support", "support/", "support/child", ".support", "caf\u00e9"} {
		if _, err := CallbackURL(generation, alias, "telegram", "token"); err == nil {
			t.Fatalf("unadmitted alias %q was normalized into a callback", alias)
		}
	}
	callback, err := CallbackURL(generation, "shop.support", "telegram", "token")
	if err != nil || !strings.Contains(callback, "/webhooks/shop.support/telegram?") {
		t.Fatalf("rooted alias not preserved in callback: %q err=%v", callback, err)
	}
}

func testHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
