package publicingress

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"golang.org/x/crypto/acme"
)

func webhookCertificateFixture(t *testing.T, root, hostname string, expiry time.Time) (*config.PublicWebhookConfig, *x509.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{hostname}, NotBefore: time.Now().Add(-48 * time.Hour), NotAfter: expiry,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encodedKey})
	certFile, keyFile := "cert.pem", "key.pem"
	for name, data := range map[string][]byte{certFile: certPEM, keyFile: keyPEM} {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return &config.PublicWebhookConfig{Hostname: hostname, CertificateFile: &certFile, PrivateKeyFile: &keyFile}, leaf, certPEM, keyPEM
}

func TestNativeTLSSuppliedCertificateIsStateBoundAndRestartOwned(t *testing.T) {
	root := t.TempDir()
	settings, _, _, _ := webhookCertificateFixture(t, root, "hooks.example.test", time.Now().Add(90*24*time.Hour))
	first, err := NativeTLSConfig(settings, root)
	if err != nil || first.GetCertificate != nil || len(first.Certificates) != 1 {
		t.Fatalf("supplied TLS must load once without an issuer: config=%v error=%v", first, err)
	}
	webhookCertificateFixture(t, root, settings.Hostname, time.Now().Add(90*24*time.Hour))
	second, err := NativeTLSConfig(settings, root)
	if err != nil || bytes.Equal(first.Certificates[0].Certificate[0], second.Certificates[0].Certificate[0]) {
		t.Fatalf("replacement must take a new startup, not mutate the loaded pair: %v", err)
	}
	for _, failure := range []string{"wrong name", "expired", "partial", "bad key", "escape", "symlink escape"} {
		t.Run(failure, func(t *testing.T) {
			state := t.TempDir()
			cfg, _, _, _ := webhookCertificateFixture(t, state, settings.Hostname, time.Now().Add(time.Hour))
			switch failure {
			case "wrong name":
				cfg.Hostname = "other.example.test"
			case "expired":
				cfg, _, _, _ = webhookCertificateFixture(t, state, settings.Hostname, time.Now().Add(-time.Hour))
			case "partial":
				cfg.PrivateKeyFile = nil
			case "bad key":
				if err := os.WriteFile(filepath.Join(state, *cfg.PrivateKeyFile), []byte("not a key"), 0600); err != nil {
					t.Fatal(err)
				}
			case "escape", "symlink escape":
				outside := filepath.Join(root, "cert.pem")
				if failure == "symlink escape" {
					outside = filepath.Join(state, "outside.pem")
					if err := os.Symlink(filepath.Join(root, "cert.pem"), outside); err != nil {
						t.Fatal(err)
					}
				}
				cfg.CertificateFile = &outside
			}
			if got, err := NativeTLSConfig(cfg, state); err == nil || got != nil {
				t.Fatalf("bad supplied pair must refuse, never fall back to ACME: %v", err)
			}
			if _, err := os.Stat(filepath.Join(state, "public-webhook-certs")); !os.IsNotExist(err) {
				t.Fatalf("supplied branch touched automatic certificate storage: %v", err)
			}
		})
	}
}

func TestNativeTLSAutomaticManagerUsesOneNameAndMockCA(t *testing.T) {
	root := t.TempDir()
	host := "hooks.example.test"
	cfg, err := NativeTLSConfig(&config.PublicWebhookConfig{Hostname: host}, root)
	if err != nil || cfg.GetCertificate == nil || len(cfg.Certificates) != 0 {
		t.Fatalf("automatic TLS configuration: %v", err)
	}
	if _, err := cfg.GetConfigForClient(&tls.ClientHelloInfo{ServerName: "other.example.test"}); err == nil {
		t.Fatal("an old cached name must not bypass configured-name restriction")
	}
	if _, err := os.Stat(filepath.Join(root, "public-webhook-certs")); !os.IsNotExist(err) {
		t.Fatalf("configuration admission must not issue or create the cache: %v", err)
	}
	var calls atomic.Int64
	ca := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "mock CA refusal", http.StatusBadRequest)
	}))
	defer ca.Close()
	manager := nativeCertificateManager(host, root, "ops@example.test")
	manager.Client = &acme.Client{DirectoryURL: ca.URL, HTTPClient: ca.Client()}
	if manager.Email != "ops@example.test" || manager.Prompt == nil || !manager.Prompt("test terms") {
		t.Fatal("explicit contact and opt-in terms were not forwarded")
	}
	hello := &tls.ClientHelloInfo{ServerName: "other.example.test", CipherSuites: []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}}
	if _, err := manager.GetCertificate(hello); err == nil || calls.Load() != 0 {
		t.Fatalf("unconfigured name reached issuance: error=%v calls=%d", err, calls.Load())
	}
	hello.ServerName = host
	if _, err := manager.GetCertificate(hello); err == nil || calls.Load() == 0 {
		t.Fatalf("configured cold start did not use the mocked CA: error=%v calls=%d", err, calls.Load())
	}
	_, _, certPEM, keyPEM := webhookCertificateFixture(t, root, host, time.Now().Add(90*24*time.Hour))
	manager = nativeCertificateManager(host, root, "")
	manager.Client = &acme.Client{DirectoryURL: ca.URL, HTTPClient: ca.Client()}
	if err := manager.Cache.Put(context.Background(), host, append(keyPEM, certPEM...)); err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	if _, err := manager.GetCertificate(hello); err != nil || calls.Load() != before {
		t.Fatalf("cached restart should not require issuance: error=%v calls=%d", err, calls.Load())
	}
}

func TestNativeTLSProbeUsesFreshVerifiedHandshakeAndRevokesExpiry(t *testing.T) {
	root := t.TempDir()
	settings, leaf, _, _ := webhookCertificateFixture(t, root, "hooks.example.test", time.Now().Add(time.Hour))
	loaded, err := NativeTLSConfig(settings, root)
	if err != nil {
		t.Fatal(err)
	}
	var active atomic.Pointer[tls.Certificate]
	active.Store(&loaded.Certificates[0])
	var handshakes atomic.Int64
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		handshakes.Add(1)
		return active.Load(), nil
	}}
	listener := reserveTestListener(t)
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	var probing atomic.Bool
	probing.Store(true)
	clientTLS := &tls.Config{RootCAs: roots, ClientSessionCache: tls.NewLRUClientSessionCache(4), VerifyConnection: func(state tls.ConnectionState) error {
		if state.DidResume && probing.Load() {
			t.Error("probe reused a TLS session instead of verifying the current certificate")
		}
		return nil
	}}
	transport := &http.Transport{TLSClientConfig: clientTLS, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	readiness := NewReadinessOwner(true)
	controller, err := NewController(Options{
		Mode: ModeNativeTLS, PublicOrigin: "https://" + settings.Hostname, ListenAddress: listener.Addr().String(), Listener: listener,
		TLSConfig: serverTLS, HTTPClient: &http.Client{Transport: transport}, Readiness: readiness,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := controller.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stop, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		if err := controller.Stop(stop); err != nil {
			t.Error(err)
		}
	}()
	for range 2 {
		if err := controller.Renew(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if handshakes.Load() != 3 || readiness.Snapshot(time.Now()).Exposure == nil {
		t.Fatalf("three probes need three TLS handshakes and exposure evidence, got %d", handshakes.Load())
	}
	probing.Store(false)
	response, plainErr := http.Get("http://" + listener.Addr().String() + "/webhooks/support/telegram")
	if plainErr == nil {
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("plaintext reached the TLS listener handler: HTTP %d", response.StatusCode)
		}
	}
	for _, path := range []string{"/v1/rpc", "/mcp", "/healthz", "/readyz", "/webhooks/support/telegram"} {
		response, err := controller.opts.HTTPClient.Get("https://" + settings.Hostname + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		want := http.StatusNotFound
		if path == "/webhooks/support/telegram" {
			want = http.StatusAccepted
		}
		if response.StatusCode != want {
			t.Fatalf("native public path %s = %d, want %d", path, response.StatusCode, want)
		}
	}
	untrusted := transport.Clone()
	untrusted.TLSClientConfig.RootCAs = x509.NewCertPool()
	controller.opts.HTTPClient = &http.Client{Transport: untrusted}
	if err := controller.Renew(ctx); err == nil || readiness.Snapshot(time.Now()).Exposure != nil {
		t.Fatalf("untrusted certificate must revoke exposure: %v", err)
	}
	untrusted.CloseIdleConnections()
	controller.opts.HTTPClient = &http.Client{Transport: transport}
	_, expired, certPEM, keyPEM := webhookCertificateFixture(t, root, settings.Hostname, time.Now().Add(-time.Hour))
	roots.AddCert(expired)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	active.Store(&pair)
	probing.Store(true)
	if err := controller.Renew(ctx); err == nil || readiness.Snapshot(time.Now()).Exposure != nil {
		t.Fatalf("expired certificate must fail a fresh handshake and revoke exposure: %v", err)
	}
	if transport.DisableKeepAlives || transport.TLSClientConfig.ClientSessionCache == nil {
		t.Fatal("probe changed another client's transport or session cache")
	}
	for _, unsafe := range []http.RoundTripper{
		roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("unverified probe dispatched"); return nil, nil }),
		&http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	} {
		if _, err := freshTLSProbeClient(&http.Client{Transport: unsafe}); err == nil {
			t.Fatal("native TLS probe accepted an unverifiable transport")
		}
	}
}
