package publicingress

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"golang.org/x/crypto/acme/autocert"
)

// NativeTLSConfig prepares serving only; automatic issuance starts on a TLS
// handshake, never during configuration admission.
func NativeTLSConfig(settings *config.PublicWebhookConfig, stateDir, invocationRoot string) (*tls.Config, error) {
	if settings == nil {
		return nil, nil
	}
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	host := strings.TrimSpace(settings.Hostname)
	if settings.CertificateFile == nil {
		if !filepath.IsAbs(stateDir) {
			return nil, fmt.Errorf("automatic public webhook TLS requires the resolved deployment state directory")
		}
		configuration := nativeCertificateManager(host, stateDir, settings.Email).TLSConfig()
		// ACME challenge-token selection precedes the manager's HostPolicy check.
		configuration.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			if !strings.EqualFold(hello.ServerName, host) {
				return nil, fmt.Errorf("public webhook TLS server name is not configured")
			}
			return nil, nil
		}
		return configuration, nil
	}
	if !filepath.IsAbs(invocationRoot) {
		return nil, fmt.Errorf("supplied public webhook paths require the captured invocation directory")
	}
	certPath := certificatePath(invocationRoot, *settings.CertificateFile)
	keyPath := certificatePath(invocationRoot, *settings.PrivateKeyFile)
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load supplied public webhook certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse supplied public webhook certificate: %w", err)
	}
	if err := leaf.VerifyHostname(host); err != nil {
		return nil, fmt.Errorf("supplied public webhook certificate identity: %w", err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil, fmt.Errorf("supplied public webhook certificate is not currently valid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}, nil
}

func nativeCertificateManager(host, stateDir, email string) *autocert.Manager {
	return &autocert.Manager{
		Prompt: autocert.AcceptTOS, HostPolicy: autocert.HostWhitelist(host),
		Cache: autocert.DirCache(filepath.Join(stateDir, "public-webhook-certs")), Email: email,
	}
}

func certificatePath(invocationRoot, reference string) string {
	path := strings.TrimSpace(reference)
	if !filepath.IsAbs(path) {
		return filepath.Join(invocationRoot, path)
	}
	return path
}

func freshTLSProbeClient(base *http.Client) (*http.Client, error) {
	client := *base
	underlying := base.Transport
	if underlying == nil {
		underlying = http.DefaultTransport
	}
	transport, ok := underlying.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("native TLS probe requires an HTTP transport with verified TLS")
	}
	transport = transport.Clone()
	transport.DisableKeepAlives = true
	if transport.TLSClientConfig != nil {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		if transport.TLSClientConfig.InsecureSkipVerify {
			return nil, fmt.Errorf("native TLS probe cannot skip certificate verification")
		}
		transport.TLSClientConfig.ClientSessionCache = nil
	}
	client.Transport = transport
	return &client, nil
}
