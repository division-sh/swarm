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
func NativeTLSConfig(settings *config.PublicWebhookConfig, stateDir string) (*tls.Config, error) {
	if settings == nil {
		return nil, nil
	}
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(stateDir) {
		return nil, fmt.Errorf("public webhook TLS requires the resolved deployment state directory")
	}
	host := strings.TrimSpace(settings.Hostname)
	if settings.CertificateFile == nil {
		configuration := nativeCertificateManager(host, stateDir, settings.Email).TLSConfig()
		// HostPolicy governs issuance, not certificates already in the cache.
		configuration.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			if !strings.EqualFold(hello.ServerName, host) {
				return nil, fmt.Errorf("public webhook TLS server name is not configured")
			}
			return nil, nil
		}
		return configuration, nil
	}
	certPath, err := stateCertificatePath(stateDir, *settings.CertificateFile)
	if err != nil {
		return nil, err
	}
	keyPath, err := stateCertificatePath(stateDir, *settings.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
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

func stateCertificatePath(stateDir, reference string) (string, error) {
	root, err := filepath.EvalSymlinks(stateDir)
	if err != nil {
		return "", fmt.Errorf("resolve public webhook state directory: %w", err)
	}
	path := strings.TrimSpace(reference)
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve supplied public webhook file: %w", err)
	}
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("supplied public webhook files must be inside the deployment state directory")
	}
	return path, nil
}

func freshTLSProbeClient(base *http.Client) (*http.Client, func(), error) {
	client := *base
	underlying := base.Transport
	if underlying == nil {
		underlying = http.DefaultTransport
	}
	transport, ok := underlying.(*http.Transport)
	if !ok {
		return nil, nil, fmt.Errorf("native TLS probe requires an HTTP transport with verified TLS")
	}
	transport = transport.Clone()
	transport.DisableKeepAlives = true
	if transport.TLSClientConfig != nil {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		if transport.TLSClientConfig.InsecureSkipVerify {
			return nil, nil, fmt.Errorf("native TLS probe cannot skip certificate verification")
		}
		transport.TLSClientConfig.ClientSessionCache = nil
	}
	client.Transport = transport
	return &client, transport.CloseIdleConnections, nil
}
