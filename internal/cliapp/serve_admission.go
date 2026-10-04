package cliapp

import (
	"fmt"
	"net"
	"strings"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
)

func ResolveServePublicIngressMode(opts ServeOptions) (string, bool, error) {
	externalOrigin := strings.TrimSpace(opts.PublicWebhookBaseURL)
	externalListen := strings.TrimSpace(opts.PublicWebhookListen)
	if opts.Expose && (externalOrigin != "" || externalListen != "") {
		return "", false, fmt.Errorf("--expose is mutually exclusive with --public-webhook-base-url and --public-webhook-listen")
	}
	if opts.Expose && !opts.Dev {
		return "", false, fmt.Errorf("--expose requires --dev")
	}
	if (externalOrigin == "") != (externalListen == "") {
		return "", false, fmt.Errorf("--public-webhook-base-url and --public-webhook-listen must be set together")
	}
	if opts.Expose {
		return publicingress.ModeManagedQuickTunnel, true, nil
	}
	if externalOrigin != "" {
		return publicingress.ModeExternalOrigin, true, nil
	}
	return "", false, nil
}

func ValidateServeAPIAuthBinding(apiListenAddr string, auth apiv1.AuthTokenResolution) error {
	if !auth.UsesDefaultLoopbackToken() {
		return nil
	}
	if err := ValidateServeListenAddr("--api-listen-addr", apiListenAddr); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(apiListenAddr))
	if err != nil {
		return fmt.Errorf("--api-listen-addr must be a host:port listen address: %w", err)
	}
	if apiv1.DefaultLoopbackAPITokenAllowedHost(host) {
		return nil
	}
	return fmt.Errorf("non-loopback API bind %s requires --api-token-file or config serve.api_token_file", strings.TrimSpace(apiListenAddr))
}
