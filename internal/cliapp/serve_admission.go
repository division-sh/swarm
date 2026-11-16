package cliapp

import (
	"fmt"
	"net"
	"strings"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
)

func ValidateServePublicWebhookPort(cfg *config.Config, channels ChannelPackLoad) error {
	if cfg.Serve.PublicWebhook == nil {
		return nil
	}
	port := cfg.Serve.PublicWebhook.PortNumber()
	switch port {
	case 443, 80, 88, 8443:
		return nil
	}
	telegram := cfg.Channels.HITL != nil && cfg.Channels.HITL.Provider == "telegram"
	for _, plan := range channels.Plans {
		telegram = telegram || plan.Transport() == packs.ChannelTransportWebhook && plan.Provider() == "telegram"
	}
	for _, binding := range channels.Bindings {
		if binding.Transport() != packs.ChannelTransportWebhook || binding.RegistrationTarget() == "" {
			continue
		}
		registration, present := binding.Registration()
		telegram = telegram || present && registration.Provider() == "telegram"
	}
	if telegram {
		return fmt.Errorf("Telegram webhooks cannot use port %d. Set serve.public_webhook.port to 443, 80, 88 or 8443. Every port uses HTTPS.", port)
	}
	return nil
}

func ResolveServePublicIngressMode(opts ServeOptions, webhook *config.PublicWebhookConfig) (string, bool, error) {
	externalOrigin := strings.TrimSpace(opts.PublicWebhookBaseURL)
	externalListen := strings.TrimSpace(opts.PublicWebhookListen)
	if webhook != nil {
		if err := webhook.Validate(); err != nil {
			return "", false, err
		}
		if opts.Expose || externalOrigin != "" || externalListen != "" {
			return "", false, fmt.Errorf("serve.public_webhook is mutually exclusive with --expose and --public-webhook-* flags")
		}
		return publicingress.ModeNativeTLS, true, nil
	}
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
