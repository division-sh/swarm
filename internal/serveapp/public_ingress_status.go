package serveapp

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/publicingress"
)

type serveWebhookOutput struct {
	owner                     *publicingress.ReadinessOwner
	mode, origin, certificate string
}

func (s serveWebhookOutput) line(snapshot publicingress.Snapshot, reason string) string {
	mode := map[string]string{publicingress.ModeNativeTLS: "direct TLS",
		publicingress.ModeExternalOrigin: "reverse proxy", publicingress.ModeManagedQuickTunnel: "development tunnel"}[s.mode]
	if mode == "" || !snapshot.PublicIngressEnabled {
		return ""
	}
	display, exposure := snapshot.Display, snapshot.Exposure
	origin := s.origin
	if display.RouteProved && exposure != nil {
		origin = exposure.PublicOrigin
	}
	address := "origin unavailable"
	if parsed, err := url.Parse(origin); err == nil && parsed.Scheme == "https" && parsed.Host != "" {
		address = "https://" + parsed.Host
	}
	route, registration := "route not proved", "registrations unverified"
	if display.RouteProved {
		route = "route proved"
	}
	if display.SelectionKnown && len(snapshot.Registrations) == 0 {
		registration = "no managed registrations"
	} else if display.SelectionKnown && display.RegistrationsCurrent && display.RouteProved {
		registration = "registrations current"
	}
	fields := []string{"public webhook: " + mode, address, route, registration}
	if s.certificate != "" {
		certificate := "certificate: " + s.certificate
		if display.RouteProved && exposure != nil && !exposure.CertificateExpiry.IsZero() {
			certificate += ", expires " + exposure.CertificateExpiry.UTC().Format("2006-01-02")
		}
		fields = append(fields, certificate)
	}
	if !display.RouteProved {
		if reason == "" {
			reason = display.RouteReason
		}
		switch reason {
		case "listener stopped", "listener closed", "route lost", "route check failed":
		default:
			reason = "route evidence not current"
		}
		fields = append(fields, "reason: "+reason)
	}
	return strings.Join(fields, " | ")
}

func (p *serveLifecyclePresenter) publicWebhookStatus(output serveWebhookOutput, snapshot publicingress.Snapshot, reason string) {
	line := output.line(snapshot, reason)
	if line == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	display := snapshot.Display
	if p.terminalWritten || display.Sampled.Before(p.webhookSampled) || !output.owner.DisplayCurrent(display.Revision) {
		return
	}
	p.webhookSampled = display.Sampled
	if line != p.webhookLine {
		p.webhookLine = line
		_, _ = fmt.Fprintln(p.out, line)
	}
}
