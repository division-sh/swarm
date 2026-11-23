package serveapp

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
)

func TestServePublicWebhookStatusUsesRedactedFacts(t *testing.T) {
	const proved = "public webhook: direct TLS | https://hooks.example.test:8443 | route proved"
	const unproved = "public webhook: direct TLS | https://hooks.example.test:8443 | route not proved"
	const expiry = " | certificate: automatic, expires 2027-02-01"
	const automatic = " | certificate: automatic"
	s := serveWebhookOutput{mode: publicingress.ModeNativeTLS, origin: "https://hooks.example.test:8443", certificate: "automatic"}
	snapshot := publicingress.Snapshot{PublicIngressEnabled: true, Failure: "raw-secret",
		Exposure: &publicingress.ExposureEvidence{
			PublicOrigin:      "https://secret@hooks.example.test:8443/provider-token?nonce=secret",
			CertificateExpiry: time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC),
		}, Display: publicingress.DisplayEvidence{RouteProved: true, RegistrationsCurrent: true}}
	assert := func(reason, want string) {
		t.Helper()
		if got := s.line(snapshot, reason); got != want {
			t.Fatalf("line = %q, want %q", got, want)
		}
	}
	assert("", proved+" | registrations unverified"+expiry)
	snapshot.Display.SelectionKnown = true
	assert("", proved+" | no managed registrations"+expiry)
	snapshot.Registrations = []publicingress.RegistrationEvidence{{CallbackURL: "https://hooks.example.test/secret"}}
	assert("", proved+" | registrations current"+expiry)
	snapshot.Display.RegistrationsCurrent = false
	assert("", proved+" | registrations unverified"+expiry)
	snapshot.Display.RouteProved = false
	assert("raw-secret", unproved+" | registrations unverified"+automatic+" | reason: route evidence not current")
	for _, reason := range []string{"listener stopped", "listener closed", "route lost", "route check failed"} {
		assert(reason, unproved+" | registrations unverified"+automatic+" | reason: "+reason)
	}
	s.mode, s.origin, s.certificate = publicingress.ModeManagedQuickTunnel, "", ""
	assert("route lost", "public webhook: development tunnel | origin unavailable | route not proved | registrations unverified | reason: route lost")
	snapshot.PublicIngressEnabled = false
	assert("", "")
}

func TestServePublicWebhookStatusDropsRepeatedAndStaleObservations(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		var out bytes.Buffer
		p := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out, Verbose: verbose})
		ready := publicingress.NewReadinessOwner(true)
		s := serveWebhookOutput{owner: ready, mode: publicingress.ModeExternalOrigin, origin: "https://hooks.example.test"}
		now := time.Now()
		ready.SetExposure(publicingress.ExposureEvidence{PublicOrigin: s.origin, ExpiresAt: now.Add(time.Second)})
		fresh := ready.Snapshot(now)
		p.publicWebhookStatus(s, fresh, "")
		p.publicWebhookStatus(s, fresh, "")
		p.publicWebhookStatus(s, ready.Snapshot(now.Add(time.Second)), "")
		p.publicWebhookStatus(s, fresh, "") // Same revision, older observation.
		ready.RevokeExposure("private diagnostic")
		p.publicWebhookStatus(s, ready.Snapshot(now.Add(time.Second)), "listener closed")
		p.publicWebhookStatus(s, fresh, "") // Now also superseded by revision.
		const want = "public webhook: reverse proxy | https://hooks.example.test | route proved | registrations unverified\n" +
			"public webhook: reverse proxy | https://hooks.example.test | route not proved | registrations unverified | reason: route evidence not current\n" +
			"public webhook: reverse proxy | https://hooks.example.test | route not proved | registrations unverified | reason: listener closed\n"
		if got := out.String(); strings.TrimSpace(got) != strings.TrimSpace(want) {
			t.Fatalf("verbose=%t, observed lines:\n%s\nwant:\n%s", verbose, got, want)
		}
	}
}
