package managedcredentials

import (
	"errors"
	"net/http"
	"net/url"
	"testing"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

type credentialEffectRoundTripper struct {
	t       *testing.T
	harness *effecttest.Harness
}

func (r credentialEffectRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Helper()
	if err := r.harness.RequireState("managed_credential", runtimeeffects.StateLaunched); err != nil {
		r.t.Fatal(err)
	}
	return nil, errors.New("injected credential transport failure")
}

func TestManagedCredentialEffectOutcomes(t *testing.T) {
	harness := effecttest.New()
	source := &TokenSource{HTTPClient: &http.Client{Transport: credentialEffectRoundTripper{t: t, harness: harness}}}
	_, err := source.exchange(harness.Context("credential-exchange"), Record{Key: "provider-key", TokenURL: "http://effect.test/token", ClientID: "client"}, url.Values{"grant_type": {"client_credentials"}})
	if err == nil {
		t.Fatal("credential transport failure returned nil")
	}
	if err := harness.RequireState("managed_credential", runtimeeffects.StateOutcomeUncertain); err != nil {
		t.Fatal(err)
	}
	stale := effecttest.New()
	stale.AuthorizeErr = errors.New("superseded generation")
	staleSource := &TokenSource{HTTPClient: &http.Client{Transport: credentialEffectRoundTripper{t: t, harness: stale}}}
	if _, err := staleSource.exchange(stale.Context("credential-exchange-stale"), Record{Key: "provider-key", TokenURL: "http://effect.test/token", ClientID: "client"}, url.Values{"grant_type": {"client_credentials"}}); err == nil {
		t.Fatal("stale credential effect was admitted")
	}
	if _, launched := stale.StateForAdapter("managed_credential"); launched {
		t.Fatal("stale credential effect reached dispatch")
	}
}
