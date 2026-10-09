package runtime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func compileSessionTriggerTransportFixture(t *testing.T) providertriggers.InboundAdmissionPlan {
	t.Helper()
	body := []byte("provider: acme\ntransport: session\nevent_name: {literal: inbound.acme}\ndelivery_id: {json_path: '$.id', required: true}\nevent_type: {json_path: '$.kind', required: true}\nack: {mode: durable_before_dispatch}\n")
	manifest, err := providertriggers.ParseManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{Manifest: manifest,
		Identity: providertriggers.PackIdentity{ID: "provider.acme", Version: "1.0.0", ManifestHash: packs.ManifestHash(body), Provenance: "external"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "chat", Provider: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

type sessionWebhookReadProbe struct{ reads int }

func (p *sessionWebhookReadProbe) Read([]byte) (int, error) {
	p.reads++
	return 0, io.EOF
}

func TestSessionTriggerHTTPGatewayRefusesBeforeCredentialsOrBody(t *testing.T) {
	plan := compileSessionTriggerTransportFixture(t)
	credentialCalls := 0
	gateway := &InboundGateway{admitCredentials: func(context.Context, InboundTarget) (runtimecredentials.SecretBinding, func(context.Context) error, error) {
		credentialCalls++
		return runtimecredentials.SecretBinding{}, nil, errors.New("must not observe a webhook credential")
	}}
	body := &sessionWebhookReadProbe{}
	request := httptest.NewRequest(http.MethodPost, "/webhooks/chat/acme", nil)
	request.Body = io.NopCloser(body)
	response := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(response, request,
		InboundTarget{Alias: "chat", Provider: "acme", AdmissionPlan: plan, ServiceID: uuid.NewString()}, semanticview.Wrap(nil))
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "session ingress") ||
		credentialCalls != 0 || body.reads != 0 {
		t.Fatalf("HTTP reached session interpretation: status=%d credentials=%d reads=%d response=%s", response.Code, credentialCalls, body.reads, response.Body.String())
	}
}

func TestSessionTriggerStructuralReadbackDoesNotObserveDummyCredentials(t *testing.T) {
	plan := compileSessionTriggerTransportFixture(t)
	subject, err := plan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{
		BundleHash: strings.Repeat("a", 64), FlowPath: ".", Alias: "chat"})
	if err != nil {
		t.Fatal(err)
	}
	// Session readiness is not a credential-store lookup. A structural session
	// remains unready even when no secret requirement exists.
	result, err := evaluateProviderTriggerCapabilitySubject(context.Background(), subject, nil)
	if err != nil || result.Status != packs.StatusNotReady || len(result.Requirements) != 0 {
		t.Fatalf("session readback = %+v, %v", result, err)
	}
}

func TestSessionTriggerCannotEnterWebhookStandingCredentialAdmission(t *testing.T) {
	plan := compileSessionTriggerTransportFixture(t)
	rt := &Runtime{}
	result, err := rt.observeStandingBindingCredentials(context.Background(), nil,
		StandingTargetDeclaration{FlowPath: ".", Alias: "chat", SourcePath: "schema.yaml"},
		StandingIngressBinding{Provider: "acme", AdmissionPlan: plan}, nil)
	var unavailable *operatorchannel.SessionProviderUnavailableError
	if !errors.As(err, &unavailable) || unavailable.Provider != "acme" || result.enabled {
		t.Fatalf("session declaration acquired webhook credential authority: %+v, %v", result, err)
	}
}
