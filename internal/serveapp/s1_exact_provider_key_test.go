package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
)

func TestS1ProcessWebhookPathKeepsRawProviderKey(t *testing.T) {
	for _, key := range []string{"telegram-sales", "telegram_sales", " telegram_sales", "telegram_sales "} {
		alias, got, ok := parseProcessWebhookPath("/webhooks/shop/" + key)
		if !ok || alias != "shop" || got != key {
			t.Errorf("path changed raw key %q to %q alias=%q present=%v", key, got, alias, ok)
		}
	}
}

func TestS1RuntimeRegistrationComparisonDoesNotRepairProviderKey(t *testing.T) {
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "shop", Provider: "telegram_sales",
		Declaration: providertriggers.AdmissionDeclaration{Kind: "raw", Acknowledge: providertriggers.UnsignedWebhookAcknowledgement, Event: "business.message", Payload: "json",
			Authentication: providertriggers.RawAuthenticationDeclaration{Kind: "none"}, DeliveryID: providertriggers.RawDeliveryIDDeclaration{Source: "body_sha256"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	registration := publicingress.RegistrationTarget{Alias: "shop", Provider: "telegram_sales", AdmissionPlanGeneration: plan.Generation(), SigningCredentialKey: "signing"}
	target := runtimepkg.StandingTarget{Alias: "shop", Provider: "telegram_sales", AdmissionPlan: plan}
	if !sameRuntimeRegistrationTarget(registration, target) {
		t.Fatal("identical registration and target keys did not match")
	}
	for _, key := range []string{" telegram_sales", "telegram_sales ", "telegram-sales", "Telegram_sales"} {
		changedRegistration, changedTarget := registration, target
		changedRegistration.Provider = key
		changedTarget.Provider = key
		if sameRuntimeRegistrationTarget(changedRegistration, target) || sameRuntimeRegistrationTarget(registration, changedTarget) {
			t.Errorf("registration comparison repaired %q into incumbent identity", key)
		}
	}
}
