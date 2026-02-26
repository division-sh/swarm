package runtime

import "testing"

func TestAgentManager_ConfigureRoutingTemplateMigration_AllowsBootstrapMutation(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	bootstrap := PersistedRoutingRule{
		VerticalID:       "v1",
		EventPattern:     "opco.*",
		SubscriberID:     "opco-ceo-v1",
		InstalledBy:      "runtime",
		Reason:           "bootstrap",
		Status:           "active",
		Source:           "bootstrap",
		BootstrapVersion: 1,
	}
	if err := am.ConfigureRouting(bootstrap); err != nil {
		t.Fatalf("seed bootstrap route: %v", err)
	}

	// Normal calls must not be able to mutate bootstrap routes.
	mut := bootstrap
	mut.Status = "deactivated"
	if err := am.ConfigureRouting(mut); err == nil {
		t.Fatalf("expected bootstrap route mutation to be rejected")
	}

	// Template migrations are the only path allowed to mutate bootstrap routes.
	if err := am.ConfigureRoutingTemplateMigration(mut); err != nil {
		t.Fatalf("expected template migration routing change to succeed, got: %v", err)
	}
}

