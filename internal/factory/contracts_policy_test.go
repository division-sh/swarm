package factory

import "testing"

func TestDeliveryRecipientsForEvent_FromContracts(t *testing.T) {
	if got := deliveryRecipientsForEvent("scan.started"); len(got) != 0 {
		t.Fatalf("expected scan.started to have no direct agent recipients, got %v", got)
	}
	if got := deliveryRecipientsForEvent("vertical.shortlisted"); len(got) != 0 {
		t.Fatalf("expected vertical.shortlisted to route via runtime, got %v", got)
	}
	if got := deliveryRecipientsForEvent("vertical.scored"); len(got) != 1 || got[0] != "empire-coordinator" {
		t.Fatalf("expected vertical.scored recipient from contracts, got %v", got)
	}
	if got := deliveryRecipientsForEvent("vertical.marginal"); len(got) != 1 || got[0] != "empire-coordinator" {
		t.Fatalf("expected vertical.marginal recipient from contracts, got %v", got)
	}
}

func TestFactoryScanModes_FromContracts(t *testing.T) {
	if got := defaultFactoryScanMode(); got != "local_services" {
		t.Fatalf("expected default scan mode from contracts to be local_services, got %q", got)
	}
	if got := normalizeFactoryScanMode("saas_gap"); got != "saas_gap" {
		t.Fatalf("expected saas_gap to remain supported, got %q", got)
	}
	if got := normalizeFactoryScanMode("automation_micro"); got != "automation_micro" {
		t.Fatalf("expected automation_micro to remain supported via contracts, got %q", got)
	}
	if got := normalizeFactoryScanMode("corpus"); got != "corpus" {
		t.Fatalf("expected corpus to remain supported via contracts, got %q", got)
	}
	if got := normalizeFactoryScanMode("not-a-mode"); got != "local_services" {
		t.Fatalf("expected invalid mode to fall back to default contract mode, got %q", got)
	}
	if !factoryModeUsesSaaSRubric("automation_micro") {
		t.Fatal("expected automation_micro to use saas-style rubric path")
	}
}
