package factory

import "testing"

func TestDeliveryRecipientsForEvent_FromContracts(t *testing.T) {
	if got := deliveryRecipientsForEvent("scan.started"); len(got) != 0 {
		t.Fatalf("expected scan.started to have no direct agent recipients, got %v", got)
	}
	if got := deliveryRecipientsForEvent("vertical.shortlisted"); len(got) != 0 {
		t.Fatalf("expected vertical.shortlisted to route via runtime, got %v", got)
	}
	if got := deliveryRecipientsForEvent("vertical.scored"); len(got) != 0 {
		t.Fatalf("expected vertical.scored to route via runtime-derived subscribers, got %v", got)
	}
	if got := deliveryRecipientsForEvent("vertical.marginal"); len(got) != 0 {
		t.Fatalf("expected vertical.marginal to route via runtime-derived subscribers, got %v", got)
	}
}

func TestFactoryScanModes_FromContracts(t *testing.T) {
	if got := defaultFactoryScanMode(); got != "saas_gap" {
		t.Fatalf("expected default scan mode from contracts to be saas_gap, got %q", got)
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
	if got := normalizeFactoryScanMode("not-a-mode"); got != "saas_gap" {
		t.Fatalf("expected invalid mode to fall back to default contract mode, got %q", got)
	}
	if got := factoryRubricName("automation_micro"); got != "saas_gap_rubric" {
		t.Fatalf("expected automation_micro to use saas_gap_rubric, got %q", got)
	}
}
