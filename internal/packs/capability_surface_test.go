package packs

import (
	"strings"
	"testing"
)

func TestNormalizeSubjectsRejectsGlobalTriggerReadiness(t *testing.T) {
	subject := Subject{
		ID: "provider.stripe", Kind: SubjectProviderTrigger, Provider: "stripe",
		Source: "trigger_pack", Applicability: "installed", Status: StatusReady,
	}
	if _, err := NormalizeSubjects([]Subject{subject}); err == nil || !strings.Contains(err.Error(), "must be AVAILABLE") {
		t.Fatalf("NormalizeSubjects error = %v, want global trigger readiness rejection", err)
	}

	subject.Status = StatusAvailable
	subject.Requirements = []Requirement{RequirementWithStatus(RequirementSecret, "webhook_signing.stripe", RequirementScopeTarget, "BOUND", "credential_store")}
	if _, err := NormalizeSubjects([]Subject{subject}); err == nil || !strings.Contains(err.Error(), "target-scoped and unevaluated") {
		t.Fatalf("NormalizeSubjects error = %v, want evaluated trigger requirement rejection", err)
	}
}

func TestRenderSubjectUsesRegistriesAndPreservesUnknownCapabilityCode(t *testing.T) {
	guarantee, err := NewGuarantee(GuaranteeActivityJournal)
	if err != nil {
		t.Fatal(err)
	}
	subject := Subject{
		ID: "acme.write", Kind: SubjectProviderConnector, Provider: "acme", Action: "write",
		Source: "flow_local", Applicability: "effective", Status: StatusNotReady,
		Capabilities: []Capability{
			{Code: "unknown_capability_code", Target: "target"},
			{Code: CapabilityCallProviderAction, Target: "write Acme records"},
		},
		Guarantees: []Guarantee{guarantee},
		Requirements: []Requirement{
			RequirementWithStatus(RequirementSecret, "acme_key", "deployment", "UNBOUND", "file"),
		},
	}
	rendered := RenderSubject(subject, false)
	for _, want := range []string{
		"provider connector acme.write NOT READY",
		"requires acme_key=UNBOUND (fix: swarm secrets set acme_key)",
		"CAN call provider action write Acme records",
		"CAN unknown_capability_code target",
		"guarantee: cannot bypass activity_attempts - enforced by activity_attempts",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("RenderSubject = %q, want %q", rendered, want)
		}
	}
	if strings.Index(rendered, "requires ") > strings.Index(rendered, "CAN ") {
		t.Fatalf("RenderSubject = %q, want unsatisfied requirement before capabilities", rendered)
	}
}

func TestGuaranteeAndRemediationRegistriesFailClosed(t *testing.T) {
	if _, err := NewGuarantee("provider_specific_guess"); err == nil {
		t.Fatal("NewGuarantee accepted an unregistered enforcement claim")
	}
	refresh := RequirementWithStatus(RequirementManagedCredential, "slack_oauth", "deployment", "REFRESH_FAILED", "managed_credential_store")
	if refresh.Satisfied == nil || *refresh.Satisfied || refresh.Remediation != "swarm connections disconnect slack_oauth && swarm connections connect slack_oauth" {
		t.Fatalf("refresh remediation = %#v", refresh)
	}
}

func TestNormalizeSubjectsOrdersDeterministicallyAndRejectsDuplicates(t *testing.T) {
	items := []Subject{
		{ID: "z.write", Kind: SubjectProviderConnector, Provider: "z", Source: "flow_local", Applicability: "effective", Status: StatusReady},
		{ID: "provider.a", Kind: SubjectProviderTrigger, Provider: "a", Source: "trigger_pack", Applicability: "installed", Status: StatusAvailable},
		{ID: "a.write", Kind: SubjectProviderConnector, Provider: "a", Source: "flow_local", Applicability: "effective", Status: StatusReady},
	}
	normalized, err := NormalizeSubjects(items)
	if err != nil {
		t.Fatal(err)
	}
	if got := normalized[0].ID + "," + normalized[1].ID + "," + normalized[2].ID; got != "a.write,z.write,provider.a" {
		t.Fatalf("normalized order = %s", got)
	}
	if _, err := NormalizeSubjects(append(items, items[0])); err == nil || !strings.Contains(err.Error(), "duplicate capability subject") {
		t.Fatalf("duplicate error = %v", err)
	}
}
