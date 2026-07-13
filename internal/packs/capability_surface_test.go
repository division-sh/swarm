package packs

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeSubjectsRejectsGlobalTriggerReadiness(t *testing.T) {
	subject := Subject{
		ID: "provider.stripe", Kind: SubjectProviderTrigger, Provider: "stripe",
		Source: "trigger_pack", Applicability: "installed", Status: StatusReady,
	}
	if _, err := NormalizeSubjects([]Subject{subject}); err == nil || !strings.Contains(err.Error(), "contradicts derived status \"AVAILABLE\"") {
		t.Fatalf("NormalizeSubjects error = %v, want global trigger readiness rejection", err)
	}

	subject.Status = StatusAvailable
	subject.Requirements = []Requirement{RequirementWithStatus(RequirementSecret, "webhook_signing.stripe", RequirementScopeTarget, "BOUND", "credential_store")}
	if _, err := NormalizeSubjects([]Subject{subject}); err == nil || !strings.Contains(err.Error(), "target-scoped and unevaluated") {
		t.Fatalf("NormalizeSubjects error = %v, want evaluated trigger requirement rejection", err)
	}
}

func TestNormalizeSubjectsOwnsEffectiveTriggerAdmissionShape(t *testing.T) {
	bundleHash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	base := Subject{
		ID: "ingress:" + bundleHash + ":chat:acme", Kind: SubjectProviderTrigger, Provider: "acme",
		Source: "trigger_pack_binding", Provenance: "external", Applicability: "effective",
		TriggerAdmission: &TriggerAdmission{
			BundleHash: bundleHash, Alias: "chat", CatalogGeneration: strings.Repeat("b", 64),
			PolicySource: "verified_pack", RequestAuthentication: "TOKEN_EQUALITY", Event: "inbound.acme",
			Pack: &TriggerPackIdentity{ID: "provider.acme", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("c", 64), Provenance: "external"},
		},
		Requirements: []Requirement{TargetScopedRequirement(RequirementSecret, "webhook_signing.acme")},
		TriggerEvents: []TriggerEventDescriptor{
			{Event: "inbound.acme", Kind: "raw", Fields: []TriggerEventFieldDescriptor{{Name: "payload", Type: "json", Required: true}}},
			{Event: "inbound.acme.record_created", Kind: "normalized", Fields: []TriggerEventFieldDescriptor{{Name: "record_id", Type: "text", Required: true, CarryEligible: true}}},
		},
	}
	normalized, err := NormalizeSubjects([]Subject{base})
	if err != nil {
		t.Fatal(err)
	}
	if normalized[0].Status != StatusAvailable {
		t.Fatalf("effective trigger status = %q, want AVAILABLE until target binding readback", normalized[0].Status)
	}

	invalid := base
	invalid.Source = "raw_declaration"
	if _, err := NormalizeSubjects([]Subject{invalid}); err == nil || !strings.Contains(err.Error(), "trigger_pack_binding source") {
		t.Fatalf("pack/source mismatch error = %v", err)
	}
	invalid = base
	invalid.TriggerAdmission = &TriggerAdmission{
		BundleHash: bundleHash, Alias: "chat", CatalogGeneration: strings.Repeat("b", 64),
		PolicySource: "raw_declaration", RequestAuthentication: "UNAUTHENTICATED", Event: "inbound.acme",
	}
	invalid.Source = "raw_declaration"
	invalid.Requirements = base.Requirements
	if _, err := NormalizeSubjects([]Subject{invalid}); err == nil || !strings.Contains(err.Error(), "must not carry secret requirements") {
		t.Fatalf("unsigned requirement error = %v", err)
	}
	invalid.Requirements = nil
	normalized, err = NormalizeSubjects([]Subject{invalid})
	if err != nil {
		t.Fatal(err)
	}
	if rendered := RenderSubject(normalized[0], false); !strings.Contains(rendered, "request_authentication=UNAUTHENTICATED") || !strings.Contains(rendered, "policy_source=raw_declaration") {
		t.Fatalf("unauthenticated trigger readback = %q", rendered)
	}
}

func TestNormalizeSubjectsOwnsConnectorReadinessRollup(t *testing.T) {
	unbound := RequirementWithStatus(RequirementSecret, "acme_key", "deployment", "UNBOUND", "credential_store")
	connected := RequirementWithStatus(RequirementManagedCredential, "acme_oauth", "deployment", "CONNECTED", "managed_credential_store")

	for _, tc := range []struct {
		name    string
		subject Subject
		want    string
	}{
		{
			name: "ready contradicts unsatisfied requirement",
			subject: Subject{ID: "acme.write", Kind: SubjectProviderConnector, Provider: "acme", Source: "flow_local", Applicability: "effective", Status: StatusReady,
				Requirements: []Requirement{unbound}},
			want: "contradicts derived status \"NOT_READY\"",
		},
		{
			name: "not ready contradicts satisfied requirements",
			subject: Subject{ID: "acme.write", Kind: SubjectProviderConnector, Provider: "acme", Source: "flow_local", Applicability: "effective", Status: StatusNotReady,
				Requirements: []Requirement{connected}},
			want: "contradicts derived status \"READY\"",
		},
		{
			name: "installed cannot claim ready",
			subject: Subject{ID: "acme.write", Kind: SubjectProviderConnector, Provider: "acme", Source: "connector_pack", Applicability: "installed", Status: StatusReady,
				Requirements: []Requirement{RequirementWithStatus(RequirementImport, "acme.write", "package", "NOT_IMPORTED", "connector_pack_registry")}},
			want: "contradicts derived status \"AVAILABLE\"",
		},
		{
			name: "satisfied flag must match status",
			subject: Subject{ID: "acme.write", Kind: SubjectProviderConnector, Provider: "acme", Source: "flow_local", Applicability: "effective",
				Requirements: []Requirement{{Kind: RequirementSecret, Name: "acme_key", Scope: "deployment", Status: "UNBOUND", Satisfied: boolPointer(true), Remediation: "swarm secrets set acme_key"}}},
			want: "contradicts satisfied=true",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NormalizeSubjects([]Subject{tc.subject}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NormalizeSubjects error = %v, want %q", err, tc.want)
			}
		})
	}

	derived, err := NormalizeSubjects([]Subject{{
		ID: "acme.write", Kind: SubjectProviderConnector, Provider: "acme", Source: "flow_local", Applicability: "effective",
		Requirements: []Requirement{unbound, connected},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(derived) != 1 || derived[0].Status != StatusNotReady {
		t.Fatalf("derived subjects = %#v, want NOT_READY", derived)
	}
}

func boolPointer(value bool) *bool { return &value }

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
		"guarantee: cannot bypass activity_attempts - enforced by internal/runtime/pipeline.pipelineActivityDispatcher.executeNonIdempotentActivityIntent",
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

func TestCloneSubjectsDoesNotExposeNestedCapabilityState(t *testing.T) {
	satisfied := true
	original := []Subject{{
		ID: "ingress:bundle:chat:acme", Kind: SubjectProviderTrigger, Provider: "acme",
		Source: "trigger_pack_binding", Applicability: "effective",
		TriggerAdmission: &TriggerAdmission{Pack: &TriggerPackIdentity{ID: "provider.acme"}},
		Requirements:     []Requirement{{Satisfied: &satisfied, Scopes: []string{"write"}, TokenRequest: &TokenRequestProfile{StaticHeaders: map[string]string{"Version": "1"}}}},
		Evidence:         []Evidence{{Fields: map[string]string{"generation": "old"}}},
	}}
	cloned := CloneSubjects(original)
	cloned[0].TriggerAdmission.Pack.ID = "mutated"
	cloned[0].Requirements[0].Scopes[0] = "mutated"
	cloned[0].Requirements[0].TokenRequest.StaticHeaders["Version"] = "2"
	cloned[0].Evidence[0].Fields["generation"] = "new"
	*cloned[0].Requirements[0].Satisfied = false
	if original[0].TriggerAdmission.Pack.ID != "provider.acme" || original[0].Requirements[0].Scopes[0] != "write" || original[0].Requirements[0].TokenRequest.StaticHeaders["Version"] != "1" || original[0].Evidence[0].Fields["generation"] != "old" || !*original[0].Requirements[0].Satisfied {
		t.Fatalf("CloneSubjects exposed nested state: %#v", original[0])
	}
}

func TestEffectiveTriggerTextAndJSONProjectTheSameTypedFacts(t *testing.T) {
	bundleHash := "bundle-v1:sha256:" + strings.Repeat("d", 64)
	subject := Subject{
		ID: "ingress:" + bundleHash + ":chat:acme", Kind: SubjectProviderTrigger, Provider: "acme",
		Source: "trigger_pack_binding", Provenance: "external", Applicability: "effective",
		TriggerAdmission: &TriggerAdmission{
			BundleHash: bundleHash, Alias: "chat", CatalogGeneration: strings.Repeat("e", 64),
			PolicySource: "verified_pack", RequestAuthentication: "HMAC_SHA256", Event: "inbound.acme",
			SignedPayload: "raw_body", DigestEncoding: "base64",
			Pack: &TriggerPackIdentity{ID: "provider.acme", Version: "1.2.3", ManifestHash: "sha256:" + strings.Repeat("f", 64), Provenance: "external"},
		},
		Requirements: []Requirement{TargetScopedRequirement(RequirementSecret, "webhook_signing.acme")},
		TriggerEvents: []TriggerEventDescriptor{
			{Event: "inbound.acme", Kind: "raw", Fields: []TriggerEventFieldDescriptor{{Name: "payload", Type: "json", Required: true}}},
			{Event: "inbound.acme.record_created", Kind: "normalized", Fields: []TriggerEventFieldDescriptor{{Name: "record_id", Type: "text", Required: true, CarryEligible: true}}},
		},
	}
	normalized, err := NormalizeSubjects([]Subject{subject})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(normalized[0])
	if err != nil {
		t.Fatal(err)
	}
	text := RenderSubject(normalized[0], false)
	for _, value := range []string{"chat", "verified_pack", "HMAC_SHA256", "inbound.acme", "inbound.acme.record_created", "record_id", strings.Repeat("e", 64), "provider.acme", "1.2.3", "webhook_signing.acme"} {
		if !strings.Contains(string(body), value) || !strings.Contains(text, value) {
			t.Fatalf("typed fact %q missing from JSON or text\njson=%s\ntext=%s", value, body, text)
		}
	}
	if !strings.Contains(string(body), `"carry_eligible":true`) || !strings.Contains(text, "record_id:text:required:carry-eligible") {
		t.Fatalf("normalized carry descriptor missing from JSON or text\njson=%s\ntext=%s", body, text)
	}
}
