package credentials

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIngressSigningUsabilityHasNoSecondProductionInterpreter(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	read := func(path string) string {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(body)
	}

	inbound := read("internal/runtime/inbound.go")
	for _, forbidden := range []string{"credentials.Get(", "strings.TrimSpace(resolved)", "SigningBound", "ObserveSecretBinding(", "SetCredentialStore("} {
		if strings.Contains(inbound, forbidden) {
			t.Errorf("inbound gateway retains target-signing interpreter %q", forbidden)
		}
	}
	if !strings.Contains(inbound, "g.admitCredentials(") || !strings.Contains(read("internal/runtime/runtime.go"), "SetCredentialAdmission(rt.AdmitInboundCredentials)") {
		t.Error("inbound gateway does not consume the exact frozen standing credential admission owner")
	}
	if !strings.Contains(read("internal/runtime/context_manager.go"), "evaluateStandingIngressAdmission(") {
		t.Error("runtime readiness bypasses frozen standing credential admission")
	}

	registration := read("internal/runtime/publicingress/registration.go")
	for _, forbidden := range []string{"if !signing.Present", "if signing.Present", "SigningBound"} {
		if strings.Contains(registration, forbidden) {
			t.Errorf("public registration retains target-signing interpreter %q", forbidden)
		}
	}
	if !strings.Contains(registration, "ObserveAdmittedActivationCredential(") {
		t.Error("public registration does not consume the canonical secret-binding owner")
	}

	serve := read("internal/serveapp/main.go")
	start := strings.Index(serve, "func serveReadyStandingIngress(")
	if start < 0 {
		t.Fatal("serve readiness function boundary not found")
	}
	end := strings.Index(serve[start:], "\nfunc ")
	if end < 0 {
		t.Fatal("serve readiness function boundary not found")
	}
	body := serve[start : start+end]
	for _, forbidden := range []string{".Get(", "SigningBound", "SigningSecret"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("serve readiness retains target-signing interpreter %q", forbidden)
		}
	}
	if !strings.Contains(body, "EvaluatedCapabilitySubjects(") {
		t.Error("serve readiness does not consume manager-evaluated capability subjects")
	}

	for _, path := range []string{
		"internal/runtime/inbound.go",
		"internal/runtime/standing_targets.go",
		"internal/runtime/context_manager.go",
		"internal/runtime/publicingress/registration.go",
		"internal/serveapp/main.go",
		"internal/serveapp/public_ingress.go",
	} {
		body := read(path)
		for _, forbidden := range []string{"flow_instances.config.secrets", "workflow_instances.metadata.credentials"} {
			if strings.Contains(body, forbidden) {
				t.Errorf("%s retains legacy credential owner %q", path, forbidden)
			}
		}
	}
	for _, path := range []string{
		"internal/runtime/standing_ingress_credentials.go",
		"internal/runtime/channel_activation_admission.go",
		"internal/runtime/pipeline/coordinator.go",
		"internal/runtime/publicingress/registration.go",
		"internal/serveapp/channel_onboarding.go",
		"internal/serveapp/channel_delivery.go",
	} {
		body := read(path)
		if !strings.Contains(body, "ObserveAdmittedActivationCredential(") {
			t.Errorf("%s bypasses exact credential admission ownership", path)
		}
		if strings.Contains(body, "ObserveValueMatchingSeal(") {
			t.Errorf("%s retains a receipt-blind learned credential interpreter", path)
		}
	}
	for _, path := range []string{"internal/serveapp/channel_native.go", "internal/serveapp/channel_delivery.go", "internal/serveapp/channel_onboarding.go"} {
		if strings.Contains(read(path), "runtimeregistration.HTTPExecutor{Client: d.httpClient}") {
			t.Errorf("%s bypasses channel provider dispatch preflight", path)
		}
	}
}
