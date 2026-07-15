package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestVerifyRejectsAmbientCheckoutProviderTriggerInventory(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := writeVerifyLintEvidenceFixture(t)
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.platformSpecPath = filepath.Join(RepoRoot(), defaultPlatformSpecPath)
	var errors []string
	for _, repo := range []string{RepoRoot(), t.TempDir()} {
		var out, errOut bytes.Buffer
		if code := runVerifyCommandWithOutput(context.Background(), repo, opts, &out, &errOut); code == 0 {
			t.Fatalf("verify unexpectedly admitted ambient inventory for repo %q:\n%s", repo, out.String())
		}
		if out.Len() != 0 {
			t.Fatalf("verify repo %q projected capabilities without configured inventory:\n%s", repo, out.String())
		}
		if !strings.Contains(errOut.String(), "provider_triggers.packs.platform_dirs is required") {
			t.Fatalf("verify repo %q error omitted configured inventory requirement:\n%s", repo, errOut.String())
		}
		errors = append(errors, errOut.String())
	}
	if errors[0] != errors[1] {
		t.Fatalf("identical configuration changed meaning by checkout presence:\ncheckout: %s\nempty repo: %s", errors[0], errors[1])
	}
}

func TestVerifyProjectsExplicitConfiguredInventoryWithoutStandingIngress(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
	root := writeVerifyLintEvidenceFixture(t)
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.platformSpecPath = filepath.Join(RepoRoot(), defaultPlatformSpecPath)
	opts.configPath = writeInboundAdmissionRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "verify.sqlite"), platformDirs, externalDirs)
	emptyRepo := t.TempDir()
	var textOut, textErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &textOut, &textErr); code != 0 {
		t.Fatalf("verify text exit=%d stdout=%s stderr=%s", code, textOut.String(), textErr.String())
	}
	for _, provider := range []string{"acme_public", "github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		if !strings.Contains(textOut.String(), "provider trigger pack provider."+provider+" AVAILABLE") {
			t.Fatalf("verify text omitted installed %s trigger:\n%s", provider, textOut.String())
		}
	}
	opts.output.asJSON = true
	var jsonOut, jsonErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &jsonOut, &jsonErr); code != 0 {
		t.Fatalf("verify JSON exit=%d stdout=%s stderr=%s", code, jsonOut.String(), jsonErr.String())
	}
	result := decodeOutputJSON[verifyCommandResult](t, jsonOut.String())
	installed := 0
	for _, subject := range result.CapabilitySubjects {
		if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "installed" {
			installed++
		}
	}
	if installed != 9 {
		t.Fatalf("verify installed trigger subjects=%d, want 9: %#v", installed, result.CapabilitySubjects)
	}
}

func TestVerifyConfiguredInventoryProjectsUnsignedWarningAndReadback(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = writeInboundAdmissionPolicyMatrixFixture(t)
	opts.platformSpecPath = filepath.Join(RepoRoot(), defaultPlatformSpecPath)
	opts.configPath = writeInboundAdmissionRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "verify.sqlite"), platformDirs, externalDirs)
	emptyRepo := t.TempDir()

	var textOut, textErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &textOut, &textErr); code != 0 {
		t.Fatalf("verify text exit=%d stdout=%s stderr=%s", code, textOut.String(), textErr.String())
	}
	if got := strings.Count(textErr.String(), "inbound_unsigned_webhook"); got != 1 {
		t.Fatalf("verify text unsigned warning count=%d, want 1:\n%s", got, textErr.String())
	}
	for _, want := range []string{`provider "partner_open" accepts unsigned webhooks`, "add admission.acknowledge: unsigned_webhook"} {
		if !strings.Contains(textErr.String(), want) {
			t.Fatalf("verify text warning omitted %q:\n%s", want, textErr.String())
		}
	}
	if strings.Contains(textErr.String(), `provider "partner_ack" accepts unsigned webhooks`) {
		t.Fatalf("verify text did not suppress acknowledged warning:\n%s", textErr.String())
	}

	opts.output.asJSON = true
	var jsonOut, jsonErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &jsonOut, &jsonErr); code != 0 {
		t.Fatalf("verify JSON exit=%d stdout=%s stderr=%s", code, jsonOut.String(), jsonErr.String())
	}
	if jsonErr.Len() != 0 {
		t.Fatalf("verify JSON stderr=%s, want empty", jsonErr.String())
	}
	result := decodeOutputJSON[verifyCommandResult](t, jsonOut.String())
	unsignedWarnings := 0
	for _, warning := range result.Warnings {
		if warning.CheckID != "inbound_unsigned_webhook" {
			continue
		}
		unsignedWarnings++
		if !strings.Contains(warning.Message, `provider "partner_open" accepts unsigned webhooks`) || warning.Remediation != "add admission.acknowledge: unsigned_webhook to confirm this intentional public endpoint" {
			t.Fatalf("verify JSON unsigned warning=%#v", warning)
		}
	}
	if unsignedWarnings != 1 {
		t.Fatalf("verify JSON unsigned warnings=%d, want 1: %#v", unsignedWarnings, result.Warnings)
	}

	readback := map[string]packs.Subject{}
	installed, effective := 0, 0
	for _, subject := range result.CapabilitySubjects {
		switch subject.Applicability {
		case "installed":
			installed++
		case "effective":
			effective++
			readback[subject.Provider] = subject
		}
	}
	if installed != 9 || effective != 6 {
		t.Fatalf("verify subject multiplicity installed=%d effective=%d", installed, effective)
	}
	for _, provider := range []string{"partner_open", "partner_ack"} {
		subject, ok := readback[provider]
		if !ok || subject.TriggerAdmission == nil || subject.TriggerAdmission.PolicySource != "raw_declaration" || subject.TriggerAdmission.RequestAuthentication != "UNAUTHENTICATED" {
			t.Fatalf("verify %s readback=%#v", provider, subject)
		}
		if rendered := packs.RenderSubject(subject, false); !strings.Contains(textOut.String(), rendered) {
			t.Fatalf("verify text/JSON readback diverged for %s:\nwant %s\ntext:\n%s", provider, rendered, textOut.String())
		}
	}
}

func TestProviderTriggerCapabilitySubjectsPreserveInstalledEffectiveMultiplicityAndRendering(t *testing.T) {
	platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
	catalog, _, err := providertriggers.NewCatalogSnapshotFromRequiredPlatformPackDirs("0.7.0", platformDirs, externalDirs)
	if err != nil {
		t.Fatal(err)
	}
	contractsRoot := writeInboundAdmissionPolicyMatrixFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), contractsRoot, runtimecontracts.DefaultPlatformSpecFile(RepoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := runtime.ProviderTriggerCapabilitySubjects(semanticview.Wrap(bundle), catalog)
	if err != nil {
		t.Fatal(err)
	}
	installed, effective, raw := 0, 0, 0
	textProjection := ""
	for _, subject := range subjects {
		textProjection += packs.RenderSubject(subject, false) + "\n"
		switch subject.Applicability {
		case "installed":
			installed++
		case "effective":
			effective++
			if subject.TriggerAdmission != nil && subject.TriggerAdmission.PolicySource == "raw_declaration" {
				raw++
			}
		}
	}
	if installed != 9 || effective != 6 || raw != 3 {
		t.Fatalf("subject multiplicity installed=%d effective=%d raw=%d", installed, effective, raw)
	}
	body, err := json.Marshal(verifyCommandResult{OK: true, CapabilitySubjects: subjects})
	if err != nil {
		t.Fatal(err)
	}
	var projected verifyCommandResult
	if err := json.Unmarshal(body, &projected); err != nil {
		t.Fatal(err)
	}
	if len(projected.CapabilitySubjects) != len(subjects) {
		t.Fatalf("JSON subjects=%d, want %d", len(projected.CapabilitySubjects), len(subjects))
	}
	for _, subject := range projected.CapabilitySubjects {
		if !strings.Contains(textProjection, subject.ID) {
			t.Fatalf("text projection omitted JSON subject %q", subject.ID)
		}
	}
}

func writeInboundAdmissionPolicyMatrixFixture(t testing.TB) string {
	t.Helper()
	return canonicalrouting.CopyInboundAdmissionPolicyMatrix(t)
}

func writeInboundAdmissionPackInventory(t *testing.T) ([]string, []string) {
	t.Helper()
	platformRoot := t.TempDir()
	platformDirs := make([]string, 0, len(providertriggers.RequiredPlatformPackIdentities()))
	for _, identity := range providertriggers.RequiredPlatformPackIdentities() {
		dir := filepath.Join(platformRoot, identity.Provider)
		copyProviderTriggerPackFixture(t, identity.Provider, dir, false)
		platformDirs = append(platformDirs, dir)
	}
	writeUnsignedProviderTriggerPack(t, filepath.Join(platformRoot, "intercom"), "provider.intercom", "intercom", "platform", "inbound.intercom")
	externalDir := filepath.Join(t.TempDir(), "acme_public")
	writeUnsignedProviderTriggerPack(t, externalDir, "provider.acme_public", "acme_public", "external", "inbound.acme_public")
	return platformDirs, []string{externalDir}
}

func writeUnsignedProviderTriggerPack(t testing.TB, dir, id, provider, provenance, event string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(fmt.Sprintf(`provider: %s
payload_object_required: true
secret: {required: false}
delivery_id: {json_path: $.id, required: true}
event_type: {literal: event, required: true}
event_name: {literal: %s}
ack: {mode: durable_before_dispatch}
`, provider, event))
	envelope := []byte(fmt.Sprintf(`id: %s
version: 0.1.0
platform_version: '>=0.7.0 <0.8.0'
type: trigger
manifest_hash: sha256:%s
provenance: {source: %s}
capabilities:
  can:
    receive_https_route: /webhooks/{alias}/%s
    emit_events: [%s]
    persist_dedupe_markers: true
  cannot: [emit_undeclared_events, run_code_before_admission, touch_unbound_resources]
requires: {}
tests: [providertriggers/%s]
`, id, strings.Repeat("0", 64), provenance, provider, event, provider))
	_, stamped, err := providertriggers.StampPackEnvelope(envelope, manifest)
	if err != nil {
		t.Fatalf("stamp %s: %v", id, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "trigger.yaml"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pack.yaml"), stamped, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeInboundAdmissionRuntimeConfig(t testing.TB, backend, sqlitePath string, platformDirs, externalDirs []string) string {
	t.Helper()
	lines := []string{"runtime:", "  recovery_on_startup: false", "workspace:", "  data_source: " + t.TempDir()}
	if backend == "sqlite" {
		lines = append(lines, "store:", "  backend: sqlite", "  sqlite:", "    path: "+sqlitePath)
	}
	lines = append(lines, "llm:", "  backend: anthropic", "provider_triggers:", "  packs:", "    platform_dirs:")
	for _, dir := range platformDirs {
		lines = append(lines, fmt.Sprintf("      - %q", dir))
	}
	lines = append(lines, "    external_dirs:")
	for _, dir := range externalDirs {
		lines = append(lines, fmt.Sprintf("      - %q", dir))
	}
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
