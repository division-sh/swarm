package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestVerifyLoadsSameEmbeddedInventoryOutsideCheckout(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := writeVerifyLintEvidenceFixture(t)
	opts := defaultVerifyCommandOptions()
	opts.sourceRoot = root
	opts.platformSpecPath = filepath.Join(RepoRoot(), defaultPlatformSpecPath)
	for _, repo := range []string{RepoRoot(), t.TempDir()} {
		var out, errOut bytes.Buffer
		if code := runVerifyCommandWithOutput(context.Background(), repo, opts, &out, &errOut); code != 0 {
			t.Fatalf("verify repo %q exit=%d stdout=%s stderr=%s", repo, code, out.String(), errOut.String())
		}
		for _, want := range []string{"pack inventory: base=embedded", "provider trigger pack provider.telegram AVAILABLE"} {
			if !strings.Contains(out.String(), want) {
				t.Fatalf("verify repo %q omitted embedded inventory %q:\n%s", repo, want, out.String())
			}
		}
	}
}

func TestVerifyProjectsExplicitConfiguredInventoryWithoutStandingIngress(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := writeVerifyLintEvidenceFixture(t)
	opts := defaultVerifyCommandOptions()
	opts.sourceRoot = root
	opts.platformSpecPath = filepath.Join(RepoRoot(), defaultPlatformSpecPath)
	opts.configPath = writeInboundAdmissionRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "verify.sqlite"))
	emptyRepo := t.TempDir()
	var textOut, textErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &textOut, &textErr); code != 0 {
		t.Fatalf("verify text exit=%d stdout=%s stderr=%s", code, textOut.String(), textErr.String())
	}
	for _, provider := range []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
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
	if result.PackInventory.BaseMode != "embedded" || len(result.PackInventory.Packs) != 14 ||
		result.PackInventory.BaseDigest == "" || result.PackInventory.EffectiveDigest == "" {
		t.Fatalf("verify pack inventory = %#v", result.PackInventory)
	}
	installed := 0
	for _, subject := range result.CapabilitySubjects {
		if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "installed" {
			installed++
		}
	}
	if installed != 8 {
		t.Fatalf("verify installed trigger subjects=%d, want 8: %#v", installed, result.CapabilitySubjects)
	}
}

func TestVerifyConfiguredInventoryProjectsUnsignedWarningAndReadback(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	opts := defaultVerifyCommandOptions()
	opts.sourceRoot = writeInboundAdmissionPolicyMatrixFixture(t)
	opts.platformSpecPath = filepath.Join(RepoRoot(), defaultPlatformSpecPath)
	opts.configPath = writeInboundAdmissionRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "verify.sqlite"))
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
		if subject.Kind != packs.SubjectProviderTrigger {
			continue
		}
		switch subject.Applicability {
		case "installed":
			installed++
		case "effective":
			effective++
			readback[subject.Provider] = subject
		}
	}
	if installed != 8 || effective != 6 {
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
	catalog := packfixture.TriggerCatalog(t)
	sourceRoot := writeInboundAdmissionPolicyMatrixFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), sourceRoot, runtimecontracts.DefaultPlatformSpecFile(RepoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	providerCredentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runtimecredentials.NewSnapshotOwner(providerCredentials)
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := runtime.ProviderTriggerCapabilitySubjects(context.Background(), semanticview.Wrap(bundle), catalog, owner)
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
			if subject.TriggerAdmission == nil {
				t.Fatalf("effective subject %q has no admission facts", subject.ID)
			}
			if subject.TriggerAdmission.RequestAuthentication == "UNAUTHENTICATED" {
				if subject.Status != packs.StatusReady || len(subject.Requirements) != 0 {
					t.Fatalf("unsigned effective subject = %#v, want READY without requirement", subject)
				}
			} else if subject.Status != packs.StatusNotReady || len(subject.Requirements) != 1 || subject.Requirements[0].Status != packs.RequirementStatusUnbound {
				t.Fatalf("unbound authenticated effective subject = %#v, want NOT_READY/UNBOUND", subject)
			}
			if subject.TriggerAdmission != nil && subject.TriggerAdmission.PolicySource == "raw_declaration" {
				raw++
			}
		}
	}
	if installed != 8 || effective != 6 || raw != 4 {
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

func writeInboundAdmissionRuntimeConfig(t testing.TB, backend, sqlitePath string) string {
	t.Helper()
	lines := []string{"runtime:", "  recovery_on_startup: false"}
	if backend == "sqlite" {
		lines = append(lines, "store:", "  backend: sqlite", "  sqlite:", "    path: "+sqlitePath)
	}
	lines = append(lines, "llm:", "  backend: anthropic")
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
