package cliapp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestDescribePackOutsideProjectNeedsNoWorkflowOrCredentials(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	credentials := t.TempDir() // Reading this as a credential document must fail.
	t.Setenv("SWARM_CREDENTIALS_FILE", credentials)
	for _, args := range [][]string{
		{"describe", "pack"}, {"describe", "pack", "--json"}, {"describe", "pack", "--quiet"},
		{"describe", "pack", "telegram"}, {"describe", "pack", "telegram", "--json"},
	} {
		code, out, diagnostic := runPacksCommand(t, root, args...)
		if code != 0 || diagnostic != "" || !strings.Contains(out, "telegram") {
			t.Fatalf("outside project %v: code=%d out=%s diagnostic=%s", args, code, out, diagnostic)
		}
		if strings.Contains(out, "BOUND") || strings.Contains(out, "CONNECTED") {
			t.Fatalf("outside project invented evaluated state: %s", out)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("read-only pack query wrote the invocation directory: entries=%v err=%v", entries, err)
	}
	code, out, diagnostic := runPacksCommand(t, root, "describe", "pack", "telegram", "--json")
	if code != 0 || diagnostic != "" {
		t.Fatal(code, out, diagnostic)
	}
	var model struct {
		ID         string `json:"id"`
		Components []struct {
			Receive *PackReceiveDescription `json:"receive"`
			Tools   []struct {
				ID    string         `json:"id"`
				Input map[string]any `json:"input"`
			} `json:"tools"`
		} `json:"components"`
		Project PackProjectDescription `json:"project"`
	}
	if err := json.Unmarshal([]byte(out), &model); err != nil || model.ID != "telegram" || len(model.Components) != 1 {
		t.Fatalf("complete JSON model: %+v err=%v", model, err)
	}
	if model.Project.Evaluated || len(model.Project.Subjects) != 0 || len(model.Project.Rows) == 0 {
		t.Fatal("names-only requirements were lost or evaluated")
	}
	for _, row := range model.Project.Rows {
		if row.Status != "not evaluated" || row.Remediation == "" {
			t.Fatalf("outside requirement lacks its exit: %+v", row)
		}
	}
	if model.Components[0].Receive == nil || len(model.Components[0].Receive.Events) < 2 || len(model.Components[0].Tools) == 0 {
		t.Fatal("JSON simplified the admitted surface")
	}
	for _, tool := range model.Components[0].Tools {
		if !strings.HasPrefix(tool.ID, "telegram.") || tool.Input["type"] != "object" {
			t.Fatalf("tool identity/schema lost: %+v", tool)
		}
	}
}

func TestDescribePackExactSelectorsAndFlagRefusals(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	for _, args := range [][]string{
		{"describe", "pack", ""},
		{"describe", "pack", "--consume"}, {"describe", "pack", "--tool", "telegram.send_message"},
		{"describe", "pack", "telegram", "--tool", ""}, {"describe", "pack", "telegram", "--tool", "send_message"},
		{"describe", "pack", "telegram", "--tool", "telegram.send_message", "--consume"},
		{"describe", "pack", "telegram", "--json", "--yaml"}, {"describe", "pack", "Telegram"},
	} {
		code, _, diagnostic := runPacksCommand(t, root, args...)
		if code == 0 || diagnostic == "" {
			t.Fatalf("invalid selector/flags admitted: %v code=%d diagnostic=%s", args, code, diagnostic)
		}
	}
	code, out, diagnostic := runPacksCommand(t, root, "describe", "pack", "not-loaded")
	if code == 0 || out != "" || !strings.Contains(diagnostic, "candidates:") || !strings.Contains(diagnostic, "searched selected pack paths:") || !strings.Contains(diagnostic, "telegram") {
		t.Fatalf("unknown provider lacks exact candidates/provenance: %d %s %s", code, out, diagnostic)
	}
	code, out, diagnostic = runPacksCommand(t, root, "describe", "pack", "telegram", "--tool", "telegram.send_message")
	for _, want := range []string{"INPUT", "chat_id", "required", "EXECUTION", "journal:", "retry:", "telegram_bot_token"} {
		if code != 0 || diagnostic != "" || !strings.Contains(out, want) {
			t.Fatalf("canonical tool detail lacks %q: %d %s %s", want, code, out, diagnostic)
		}
	}
}

func TestDescribePackDevelopmentInventoryWithoutWorkflow(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	_, directories := packfixture.DevelopmentBase(t, nil)
	lines := []string{"platform:", "  packs:", "    platform_dirs:"}
	for _, directory := range directories {
		lines = append(lines, "      - "+directory)
	}
	config := filepath.Join(root, "selected.yaml")
	writeRuntimeConfigText(t, config, strings.Join(lines, "\n")+"\n")
	t.Setenv("SWARM_CREDENTIALS_FILE", t.TempDir())
	for _, args := range [][]string{{"describe", "pack"}, {"describe", "pack", "telegram"}, {"describe", "pack", "missing"}} {
		args = append(args, "--config", config)
		code, out, diagnostic := runPacksCommand(t, root, args...)
		if args[2] == "missing" {
			if code == 0 || !strings.Contains(diagnostic, directories[0]) {
				t.Fatalf("configured not-found paths missing: %s", diagnostic)
			}
			continue
		}
		if code != 0 || diagnostic != "" || !strings.Contains(out, "development_override") {
			t.Fatalf("configured declaration query required a workflow/secret: %d %s %s", code, out, diagnostic)
		}
	}
}

func TestDescribePackPreservesInstalledAndCapturedComponents(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	loaded, err := loadDescribePackContext(packCommandOptions{repoRoot: root, sourceRoot: root}, true)
	if err != nil {
		t.Fatal(err)
	}
	model, err := buildPackDescription(context.Background(), loaded, "telegram")
	if err != nil {
		t.Fatal(err)
	}
	if len(model.Components) != 2 || model.Components[0].Source != "embedded" || model.Components[1].Source != "captured" || model.Components[1].Scope != "." {
		t.Fatalf("installed/captured identities coalesced: %+v", model.Components)
	}
	for _, component := range model.Components {
		if component.ManifestHash == "" || component.Receive == nil || component.Receive.Identity.ManifestHash == "" || component.Receive.SourcePath == "" {
			t.Fatal("component lost exact declaration provenance")
		}
	}
	selected, err := selectPackDescriptionTool(model, "telegram.send_message")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Components[0].Tools) != 1 || len(selected.Components[1].Tools) != 1 {
		t.Fatal("canonical tool selection discarded a distinct matching component")
	}
}

func TestDescribePackChatConsumptionCompilesConcreteSubstitution(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	loaded, err := loadDescribePackContext(packCommandOptions{repoRoot: t.TempDir()}, false)
	if err != nil {
		t.Fatal(err)
	}
	model, err := buildPackDescription(context.Background(), loaded, "telegram")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	if _, err := packartifact.ImportEmbeddedPack(root, "telegram", packfixture.EmbeddedBase(t)); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range model.Fragments {
		if fragment.File == "provider setup" {
			continue
		}
		if fragment.File == "policy.yaml" && !strings.Contains(fragment.YAML, `operator_account: "REPLACE_OPERATOR_ACCOUNT"`) {
			t.Fatal("account placeholder must retain string typing after a numeric-looking substitution")
		}
		if strings.HasSuffix(fragment.File, "/nodes.yaml") && fragment.Purpose != "tool example: telegram.send_message" {
			continue
		}
		body := strings.ReplaceAll(fragment.YAML, "REPLACE_CHAT_ID", "12345")
		body = strings.ReplaceAll(body, "REPLACE_TEXT", "synthetic reply")
		body = strings.ReplaceAll(body, "REPLACE_OPERATOR_ACCOUNT", "420079")
		if strings.Contains(body, "resolution.instance_key") || strings.Contains(body, "connector_packs:") || strings.Contains(body, "pins:") {
			t.Fatal("generated retired authoring grammar")
		}
		writeDescribeTestFile(t, filepath.Join(root, fragment.File), body)
	}
	_, bundle, err := NewSwarmWorkflowModuleWithPackBase(RepoRoot(), root, packfixture.EmbeddedBase(t))
	if err != nil {
		t.Fatalf("concrete generated fragments did not compile: %v", err)
	}
	if len(bundle.ActivitySites()) != 1 {
		t.Fatalf("concrete example lost its real activity: %+v", bundle.ActivitySites())
	}
	if site := bundle.ActivitySites()[0]; site.Spec.Tool != "telegram.send_message" {
		t.Fatal("example switched canonical tools")
	}
	chatInstance := false
	for _, fragment := range model.Fragments {
		chatInstance = chatInstance || strings.HasSuffix(fragment.File, "/schema.yaml") && strings.Contains(fragment.YAML, "instance: conversation_reference")
	}
	if !chatInstance {
		t.Fatal("declared chat subject not used as the receiver instance")
	}
}

func TestDescribePackConsumptionDoesNotGuessIdentity(t *testing.T) {
	for _, subject := range []PackAuthorSubject{{}, {Type: "person", Field: "sender"}} {
		component := PackComponentDescription{Receive: &PackReceiveDescription{Transport: packs.ChannelTransportWebhook, Events: []PackEventDescription{{
			TriggerEventDescriptor: packs.TriggerEventDescriptor{Event: "inbound.partner.message", Kind: "normalized"}, AuthorSubject: subject,
		}}}}
		fragments, err := packConsumptionFragments("partner", component)
		if err != nil {
			t.Fatal(err)
		}
		for _, fragment := range fragments {
			if strings.HasSuffix(fragment.File, "/schema.yaml") && (strings.Contains(fragment.YAML, "instance:") || !fragment.RequiresSubstitution) {
				t.Fatalf("undeclared/non-chat identity promoted: subject=%+v fragment=%+v", subject, fragment)
			}
		}
	}
}

func TestDescribePackConnectorAndRawOnlyEmptyStates(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	for provider, want := range map[string]string{"notion": "connector-only pack", "typeform": "no connector tools", "stripe": "declares no normalized outputs", "whatsapp": "session trigger"} {
		code, out, diagnostic := runPacksCommand(t, root, "describe", "pack", provider)
		if code != 0 || diagnostic != "" || !strings.Contains(out, want) {
			t.Fatalf("%s empty/transport state: %d %s %s", provider, code, out, diagnostic)
		}
	}
	code, out, diagnostic := runPacksCommand(t, root, "describe", "pack", "whatsapp", "--consume")
	if code != 0 || diagnostic != "" || !strings.Contains(out, "whatsapp.send_text activity example withheld") ||
		!strings.Contains(out, "account: policy.operator_account") || !strings.Contains(out, "activity's owning flow") ||
		strings.Contains(out, "tool: whatsapp.send_text") {
		t.Fatalf("invalid WhatsApp session activity example was emitted or lacked remediation: %d %s %s", code, out, diagnostic)
	}
	code, out, diagnostic = runPacksCommand(t, root, "describe", "pack", "whatsapp", "--json")
	var description PackDescription
	if code != 0 || diagnostic != "" || json.Unmarshal([]byte(out), &description) != nil {
		t.Fatalf("WhatsApp JSON description: %d %s %s", code, out, diagnostic)
	}
	if len(description.Components) != 1 || len(description.Components[0].Tools) == 0 {
		t.Fatal("withheld activity erased the described WhatsApp tool")
	}
	for _, fragment := range description.Fragments {
		if fragment.Purpose == "tool example: whatsapp.send_text" || strings.Contains(fragment.YAML, "tool: whatsapp.send_text") {
			t.Fatalf("JSON fragments emitted invalid WhatsApp activity: %+v", fragment)
		}
	}
}

func TestDescribePackSignatureReadbackIsDefensive(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	loaded, err := loadDescribePackContext(packCommandOptions{repoRoot: t.TempDir()}, false)
	if err != nil {
		t.Fatal(err)
	}
	entry, _ := loaded.projection.ProviderTriggers.EntryByProvider("telegram")
	first, err := entry.Manifest.InspectSignature()
	if err != nil || first.Header != "X-Telegram-Bot-Api-Secret-Token" {
		t.Fatalf("admitted header missing: %+v %v", first, err)
	}
	original := first
	first.Header = "changed"
	second, err := entry.Manifest.InspectSignature()
	if err != nil || !reflect.DeepEqual(original, second) {
		t.Fatal("read-only inspection changed admitted policy")
	}
	if _, err := (providertriggers.Manifest{}).InspectSignature(); err == nil {
		t.Fatal("inspection accepted a missing admitted manifest")
	}
	stripe, _ := loaded.projection.ProviderTriggers.EntryByProvider("stripe")
	signature, err := stripe.Manifest.InspectSignature()
	if err != nil || signature.Encoding != "hex" || signature.TimestampParam != "t" || signature.TimestampTolerance != "5m" {
		t.Fatalf("effective signature defaults/timestamp policy lost: %+v %v", signature, err)
	}
}

func TestDescribePackProjectKeepsTargetSpecificCredentialFacts(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeDescribeTestFile(t, filepath.Join(root, "schema.yaml"), "stages: []\n")
	base := packfixture.EmbeddedBase(t)
	for _, child := range []string{"alpha", "beta"} {
		directory := filepath.Join(root, child)
		writeDescribeTestFile(t, filepath.Join(directory, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
		writeDescribeTestFile(t, filepath.Join(directory, "schema.yaml"), "stages: []\ninputs: [inbound.telegram.text_message]\n")
		writeDescribeTestFile(t, filepath.Join(directory, "policy.yaml"), "operator_account: '420079'\n")
		if _, err := packartifact.ImportEmbeddedPack(directory, "telegram", base); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(root, ".swarm", "swarm.yaml")
	writeDescribeTestFile(t, config, "runtime:\n  recovery_on_startup: false\n")
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	store, err := CredentialFileStore()
	if err != nil {
		t.Fatal(err)
	}
	const secret = "synthetic-do-not-render-this-secret"
	if err := store.Set(context.Background(), "telegram.webhook_signing_secret", secret); err != nil {
		t.Fatal(err)
	}
	code, out, diagnostic := runPacksCommand(t, root, "describe", "pack", "telegram", "--json")
	if code != 0 || diagnostic != "" || strings.Contains(out, secret) {
		t.Fatalf("project credential readback: code=%d out=%s diagnostic=%s", code, out, diagnostic)
	}
	var result struct {
		Project PackProjectDescription `json:"project"`
	}
	if err := json.Unmarshal([]byte(out), &result); err != nil || !result.Project.Evaluated {
		t.Fatalf("project evaluation missing: %+v err=%v", result, err)
	}
	var targets []string
	for _, subject := range result.Project.Subjects {
		if subject.Kind != packs.SubjectProviderTrigger {
			continue
		}
		if subject.TriggerAdmission == nil || len(subject.Requirements) != 1 || subject.Requirements[0].Status != "BOUND" {
			t.Fatalf("target-scoped secret fact lost: %+v", subject)
		}
		targets = append(targets, subject.TriggerAdmission.FlowPath)
	}
	if !reflect.DeepEqual(targets, []string{"alpha", "beta"}) {
		t.Fatalf("distinct target observations coalesced: %v", targets)
	}
	if err := store.Delete(context.Background(), "telegram.webhook_signing_secret"); err != nil {
		t.Fatal(err)
	}
	code, out, diagnostic = runPacksCommand(t, root, "describe", "pack", "telegram")
	if code != 0 || diagnostic != "" || !strings.Contains(out, "UNBOUND") || !strings.Contains(out, "swarm secrets set") || strings.Contains(out, "[installed]: BOUND") {
		t.Fatalf("missing target secret lacks exact exit: %d %s %s", code, out, diagnostic)
	}
}

func TestDescribePackImplicitSourceMatchesDotWithoutConfig(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", t.TempDir())
	telegram := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	manifestOnly := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(manifestOnly, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	if _, err := packartifact.ImportEmbeddedPack(manifestOnly, "notion", packfixture.EmbeddedBase(t)); err != nil {
		t.Fatal(err)
	}
	for _, source := range []struct{ name, root, provider string }{
		{"schema", telegram, "telegram"}, {"manifest_only", manifestOnly, "notion"},
	} {
		t.Run(source.name, func(t *testing.T) {
			requireDescribePackImplicitSourceMatchesDot(t, source.root, source.provider)
		})
	}
}

func requireDescribePackImplicitSourceMatchesDot(t *testing.T, root, provider string) {
	t.Helper()
	var projections []any
	for _, args := range [][]string{
		{"describe", "pack", provider, "--json"},
		{"describe", "pack", provider, ".", "--json"},
	} {
		code, out, diagnostic := runPacksCommand(t, root, args...)
		if code != 0 || diagnostic != "" {
			t.Fatalf("source-only invocation %v: %d %s %s", args, code, out, diagnostic)
		}
		var model any
		if err := json.Unmarshal([]byte(out), &model); err != nil {
			t.Fatal(err)
		}
		projections = append(projections, model)
	}
	if !reflect.DeepEqual(projections[0], projections[1]) {
		t.Fatal("implicit source and explicit dot describe different admitted components")
	}
	model := projections[0].(map[string]any)
	components := model["components"].([]any)
	if len(components) != 2 || components[1].(map[string]any)["source"] != "captured" || model["project"].(map[string]any)["evaluated"] != false {
		t.Fatal("source-only cwd lost captured metadata or read credentials")
	}
}

func TestDescribePackTargetRequiresCapturedBodyIdentity(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	path := filepath.Join(root, "telegram", "trigger.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(raw), "telegram update object is required", "local authored Telegram object is required", 1)
	if changed == string(raw) {
		t.Fatal("fixture did not change the captured BODY")
	}
	writeDescribeTestFile(t, path, changed)
	writeDescribeTestFile(t, filepath.Join(root, ".swarm", "swarm.yaml"), "runtime:\n  recovery_on_startup: false\n")
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	loaded, err := loadDescribePackContext(packCommandOptions{repoRoot: root}, true)
	if err != nil {
		t.Fatal(err)
	}
	model, err := buildPackDescription(context.Background(), loaded, "telegram")
	if err != nil {
		t.Fatal(err)
	}
	if len(model.Components) != 2 {
		t.Fatal("installed and captured component inventory changed")
	}
	installed, captured := model.Components[0], model.Components[1]
	if installed.Receive.Identity.ManifestHash == captured.Receive.Identity.ManifestHash {
		t.Fatal("different captured BODY lost its identity")
	}
	if packComponentHasTarget(installed, model.Project.Subjects) || !packComponentHasTarget(captured, model.Project.Subjects) {
		t.Fatal("installed declaration borrowed captured target applicability, or exact captured target was lost")
	}
	for _, change := range []string{"body", "version", "provenance", "flow"} {
		candidate := captured
		receive := *captured.Receive
		candidate.Receive = &receive
		switch change {
		case "body":
			candidate.Receive.Identity.ManifestHash = installed.Receive.Identity.ManifestHash
		case "version":
			candidate.Receive.Identity.Version = "9.0.0"
		case "provenance":
			candidate.Receive.Identity.Provenance = "foreign"
		case "flow":
			candidate.Scope = "other"
		}
		if packComponentHasTarget(candidate, model.Project.Subjects) {
			t.Fatalf("%s contradiction matched a captured target", change)
		}
	}
	for _, row := range model.Project.Rows {
		if row.Kind == "target" && row.Scope == "" && row.Status == "not effective (installed declaration)" && row.Remediation != "" {
			return
		}
	}
	t.Fatal("installed component's missing effective-target explanation was masked")
}

func TestDescribePackConnectorOnlyKeepsUnqualifiedToolFragments(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", t.TempDir())
	loaded, err := loadDescribePackContext(packCommandOptions{repoRoot: t.TempDir()}, false)
	if err != nil {
		t.Fatal(err)
	}
	model, err := buildPackDescription(context.Background(), loaded, "notion")
	if err != nil {
		t.Fatal(err)
	}
	if len(model.Components) != 1 || model.Components[0].Receive != nil || len(model.Components[0].Tools) == 0 {
		t.Fatal("fixture is not the declared connector-only surface")
	}
	for _, tool := range model.Components[0].Tools {
		found := false
		for _, fragment := range model.Fragments {
			if fragment.Purpose != "tool activity fragment: "+tool.ID {
				continue
			}
			found = true
			if !fragment.RequiresSubstitution || fragment.File != "nodes.yaml" || !strings.Contains(fragment.YAML, "activity:") || !strings.Contains(fragment.YAML, "tool: "+tool.ID) {
				t.Fatalf("connector tool %s lost its actual unqualified activity: %+v", tool.ID, fragment)
			}
			for _, forbidden := range []string{"inbound.", "connect:", "resolution:", "instance:", "payload."} {
				if strings.Contains(fragment.YAML, forbidden) {
					t.Fatalf("connector-only fragment invented %s", forbidden)
				}
			}
			for _, name := range tool.Input.RequiredProperties() {
				if !strings.Contains(fragment.YAML, name+":") {
					t.Fatalf("descriptor-required input %s missing from %s", name, tool.ID)
				}
			}
		}
		if !found {
			t.Fatalf("connector-only consumption dropped %s", tool.ID)
		}
	}
}
